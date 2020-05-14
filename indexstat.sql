/* ===== Indexes stats functions ===== */

CREATE OR REPLACE FUNCTION top_indexes(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    node_id integer,
    datid oid,
    relid oid,
    indexrelid oid,
    indisunique boolean,
    dbname name,
    tablespacename name,
    schemaname name,
    relname name,
    indexrelname name,
    idx_scan bigint,
    growth bigint,
    tbl_n_tup_ins bigint,
    tbl_n_tup_upd bigint,
    tbl_n_tup_del bigint,
    tbl_n_tup_hot_upd bigint
)
SET search_path=@extschema@,public AS $$
    SELECT
        st.node_id,
        st.datid,
        st.relid,
        st.indexrelid,
        st.indisunique,
        snap_db.datname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name as relname,
        st.indexrelname,
        sum(st.idx_scan)::bigint as idx_scan,
        sum(st.relsize_diff)::bigint as growth,
        sum(tbl.n_tup_ins)::bigint as tbl_n_tup_ins,
        sum(tbl.n_tup_upd)::bigint as tbl_n_tup_upd,
        sum(tbl.n_tup_del)::bigint as tbl_n_tup_del,
        sum(tbl.n_tup_hot_upd)::bigint as tbl_n_tup_hot_upd
    FROM v_snap_stat_indexes st JOIN v_snap_stat_tables tbl USING (node_id, snap_id, datid, relid)
        -- Database name
        JOIN snap_stat_database snap_db
        ON (st.node_id=snap_db.node_id AND st.snap_id=snap_db.snap_id AND st.datid=snap_db.datid)
        JOIN tablespaces_list ON  (st.node_id=tablespaces_list.node_id AND st.tablespaceid=tablespaces_list.tablespaceid)
        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON (st.node_id = mtbl.node_id AND st.datid = mtbl.datid AND st.relid = mtbl.reltoastrelid)
    WHERE st.node_id=snode_id AND snap_db.datname NOT LIKE 'template_' AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY st.node_id,st.datid,st.relid,st.indexrelid,st.indisunique,snap_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname),COALESCE(mtbl.relname||'(TOAST)',st.relname), tablespaces_list.tablespacename,st.indexrelname
    --HAVING min(snap_db.stats_reset) = max(snap_db.stats_reset)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION top_growth_indexes_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer,
  IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Indexes stats template
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        pg_size_pretty(st.growth) as growth,
        pg_size_pretty(st_last.relsize) as relsize,
        tbl_n_tup_ins,
        tbl_n_tup_upd,
        tbl_n_tup_del
    FROM top_indexes(snode_id, start_id, end_id) st
        JOIN v_snap_stat_indexes st_last using (node_id,datid,relid,indexrelid)
    WHERE st_last.snap_id=end_id AND st.growth > 0
    ORDER BY st.growth DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th rowspan="2">DB</th><th rowspan="2">Tablespace</th><th rowspan="2">Schema</th><th rowspan="2">Table</th><th rowspan="2">Index</th><th colspan="2">Index</th><th colspan="3">Table</th></tr>'||
        '<tr><th>Size</th><th>Growth</th><th>Ins</th><th>Upd</th><th>Del</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize,
            r_result.growth,
            r_result.tbl_n_tup_ins,
            r_result.tbl_n_tup_upd,
            r_result.tbl_n_tup_del
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_growth_indexes_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(ix1.dbname,ix2.dbname) as dbname,
        COALESCE(ix1.tablespacename,ix2.tablespacename) as tablespacename,
        COALESCE(ix1.schemaname,ix2.schemaname) as schemaname,
        COALESCE(ix1.relname,ix2.relname) as relname,
        COALESCE(ix1.indexrelname,ix2.indexrelname) as indexrelname,
        pg_size_pretty(ix1.growth) as growth1,
        pg_size_pretty(ix_last1.relsize) as relsize1,
        ix1.tbl_n_tup_ins as tbl_n_tup_ins1,
        ix1.tbl_n_tup_upd as tbl_n_tup_upd1,
        ix1.tbl_n_tup_del as tbl_n_tup_del1,
        pg_size_pretty(ix2.growth) as growth2,
        pg_size_pretty(ix_last2.relsize) as relsize2,
        ix2.tbl_n_tup_ins as tbl_n_tup_ins2,
        ix2.tbl_n_tup_upd as tbl_n_tup_upd2,
        ix2.tbl_n_tup_del as tbl_n_tup_del2,
        row_number() over (ORDER BY ix1.growth DESC NULLS LAST) as rn_growth1,
        row_number() over (ORDER BY ix2.growth DESC NULLS LAST) as rn_growth2
    FROM top_indexes(snode_id, start1_id, end1_id) ix1
        FULL OUTER JOIN top_indexes(snode_id, start2_id, end2_id) ix2 USING (node_id, datid, indexrelid)
        LEFT OUTER JOIN v_snap_stat_indexes ix_last1
            ON (ix_last1.snap_id = end1_id AND ix_last1.node_id=ix1.node_id AND ix_last1.datid = ix1.datid AND ix_last1.indexrelid = ix1.indexrelid AND ix_last1.relid = ix1.relid)
        LEFT OUTER JOIN v_snap_stat_indexes ix_last2
            ON (ix_last2.snap_id = end2_id AND ix_last2.node_id=ix2.node_id AND ix_last2.datid = ix2.datid AND ix_last2.indexrelid = ix2.indexrelid AND ix_last2.relid = ix2.relid)
    WHERE COALESCE(ix1.growth,ix2.growth) > 0
    ORDER BY COALESCE(ix1.growth,0) + COALESCE(ix2.growth,0) DESC) t1
    WHERE rn_growth1 <= topn OR rn_growth2 <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th rowspan="2">DB</th><th rowspan="2">Tablespace</th><th rowspan="2">Schema</th><th rowspan="2">Table</th><th rowspan="2">Index</th><th rowspan="2">I</th><th colspan="2">Index</th><th colspan="3">Table</th></tr>'||
        '<tr><th>Size</th><th>Growth</th><th>Ins</th><th>Upd</th><th>Del</th></tr>{rows}</table>',
      'row_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize1,
            r_result.growth1,
            r_result.tbl_n_tup_ins1,
            r_result.tbl_n_tup_upd1,
            r_result.tbl_n_tup_del1,
            r_result.relsize2,
            r_result.growth2,
            r_result.tbl_n_tup_ins2,
            r_result.tbl_n_tup_upd2,
            r_result.tbl_n_tup_del2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_unused_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        pg_size_pretty(st.growth) as growth,
        pg_size_pretty(st_last.relsize) as relsize,
        tbl_n_tup_ins,
        tbl_n_tup_upd - tbl_n_tup_hot_upd as tbl_n_ind_upd,
        tbl_n_tup_del
    FROM top_indexes(snode_id, start_id, end_id) st
        JOIN v_snap_stat_indexes st_last using (node_id,datid,relid,indexrelid)
    WHERE st_last.snap_id=end_id AND st.idx_scan = 0 AND NOT st.indisunique
      AND tbl_n_tup_ins + tbl_n_tup_upd + tbl_n_tup_del > 0
    ORDER BY tbl_n_tup_ins + tbl_n_tup_upd + tbl_n_tup_del DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th rowspan="2">DB</th><th rowspan="2">Tablespaces</th><th rowspan="2">Schema</th><th rowspan="2">Table</th><th rowspan="2">Index</th><th colspan="2">Index</th><th colspan="3">Table</th></tr>'||
        '<tr><th>Size</th><th>Growth</th><th>Ins</th><th>Upd</th><th>Del</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_ix_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize,
            r_result.growth,
            r_result.tbl_n_tup_ins,
            r_result.tbl_n_ind_upd,
            r_result.tbl_n_tup_del
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
