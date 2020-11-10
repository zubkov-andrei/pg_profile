/* ===== Indexes stats functions ===== */

CREATE FUNCTION top_indexes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    datid               oid,
    relid               oid,
    indexrelid          oid,
    indisunique         boolean,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelname        name,
    idx_scan            bigint,
    growth              bigint,
    tbl_n_tup_ins       bigint,
    tbl_n_tup_upd       bigint,
    tbl_n_tup_del       bigint,
    tbl_n_tup_hot_upd   bigint,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    vacuum_bytes        bigint
)
SET search_path=@extschema@,public AS $$
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        st.indexrelid,
        st.indisunique,
        sample_db.datname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name as relname,
        st.indexrelname,
        sum(st.idx_scan)::bigint as idx_scan,
        sum(st.relsize_diff)::bigint as growth,
        sum(tbl.n_tup_ins)::bigint as tbl_n_tup_ins,
        sum(tbl.n_tup_upd)::bigint as tbl_n_tup_upd,
        sum(tbl.n_tup_del)::bigint as tbl_n_tup_del,
        sum(tbl.n_tup_hot_upd)::bigint as tbl_n_tup_hot_upd,
        sum(tbl.vacuum_count)::bigint as vacuum_count,
        sum(tbl.autovacuum_count)::bigint as autovacuum_count,
        sum((COALESCE(tbl.vacuum_count,0) + COALESCE(tbl.autovacuum_count,0)) * st.relsize)::bigint as vacuum_bytes
    FROM v_sample_stat_indexes st JOIN v_sample_stat_tables tbl USING (server_id, sample_id, datid, relid)
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
        JOIN tablespaces_list ON  (st.server_id=tablespaces_list.server_id AND st.tablespaceid=tablespaces_list.tablespaceid)
        /* Start sample existance condition
        Start sample stats does not account in report, but we must be sure
        that start sample exists, as it is reference point of next sample
        */
        JOIN samples sample_s ON (st.server_id = sample_s.server_id AND sample_s.sample_id = start_id)
        /* End sample existance condition
        Make sure that end sample exists, so we really account full interval
        */
        JOIN samples sample_e ON (st.server_id = sample_e.server_id AND sample_e.sample_id = end_id)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON (st.server_id = mtbl.server_id AND st.datid = mtbl.datid AND st.relid = mtbl.reltoastrelid)
    WHERE st.server_id=sserver_id AND sample_db.datname NOT LIKE 'template_' AND st.sample_id BETWEEN sample_s.sample_id + 1 AND sample_e.sample_id
    GROUP BY st.server_id,st.datid,st.relid,st.indexrelid,st.indisunique,sample_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname),COALESCE(mtbl.relname||'(TOAST)',st.relname), tablespaces_list.tablespacename,st.indexrelname
    --HAVING min(sample_db.stats_reset) = max(sample_db.stats_reset)
$$ LANGUAGE sql;

CREATE FUNCTION top_growth_indexes_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer,
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
        pg_size_pretty(NULLIF(st.growth, 0)) as growth,
        pg_size_pretty(NULLIF(st_last.relsize, 0)) as relsize,
        NULLIF(tbl_n_tup_ins, 0) as tbl_n_tup_ins,
        NULLIF(tbl_n_tup_upd - COALESCE(tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd,
        NULLIF(tbl_n_tup_del, 0) as tbl_n_tup_del
    FROM top_indexes(sserver_id, start_id, end_id) st
        JOIN v_sample_stat_indexes st_last using (server_id,datid,relid,indexrelid)
    WHERE st_last.sample_id = end_id
      AND st.growth > 0
    ORDER BY st.growth DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
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

CREATE FUNCTION top_growth_indexes_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
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
        pg_size_pretty(NULLIF(ix1.growth, 0)) as growth1,
        pg_size_pretty(NULLIF(ix_last1.relsize, 0)) as relsize1,
        NULLIF(ix1.tbl_n_tup_ins, 0) as tbl_n_tup_ins1,
        NULLIF(ix1.tbl_n_tup_upd - COALESCE(ix1.tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd1,
        NULLIF(ix1.tbl_n_tup_del, 0) as tbl_n_tup_del1,
        pg_size_pretty(NULLIF(ix2.growth, 0)) as growth2,
        pg_size_pretty(NULLIF(ix_last2.relsize, 0)) as relsize2,
        NULLIF(ix2.tbl_n_tup_ins, 0) as tbl_n_tup_ins2,
        NULLIF(ix2.tbl_n_tup_upd - COALESCE(ix2.tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd2,
        NULLIF(ix2.tbl_n_tup_del, 0) as tbl_n_tup_del2,
        row_number() over (ORDER BY ix1.growth DESC NULLS LAST) as rn_growth1,
        row_number() over (ORDER BY ix2.growth DESC NULLS LAST) as rn_growth2
    FROM top_indexes(sserver_id, start1_id, end1_id) ix1
        FULL OUTER JOIN top_indexes(sserver_id, start2_id, end2_id) ix2 USING (server_id, datid, indexrelid)
        LEFT OUTER JOIN v_sample_stat_indexes ix_last1
            ON (ix_last1.sample_id = end1_id AND ix_last1.server_id=ix1.server_id AND ix_last1.datid = ix1.datid AND ix_last1.indexrelid = ix1.indexrelid AND ix_last1.relid = ix1.relid)
        LEFT OUTER JOIN v_sample_stat_indexes ix_last2
            ON (ix_last2.sample_id = end2_id AND ix_last2.server_id=ix2.server_id AND ix_last2.datid = ix2.datid AND ix_last2.indexrelid = ix2.indexrelid AND ix_last2.relid = ix2.relid)
    WHERE COALESCE(ix1.growth, 0) + COALESCE(ix2.growth, 0) > 0
    ORDER BY COALESCE(ix1.growth, 0) + COALESCE(ix2.growth, 0) DESC) t1
    WHERE least(
        rn_growth1,
        rn_growth2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
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

CREATE FUNCTION ix_unused_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
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
        pg_size_pretty(NULLIF(st.growth, 0)) as growth,
        pg_size_pretty(NULLIF(st_last.relsize, 0)) as relsize,
        NULLIF(tbl_n_tup_ins, 0) as tbl_n_tup_ins,
        NULLIF(tbl_n_tup_upd - COALESCE(tbl_n_tup_hot_upd,0), 0) as tbl_n_ind_upd,
        NULLIF(tbl_n_tup_del, 0) as tbl_n_tup_del
    FROM top_indexes(sserver_id, start_id, end_id) st
        JOIN v_sample_stat_indexes st_last using (server_id,datid,relid,indexrelid)
    WHERE st_last.sample_id=end_id AND COALESCE(st.idx_scan, 0) = 0 AND NOT st.indisunique
      AND COALESCE(tbl_n_tup_ins, 0) + COALESCE(tbl_n_tup_upd, 0) + COALESCE(tbl_n_tup_del, 0) > 0
    ORDER BY COALESCE(tbl_n_tup_ins, 0) + COALESCE(tbl_n_tup_upd, 0) + COALESCE(tbl_n_tup_del, 0) DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespaces</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
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


CREATE FUNCTION top_vacuumed_indexes_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer,
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
        NULLIF(st.vacuum_count, 0) as vacuum_count,
        NULLIF(st.autovacuum_count, 0) as autovacuum_count,
        NULLIF(st.vacuum_bytes, 0) as vacuum_bytes
    FROM top_indexes(sserver_id, start_id, end_id) st
    WHERE st.vacuum_bytes > 0
    ORDER BY vacuum_bytes DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th title="Estimated implicit vacuum load caused by table indexes">~Vacuum bytes</th>'
            '<th title="Vacuum count on underlying table">Vacuum cnt</th>'
            '<th title="Autovacuum count on underlying table">Autovacuum cnt</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
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
            pg_size_pretty(r_result.vacuum_bytes),
            r_result.vacuum_count,
            r_result.autovacuum_count
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_vacuumed_indexes_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
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
        NULLIF(ix1.vacuum_count, 0) as vacuum_count1,
        NULLIF(ix1.autovacuum_count, 0) as autovacuum_count1,
        NULLIF(ix1.vacuum_bytes, 0) as vacuum_bytes1,
        NULLIF(ix2.vacuum_count, 0) as vacuum_count2,
        NULLIF(ix2.autovacuum_count, 0) as autovacuum_count2,
        NULLIF(ix2.vacuum_bytes, 0) as vacuum_bytes2,
        row_number() over (ORDER BY ix1.vacuum_bytes DESC NULLS LAST) as rn_vacuum_bytes1,
        row_number() over (ORDER BY ix2.vacuum_bytes DESC NULLS LAST) as rn_vacuum_bytes2
    FROM top_indexes(sserver_id, start1_id, end1_id) ix1
        FULL OUTER JOIN top_indexes(sserver_id, start2_id, end2_id) ix2 USING (server_id, datid, indexrelid)
    WHERE COALESCE(ix1.vacuum_bytes, 0) + COALESCE(ix2.vacuum_bytes, 0) > 0
    ORDER BY COALESCE(ix1.vacuum_bytes, 0) + COALESCE(ix2.vacuum_bytes, 0) DESC) t1
    WHERE least(
        rn_vacuum_bytes1,
        rn_vacuum_bytes2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th>I</th>'
            '<th title="Estimated implicit vacuum load caused by table indexes">~Vacuum bytes</th>'
            '<th title="Vacuum count on underlying table">Vacuum cnt</th>'
            '<th title="Autovacuum count on underlying table">Autovacuum cnt</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
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
            pg_size_pretty(r_result.vacuum_bytes1),
            r_result.vacuum_count1,
            r_result.autovacuum_count1,
            pg_size_pretty(r_result.vacuum_bytes2),
            r_result.vacuum_count2,
            r_result.autovacuum_count2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
