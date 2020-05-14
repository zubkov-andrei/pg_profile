/* ===== Top IO objects ===== */

CREATE OR REPLACE FUNCTION top_io_tables(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    node_id                     integer,
    datid                       oid,
    relid                       oid,
    dbname                      name,
    tablespacename              name,
    schemaname                  name,
    relname                     name,
    heap_blks_read              bigint,
    heap_blks_read_pct          numeric,
    heap_blks_get         bigint,
    heap_blks_proc_pct          numeric,
    idx_blks_read               bigint,
    idx_blks_read_pct           numeric,
    idx_blks_get          bigint,
    idx_blks_proc_pct           numeric,
    toast_blks_read             bigint,
    toast_blks_read_pct         numeric,
    toast_blks_get        bigint,
    toast_blks_proc_pct         numeric,
    tidx_blks_read              bigint,
    tidx_blks_read_pct          numeric,
    tidx_blks_get         bigint,
    tidx_blks_proc_pct          numeric,
    seq_scan                    bigint,
    idx_scan                    bigint
) SET search_path=@extschema@,public AS $$
    WITH total AS (SELECT
      GREATEST(sum(heap_blks_read + idx_blks_read),1) AS total_blks_read,
      GREATEST(sum(heap_blks_read + idx_blks_read + heap_blks_hit + idx_blks_hit),1) AS total_blks_get
    FROM snap_stat_tables_total
    WHERE node_id = snode_id AND snap_id BETWEEN start_id + 1 AND end_id
    )
    SELECT
        st.node_id,
        st.datid,
        st.relid,
        snap_db.datname AS dbname,
        tablespaces_list.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.heap_blks_read)::bigint AS heap_blks_read,
        sum(st.heap_blks_read) * 100 / min(total.total_blks_read) AS heap_blks_read_pct,
        sum(st.heap_blks_read + st.heap_blks_hit)::bigint AS heap_blks_get,
        sum(st.heap_blks_read + st.heap_blks_hit) * 100 / min(total.total_blks_get) AS heap_blks_proc_pct,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.idx_blks_read) * 100 / min(total.total_blks_read) AS idx_blks_read_pct,
        sum(st.idx_blks_read + st.idx_blks_hit)::bigint AS idx_blks_get,
        sum(st.idx_blks_read + st.idx_blks_hit) * 100 / min(total.total_blks_get) AS idx_blks_proc_pct,
        sum(st.toast_blks_read)::bigint AS toast_blks_read,
        sum(st.toast_blks_read) * 100 / min(total.total_blks_read) AS toast_blks_read_pct,
        sum(st.toast_blks_read + st.toast_blks_hit)::bigint AS toast_blks_get,
        sum(st.toast_blks_read + st.toast_blks_hit) * 100 / min(total.total_blks_get) AS toast_blks_proc_pct,
        sum(st.tidx_blks_read)::bigint AS tidx_blks_read,
        sum(st.tidx_blks_read) * 100 / min(total.total_blks_read) AS tidx_blks_read_pct,
        sum(st.tidx_blks_read + st.tidx_blks_hit)::bigint AS tidx_blks_get,
        sum(st.tidx_blks_read + st.tidx_blks_hit) * 100 / min(total.total_blks_get) AS tidx_blks_proc_pct,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.idx_scan)::bigint AS idx_scan
    FROM v_snap_stat_tables st
        -- Database name
        JOIN snap_stat_database snap_db
          USING (node_id, snap_id, datid)
        JOIN tablespaces_list USING(node_id,tablespaceid)
        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
        CROSS JOIN total
    WHERE st.node_id = snode_id
      AND st.relkind IN ('r','m')
      AND snap_db.datname NOT LIKE 'template_'
      AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY st.node_id,st.datid,st.relid,snap_db.datname,tablespaces_list.tablespacename, st.schemaname,st.relname
    HAVING min(snap_db.stats_reset) = max(snap_db.stats_reset)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION top_io_indexes(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    node_id             integer,
    datid               oid,
    relid               oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelid          oid,
    indexrelname        name,
    idx_blks_read       bigint,
    idx_blks_read_pct   numeric,
    idx_blks_hit        bigint,
    idx_blks_get  bigint,
    idx_blks_proc_pct   numeric
) SET search_path=@extschema@,public AS $$
    WITH total AS (SELECT
      GREATEST(sum(heap_blks_read + idx_blks_read),1) AS total_blks_read,
      GREATEST(sum(heap_blks_read + idx_blks_read + heap_blks_hit + idx_blks_hit),1) AS total_blks_get
    FROM snap_stat_tables_total
    WHERE node_id = snode_id AND snap_id BETWEEN start_id + 1 AND end_id
    )
    SELECT
        st.node_id,
        st.datid,
        st.relid,
        snap_db.datname AS dbname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name AS relname,
        st.indexrelid,
        st.indexrelname,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.idx_blks_read) * 100 / min(total.total_blks_read) AS idx_blks_read_pct,
        sum(st.idx_blks_hit)::bigint AS idx_blks_hit,
        sum(st.idx_blks_read + st.idx_blks_hit)::bigint AS idx_blks_get,
        sum(st.idx_blks_read + st.idx_blks_hit) * 100 / min(total_blks_get) AS idx_blks_proc_pct
    FROM v_snap_stat_indexes st
        -- Database name
        JOIN snap_stat_database snap_db
        ON (st.node_id=snap_db.node_id AND st.snap_id=snap_db.snap_id AND st.datid=snap_db.datid)
        JOIN tablespaces_list ON  (st.node_id=tablespaces_list.node_id AND st.tablespaceid=tablespaces_list.tablespaceid)
        /*Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON (st.node_id = mtbl.node_id AND st.datid = mtbl.datid AND st.relid = mtbl.reltoastrelid)
        CROSS JOIN total
    WHERE st.node_id = snode_id AND snap_db.datname NOT LIKE 'template_' AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY st.node_id,st.datid,st.relid,snap_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname), COALESCE(mtbl.relname||'(TOAST)',st.relname),
      st.schemaname,st.relname,tablespaces_list.tablespacename, st.indexrelid,st.indexrelname
    HAVING min(snap_db.stats_reset) = max(snap_db.stats_reset)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION tbl_top_io_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        heap_blks_read,
        heap_blks_read_pct,
        idx_blks_read,
        idx_blks_read_pct,
        toast_blks_read,
        toast_blks_read_pct,
        tidx_blks_read,
        tidx_blks_read_pct
    FROM top_io_tables(snode_id,start_id,end_id)
    WHERE COALESCE(heap_blks_read,0) + COALESCE(idx_blks_read,0) + COALESCE(toast_blks_read,0) + COALESCE(tidx_blks_read,0) > 0
    ORDER BY COALESCE(heap_blks_read,0) + COALESCE(idx_blks_read,0) + COALESCE(toast_blks_read,0) + COALESCE(tidx_blks_read,0) DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th rowspan="2">DB</th><th rowspan="2">Tablespace</th><th rowspan="2">Schema</th><th rowspan="2">Table</th><th colspan="2">Heap</th><th colspan="2">Ix</th><th colspan="2">TOAST</th><th colspan="2">TOAST-Ix</th></tr>'||
      '<tr><th>Read</th><th>%Total</th><th>Read</th><th>%Total</th><th>Read</th><th>%Total</th><th>Read</th><th>%Total</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_read,
            round(r_result.heap_blks_read_pct,2),
            r_result.idx_blks_read,
            round(r_result.idx_blks_read_pct,2),
            r_result.toast_blks_read,
            round(r_result.toast_blks_read_pct,2),
            r_result.tidx_blks_read,
            round(r_result.tidx_blks_read_pct,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_io_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) AS dbname,
        COALESCE(st1.schemaname,st2.schemaname) AS schemaname,
        COALESCE(st1.relname,st2.relname) AS relname,
        st1.heap_blks_read AS heap_blks_read1,
        st1.heap_blks_read_pct AS heap_blks_read_pct1,
        st1.idx_blks_read AS idx_blks_read1,
        st1.idx_blks_read_pct AS idx_blks_read_pct1,
        st1.toast_blks_read AS toast_blks_read1,
        st1.toast_blks_read_pct AS toast_blks_read_pct1,
        st1.tidx_blks_read AS tidx_blks_read1,
        st1.tidx_blks_read_pct AS tidx_blks_read_pct1,
        st2.heap_blks_read AS heap_blks_read2,
        st2.heap_blks_read_pct AS heap_blks_read_pct2,
        st2.idx_blks_read AS idx_blks_read2,
        st2.idx_blks_read_pct AS idx_blks_read_pct2,
        st2.toast_blks_read AS toast_blks_read2,
        st2.toast_blks_read_pct AS toast_blks_read_pct2,
        st2.tidx_blks_read AS tidx_blks_read2,
        st2.tidx_blks_read_pct AS tidx_blks_read_pct2,
        row_number() OVER (ORDER BY st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read + st1.tidx_blks_read DESC NULLS LAST) rn_read1,
        row_number() OVER (ORDER BY st2.heap_blks_read + st2.idx_blks_read + st2.toast_blks_read + st2.tidx_blks_read DESC NULLS LAST) rn_read2
    FROM top_io_tables(snode_id,start1_id,end1_id) st1
        FULL OUTER JOIN top_io_tables(snode_id,start2_id,end2_id) st2 USING (node_id, datid, relid)
    WHERE COALESCE(st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read + st1.tidx_blks_read,
        st2.heap_blks_read + st2.idx_blks_read + st2.toast_blks_read + st2.tidx_blks_read) > 0
    ORDER BY COALESCE(st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read + st1.tidx_blks_read,0) +
        COALESCE(st2.heap_blks_read + st2.idx_blks_read + st2.toast_blks_read + st2.tidx_blks_read,0) DESC) t1
    WHERE rn_read1 <= topn OR rn_read2 <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th rowspan="2">DB</th><th rowspan="2">Schema</th><th rowspan="2">Table</th><th rowspan="2">I</th><th colspan="2">Heap</th><th colspan="2">Ix</th><th colspan="2">TOAST</th><th colspan="2">TOAST-Ix</th></tr>'||
      '<tr><th>Read</th><th>%Total</th><th>Read</th><th>%Total</th><th>Read</th><th>%Total</th><th>Read</th><th>%Total</th></tr>{rows}</table>',
      'row_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td>'||
        '<td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_read1,
            round(r_result.heap_blks_read_pct1,2),
            r_result.idx_blks_read1,
            round(r_result.idx_blks_read_pct1,2),
            r_result.toast_blks_read1,
            round(r_result.toast_blks_read_pct1,2),
            r_result.tidx_blks_read1,
            round(r_result.tidx_blks_read_pct1,2),
            r_result.heap_blks_read2,
            round(r_result.heap_blks_read_pct2,2),
            r_result.idx_blks_read2,
            round(r_result.idx_blks_read_pct2,2),
            r_result.toast_blks_read2,
            round(r_result.toast_blks_read_pct2,2),
            r_result.tidx_blks_read2,
            round(r_result.tidx_blks_read_pct2,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_gets_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer,
  IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        heap_blks_get,
        heap_blks_proc_pct,
        idx_blks_get,
        idx_blks_proc_pct,
        toast_blks_get,
        toast_blks_proc_pct,
        tidx_blks_get,
        tidx_blks_proc_pct
    FROM top_io_tables(snode_id,start_id,end_id)
    WHERE COALESCE(heap_blks_get,0) + COALESCE(idx_blks_get,0) + COALESCE(toast_blks_get,0) + COALESCE(tidx_blks_get,0) > 0
    ORDER BY COALESCE(heap_blks_get,0) + COALESCE(idx_blks_get,0) + COALESCE(toast_blks_get,0) + COALESCE(tidx_blks_get,0) DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th rowspan="2">DB</th><th rowspan="2">Tablespace</th><th rowspan="2">Schema</th><th rowspan="2">Table</th><th colspan="2">Heap</th><th colspan="2">Ix</th><th colspan="2">TOAST</th><th colspan="2">TOAST-Ix</th></tr>'||
      '<tr><th>Pages</th><th>%Total</th><th>Pages</th><th>%Total</th><th>Pages</th><th>%Total</th><th>Pages</th><th>%Total</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_get,
            round(r_result.heap_blks_proc_pct,2),
            r_result.idx_blks_get,
            round(r_result.idx_blks_proc_pct,2),
            r_result.toast_blks_get,
            round(r_result.toast_blks_proc_pct,2),
            r_result.tidx_blks_get,
            round(r_result.tidx_blks_proc_pct,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_gets_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) AS dbname,
        COALESCE(st1.tablespacename,st2.tablespacename) AS tablespacename,
        COALESCE(st1.schemaname,st2.schemaname) AS schemaname,
        COALESCE(st1.relname,st2.relname) AS relname,
        st1.heap_blks_get AS heap_blks_get1,
        st1.heap_blks_proc_pct AS heap_blks_proc_pct1,
        st1.idx_blks_get AS idx_blks_get1,
        st1.idx_blks_proc_pct AS idx_blks_proc_pct1,
        st1.toast_blks_get AS toast_blks_get1,
        st1.toast_blks_proc_pct AS toast_blks_proc_pct1,
        st1.tidx_blks_get AS tidx_blks_get1,
        st1.tidx_blks_proc_pct AS tidx_blks_proc_pct1,
        st2.heap_blks_get AS heap_blks_get2,
        st2.heap_blks_proc_pct AS heap_blks_proc_pct2,
        st2.idx_blks_get AS idx_blks_get2,
        st2.idx_blks_proc_pct AS idx_blks_proc_pct2,
        st2.toast_blks_get AS toast_blks_get2,
        st2.toast_blks_proc_pct AS toast_blks_proc_pct2,
        st2.tidx_blks_get AS tidx_blks_get2,
        st2.tidx_blks_proc_pct AS tidx_blks_proc_pct2,
        row_number() OVER (ORDER BY st1.heap_blks_get + st1.idx_blks_get + st1.toast_blks_get + st1.tidx_blks_get DESC NULLS LAST) rn_processed1,
        row_number() OVER (ORDER BY st2.heap_blks_get + st2.idx_blks_get + st2.toast_blks_get + st2.tidx_blks_get DESC NULLS LAST) rn_processed2
    FROM top_io_tables(snode_id,start1_id,end1_id) st1
        FULL OUTER JOIN top_io_tables(snode_id,start2_id,end2_id) st2 USING (node_id, datid, relid)
    WHERE COALESCE(st1.heap_blks_get + st1.idx_blks_get + st1.toast_blks_get + st1.tidx_blks_get,
        st2.heap_blks_get + st2.idx_blks_get + st2.toast_blks_get + st2.tidx_blks_get) > 0
    ORDER BY COALESCE(st1.heap_blks_get + st1.idx_blks_get + st1.toast_blks_get + st1.tidx_blks_get,0) +
        COALESCE(st2.heap_blks_get + st2.idx_blks_get + st2.toast_blks_get + st2.tidx_blks_get,0) DESC) t1
    WHERE rn_processed1 <= topn OR rn_processed2 <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th rowspan="2">DB</th><th rowspan="2">Tablespace</th><th rowspan="2">Schema</th><th rowspan="2">Table</th><th rowspan="2">I</th><th colspan="2">Heap</th><th colspan="2">Ix</th><th colspan="2">TOAST</th><th colspan="2">TOAST-Ix</th></tr>'||
	'<tr><th>Pages</th><th>%Total</th><th>Pages</th><th>%Total</th><th>Pages</th><th>%Total</th><th>Pages</th><th>%Total</th></tr>{rows}</table>',
      'row_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td>'||
        '<td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_get1,
            round(r_result.heap_blks_proc_pct1,2),
            r_result.idx_blks_get1,
            round(r_result.idx_blks_proc_pct1,2),
            r_result.toast_blks_get1,
            round(r_result.toast_blks_proc_pct1,2),
            r_result.tidx_blks_get1,
            round(r_result.tidx_blks_proc_pct1,2),
            r_result.heap_blks_get2,
            round(r_result.heap_blks_proc_pct2,2),
            r_result.idx_blks_get2,
            round(r_result.idx_blks_proc_pct2,2),
            r_result.toast_blks_get2,
            round(r_result.toast_blks_proc_pct2,2),
            r_result.tidx_blks_get2,
            round(r_result.tidx_blks_proc_pct2,2)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_top_io_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        indexrelname,
        idx_blks_read,
        idx_blks_read_pct,
        idx_blks_hit
    FROM top_io_indexes(snode_id,start_id,end_id)
    WHERE idx_blks_read > 0
    ORDER BY idx_blks_read DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>DB</th><th>Tablespace</th><th>Schema</th><th>Table</th><th>Index</th><th>Blk Reads</th><th>%Total</th><th>Blk Hits</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_blks_read,
        round(r_result.idx_blks_read_pct,2),
        r_result.idx_blks_hit
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_top_io_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.tablespacename,st2.tablespacename) as tablespacename,
        COALESCE(st1.schemaname,st2.schemaname) as schemaname,
        COALESCE(st1.relname,st2.relname) as relname,
        COALESCE(st1.indexrelname,st2.indexrelname) as indexrelname,
        st1.idx_blks_read as idx_blks_read1,
        st1.idx_blks_read_pct as idx_blks_read_pct1,
        st1.idx_blks_hit as idx_blks_hit1,
        st2.idx_blks_read as idx_blks_read2,
        st2.idx_blks_read_pct as idx_blks_read_pct2,
        st2.idx_blks_hit as idx_blks_hit2,
        row_number() OVER (ORDER BY st1.idx_blks_read DESC NULLS LAST) as rn_read1,
        row_number() OVER (ORDER BY st2.idx_blks_read DESC NULLS LAST) as rn_read2
    FROM
        top_io_indexes(snode_id,start1_id,end1_id) st1
        FULL OUTER JOIN top_io_indexes(snode_id,start2_id,end2_id) st2 USING (node_id, datid, relid, indexrelid)
    WHERE COALESCE(st1.idx_blks_read, st2.idx_blks_read) > 0
    ORDER BY COALESCE(st1.idx_blks_read,0) + COALESCE(st2.idx_blks_read,0) DESC ) t1
    WHERE rn_read1 <= topn OR rn_read2 <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>DB</th><th>Tablespace</th><th>Schema</th><th>Table</th><th>Index</th><th>I</th><th>Blk Reads</th><th>%Total</th><th>Blk Hits</th></tr>{rows}</table>',
      'row_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_blks_read1,
        round(r_result.idx_blks_read_pct1,2),
        r_result.idx_blks_hit1,
        r_result.idx_blks_read2,
        round(r_result.idx_blks_read_pct2,2),
        r_result.idx_blks_hit2
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_top_gets_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        indexrelname,
        idx_blks_get,
        idx_blks_proc_pct
    FROM top_io_indexes(snode_id,start_id,end_id)
    WHERE idx_blks_get > 0
    ORDER BY idx_blks_get DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>DB</th><th>Tablespace</th><th>Schema</th><th>Table</th><th>Index</th><th>Pages</th><th>%Total</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_blks_get,
        round(r_result.idx_blks_proc_pct,2)
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_top_gets_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.tablespacename,st2.tablespacename) as tablespacename,
        COALESCE(st1.schemaname,st2.schemaname) as schemaname,
        COALESCE(st1.relname,st2.relname) as relname,
        COALESCE(st1.indexrelname,st2.indexrelname) as indexrelname,
        st1.idx_blks_get as idx_blks_get1,
        st1.idx_blks_proc_pct as idx_blks_proc_pct1,
        st2.idx_blks_get as idx_blks_get2,
        st2.idx_blks_proc_pct as idx_blks_proc_pct2,
        row_number() OVER (ORDER BY st1.idx_blks_get DESC NULLS LAST) as rn_processed1,
        row_number() OVER (ORDER BY st2.idx_blks_get DESC NULLS LAST) as rn_processed2
    FROM
        top_io_indexes(snode_id,start1_id,end1_id) st1
        FULL OUTER JOIN top_io_indexes(snode_id,start2_id,end2_id) st2 USING (node_id, datid, relid, indexrelid)
    WHERE COALESCE(st1.idx_blks_get, st2.idx_blks_get) > 0
    ORDER BY COALESCE(st1.idx_blks_get,0) + COALESCE(st2.idx_blks_get,0) DESC ) t1
    WHERE rn_processed1 <= topn OR rn_processed2 <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>DB</th><th>Tablespace</th><th>Schema</th><th>Table</th><th>Index</th><th>I</th><th>Pages</th><th>%Total</th></tr>{rows}</table>',
      'row_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates

    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR r_result IN c_tbl_stats LOOP
    report := report||format(
        jtab_tpl #>> ARRAY['row_tpl'],
        r_result.dbname,
        r_result.tablespacename,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_blks_get1,
        round(r_result.idx_blks_proc_pct1,2),
        r_result.idx_blks_get2,
        round(r_result.idx_blks_proc_pct2,2)
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
