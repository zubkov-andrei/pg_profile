/* ========= Statement stats functions ========= */

CREATE OR REPLACE FUNCTION profile_checkavail_statstatements(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@,public AS $$
  SELECT count(sn.snap_id) = count(st.snap_id)
  FROM snapshots sn LEFT OUTER JOIN snap_statements_total st USING (node_id, snap_id)
  WHERE sn.node_id = snode_id AND sn.snap_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION statements_stats(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS TABLE(
        dbname name,
        datid oid,
        calls bigint,
        total_time double precision,
        shared_gets bigint,
        local_gets bigint,
        shared_blks_dirtied bigint,
        local_blks_dirtied bigint,
        temp_blks_read bigint,
        temp_blks_written bigint,
        local_blks_read bigint,
        local_blks_written bigint,
        statements bigint
)
SET search_path=@extschema@,public AS $$
    SELECT
        snap_db.datname AS dbname,
        snap_db.datid AS datid,
        sum(st.calls)::bigint AS calls,
        sum(st.total_time)/1000::double precision AS total_time,
        sum(st.shared_blks_hit + st.shared_blks_read)::bigint AS shared_gets,
        sum(st.local_blks_hit + st.local_blks_read)::bigint AS local_gets,
        sum(st.shared_blks_dirtied)::bigint AS shared_blks_dirtied,
        sum(st.local_blks_dirtied)::bigint AS local_blks_dirtied,
        sum(st.temp_blks_read)::bigint AS temp_blks_read,
        sum(st.temp_blks_written)::bigint AS temp_blks_written,
        sum(st.local_blks_read)::bigint AS local_blks_read,
        sum(st.local_blks_written)::bigint AS local_blks_written,
        sum(st.statements)::bigint AS statements
    FROM snap_statements_total st
        -- Database name
        JOIN snap_stat_database snap_db
        ON (st.node_id=snap_db.node_id AND st.snap_id=snap_db.snap_id AND st.datid=snap_db.datid)
        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY snap_db.datname, snap_db.datid;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION statements_stats_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        COALESCE(dbname,'Total') as dbname_t,
        sum(calls) as calls,
        sum(total_time) as total_time,
        sum(shared_gets) as shared_gets,
        sum(local_gets) as local_gets,
        sum(shared_blks_dirtied) as shared_blks_dirtied,
        sum(local_blks_dirtied) as local_blks_dirtied,
        sum(temp_blks_read) as temp_blks_read,
        sum(temp_blks_written) as temp_blks_written,
        sum(local_blks_read) as local_blks_read,
        sum(local_blks_written) as local_blks_written,
        sum(statements) as statements
    FROM statements_stats(snode_id,start_id,end_id,topn)
    GROUP BY ROLLUP(dbname)
    ORDER BY dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Database</th><th>Calls</th><th>Total time(s)</th><th>Shared gets</th><th>Local gets</th><th>Shared dirtied</th><th>Local dirtied</th><th>Work_r (blk)</th><th>Work_w (blk)</th><th>Local_r (blk)</th><th>Local_w (blk)</th><th>Statements</th></tr>{rows}</table>',
      'stdb_tpl','<tr><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stdb_tpl'],
            r_result.dbname_t,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            r_result.shared_gets,
            r_result.local_gets,
            r_result.shared_blks_dirtied,
            r_result.local_blks_dirtied,
            r_result.temp_blks_read,
            r_result.temp_blks_written,
            r_result.local_blks_read,
            r_result.local_blks_written,
            r_result.statements
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION statements_stats_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.calls as calls1,
        st1.total_time as total_time1,
        st1.shared_gets as shared_gets1,
        st1.local_gets as local_gets1,
        st1.shared_blks_dirtied as shared_blks_dirtied1,
        st1.local_blks_dirtied as local_blks_dirtied1,
        st1.temp_blks_read as temp_blks_read1,
        st1.temp_blks_written as temp_blks_written1,
        st1.local_blks_read as local_blks_read1,
        st1.local_blks_written as local_blks_written1,
        st1.statements as statements1,
        st2.calls as calls2,
        st2.total_time as total_time2,
        st2.shared_gets as shared_gets2,
        st2.local_gets as local_gets2,
        st2.shared_blks_dirtied as shared_blks_dirtied2,
        st2.local_blks_dirtied as local_blks_dirtied2,
        st2.temp_blks_read as temp_blks_read2,
        st2.temp_blks_written as temp_blks_written2,
        st2.local_blks_read as local_blks_read2,
        st2.local_blks_written as local_blks_written2,
        st2.statements as statements2
    FROM statements_stats(snode_id,start1_id,end1_id,topn) st1
        FULL OUTER JOIN statements_stats(snode_id,start2_id,end2_id,topn) st2 USING (datid)
    ORDER BY COALESCE(st1.dbname,st2.dbname);

    c_dbstats_total CURSOR FOR
    SELECT
        'Total' as dbname,
        sum(st1.calls) as calls1,
        sum(st1.total_time) as total_time1,
        sum(st1.shared_gets) as shared_gets1,
        sum(st1.local_gets) as local_gets1,
        sum(st1.shared_blks_dirtied) as shared_blks_dirtied1,
        sum(st1.local_blks_dirtied) as local_blks_dirtied1,
        sum(st1.temp_blks_read) as temp_blks_read1,
        sum(st1.temp_blks_written) as temp_blks_written1,
        sum(st1.local_blks_read) as local_blks_read1,
        sum(st1.local_blks_written) as local_blks_written1,
        sum(st1.statements) as statements1,
        sum(st2.calls) as calls2,
        sum(st2.total_time) as total_time2,
        sum(st2.shared_gets) as shared_gets2,
        sum(st2.local_gets) as local_gets2,
        sum(st2.shared_blks_dirtied) as shared_blks_dirtied2,
        sum(st2.local_blks_dirtied) as local_blks_dirtied2,
        sum(st2.temp_blks_read) as temp_blks_read2,
        sum(st2.temp_blks_written) as temp_blks_written2,
        sum(st2.local_blks_read) as local_blks_read2,
        sum(st2.local_blks_written) as local_blks_written2,
        sum(st2.statements) as statements2
    FROM statements_stats(snode_id,start1_id,end1_id,topn) st1
        FULL OUTER JOIN statements_stats(snode_id,start2_id,end2_id,topn) st2 USING (datid);

    r_result RECORD;
BEGIN
    -- Statements stats per database TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Database</th><th>I</th><th>Calls</th><th>Total time(s)</th><th>Shared gets</th><th>Local gets</th><th>Shared dirtied</th><th>Local dirtied</th><th>Work_r (blk)</th><th>Work_w (blk)</th><th>Local_r (blk)</th><th>Local_w (blk)</th><th>Statements</th></tr>{rows}</table>',
      'stdb_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stdb_tpl'],
            r_result.dbname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            r_result.shared_gets1,
            r_result.local_gets1,
            r_result.shared_blks_dirtied1,
            r_result.local_blks_dirtied1,
            r_result.temp_blks_read1,
            r_result.temp_blks_written1,
            r_result.local_blks_read1,
            r_result.local_blks_written1,
            r_result.statements1,
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            r_result.shared_gets2,
            r_result.local_gets2,
            r_result.shared_blks_dirtied2,
            r_result.local_blks_dirtied2,
            r_result.temp_blks_read2,
            r_result.temp_blks_written2,
            r_result.local_blks_read2,
            r_result.local_blks_written2,
            r_result.statements2
        );
    END LOOP;
    FOR r_result IN c_dbstats_total LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stdb_tpl'],
            r_result.dbname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            r_result.shared_gets1,
            r_result.local_gets1,
            r_result.shared_blks_dirtied1,
            r_result.local_blks_dirtied1,
            r_result.temp_blks_read1,
            r_result.temp_blks_written1,
            r_result.local_blks_read1,
            r_result.local_blks_written1,
            r_result.statements1,
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            r_result.shared_gets2,
            r_result.local_gets2,
            r_result.shared_blks_dirtied2,
            r_result.local_blks_dirtied2,
            r_result.temp_blks_read2,
            r_result.temp_blks_written2,
            r_result.local_blks_read2,
            r_result.local_blks_written2,
            r_result.statements2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;