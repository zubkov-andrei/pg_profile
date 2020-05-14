/* ===== Statements stats functions ===== */

CREATE OR REPLACE FUNCTION top_statements(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    node_id integer,
    datid oid,
    dbname name,
    userid oid,
    queryid_md5 char(10),
    calls bigint,
    calls_pct float,
    total_time double precision,
    total_time_pct float,
    min_time double precision,
    max_time double precision,
    mean_time double precision,
    stddev_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    hit_pct float,
    shared_blks_read bigint,
    read_pct float,
    gets bigint,
    gets_pct float,
    shared_blks_dirtied bigint,
    dirtied_pct float,
    shared_blks_written bigint,
    tot_written_pct float,
    backend_written_pct float,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    io_time double precision,
    io_time_pct float,
    temp_read_total_pct float,
    temp_write_total_pct float,
    local_read_total_pct float,
    local_write_total_pct float
) SET search_path=@extschema@,public AS $$
    WITH
      tot AS (
        SELECT
            GREATEST(sum(total_time),1) AS total_time,
            CASE WHEN sum(blk_read_time) = 0 THEN 1 ELSE sum(blk_read_time) END AS blk_read_time,
            CASE WHEN sum(blk_write_time) = 0 THEN 1 ELSE sum(blk_write_time) END AS blk_write_time,
            GREATEST(sum(shared_blks_hit),1) AS shared_blks_hit,
            GREATEST(sum(shared_blks_read),1) AS shared_blks_read,
            GREATEST(sum(shared_blks_dirtied),1) AS shared_blks_dirtied,
            GREATEST(sum(temp_blks_read),1) AS temp_blks_read,
            GREATEST(sum(temp_blks_written),1) AS temp_blks_written,
            GREATEST(sum(local_blks_read),1) AS local_blks_read,
            GREATEST(sum(local_blks_written),1) AS local_blks_written,
            GREATEST(sum(calls),1) AS calls
        FROM snap_statements_total
        WHERE node_id = snode_id AND snap_id BETWEEN start_id + 1 AND end_id
      ),
      totbgwr AS (
        SELECT
          GREATEST(sum(buffers_checkpoint + buffers_clean + buffers_backend),1) AS written,
          GREATEST(sum(buffers_backend),1) AS buffers_backend
        FROM snap_stat_cluster
        WHERE node_id = snode_id AND snap_id BETWEEN start_id + 1 AND end_id
      )
    SELECT
        st.node_id as node_id,
        st.datid as datid,
        snap_db.datname as dbname,
        st.userid as userid,
        st.queryid_md5 as queryid_md5,
        sum(st.calls)::bigint as calls,
        sum(st.calls*100/tot.calls)::float as calls_pct,
        sum(st.total_time)/1000 as total_time,
        sum(st.total_time*100/tot.total_time) as total_time_pct,
        min(st.min_time) as min_time,
        max(st.max_time) as max_time,
        sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
        sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
        sum(st.rows)::bigint as rows,
        sum(st.shared_blks_hit)::bigint as shared_blks_hit,
        (sum(st.shared_blks_hit) * 100 / min(tot.shared_blks_hit))::float as hit_pct,
        sum(st.shared_blks_read)::bigint as shared_blks_read,
        (sum(st.shared_blks_read) * 100 / min(tot.shared_blks_read))::float as read_pct,
        (sum(st.shared_blks_hit) + sum(st.shared_blks_read))::bigint as gets,
        (sum(st.shared_blks_hit + st.shared_blks_read) * 100 / min(tot.shared_blks_hit + tot.shared_blks_read))::float as gets_pct,
        sum(st.shared_blks_dirtied)::bigint as shared_blks_dirtied,
        (sum(st.shared_blks_dirtied) * 100 / min(tot.shared_blks_dirtied))::float as dirtied_pct,
        sum(st.shared_blks_written)::bigint as shared_blks_written,
        (sum(st.shared_blks_written) * 100 / min(totbgwr.written))::float as tot_written_pct,
        (sum(st.shared_blks_written) * 100 / min(totbgwr.buffers_backend))::float as backend_written_pct,
        sum(st.local_blks_hit)::bigint as local_blks_hit,
        sum(st.local_blks_read)::bigint as local_blks_read,
        sum(st.local_blks_dirtied)::bigint as local_blks_dirtied,
        sum(st.local_blks_written)::bigint as local_blks_written,
        sum(st.temp_blks_read)::bigint as temp_blks_read,
        sum(st.temp_blks_written)::bigint as temp_blks_written,
        sum(st.blk_read_time)/1000 as blk_read_time,
        sum(st.blk_write_time)/1000 as blk_write_time,
        (sum(st.blk_read_time + st.blk_write_time))/1000 as io_time,
        (sum(st.blk_read_time + st.blk_write_time)*100/min(tot.blk_read_time+tot.blk_write_time)) as io_time_pct,
        sum(st.temp_blks_read*100/tot.temp_blks_read)::float as temp_read_total_pct,
        sum(st.temp_blks_written*100/tot.temp_blks_written)::float as temp_write_total_pct,
        sum(st.local_blks_read*100/tot.local_blks_read)::float as local_read_total_pct,
        sum(st.local_blks_written*100/tot.local_blks_written)::float as local_write_total_pct
    FROM v_snap_statements st
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
        -- Total stats
        CROSS JOIN tot CROSS JOIN totbgwr
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY st.node_id,st.datid,snap_db.datname,st.userid,st.queryid,st.queryid_md5
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION top_elapsed_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by epapsed time
    c_elapsed_time CURSOR FOR
    SELECT
        st.queryid_md5 as queryid,
        st.dbname,
        st.calls,
        st.total_time,
        st.total_time_pct,
        st.min_time,
        st.max_time,
        st.mean_time,
        st.stddev_time,
        st.rows
    FROM top_statements(snode_id, start_id, end_id) st
    ORDER BY st.total_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr><td {mono}><a HREF="#%s">%s</a></td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.total_time_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_time AS numeric),3),
            round(CAST(r_result.min_time AS numeric),3),
            round(CAST(r_result.max_time AS numeric),3),
            round(CAST(r_result.stddev_time AS numeric),3),
            r_result.calls
        );
        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_elapsed_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by epapsed time
    c_elapsed_time CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.calls as calls1,
        st1.total_time as total_time1,
        st1.total_time_pct as total_time_pct1,
        st1.min_time as min_time1,
        st1.max_time as max_time1,
        st1.mean_time as mean_time1,
        st1.stddev_time as stddev_time1,
        st1.rows as rows1,
        st2.calls as calls2,
        st2.total_time as total_time2,
        st2.total_time_pct as total_time_pct2,
        st2.min_time as min_time2,
        st2.max_time as max_time2,
        st2.mean_time as mean_time2,
        st2.stddev_time as stddev_time2,
        st2.rows as rows2,
        row_number() over (ORDER BY st1.total_time DESC NULLS LAST) as rn_time1,
        row_number() over (ORDER BY st2.total_time DESC NULLS LAST) as rn_time2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, datid, userid, queryid_md5)
    ORDER BY COALESCE(st1.total_time,0) + COALESCE(st2.total_time,0) DESC ) t1
    WHERE rn_time1 <= topn OR rn_time2 <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Query ID</th><th>Database</th><th>I</th><th>Elapsed(s)</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr {interval1}><td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            round(CAST(r_result.total_time_pct1 AS numeric),2),
            r_result.rows1,
            round(CAST(r_result.mean_time1 AS numeric),3),
            round(CAST(r_result.min_time1 AS numeric),3),
            round(CAST(r_result.max_time1 AS numeric),3),
            round(CAST(r_result.stddev_time1 AS numeric),3),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            round(CAST(r_result.total_time_pct2 AS numeric),2),
            r_result.rows2,
            round(CAST(r_result.mean_time2 AS numeric),3),
            round(CAST(r_result.min_time2 AS numeric),3),
            round(CAST(r_result.max_time2 AS numeric),3),
            round(CAST(r_result.stddev_time2 AS numeric),3),
            r_result.calls2
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_exec_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    -- Cursor for topn querues ordered by executions
    c_calls CURSOR FOR
    SELECT
        st.queryid_md5 as queryid,
        st.dbname,
        st.calls,
        st.calls_pct,
        st.total_time,
        st.min_time,
        st.max_time,
        st.mean_time,
        st.stddev_time,
        st.rows
    FROM top_statements(snode_id, start_id, end_id) st
    ORDER BY st.calls DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Query ID</th><th>Database</th><th>Executions</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Elapsed(s)</th></tr>{rows}</table>',
      'stmt_tpl','<tr><td {mono}><a HREF="#%s">%s</a></td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.calls,
            round(CAST(r_result.calls_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_time AS numeric),3),
            round(CAST(r_result.min_time AS numeric),3),
            round(CAST(r_result.max_time AS numeric),3),
            round(CAST(r_result.stddev_time AS numeric),3),
            round(CAST(r_result.total_time AS numeric),1)
        );
        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_exec_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    -- Cursor for topn querues ordered by executions
    c_calls CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.calls as calls1,
        st1.calls_pct as calls_pct1,
        st1.total_time as total_time1,
        st1.min_time as min_time1,
        st1.max_time as max_time1,
        st1.mean_time as mean_time1,
        st1.stddev_time as stddev_time1,
        st1.rows as rows1,
        st2.calls as calls2,
        st2.calls_pct as calls_pct2,
        st2.total_time as total_time2,
        st2.min_time as min_time2,
        st2.max_time as max_time2,
        st2.mean_time as mean_time2,
        st2.stddev_time as stddev_time2,
        st2.rows as rows2,
        row_number() over (ORDER BY st1.calls DESC NULLS LAST) as rn_calls1,
        row_number() over (ORDER BY st2.calls DESC NULLS LAST) as rn_calls2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, datid, userid, queryid_md5)
    ORDER BY COALESCE(st1.calls,0) + COALESCE(st2.calls,0) DESC ) t1
    WHERE rn_calls1 <= topn OR rn_calls2 <= topn;

    r_result RECORD;
BEGIN
    -- Executions sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Query ID</th><th>Database</th><th>I</th><th>Executions</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Elapsed(s)</th></tr>{rows}</table>',
      'stmt_tpl','<tr {interval1}><td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.calls1,
            round(CAST(r_result.calls_pct1 AS numeric),2),
            r_result.rows1,
            round(CAST(r_result.mean_time1 AS numeric),3),
            round(CAST(r_result.min_time1 AS numeric),3),
            round(CAST(r_result.max_time1 AS numeric),3),
            round(CAST(r_result.stddev_time1 AS numeric),3),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.calls2,
            round(CAST(r_result.calls_pct2 AS numeric),2),
            r_result.rows2,
            round(CAST(r_result.mean_time2 AS numeric),3),
            round(CAST(r_result.min_time2 AS numeric),3),
            round(CAST(r_result.max_time2 AS numeric),3),
            round(CAST(r_result.stddev_time2 AS numeric),3),
            round(CAST(r_result.total_time2 AS numeric),1)
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_iowait_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by I/O Wait time
    c_iowait_time CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.io_time,
        st.blk_read_time,
        st.blk_write_time,
        st.io_time_pct,
        st.shared_blks_read,
        st.local_blks_read,
        st.temp_blks_read,
        st.shared_blks_written,
        st.local_blks_written,
        st.temp_blks_written,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE st.io_time > 0
    ORDER BY st.io_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th rowspan="2">Query ID</th><th rowspan="2">Database</th><th rowspan="2">Elapsed(s)</th><th rowspan="2">IO(s)</th><th rowspan="2">R(s)</th><th rowspan="2">W(s)</th><th rowspan="2">%Total</th><th colspan="3">Reads</th><th colspan="3">Writes</th><th rowspan="2">Executions</th></tr>'||
      '<tr><th>Shr</th><th>Loc</th><th>Tmp</th><th>Shr</th><th>Loc</th><th>Tmp</th></tr>{rows}</table>',
      'stmt_tpl','<tr><td {mono}><a HREF="#%s">%s</a></td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.io_time AS numeric),3),
            round(CAST(r_result.blk_read_time AS numeric),3),
            round(CAST(r_result.blk_write_time AS numeric),3),
            round(CAST(r_result.io_time_pct AS numeric),2),
            round(CAST(r_result.shared_blks_read AS numeric)),
            round(CAST(r_result.local_blks_read AS numeric)),
            round(CAST(r_result.temp_blks_read AS numeric)),
            round(CAST(r_result.shared_blks_written AS numeric)),
            round(CAST(r_result.local_blks_written AS numeric)),
            round(CAST(r_result.temp_blks_written AS numeric)),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_iowait_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by I/O Wait time
    c_iowait_time CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.calls as calls1,
        st1.total_time as total_time1,
        st1.io_time as io_time1,
        st1.blk_read_time as blk_read_time1,
        st1.blk_write_time as blk_write_time1,
        st1.io_time_pct as io_time_pct1,
        st1.shared_blks_read as shared_blks_read1,
        st1.local_blks_read as local_blks_read1,
        st1.temp_blks_read as temp_blks_read1,
        st1.shared_blks_written as shared_blks_written1,
        st1.local_blks_written as local_blks_written1,
        st1.temp_blks_written as temp_blks_written1,
        st2.calls as calls2,
        st2.total_time as total_time2,
        st2.io_time as io_time2,
        st2.blk_read_time as blk_read_time2,
        st2.blk_write_time as blk_write_time2,
        st2.io_time_pct as io_time_pct2,
        st2.shared_blks_read as shared_blks_read2,
        st2.local_blks_read as local_blks_read2,
        st2.temp_blks_read as temp_blks_read2,
        st2.shared_blks_written as shared_blks_written2,
        st2.local_blks_written as local_blks_written2,
        st2.temp_blks_written as temp_blks_written2,
        row_number() over (ORDER BY st1.io_time DESC NULLS LAST) as rn_iotime1,
        row_number() over (ORDER BY st2.io_time DESC NULLS LAST) as rn_iotime2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.io_time,st2.io_time) > 0
    ORDER BY COALESCE(st1.io_time,0) + COALESCE(st2.io_time,0) DESC ) t1
    WHERE rn_iotime1 <= topn OR rn_iotime2 <= topn;

    r_result RECORD;
BEGIN
    -- IOWait time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th rowspan="2">Query ID</th><th rowspan="2">Database</th><th rowspan="2">I</th><th rowspan="2">Elapsed(s)</th><th rowspan="2">IO(s)</th><th rowspan="2">R(s)</th><th rowspan="2">W(s)</th><th rowspan="2">%Total</th><th colspan="3">Reads</th><th colspan="3">Writes</th><th rowspan="2">Executions</th></tr>'||
      '<tr><th>Shr</th><th>Loc</th><th>Tmp</th><th>Shr</th><th>Loc</th><th>Tmp</th></tr>{rows}</table>',
      'stmt_tpl','<tr {interval1}><td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            round(CAST(r_result.io_time1 AS numeric),3),
            round(CAST(r_result.blk_read_time1 AS numeric),3),
            round(CAST(r_result.blk_write_time1 AS numeric),3),
            round(CAST(r_result.io_time_pct1 AS numeric),2),
            round(CAST(r_result.shared_blks_read1 AS numeric)),
            round(CAST(r_result.local_blks_read1 AS numeric)),
            round(CAST(r_result.temp_blks_read1 AS numeric)),
            round(CAST(r_result.shared_blks_written1 AS numeric)),
            round(CAST(r_result.local_blks_written1 AS numeric)),
            round(CAST(r_result.temp_blks_written1 AS numeric)),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            round(CAST(r_result.io_time2 AS numeric),3),
            round(CAST(r_result.blk_read_time2 AS numeric),3),
            round(CAST(r_result.blk_write_time2 AS numeric),3),
            round(CAST(r_result.io_time_pct2 AS numeric),2),
            round(CAST(r_result.shared_blks_read2 AS numeric)),
            round(CAST(r_result.local_blks_read2 AS numeric)),
            round(CAST(r_result.temp_blks_read2 AS numeric)),
            round(CAST(r_result.shared_blks_written2 AS numeric)),
            round(CAST(r_result.local_blks_written2 AS numeric)),
            round(CAST(r_result.temp_blks_written2 AS numeric)),
            r_result.calls2
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_gets_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by gets
    c_gets CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.gets,
        st.gets_pct,
        st.hit_pct,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE gets > 0
    ORDER BY st.gets DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>Rows</th><th>Gets</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr><td {mono}><a HREF="#%s">%s</a></td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by gets
    FOR r_result IN c_gets LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.gets_pct AS numeric),2),
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_gets_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by gets
    c_gets CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.gets as gets1,
        st1.gets_pct as gets_pct1,
        st1.hit_pct as hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.gets as gets2,
        st2.gets_pct as gets_pct2,
        st2.hit_pct as hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.gets DESC NULLS LAST) as rn_gets1,
        row_number() over (ORDER BY st2.gets DESC NULLS LAST) as rn_gets2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.gets,st2.gets) > 0
    ORDER BY COALESCE(st1.gets,0) + COALESCE(st2.gets,0) DESC ) t1
    WHERE rn_gets1 <= topn OR rn_gets2 <= topn;

    r_result RECORD;
BEGIN
    -- Gets sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Query ID</th><th>Database</th><th>I</th><th>Elapsed(s)</th><th>Rows</th><th>Gets</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr {interval1}><td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by gets
    FOR r_result IN c_gets LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.gets1,
            round(CAST(r_result.gets_pct1 AS numeric),2),
            round(CAST(r_result.hit_pct1 AS numeric),2),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
            r_result.gets2,
            round(CAST(r_result.gets_pct2 AS numeric),2),
            round(CAST(r_result.hit_pct2 AS numeric),2),
            r_result.calls2
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_shared_reads_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by reads
    c_gets CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.shared_blks_read,
        st.read_pct,
        st.hit_pct,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE st.shared_blks_read > 0
    ORDER BY st.shared_blks_read DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>Rows</th><th>Reads</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr><td {mono}><a HREF="#%s">%s</a></td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by reads
    FOR r_result IN c_gets LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.shared_blks_read,
            round(CAST(r_result.read_pct AS numeric),2),
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_shared_reads_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by reads
    c_gets CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.shared_blks_read as shared_blks_read1,
        st1.read_pct as read_pct1,
        st1.hit_pct as hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.shared_blks_read as shared_blks_read2,
        st2.read_pct as read_pct2,
        st2.hit_pct as hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.shared_blks_read DESC NULLS LAST) as rn_reads1,
        row_number() over (ORDER BY st2.shared_blks_read DESC NULLS LAST) as rn_reads2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.shared_blks_read,st2.shared_blks_read) > 0
    ORDER BY COALESCE(st1.shared_blks_read,0) + COALESCE(st2.shared_blks_read,0) DESC ) t1
    WHERE LEAST(rn_reads1, rn_reads2) <= topn;

    r_result RECORD;
BEGIN
    -- Reads sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Query ID</th><th>Database</th><th>I</th><th>Elapsed(s)</th><th>Rows</th><th>Reads</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr {interval1}><td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by reads
    FOR r_result IN c_gets LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.shared_blks_read1,
            round(CAST(r_result.read_pct1 AS numeric),2),
            round(CAST(r_result.hit_pct1 AS numeric),2),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
            r_result.shared_blks_read2,
            round(CAST(r_result.read_pct2 AS numeric),2),
            round(CAST(r_result.hit_pct2 AS numeric),2),
            r_result.calls2
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_shared_dirtied_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared dirtied
    c_gets CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.shared_blks_dirtied,
        st.dirtied_pct,
        st.hit_pct,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE st.shared_blks_dirtied > 0
    ORDER BY st.shared_blks_dirtied DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>Rows</th><th>Dirtied</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr><td {mono}><a HREF="#%s">%s</a></td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by shared dirtied
    FOR r_result IN c_gets LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.shared_blks_dirtied,
            round(CAST(r_result.dirtied_pct AS numeric),2),
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_shared_dirtied_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared dirtied
    c_gets CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.shared_blks_dirtied as shared_blks_dirtied1,
        st1.dirtied_pct as dirtied_pct1,
        st1.hit_pct as hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.shared_blks_dirtied as shared_blks_dirtied2,
        st2.dirtied_pct as dirtied_pct2,
        st2.hit_pct as hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.shared_blks_dirtied DESC NULLS LAST) as rn_dirtied1,
        row_number() over (ORDER BY st2.shared_blks_dirtied DESC NULLS LAST) as rn_dirtied2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.shared_blks_dirtied,st2.shared_blks_dirtied) > 0
    ORDER BY COALESCE(st1.shared_blks_dirtied,0) + COALESCE(st2.shared_blks_dirtied,0) DESC ) t1
    WHERE LEAST(rn_dirtied1, rn_dirtied2) <= topn;

    r_result RECORD;
BEGIN
    -- Dirtied sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Query ID</th><th>Database</th><th>I</th><th>Elapsed(s)</th><th>Rows</th><th>Dirtied</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr {interval1}><td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by shared dirtied
    FOR r_result IN c_gets LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.shared_blks_dirtied1,
            round(CAST(r_result.dirtied_pct1 AS numeric),2),
            round(CAST(r_result.hit_pct1 AS numeric),2),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
            r_result.shared_blks_dirtied2,
            round(CAST(r_result.dirtied_pct2 AS numeric),2),
            round(CAST(r_result.hit_pct2 AS numeric),2),
            r_result.calls2
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_shared_written_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared written
    c_gets CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.shared_blks_written,
        st.tot_written_pct,
        st.backend_written_pct,
        st.hit_pct,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE st.shared_blks_dirtied > 0
    ORDER BY st.shared_blks_dirtied DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>Rows</th><th>Written</th><th>%Total</th><th>%BackendW</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr><td {mono}><a HREF="#%s">%s</a></td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by shared written
    FOR r_result IN c_gets LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.shared_blks_written,
            round(CAST(r_result.tot_written_pct AS numeric),2),
            round(CAST(r_result.backend_written_pct AS numeric),2),
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_shared_written_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by shared written
    c_gets CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.shared_blks_written as shared_blks_written1,
        st1.tot_written_pct as tot_written_pct1,
        st1.backend_written_pct as backend_written_pct1,
        st1.hit_pct as hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.shared_blks_written as shared_blks_written2,
        st2.tot_written_pct as tot_written_pct2,
        st2.backend_written_pct as backend_written_pct2,
        st2.hit_pct as hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.shared_blks_written DESC NULLS LAST) as rn_written1,
        row_number() over (ORDER BY st2.shared_blks_written DESC NULLS LAST) as rn_written2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.shared_blks_written,st2.shared_blks_written) > 0
    ORDER BY COALESCE(st1.shared_blks_written,0) + COALESCE(st2.shared_blks_written,0) DESC ) t1
    WHERE LEAST(rn_written1, rn_written2) <= topn;

    r_result RECORD;
BEGIN
    -- Shared written sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Query ID</th><th>Database</th><th>I</th><th>Elapsed(s)</th><th>Rows</th><th>Written</th><th>%Total</th><th>%BackendW</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr {interval1}><td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by shared written
    FOR r_result IN c_gets LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.shared_blks_written1,
            round(CAST(r_result.tot_written_pct1 AS numeric),2),
            round(CAST(r_result.backend_written_pct1 AS numeric),2),
            round(CAST(r_result.hit_pct1 AS numeric),2),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
            r_result.shared_blks_written2,
            round(CAST(r_result.tot_written_pct2 AS numeric),2),
            round(CAST(r_result.backend_written_pct2 AS numeric),2),
            round(CAST(r_result.hit_pct2 AS numeric),2),
            r_result.calls2
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_temp_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by temp usage
    c_temp CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.gets,
        st.hit_pct,
        st.temp_blks_written,
        st.temp_write_total_pct,
        st.temp_blks_read,
        st.temp_read_total_pct,
        st.local_blks_written,
        st.local_write_total_pct,
        st.local_blks_read,
        st.local_read_total_pct,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written > 0
    ORDER BY st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written DESC
    LIMIT topn;

    r_result RECORD;
BEGIN

    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>Rows</th><th>Gets</th><th>Hits(%)</th><th>Work_w(blk)</th><th>%Total</th><th>Work_r(blk)</th><th>%Total</th><th>Local_w(blk)</th><th>%Total</th><th>Local_r(blk)</th><th>%Total</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr><td {mono}><a HREF="#%s">%s</a></td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.temp_blks_written,
            round(CAST(r_result.temp_write_total_pct AS numeric),2),
            r_result.temp_blks_read,
            round(CAST(r_result.temp_read_total_pct AS numeric),2),
            r_result.local_blks_written,
            round(CAST(r_result.local_write_total_pct AS numeric),2),
            r_result.local_blks_read,
            round(CAST(r_result.local_read_total_pct AS numeric),2),
            r_result.calls
        );
        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_temp_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) querues ordered by temp usage
    c_temp CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.gets as gets1,
        st1.hit_pct as hit_pct1,
        st1.temp_blks_written as temp_blks_written1,
        st1.temp_write_total_pct as temp_write_total_pct1,
        st1.temp_blks_read as temp_blks_read1,
        st1.temp_read_total_pct as temp_read_total_pct1,
        st1.local_blks_written as local_blks_written1,
        st1.local_write_total_pct as local_write_total_pct1,
        st1.local_blks_read as local_blks_read1,
        st1.local_read_total_pct as local_read_total_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.gets as gets2,
        st2.hit_pct as hit_pct2,
        st2.temp_blks_written as temp_blks_written2,
        st2.temp_write_total_pct as temp_write_total_pct2,
        st2.temp_blks_read as temp_blks_read2,
        st2.temp_read_total_pct as temp_read_total_pct2,
        st2.local_blks_written as local_blks_written2,
        st2.local_write_total_pct as local_write_total_pct2,
        st2.local_blks_read as local_blks_read2,
        st2.local_read_total_pct as local_read_total_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.temp_blks_read + st1.temp_blks_written + st1.local_blks_read + st1.local_blks_written DESC NULLS LAST) as rn_temp1,
        row_number() over (ORDER BY st2.temp_blks_read + st2.temp_blks_written + st2.local_blks_read + st2.local_blks_written DESC NULLS LAST) as rn_temp2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.temp_blks_read + st1.temp_blks_written + st1.local_blks_read + st1.local_blks_written,
        st2.temp_blks_read + st2.temp_blks_written + st2.local_blks_read + st2.local_blks_written) > 0
    ORDER BY COALESCE(st1.temp_blks_read + st1.temp_blks_written + st1.local_blks_read + st1.local_blks_written,0) +
        COALESCE(st2.temp_blks_read + st2.temp_blks_written + st2.local_blks_read + st2.local_blks_written,0) DESC ) t1
    WHERE rn_temp1 <= topn OR rn_temp2 <= topn;

    r_result RECORD;
BEGIN
    -- Temp usage sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Query ID</th><th>Database</th><th>I</th><th>Elapsed(s)</th><th>Rows</th><th>Gets</th><th>Hits(%)</th><th>Work_w(blk)</th><th>%Total</th><th>Work_r(blk)</th><th>%Total</th><th>Local_w(blk)</th><th>%Total</th><th>Local_r(blk)</th><th>%Total</th><th>Executions</th></tr>{rows}</table>',
      'stmt_tpl','<tr {interval1}><td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.gets1,
            round(CAST(r_result.hit_pct1 AS numeric),2),
            r_result.temp_blks_written1,
            round(CAST(r_result.temp_write_total_pct1 AS numeric),2),
            r_result.temp_blks_read1,
            round(CAST(r_result.temp_read_total_pct1 AS numeric),2),
            r_result.local_blks_written1,
            round(CAST(r_result.local_write_total_pct1 AS numeric),2),
            r_result.local_blks_read1,
            round(CAST(r_result.local_read_total_pct1 AS numeric),2),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
            r_result.gets2,
            round(CAST(r_result.hit_pct2 AS numeric),2),
            r_result.temp_blks_written2,
            round(CAST(r_result.temp_write_total_pct2 AS numeric),2),
            r_result.temp_blks_read2,
            round(CAST(r_result.temp_read_total_pct2 AS numeric),2),
            r_result.local_blks_written2,
            round(CAST(r_result.local_write_total_pct2 AS numeric),2),
            r_result.local_blks_read2,
            round(CAST(r_result.local_read_total_pct2 AS numeric),2),
            r_result.calls2
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION collect_queries(IN query_id char(10)) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    INSERT INTO queries_list
    VALUES (query_id)
    ON CONFLICT DO NOTHING;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION report_queries(IN jreportset jsonb) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    c_queries CURSOR FOR
    SELECT queryid_md5 AS queryid, query AS querytext
    FROM queries_list JOIN stmt_list USING (queryid_md5)
    ORDER BY queryid_md5;
    qr_result   RECORD;
    report      text := '';
    query_text  text := '';
    jtab_tpl    jsonb;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>QueryID</th><th>Query Text</th></tr>{rows}</table>',
      'stmt_tpl','<tr id="%s"><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR qr_result IN c_queries LOOP
        query_text := replace(qr_result.querytext,'<','&lt;');
        query_text := replace(query_text,'>','&gt;');
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            qr_result.queryid,
            qr_result.queryid,
            query_text
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
