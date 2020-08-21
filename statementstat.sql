/* ===== Statements stats functions ===== */

CREATE OR REPLACE FUNCTION top_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id                 integer,
    datid                   oid,
    dbname                  name,
    userid                  oid,
    queryid_md5             char(10),
    plans                   bigint,
    plans_pct               float,
    calls                   bigint,
    calls_pct               float,
    total_time              double precision,
    total_time_pct          double precision,
    total_plan_time         double precision,
    plan_time_pct           float,
    total_exec_time         double precision,
    total_exec_time_pct     float,
    exec_time_pct           float,
    min_exec_time           double precision,
    max_exec_time           double precision,
    mean_exec_time          double precision,
    stddev_exec_time        double precision,
    min_plan_time           double precision,
    max_plan_time           double precision,
    mean_plan_time          double precision,
    stddev_plan_time        double precision,
    rows                    bigint,
    shared_blks_hit         bigint,
    shared_hit_pct          float,
    shared_blks_read        bigint,
    read_pct                float,
    shared_blks_fetched     bigint,
    shared_blks_fetched_pct float,
    shared_blks_dirtied     bigint,
    dirtied_pct             float,
    shared_blks_written     bigint,
    tot_written_pct         float,
    backend_written_pct     float,
    local_blks_hit          bigint,
    local_hit_pct           float,
    local_blks_read         bigint,
    local_blks_fetched      bigint,
    local_blks_dirtied      bigint,
    local_blks_written      bigint,
    temp_blks_read          bigint,
    temp_blks_written       bigint,
    blk_read_time           double precision,
    blk_write_time          double precision,
    io_time                 double precision,
    io_time_pct             float,
    temp_read_total_pct     float,
    temp_write_total_pct    float,
    local_read_total_pct    float,
    local_write_total_pct   float,
    wal_records             bigint,
    wal_fpi                 bigint,
    wal_bytes               numeric,
    wal_bytes_pct           float,
    user_time               double precision,
    system_time             double precision,
    reads                   bigint,
    writes                  bigint
) SET search_path=@extschema@,public AS $$
    WITH
      tot AS (
        SELECT
            COALESCE(sum(total_plan_time), 0) + sum(total_exec_time) AS total_time,
            sum(blk_read_time) AS blk_read_time,
            sum(blk_write_time) AS blk_write_time,
            sum(shared_blks_hit) AS shared_blks_hit,
            sum(shared_blks_read) AS shared_blks_read,
            sum(shared_blks_dirtied) AS shared_blks_dirtied,
            sum(temp_blks_read) AS temp_blks_read,
            sum(temp_blks_written) AS temp_blks_written,
            sum(local_blks_read) AS local_blks_read,
            sum(local_blks_written) AS local_blks_written,
            sum(calls) AS calls,
            sum(plans) AS plans
        FROM sample_statements_total st
        WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
      ),
      totbgwr AS (
        SELECT
          sum(buffers_checkpoint + buffers_clean + buffers_backend) AS written,
          sum(buffers_backend) AS buffers_backend,
          sum(wal_size) AS wal_size
        FROM sample_stat_cluster
        WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
      )
    SELECT
        st.server_id as server_id,
        st.datid as datid,
        sample_db.datname as dbname,
        st.userid as userid,
        st.queryid_md5 as queryid_md5,
        sum(st.plans)::bigint as plans,
        (sum(st.plans)*100/NULLIF(min(tot.plans), 0))::float as plans_pct,
        sum(st.calls)::bigint as calls,
        (sum(st.calls)*100/NULLIF(min(tot.calls), 0))::float as calls_pct,
        (sum(st.total_exec_time) + coalesce(sum(st.total_plan_time), 0))/1000 as total_time,
        (sum(st.total_exec_time) + coalesce(sum(st.total_plan_time), 0))*100/NULLIF(min(tot.total_time), 0) as total_time_pct,
        sum(st.total_plan_time)/1000 as total_plan_time,
        sum(st.total_plan_time)*100/NULLIF(min(st.total_exec_time) + min(st.total_plan_time), 0) as plan_time_pct,
        sum(st.total_exec_time)/1000 as total_exec_time,
        sum(st.total_exec_time)*100/NULLIF(min(tot.total_time), 0) as total_exec_time_pct,
        sum(st.total_exec_time)*100/NULLIF(min(st.total_exec_time) + min(st.total_plan_time), 0) as exec_time_pct,
        min(st.min_exec_time) as min_exec_time,
        max(st.max_exec_time) as max_exec_time,
        sum(st.mean_exec_time*st.calls)/NULLIF(sum(st.calls), 0) as mean_exec_time,
        sqrt(sum((power(st.stddev_exec_time,2)+power(st.mean_exec_time,2))*st.calls)/NULLIF(sum(st.calls),0)-power(sum(st.mean_exec_time*st.calls)/NULLIF(sum(st.calls),0),2)) as stddev_exec_time,
        min(st.min_plan_time) as min_plan_time,
        max(st.max_plan_time) as max_plan_time,
        sum(st.mean_plan_time*st.plans)/NULLIF(sum(st.plans),0) as mean_plan_time,
        sqrt(sum((power(st.stddev_plan_time,2)+power(st.mean_plan_time,2))*st.plans)/NULLIF(sum(st.plans),0)-power(sum(st.mean_plan_time*st.plans)/NULLIF(sum(st.plans),0),2)) as stddev_plan_time,
        sum(st.rows)::bigint as rows,
        sum(st.shared_blks_hit)::bigint as shared_blks_hit,
        (sum(st.shared_blks_hit) * 100 / NULLIF(sum(st.shared_blks_hit) + sum(st.shared_blks_read), 0))::float as shared_hit_pct,
        sum(st.shared_blks_read)::bigint as shared_blks_read,
        (sum(st.shared_blks_read) * 100 / NULLIF(min(tot.shared_blks_read), 0))::float as read_pct,
        (sum(st.shared_blks_hit) + sum(st.shared_blks_read))::bigint as shared_blks_fetched,
        ((sum(st.shared_blks_hit) + sum(st.shared_blks_read)) * 100 / NULLIF(min(tot.shared_blks_hit) + min(tot.shared_blks_read), 0))::float as shared_blks_fetched_pct,
        sum(st.shared_blks_dirtied)::bigint as shared_blks_dirtied,
        (sum(st.shared_blks_dirtied) * 100 / NULLIF(min(tot.shared_blks_dirtied), 0))::float as dirtied_pct,
        sum(st.shared_blks_written)::bigint as shared_blks_written,
        (sum(st.shared_blks_written) * 100 / NULLIF(min(totbgwr.written), 0))::float as tot_written_pct,
        (sum(st.shared_blks_written) * 100 / NULLIF(min(totbgwr.buffers_backend), 0))::float as backend_written_pct,
        sum(st.local_blks_hit)::bigint as local_blks_hit,
        (sum(st.local_blks_hit) * 100 / NULLIF(sum(st.local_blks_hit) + sum(st.local_blks_read),0))::float as local_hit_pct,
        sum(st.local_blks_read)::bigint as local_blks_read,
        (sum(st.local_blks_hit) + sum(st.local_blks_read))::bigint as local_blks_fetched,
        sum(st.local_blks_dirtied)::bigint as local_blks_dirtied,
        sum(st.local_blks_written)::bigint as local_blks_written,
        sum(st.temp_blks_read)::bigint as temp_blks_read,
        sum(st.temp_blks_written)::bigint as temp_blks_written,
        sum(st.blk_read_time)/1000 as blk_read_time,
        sum(st.blk_write_time)/1000 as blk_write_time,
        (sum(st.blk_read_time + st.blk_write_time))/1000 as io_time,
        (sum(st.blk_read_time) + sum(st.blk_write_time)) * 100 / NULLIF(min(tot.blk_read_time) + min(tot.blk_write_time),0) as io_time_pct,
        (sum(st.temp_blks_read) * 100 / NULLIF(min(tot.temp_blks_read), 0))::float as temp_read_total_pct,
        (sum(st.temp_blks_written) * 100 / NULLIF(min(tot.temp_blks_written), 0))::float as temp_write_total_pct,
        (sum(st.local_blks_read) * 100 / NULLIF(min(tot.local_blks_read), 0))::float as local_read_total_pct,
        (sum(st.local_blks_written) * 100 / NULLIF(min(tot.local_blks_written), 0))::float as local_write_total_pct,
        sum(st.wal_records)::bigint as wal_records,
        sum(st.wal_fpi)::bigint as wal_fpi,
        sum(st.wal_bytes) as wal_bytes,
        (sum(st.wal_bytes) * 100 / NULLIF(min(totbgwr.wal_size), 0))::float wal_bytes_pct,
        -- kcache stats
        sum(kc.user_time) as user_time,
        sum(kc.system_time) as system_time,
        sum(kc.reads)::bigint as reads,
        sum(kc.writes)::bigint as writes
    FROM v_sample_statements st
        -- kcache join
        LEFT OUTER JOIN sample_kcache kc USING(server_id, sample_id, userid, datid, queryid)
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
        /* Start sample existance condition
        Start sample stats does not account in report, but we must be sure
        that start sample exists, as it is reference point of next sample
        */
        JOIN samples sample_s ON (st.server_id = sample_s.server_id AND sample_s.sample_id = start_id)
        /* End sample existance condition
        Make sure that end sample exists, so we really account full interval
        */
        JOIN samples sample_e ON (st.server_id = sample_e.server_id AND sample_e.sample_id = end_id)
        -- Total stats
        CROSS JOIN tot CROSS JOIN totbgwr
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN sample_s.sample_id + 1 AND sample_e.sample_id
    GROUP BY st.server_id,st.datid,sample_db.datname,st.userid,st.queryid,st.queryid_md5
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION top_elapsed_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by epapsed time
    c_elapsed_time CURSOR FOR
    SELECT
        st.queryid_md5 as queryid,
        st.dbname,
        st.total_time_pct,
        st.total_time,
        st.total_plan_time,
        st.total_exec_time,
        st.blk_read_time,
        st.blk_write_time,
        st.user_time,
        st.system_time,
        st.calls,
        st.plans
    FROM top_statements(sserver_id, start_id, end_id) st
    ORDER BY st.total_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when planning timing is available
    IF NOT jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Elapsed time as a percentage of total cluster elapsed time">%Total</th>'
            '<th colspan="3">Time (s)</th>'
            '<th colspan="2">I/O time (s)</th>'
            '{kcache_hdr1}'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by the statement">Elapsed</th>'
            '<th title="Time spent planning statement">Plan</th>'
            '<th title="Time spent executing statement">Exec</th>'
            '<th title="Time spent reading blocks by statement">Read</th>'
            '<th title="Time spent writing blocks by statement">Write</th>'
            '{kcache_hdr2}'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%1$s">%1$s</a></td>'
          '<td>%2$s</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{kcache_row}'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
        '</tr>',
      'kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcache_row',
        '<td {value}>%9$s</td>'
        '<td {value}>%10$s</td>'
      );
    -- Conditional template
    IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}',jtab_tpl->>'kcache_hdr1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}',jtab_tpl->>'kcache_hdr2')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row}',jtab_tpl->>'kcache_row')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row}','')));
    END IF;
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.total_exec_time AS numeric),2),
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2),
            round(CAST(r_result.user_time AS numeric),2),
            round(CAST(r_result.system_time AS numeric),2),
            r_result.plans,
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

CREATE OR REPLACE FUNCTION top_elapsed_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by epapsed time
    c_elapsed_time CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.total_time_pct as total_time_pct1,
        st1.total_plan_time as total_plan_time1,
        st1.total_exec_time as total_exec_time1,
        st1.blk_read_time as blk_read_time1,
        st1.blk_write_time as blk_write_time1,
        st1.user_time as user_time1,
        st1.system_time as system_time1,
        st1.calls as calls1,
        st1.plans as plans1,
        st2.total_time as total_time2,
        st2.total_time_pct as total_time_pct2,
        st2.total_plan_time as total_plan_time2,
        st2.total_exec_time as total_exec_time2,
        st2.blk_read_time as blk_read_time2,
        st2.blk_write_time as blk_write_time2,
        st2.user_time as user_time2,
        st2.system_time as system_time2,
        st2.calls as calls2,
        st2.plans as plans2,
        row_number() over (ORDER BY st1.total_time DESC NULLS LAST) as rn_time1,
        row_number() over (ORDER BY st2.total_time DESC NULLS LAST) as rn_time2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    ORDER BY COALESCE(st1.total_time,0) + COALESCE(st2.total_time,0) DESC ) t1
    WHERE rn_time1 <= topn OR rn_time2 <= topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when planning timing is available
    IF NOT jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
      RETURN '';
    END IF;

    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Elapsed time as a percentage of total cluster elapsed time">%Total</th>'
            '<th colspan="3">Time (s)</th>'
            '<th colspan="2">I/O time (s)</th>'
            '{kcache_hdr1}'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by the statement">Elapsed</th>'
            '<th title="Time spent planning statement">Plan</th>'
            '<th title="Time spent executing statement">Exec</th>'
            '<th title="Time spent reading blocks by statement">Read</th>'
            '<th title="Time spent writing blocks by statement">Write</th>'
            '{kcache_hdr2}'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%1$s">%1$s</a></td>'
          '<td {rowtdspanhdr}>%2$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '{kcache_row1}'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
          '{kcache_row2}'
          '<td {value}>%21$s</td>'
          '<td {value}>%22$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcache_row1',
        '<td {value}>%9$s</td>'
        '<td {value}>%10$s</td>',
      'kcache_row2',
        '<td {value}>%19$s</td>'
        '<td {value}>%20$s</td>'
      );
    -- Conditional template
    IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}',jtab_tpl->>'kcache_hdr1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}',jtab_tpl->>'kcache_hdr2')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row1}',jtab_tpl->>'kcache_row1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row2}',jtab_tpl->>'kcache_row2')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row2}','')));
    END IF;
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),2),
            round(CAST(r_result.total_plan_time1 AS numeric),2),
            round(CAST(r_result.total_exec_time1 AS numeric),2),
            round(CAST(r_result.blk_read_time1 AS numeric),2),
            round(CAST(r_result.blk_write_time1 AS numeric),2),
            round(CAST(r_result.user_time1 AS numeric),2),
            round(CAST(r_result.system_time1 AS numeric),2),
            r_result.plans1,
            r_result.calls1,
            round(CAST(r_result.total_time_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),2),
            round(CAST(r_result.total_plan_time2 AS numeric),2),
            round(CAST(r_result.total_exec_time2 AS numeric),2),
            round(CAST(r_result.blk_read_time2 AS numeric),2),
            round(CAST(r_result.blk_write_time2 AS numeric),2),
            round(CAST(r_result.user_time2 AS numeric),2),
            round(CAST(r_result.system_time2 AS numeric),2),
            r_result.plans2,
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


CREATE OR REPLACE FUNCTION top_plan_time_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by planning time
    c_elapsed_time CURSOR FOR
    SELECT
        st.queryid_md5 as queryid,
        st.dbname,
        st.plans,
        st.calls,
        st.total_plan_time,
        st.plan_time_pct,
        st.min_plan_time,
        st.max_plan_time,
        st.mean_plan_time,
        st.stddev_plan_time
    FROM top_statements(sserver_id, start_id, end_id) st
    ORDER BY st.total_plan_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when planning timing is available
    IF NOT jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Time spent planning statement">Plan elapsed (s)</th>'
            '<th rowspan="2" title="Plan elapsed as a percentage of statement elapsed time">%Elapsed</th>'
            '<th colspan="4" title="Planning time statistics">Plan times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%1$s">%1$s</a></td>'
          '<td>%2$s</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
        '</tr>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.plan_time_pct AS numeric),2),
            round(CAST(r_result.mean_plan_time AS numeric),3),
            round(CAST(r_result.min_plan_time AS numeric),3),
            round(CAST(r_result.max_plan_time AS numeric),3),
            round(CAST(r_result.stddev_plan_time AS numeric),3),
            r_result.plans,
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

CREATE OR REPLACE FUNCTION top_plan_time_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by epapsed time
    c_elapsed_time CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.plans as plans1,
        st1.calls as calls1,
        st1.total_plan_time as total_plan_time1,
        st1.plan_time_pct as plan_time_pct1,
        st1.min_plan_time as min_plan_time1,
        st1.max_plan_time as max_plan_time1,
        st1.mean_plan_time as mean_plan_time1,
        st1.stddev_plan_time as stddev_plan_time1,
        st2.plans as plans2,
        st2.calls as calls2,
        st2.total_plan_time as total_plan_time2,
        st2.plan_time_pct as plan_time_pct2,
        st2.min_plan_time as min_plan_time2,
        st2.max_plan_time as max_plan_time2,
        st2.mean_plan_time as mean_plan_time2,
        st2.stddev_plan_time as stddev_plan_time2,
        row_number() over (ORDER BY st1.total_plan_time DESC NULLS LAST) as rn_time1,
        row_number() over (ORDER BY st2.total_plan_time DESC NULLS LAST) as rn_time2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    ORDER BY COALESCE(st1.total_plan_time,0) + COALESCE(st2.total_plan_time,0) DESC ) t1
    WHERE rn_time1 <= topn OR rn_time2 <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Time spent planning statement">Plan elapsed (s)</th>'
            '<th rowspan="2" title="Plan elapsed as a percentage of statement elapsed time">%Elapsed</th>'
            '<th colspan="4" title="Planning time statistics">Plan times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was planned">Plans</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%1$s">%1$s</a></td>'
          '<td {rowtdspanhdr}>%2$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_plan_time1 AS numeric),2),
            round(CAST(r_result.plan_time_pct1 AS numeric),2),
            round(CAST(r_result.mean_plan_time1 AS numeric),3),
            round(CAST(r_result.min_plan_time1 AS numeric),3),
            round(CAST(r_result.max_plan_time1 AS numeric),3),
            round(CAST(r_result.stddev_plan_time1 AS numeric),3),
            r_result.plans1,
            r_result.calls1,
            round(CAST(r_result.total_plan_time2 AS numeric),2),
            round(CAST(r_result.plan_time_pct2 AS numeric),2),
            round(CAST(r_result.mean_plan_time2 AS numeric),3),
            round(CAST(r_result.min_plan_time2 AS numeric),3),
            round(CAST(r_result.max_plan_time2 AS numeric),3),
            round(CAST(r_result.stddev_plan_time2 AS numeric),3),
            r_result.plans2,
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

CREATE OR REPLACE FUNCTION top_exec_time_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by execution time
    c_elapsed_time CURSOR FOR
    SELECT
        st.queryid_md5 as queryid,
        st.dbname,
        st.calls,
        st.total_exec_time,
        st.total_exec_time_pct,
        st.exec_time_pct,
        st.blk_read_time,
        st.blk_write_time,
        st.min_exec_time,
        st.max_exec_time,
        st.mean_exec_time,
        st.stddev_exec_time,
        st.rows,
        st.user_time,
        st.system_time
    FROM top_statements(sserver_id, start_id, end_id) st
    ORDER BY st.total_exec_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Time spent executing statement">Exec (s)</th>'
            '{elapsed_pct_hdr}'
            '<th rowspan="2" title="Exec time as a percentage of total cluster elapsed time">%Total</th>'
            '<th colspan="2">I/O time (s)</th>'
            '{kcache_hdr1}'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th colspan="4" title="Execution time statistics">Execution times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent reading blocks by statement">Read</th>'
            '<th title="Time spent writing blocks by statement">Write</th>'
            '{kcache_hdr2}'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%1$s">%1$s</a></td>'
          '<td>%2$s</td>'
          '<td {value}>%3$s</td>'
          '{elapsed_pct_row}'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '{kcache_row}'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
        '</tr>',
      'elapsed_pct_hdr',
        '<th rowspan="2" title="Exec time as a percentage of statement elapsed time">%Elapsed</th>',
      'elapsed_pct_row',
        '<td {value}>%15$s</td>',
      'kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcache_row',
        '<td {value}>%7$s</td>'
        '<td {value}>%8$s</td>'
      );
    -- Conditional template
    -- kcache
    IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean AND
      NOT jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
      -- We won't show kcache CPU stats here since v1.8 as CPU stats is shown in "Top SQL by elapsed time" section
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}',jtab_tpl->>'kcache_hdr1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}',jtab_tpl->>'kcache_hdr2')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row}',jtab_tpl->>'kcache_row')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row}','')));
    END IF;
    -- stat_statements v1.8
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{elapsed_pct_hdr}',jtab_tpl->>'elapsed_pct_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{elapsed_pct_row}',jtab_tpl->>'elapsed_pct_row')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{elapsed_pct_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{elapsed_pct_row}','')));
    END IF;
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_exec_time AS numeric),2),
            round(CAST(r_result.total_exec_time_pct AS numeric),2),
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2),
            round(CAST(r_result.user_time AS numeric),2),
            round(CAST(r_result.system_time AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_exec_time AS numeric),3),
            round(CAST(r_result.min_exec_time AS numeric),3),
            round(CAST(r_result.max_exec_time AS numeric),3),
            round(CAST(r_result.stddev_exec_time AS numeric),3),
            r_result.calls,
            round(CAST(r_result.exec_time_pct AS numeric),2)
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

CREATE OR REPLACE FUNCTION top_exec_time_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
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
        st1.total_exec_time as total_exec_time1,
        st1.total_exec_time_pct as total_exec_time_pct1,
        st1.exec_time_pct as exec_time_pct1,
        st1.blk_read_time as blk_read_time1,
        st1.blk_write_time as blk_write_time1,
        st1.min_exec_time as min_exec_time1,
        st1.max_exec_time as max_exec_time1,
        st1.mean_exec_time as mean_exec_time1,
        st1.stddev_exec_time as stddev_exec_time1,
        st1.rows as rows1,
        st1.user_time as user_time1,
        st1.system_time as system_time1,
        st2.calls as calls2,
        st2.total_exec_time as total_exec_time2,
        st2.total_exec_time_pct as total_exec_time_pct2,
        st2.exec_time_pct as exec_time_pct2,
        st2.blk_read_time as blk_read_time2,
        st2.blk_write_time as blk_write_time2,
        st2.min_exec_time as min_exec_time2,
        st2.max_exec_time as max_exec_time2,
        st2.mean_exec_time as mean_exec_time2,
        st2.stddev_exec_time as stddev_exec_time2,
        st2.rows as rows2,
        st2.user_time as user_time2,
        st2.system_time as system_time2,
        row_number() over (ORDER BY st1.total_exec_time DESC NULLS LAST) as rn_time1,
        row_number() over (ORDER BY st2.total_exec_time DESC NULLS LAST) as rn_time2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    ORDER BY COALESCE(st1.total_exec_time,0) + COALESCE(st2.total_exec_time,0) DESC ) t1
    WHERE rn_time1 <= topn OR rn_time2 <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Time spent executing statement">Exec (s)</th>'
            '{elapsed_pct_hdr}'
            '<th rowspan="2" title="Exec time as a percentage of total cluster elapsed time">%Total</th>'
            '<th colspan="2">I/O time (s)</th>'
            '{kcache_hdr1}'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th colspan="4" title="Execution time statistics">Execution times (ms)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent reading blocks by statement">Read</th>'
            '<th title="Time spent writing blocks by statement">Write</th>'
            '{kcache_hdr2}'
            '<th>Mean</th>'
            '<th>Min</th>'
            '<th>Max</th>'
            '<th>StdErr</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%1$s">%1$s</a></td>'
          '<td {rowtdspanhdr}>%2$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%3$s</td>'
          '{elapsed_pct_row1}'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '{kcache_row1}'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%15$s</td>'
          '{elapsed_pct_row2}'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
          '{kcache_row2}'
          '<td {value}>%21$s</td>'
          '<td {value}>%22$s</td>'
          '<td {value}>%23$s</td>'
          '<td {value}>%24$s</td>'
          '<td {value}>%25$s</td>'
          '<td {value}>%26$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'elapsed_pct_hdr',
        '<th rowspan="2" title="Exec time as a percentage of statement elapsed time">%Elapsed</th>',
      'elapsed_pct_row1',
        '<td {value}>%27$s</td>',
      'elapsed_pct_row2',
        '<td {value}>%28$s</td>',
      'kcache_hdr1',
        '<th colspan="2">CPU time (s)</th>',
      'kcache_hdr2',
        '<th>Usr</th>'
        '<th>Sys</th>',
      'kcache_row1',
        '<td {value}>%7$s</td>'
        '<td {value}>%8$s</td>',
      'kcache_row2',
        '<td {value}>%19$s</td>'
        '<td {value}>%20$s</td>'
      );
    -- Conditional template
    IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean AND
      NOT jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}',jtab_tpl->>'kcache_hdr1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}',jtab_tpl->>'kcache_hdr2')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row1}',jtab_tpl->>'kcache_row1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row2}',jtab_tpl->>'kcache_row2')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{kcache_hdr2}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{kcache_row2}','')));
    END IF;
    -- stat_statements v1.8
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{elapsed_pct_hdr}',jtab_tpl->>'elapsed_pct_hdr')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{elapsed_pct_row1}',jtab_tpl->>'elapsed_pct_row1')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{elapsed_pct_row2}',jtab_tpl->>'elapsed_pct_row2')));
    ELSE
      jtab_tpl := jsonb_set(jtab_tpl,'{tab_hdr}',to_jsonb(replace(jtab_tpl->>'tab_hdr','{elapsed_pct_hdr}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{elapsed_pct_row1}','')));
      jtab_tpl := jsonb_set(jtab_tpl,'{stmt_tpl}',to_jsonb(replace(jtab_tpl->>'stmt_tpl','{elapsed_pct_row2}','')));
    END IF;
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_exec_time1 AS numeric),2),
            round(CAST(r_result.total_exec_time_pct1 AS numeric),2),
            round(CAST(r_result.blk_read_time1 AS numeric),2),
            round(CAST(r_result.blk_write_time1 AS numeric),2),
            round(CAST(r_result.user_time1 AS numeric),2),
            round(CAST(r_result.system_time1 AS numeric),2),
            r_result.rows1,
            round(CAST(r_result.mean_exec_time1 AS numeric),3),
            round(CAST(r_result.min_exec_time1 AS numeric),3),
            round(CAST(r_result.max_exec_time1 AS numeric),3),
            round(CAST(r_result.stddev_exec_time1 AS numeric),3),
            r_result.calls1,
            round(CAST(r_result.total_exec_time2 AS numeric),2),
            round(CAST(r_result.total_exec_time_pct2 AS numeric),2),
            round(CAST(r_result.blk_read_time2 AS numeric),2),
            round(CAST(r_result.blk_write_time2 AS numeric),2),
            round(CAST(r_result.user_time2 AS numeric),2),
            round(CAST(r_result.system_time2 AS numeric),2),
            r_result.rows2,
            round(CAST(r_result.mean_exec_time2 AS numeric),3),
            round(CAST(r_result.min_exec_time2 AS numeric),3),
            round(CAST(r_result.max_exec_time2 AS numeric),3),
            round(CAST(r_result.stddev_exec_time2 AS numeric),3),
            r_result.calls2,
            round(CAST(r_result.exec_time_pct1 AS numeric),2),
            round(CAST(r_result.exec_time_pct2 AS numeric),2)
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

CREATE OR REPLACE FUNCTION top_exec_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
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
        st.total_exec_time,
        st.min_exec_time,
        st.max_exec_time,
        st.mean_exec_time,
        st.stddev_exec_time,
        st.rows
    FROM top_statements(sserver_id, start_id, end_id) st
    ORDER BY st.calls DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
            '<th title="Executions of this statement as a percentage of total executions of all statements in a cluster">%Total</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th>Mean(ms)</th>'
            '<th>Min(ms)</th>'
            '<th>Max(ms)</th>'
            '<th>StdErr(ms)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
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
            round(CAST(r_result.mean_exec_time AS numeric),3),
            round(CAST(r_result.min_exec_time AS numeric),3),
            round(CAST(r_result.max_exec_time AS numeric),3),
            round(CAST(r_result.stddev_exec_time AS numeric),3),
            round(CAST(r_result.total_exec_time AS numeric),1)
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

CREATE OR REPLACE FUNCTION top_exec_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
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
        st1.total_exec_time as total_exec_time1,
        st1.min_exec_time as min_exec_time1,
        st1.max_exec_time as max_exec_time1,
        st1.mean_exec_time as mean_exec_time1,
        st1.stddev_exec_time as stddev_exec_time1,
        st1.rows as rows1,
        st2.calls as calls2,
        st2.calls_pct as calls_pct2,
        st2.total_exec_time as total_exec_time2,
        st2.min_exec_time as min_exec_time2,
        st2.max_exec_time as max_exec_time2,
        st2.mean_exec_time as mean_exec_time2,
        st2.stddev_exec_time as stddev_exec_time2,
        st2.rows as rows2,
        row_number() over (ORDER BY st1.calls DESC NULLS LAST) as rn_calls1,
        row_number() over (ORDER BY st2.calls DESC NULLS LAST) as rn_calls2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    ORDER BY COALESCE(st1.calls,0) + COALESCE(st2.calls,0) DESC ) t1
    WHERE rn_calls1 <= topn OR rn_calls2 <= topn;

    r_result RECORD;
BEGIN
    -- Executions sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>I</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
            '<th title="Executions of this statement as a percentage of total executions of all statements in a cluster">%Total</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th>Mean(ms)</th>'
            '<th>Min(ms)</th>'
            '<th>Max(ms)</th>'
            '<th>StdErr(ms)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
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
            round(CAST(r_result.mean_exec_time1 AS numeric),3),
            round(CAST(r_result.min_exec_time1 AS numeric),3),
            round(CAST(r_result.max_exec_time1 AS numeric),3),
            round(CAST(r_result.stddev_exec_time1 AS numeric),3),
            round(CAST(r_result.total_exec_time1 AS numeric),1),
            r_result.calls2,
            round(CAST(r_result.calls_pct2 AS numeric),2),
            r_result.rows2,
            round(CAST(r_result.mean_exec_time2 AS numeric),3),
            round(CAST(r_result.min_exec_time2 AS numeric),3),
            round(CAST(r_result.max_exec_time2 AS numeric),3),
            round(CAST(r_result.stddev_exec_time2 AS numeric),3),
            round(CAST(r_result.total_exec_time2 AS numeric),1)
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

CREATE OR REPLACE FUNCTION top_iowait_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
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
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.io_time > 0
    ORDER BY st.io_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Time spent by the statement reading and writing blocks">IO(s)</th>'
            '<th rowspan="2" title="Time spent by the statement reading blocks">R(s)</th>'
            '<th rowspan="2" title="Time spent by the statement writing blocks">W(s)</th>'
            '<th rowspan="2" title="I/O time of this statement as a percentage of total I/O time for all statements in a cluster">%Total</th>'
            '<th colspan="3" title="Number of blocks read by the statement">Reads</th>'
            '<th colspan="3" title="Number of blocks written by the statement">Writes</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of shared blocks read by the statement">Shr</th>'
            '<th title="Number of local blocks read by the statement (usually used for temporary tables)">Loc</th>'
            '<th title="Number of temp blocks read by the statement (usually used for operations like sorts and joins)">Tmp</th>'
            '<th title="Number of shared blocks written by the statement">Shr</th>'
            '<th title="Number of local blocks written by the statement (usually used for temporary tables)">Loc</th>'
            '<th title="Number of temp blocks written by the statement (usually used for operations like sorts and joins)">Tmp</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
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
            round(CAST(r_result.total_time AS numeric),1),
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

CREATE OR REPLACE FUNCTION top_iowait_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
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
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.io_time,st2.io_time) > 0
    ORDER BY COALESCE(st1.io_time,0) + COALESCE(st2.io_time,0) DESC ) t1
    WHERE rn_iotime1 <= topn OR rn_iotime2 <= topn;

    r_result RECORD;
BEGIN
    -- IOWait time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Time spent by the statement reading and writing blocks">IO(s)</th>'
            '<th rowspan="2" title="Time spent by the statement reading blocks">R(s)</th>'
            '<th rowspan="2" title="Time spent by the statement writing blocks">W(s)</th>'
            '<th rowspan="2" title="I/O time of this statement as a percentage of total I/O time for all statements in a cluster">%Total</th>'
            '<th colspan="3" title="Number of blocks read by the statement">Reads</th>'
            '<th colspan="3" title="Number of blocks written by the statement">Writes</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of shared blocks read by the statement">Shr</th>'
            '<th title="Number of local blocks read by the statement (usually used for temporary tables)">Loc</th>'
            '<th title="Number of temp blocks read by the statement (usually used for operations like sorts and joins)">Tmp</th>'
            '<th title="Number of shared blocks written by the statement">Shr</th>'
            '<th title="Number of local blocks written by the statement (usually used for temporary tables)">Loc</th>'
            '<th title="Number of temp blocks written by the statement (usually used for operations like sorts and joins)">Tmp</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
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
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.calls1,
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
            round(CAST(r_result.total_time2 AS numeric),1),
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

CREATE OR REPLACE FUNCTION top_shared_blks_fetched_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by shared_blks_fetched
    c_shared_blks_fetched CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.shared_blks_fetched,
        st.shared_blks_fetched_pct,
        st.shared_hit_pct,
        st.calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE shared_blks_fetched > 0
    ORDER BY st.shared_blks_fetched DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th title="Shared blocks fetched (read and hit) by the statement">blks fetched</th>'
            '<th title="Shared blocks fetched by this statement as a percentage of all shared blocks fetched in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by shared_blks_fetched
    FOR r_result IN c_shared_blks_fetched LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.shared_blks_fetched,
            round(CAST(r_result.shared_blks_fetched_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE OR REPLACE FUNCTION top_shared_blks_fetched_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by shared_blks_fetched
    c_shared_blks_fetched CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.shared_blks_fetched as shared_blks_fetched1,
        st1.shared_blks_fetched_pct as shared_blks_fetched_pct1,
        st1.shared_hit_pct as shared_hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.shared_blks_fetched as shared_blks_fetched2,
        st2.shared_blks_fetched_pct as shared_blks_fetched_pct2,
        st2.shared_hit_pct as shared_hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.shared_blks_fetched DESC NULLS LAST) as rn_shared_blks_fetched1,
        row_number() over (ORDER BY st2.shared_blks_fetched DESC NULLS LAST) as rn_shared_blks_fetched2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.shared_blks_fetched,st2.shared_blks_fetched) > 0
    ORDER BY COALESCE(st1.shared_blks_fetched,0) + COALESCE(st2.shared_blks_fetched,0) DESC ) t1
    WHERE rn_shared_blks_fetched1 <= topn OR rn_shared_blks_fetched2 <= topn;

    r_result RECORD;
BEGIN
    -- Fetched (blk) sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>I</th>'
            '<th title="Shared blocks fetched (read and hit) by the statement">blks fetched</th>'
            '<th title="Shared blocks fetched by this statement as a percentage of all shared blocks fetched in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by shared_blks_fetched
    FOR r_result IN c_shared_blks_fetched LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.shared_blks_fetched1,
            round(CAST(r_result.shared_blks_fetched_pct1 AS numeric),2),
            round(CAST(r_result.shared_hit_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.shared_blks_fetched2,
            round(CAST(r_result.shared_blks_fetched_pct2 AS numeric),2),
            round(CAST(r_result.shared_hit_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE OR REPLACE FUNCTION top_shared_reads_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by reads
    c_sh_reads CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.shared_blks_read,
        st.read_pct,
        st.shared_hit_pct,
        st.calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.shared_blks_read > 0
    ORDER BY st.shared_blks_read DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th title="Total number of shared blocks read by the statement">Reads</th>'
            '<th title="Shared blocks read by this statement as a percentage of all shared blocks read in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by reads
    FOR r_result IN c_sh_reads LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.shared_blks_read,
            round(CAST(r_result.read_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE OR REPLACE FUNCTION top_shared_reads_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by reads
    c_sh_reads CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.shared_blks_read as shared_blks_read1,
        st1.read_pct as read_pct1,
        st1.shared_hit_pct as shared_hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.shared_blks_read as shared_blks_read2,
        st2.read_pct as read_pct2,
        st2.shared_hit_pct as shared_hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.shared_blks_read DESC NULLS LAST) as rn_reads1,
        row_number() over (ORDER BY st2.shared_blks_read DESC NULLS LAST) as rn_reads2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.shared_blks_read,st2.shared_blks_read) > 0
    ORDER BY COALESCE(st1.shared_blks_read,0) + COALESCE(st2.shared_blks_read,0) DESC ) t1
    WHERE LEAST(rn_reads1, rn_reads2) <= topn;

    r_result RECORD;
BEGIN
    -- Reads sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>I</th>'
            '<th title="Total number of shared blocks read by the statement">Reads</th>'
            '<th title="Shared blocks read by this statement as a percentage of all shared blocks read in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by reads
    FOR r_result IN c_sh_reads LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.shared_blks_read1,
            round(CAST(r_result.read_pct1 AS numeric),2),
            round(CAST(r_result.shared_hit_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.shared_blks_read2,
            round(CAST(r_result.read_pct2 AS numeric),2),
            round(CAST(r_result.shared_hit_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE OR REPLACE FUNCTION top_shared_dirtied_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared dirtied
    c_sh_dirt CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.shared_blks_dirtied,
        st.dirtied_pct,
        st.shared_hit_pct,
        st.calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.shared_blks_dirtied > 0
    ORDER BY st.shared_blks_dirtied DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Shared blocks dirtied by this statement as a percentage of all shared blocks dirtied in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by shared dirtied
    FOR r_result IN c_sh_dirt LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.shared_blks_dirtied,
            round(CAST(r_result.dirtied_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE OR REPLACE FUNCTION top_shared_dirtied_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared dirtied
    c_sh_dirt CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.shared_blks_dirtied as shared_blks_dirtied1,
        st1.dirtied_pct as dirtied_pct1,
        st1.shared_hit_pct as shared_hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.shared_blks_dirtied as shared_blks_dirtied2,
        st2.dirtied_pct as dirtied_pct2,
        st2.shared_hit_pct as shared_hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.shared_blks_dirtied DESC NULLS LAST) as rn_dirtied1,
        row_number() over (ORDER BY st2.shared_blks_dirtied DESC NULLS LAST) as rn_dirtied2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.shared_blks_dirtied,st2.shared_blks_dirtied) > 0
    ORDER BY COALESCE(st1.shared_blks_dirtied,0) + COALESCE(st2.shared_blks_dirtied,0) DESC ) t1
    WHERE LEAST(rn_dirtied1, rn_dirtied2) <= topn;

    r_result RECORD;
BEGIN
    -- Dirtied sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>I</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Shared blocks dirtied by this statement as a percentage of all shared blocks dirtied in a cluster">%Total</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by shared dirtied
    FOR r_result IN c_sh_dirt LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.shared_blks_dirtied1,
            round(CAST(r_result.dirtied_pct1 AS numeric),2),
            round(CAST(r_result.shared_hit_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.shared_blks_dirtied2,
            round(CAST(r_result.dirtied_pct2 AS numeric),2),
            round(CAST(r_result.shared_hit_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE OR REPLACE FUNCTION top_shared_written_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
  RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by shared written
    c_sh_wr CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.dbname,
        st.total_time,
        st.rows,
        st.shared_blks_written,
        st.tot_written_pct,
        st.backend_written_pct,
        st.shared_hit_pct,
        st.calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.shared_blks_written > 0
    ORDER BY st.shared_blks_written DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th title="Total number of shared blocks written by the statement">Written</th>'
            '<th title="Shared blocks written by this statement as a percentage of all shared blocks written in a cluster (sum of pg_stat_bgwriter fields buffers_checkpoint, buffers_clean and buffers_backend)">%Total</th>'
            '<th title="Shared blocks written by this statement as a percentage total buffers written directly by a backends (buffers_backend of pg_stat_bgwriter view)">%BackendW</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by shared written
    FOR r_result IN c_sh_wr LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.shared_blks_written,
            round(CAST(r_result.tot_written_pct AS numeric),2),
            round(CAST(r_result.backend_written_pct AS numeric),2),
            round(CAST(r_result.shared_hit_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE OR REPLACE FUNCTION top_shared_written_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by shared written
    c_sh_wr CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.shared_blks_written as shared_blks_written1,
        st1.tot_written_pct as tot_written_pct1,
        st1.backend_written_pct as backend_written_pct1,
        st1.shared_hit_pct as shared_hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.shared_blks_written as shared_blks_written2,
        st2.tot_written_pct as tot_written_pct2,
        st2.backend_written_pct as backend_written_pct2,
        st2.shared_hit_pct as shared_hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.shared_blks_written DESC NULLS LAST) as rn_written1,
        row_number() over (ORDER BY st2.shared_blks_written DESC NULLS LAST) as rn_written2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.shared_blks_written,st2.shared_blks_written) > 0
    ORDER BY COALESCE(st1.shared_blks_written,0) + COALESCE(st2.shared_blks_written,0) DESC ) t1
    WHERE LEAST(rn_written1, rn_written2) <= topn;

    r_result RECORD;
BEGIN
    -- Shared written sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>I</th>'
            '<th title="Total number of shared blocks written by the statement">Written</th>'
            '<th title="Shared blocks written by this statement as a percentage of all shared blocks written in a cluster (sum of pg_stat_bgwriter fields buffers_checkpoint, buffers_clean and buffers_backend)">%Total</th>'
            '<th title="Shared blocks written by this statement as a percentage total buffers written directly by a backends (buffers_backend field of pg_stat_bgwriter view)">%BackendW</th>'
            '<th title="Shared blocks hits as a percentage of shared blocks fetched (read + hit)">Hits(%)</th>'
            '<th title="Time spent by the statement">Elapsed(s)</th>'
            '<th title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by shared written
    FOR r_result IN c_sh_wr LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.shared_blks_written1,
            round(CAST(r_result.tot_written_pct1 AS numeric),2),
            round(CAST(r_result.backend_written_pct1 AS numeric),2),
            round(CAST(r_result.shared_hit_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.shared_blks_written2,
            round(CAST(r_result.tot_written_pct2 AS numeric),2),
            round(CAST(r_result.backend_written_pct2 AS numeric),2),
            round(CAST(r_result.shared_hit_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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

CREATE OR REPLACE FUNCTION top_wal_size_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for queries ordered by WAL bytes
    c_wal_size CURSOR FOR
    SELECT
        st.queryid_md5 as queryid,
        st.dbname,
        st.wal_bytes,
        st.wal_bytes_pct,
        st.shared_blks_dirtied,
        st.wal_fpi,
        st.wal_records
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.wal_bytes > 0
    ORDER BY st.wal_bytes DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- This report section is meaningful only when WAL stats is available
    IF NOT jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
      RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th title="Total amount of WAL bytes generated by the statement">WAL</th>'
            '<th title="WAL bytes of this statement as a percentage of total WAL bytes for all statements in a cluster">%Total</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Total number of WAL full page images generated by the statement">WAL FPI</th>'
            '<th title="Total number of WAL records generated by the statement">WAL records</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%1$s">%1$s</a></td>'
          '<td>%2$s</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
        '</tr>'
      );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_wal_size LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.dbname,
            pg_size_pretty(r_result.wal_bytes),
            round(CAST(r_result.wal_bytes_pct AS numeric),2),
            r_result.shared_blks_dirtied,
            r_result.wal_fpi,
            r_result.wal_records
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

CREATE OR REPLACE FUNCTION top_wal_size_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top queries ordered by WAL bytes
    c_wal_size CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.wal_bytes as wal_bytes1,
        st1.wal_bytes_pct as wal_bytes_pct1,
        st1.shared_blks_dirtied as shared_blks_dirtied1,
        st1.wal_fpi as wal_fpi1,
        st1.wal_records as wal_records1,
        st2.wal_bytes as wal_bytes2,
        st2.wal_bytes_pct as wal_bytes_pct2,
        st2.shared_blks_dirtied as shared_blks_dirtied2,
        st2.wal_fpi as wal_fpi2,
        st2.wal_records as wal_records2,
        row_number() over (ORDER BY st1.wal_bytes DESC NULLS LAST) as rn_wal1,
        row_number() over (ORDER BY st2.wal_bytes DESC NULLS LAST) as rn_wal2
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.wal_bytes,st2.wal_bytes) > 0
    ORDER BY COALESCE(st1.wal_bytes,0) + COALESCE(st2.wal_bytes,0) DESC ) t1
    WHERE rn_wal1 <= topn OR rn_wal2 <= topn;

    r_result RECORD;
BEGIN
    -- WAL sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>I</th>'
            '<th title="Total amount of WAL bytes generated by the statement">WAL</th>'
            '<th title="WAL bytes of this statement as a percentage of total WAL bytes for all statements in a cluster">%Total</th>'
            '<th title="Total number of shared blocks dirtied by the statement">Dirtied</th>'
            '<th title="Total number of WAL full page images generated by the statement">WAL FPI</th>'
            '<th title="Total number of WAL records generated by the statement">WAL records</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%1$s">%1$s</a></td>'
          '<td {rowtdspanhdr}>%2$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by shared_blks_fetched
    FOR r_result IN c_wal_size LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.dbname,
            pg_size_pretty(r_result.wal_bytes1),
            round(CAST(r_result.wal_bytes_pct1 AS numeric),2),
            r_result.shared_blks_dirtied1,
            r_result.wal_fpi1,
            r_result.wal_records1,
            pg_size_pretty(r_result.wal_bytes2),
            round(CAST(r_result.wal_bytes_pct2 AS numeric),2),
            r_result.shared_blks_dirtied2,
            r_result.wal_fpi2,
            r_result.wal_records2
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

CREATE OR REPLACE FUNCTION top_temp_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
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
        st.local_blks_fetched,
        st.local_hit_pct,
        st.temp_blks_written,
        st.temp_write_total_pct,
        st.temp_blks_read,
        st.temp_read_total_pct,
        st.local_blks_written,
        st.local_write_total_pct,
        st.local_blks_read,
        st.local_read_total_pct,
        st.calls
    FROM top_statements(sserver_id, start_id, end_id) st
    WHERE st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written > 0
    ORDER BY st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written DESC
    LIMIT topn;

    r_result RECORD;
BEGIN

    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2" title="Number of local blocks fetched (hit + read)">Local fetched</th>'
            '<th rowspan="2" title="Local blocks hit percentage">Hits(%)</th>'
            '<th colspan="4" title="Number of blocks, used for temporary tables">Local (blk)</th>'
            '<th colspan="4" title="Number of blocks, used in operations (like sorts and joins)">Temp (blk)</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of written local blocks">Write</th>'
            '<th title="Percentage of all local blocks written">%Total</th>'
            '<th title="Number of read local blocks">Read</th>'
            '<th title="Percentage of all local blocks read">%Total</th>'
            '<th title="Number of written temp blocks">Write</th>'
            '<th title="Percentage of all temp blocks written">%Total</th>'
            '<th title="Number of read temp blocks">Read</th>'
            '<th title="Percentage of all temp blocks read">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.local_blks_fetched,
            round(CAST(r_result.local_hit_pct AS numeric),2),
            r_result.local_blks_written,
            round(CAST(r_result.local_write_total_pct AS numeric),2),
            r_result.local_blks_read,
            round(CAST(r_result.local_read_total_pct AS numeric),2),
            r_result.temp_blks_written,
            round(CAST(r_result.temp_write_total_pct AS numeric),2),
            r_result.temp_blks_read,
            round(CAST(r_result.temp_read_total_pct AS numeric),2),
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
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

CREATE OR REPLACE FUNCTION top_temp_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
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
        st1.local_blks_fetched as local_blks_fetched1,
        st1.local_hit_pct as local_hit_pct1,
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
        st2.local_blks_fetched as local_blks_fetched2,
        st2.local_hit_pct as local_hit_pct2,
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
    FROM top_statements(sserver_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(st1.temp_blks_read + st1.temp_blks_written + st1.local_blks_read + st1.local_blks_written,
        st2.temp_blks_read + st2.temp_blks_written + st2.local_blks_read + st2.local_blks_written) > 0
    ORDER BY COALESCE(st1.temp_blks_read + st1.temp_blks_written + st1.local_blks_read + st1.local_blks_written,0) +
        COALESCE(st2.temp_blks_read + st2.temp_blks_written + st2.local_blks_read + st2.local_blks_written,0) DESC ) t1
    WHERE rn_temp1 <= topn OR rn_temp2 <= topn;

    r_result RECORD;
BEGIN
    -- Temp usage sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Query ID</th>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of local blocks fetched (hit + read)">Local fetched</th>'
            '<th rowspan="2" title="Local blocks hit percentage">Hits(%)</th>'
            '<th colspan="4" title="Number of blocks, used for temporary tables">Local (blk)</th>'
            '<th colspan="4" title="Number of blocks, used in operations (like sorts and joins)">Temp (blk)</th>'
            '<th rowspan="2" title="Time spent by the statement">Elapsed(s)</th>'
            '<th rowspan="2" title="Total number of rows retrieved or affected by the statement">Rows</th>'
            '<th rowspan="2" title="Number of times the statement was executed">Executions</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of written local blocks">Write</th>'
            '<th title="Percentage of all local blocks written">%Total</th>'
            '<th title="Number of read local blocks">Read</th>'
            '<th title="Percentage of all local blocks read">%Total</th>'
            '<th title="Number of written temp blocks">Write</th>'
            '<th title="Percentage of all temp blocks written">%Total</th>'
            '<th title="Number of read temp blocks">Read</th>'
            '<th title="Percentage of all temp blocks read">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.local_blks_fetched1,
            round(CAST(r_result.local_hit_pct1 AS numeric),2),
            r_result.local_blks_written1,
            round(CAST(r_result.local_write_total_pct1 AS numeric),2),
            r_result.local_blks_read1,
            round(CAST(r_result.local_read_total_pct1 AS numeric),2),
            r_result.temp_blks_written1,
            round(CAST(r_result.temp_write_total_pct1 AS numeric),2),
            r_result.temp_blks_read1,
            round(CAST(r_result.temp_read_total_pct1 AS numeric),2),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.calls1,
            r_result.local_blks_fetched2,
            round(CAST(r_result.local_hit_pct2 AS numeric),2),
            r_result.local_blks_written2,
            round(CAST(r_result.local_write_total_pct2 AS numeric),2),
            r_result.local_blks_read2,
            round(CAST(r_result.local_read_total_pct2 AS numeric),2),
            r_result.temp_blks_written2,
            round(CAST(r_result.temp_write_total_pct2 AS numeric),2),
            r_result.temp_blks_read2,
            round(CAST(r_result.temp_read_total_pct2 AS numeric),2),
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
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
    INSERT INTO queries_list(
      queryid_md5
    )
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
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>QueryID</th>'
            '<th>Query Text</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr id="%1$s">'
          '<td {mono}>%1$s</td>'
          '<td {mono}>%2$s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    FOR qr_result IN c_queries LOOP
        query_text := replace(qr_result.querytext,'<','&lt;');
        query_text := replace(query_text,'>','&gt;');
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
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
