/* ========= Check available statement stats for report ========= */

CREATE FUNCTION profile_checkavail_statstatements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there was available pg_stat_statements statistics for report interval
  SELECT count(sn.sample_id) = count(st.sample_id)
  FROM samples sn LEFT OUTER JOIN sample_statements_total st USING (server_id, sample_id)
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_planning_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(total_plan_time), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_stmt_io_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(blk_read_time), 0) + COALESCE(sum(blk_write_time), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_stmt_wal_bytes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have statement wal sizes collected for report interval
  SELECT COALESCE(sum(wal_bytes), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

/* ========= Statement stats functions ========= */

CREATE FUNCTION statements_stats(IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS TABLE(
        dbname              name,
        datid               oid,
        calls               bigint,
        plans               bigint,
        total_exec_time     double precision,
        total_plan_time     double precision,
        blk_read_time       double precision,
        blk_write_time      double precision,
        trg_fn_total_time   double precision,
        shared_gets         bigint,
        local_gets          bigint,
        shared_blks_dirtied bigint,
        local_blks_dirtied  bigint,
        temp_blks_read      bigint,
        temp_blks_written   bigint,
        local_blks_read     bigint,
        local_blks_written  bigint,
        statements          bigint,
        wal_bytes           bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        sample_db.datname AS dbname,
        sample_db.datid AS datid,
        sum(st.calls)::bigint AS calls,
        sum(st.plans)::bigint AS plans,
        sum(st.total_exec_time)/1000::double precision AS total_exec_time,
        sum(st.total_plan_time)/1000::double precision AS total_plan_time,
        sum(st.blk_read_time)/1000::double precision AS blk_read_time,
        sum(st.blk_write_time)/1000::double precision AS blk_write_time,
        (sum(trg.total_time)/1000)::double precision AS trg_fn_total_time,
        sum(st.shared_blks_hit)::bigint + sum(st.shared_blks_read)::bigint AS shared_gets,
        sum(st.local_blks_hit)::bigint + sum(st.local_blks_read)::bigint AS local_gets,
        sum(st.shared_blks_dirtied)::bigint AS shared_blks_dirtied,
        sum(st.local_blks_dirtied)::bigint AS local_blks_dirtied,
        sum(st.temp_blks_read)::bigint AS temp_blks_read,
        sum(st.temp_blks_written)::bigint AS temp_blks_written,
        sum(st.local_blks_read)::bigint AS local_blks_read,
        sum(st.local_blks_written)::bigint AS local_blks_written,
        sum(st.statements)::bigint AS statements,
        sum(st.wal_bytes)::bigint AS wal_bytes
    FROM sample_statements_total st
        LEFT OUTER JOIN sample_stat_user_func_total trg
          ON (st.server_id = trg.server_id AND st.sample_id = trg.sample_id AND st.datid = trg.datid AND trg.trg_fn)
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY sample_db.datname, sample_db.datid;
$$ LANGUAGE sql;

CREATE FUNCTION statements_stats_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        COALESCE(dbname,'Total') as dbname_t,
        NULLIF(sum(calls), 0) as calls,
        NULLIF(sum(total_exec_time), 0.0) as total_exec_time,
        NULLIF(sum(total_plan_time), 0.0) as total_plan_time,
        NULLIF(sum(blk_read_time), 0.0) as blk_read_time,
        NULLIF(sum(blk_write_time), 0.0) as blk_write_time,
        NULLIF(sum(trg_fn_total_time), 0.0) as trg_fn_total_time,
        NULLIF(sum(shared_gets), 0) as shared_gets,
        NULLIF(sum(local_gets), 0) as local_gets,
        NULLIF(sum(shared_blks_dirtied), 0) as shared_blks_dirtied,
        NULLIF(sum(local_blks_dirtied), 0) as local_blks_dirtied,
        NULLIF(sum(temp_blks_read), 0) as temp_blks_read,
        NULLIF(sum(temp_blks_written), 0) as temp_blks_written,
        NULLIF(sum(local_blks_read), 0) as local_blks_read,
        NULLIF(sum(local_blks_written), 0) as local_blks_written,
        NULLIF(sum(statements), 0) as statements,
        NULLIF(sum(wal_bytes), 0) as wal_bytes
    FROM statements_stats(sserver_id,start_id,end_id,topn)
    GROUP BY ROLLUP(dbname)
    ORDER BY dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2"title="Number of query executions">Calls</th>'
            '{planning_times?time_hdr}'
            '<th colspan="2" title="Number of blocks fetched (hit + read)">Fetched (blk)</th>'
            '<th colspan="2" title="Number of blocks dirtied">Dirtied (blk)</th>'
            '<th colspan="2" title="Number of blocks, used in operations (like sorts and joins)">Temp (blk)</th>'
            '<th colspan="2" title="Number of blocks, used for temporary tables">Local (blk)</th>'
            '<th rowspan="2">Statements</th>'
            '{statement_wal_bytes?wal_bytes_hdr}'
          '</tr>'
          '<tr>'
            '{planning_times?plan_time_hdr}'
            '<th title="Time spent executing queries">Exec</th>'
            '<th title="Time spent reading blocks">Read</th>'   -- I/O time
            '<th title="Time spent writing blocks">Write</th>'
            '<th title="Time spent in trigger functions">Trg</th>'    -- Trigger functions time
            '<th>Shared</th>' -- Fetched
            '<th>Local</th>'
            '<th>Shared</th>' -- Dirtied
            '<th>Local</th>'
            '<th>Read</th>'   -- Work area read blks
            '<th>Write</th>'  -- Work area write blks
            '<th>Read</th>'   -- Local read blks
            '<th>Write</th>'  -- Local write blks
          '</tr>'
          '{rows}'
        '</table>',
      'stdb_tpl',
        '<tr>'
          '<td>%1$s</td>'
          '<td {value}>%2$s</td>'
          '{planning_times?plan_time_cell}'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '{statement_wal_bytes?wal_bytes_cell}'
        '</tr>',
      '!planning_times?time_hdr', -- Time header for stat_statements less then v1.8
        '<th colspan="4">Time (s)</th>',
      'planning_times?time_hdr', -- Time header for stat_statements v1.8 - added plan time field
        '<th colspan="5">Time (s)</th>',
      'planning_times?plan_time_hdr',
        '<th title="Time spent planning queries">Plan</th>',
      'planning_times?plan_time_cell',
        '<td {value}>%3$s</td>',
      'statement_wal_bytes?wal_bytes_hdr',
        '<th rowspan="2">WAL size</th>',
      'statement_wal_bytes?wal_bytes_cell',
        '<td {value}>%17$s</td>'
    );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stdb_tpl'],
            r_result.dbname_t,
            r_result.calls,
            round(CAST(r_result.total_plan_time AS numeric),2),
            round(CAST(r_result.total_exec_time AS numeric),2),
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2),
            round(CAST(r_result.trg_fn_total_time AS numeric),2),
            r_result.shared_gets,
            r_result.local_gets,
            r_result.shared_blks_dirtied,
            r_result.local_blks_dirtied,
            r_result.temp_blks_read,
            r_result.temp_blks_written,
            r_result.local_blks_read,
            r_result.local_blks_written,
            r_result.statements,
            pg_size_pretty(r_result.wal_bytes)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION statements_stats_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        COALESCE(COALESCE(st1.dbname,st2.dbname),'Total') as dbname,
        NULLIF(sum(st1.calls), 0) as calls1,
        NULLIF(sum(st1.total_exec_time), 0.0) as total_exec_time1,
        NULLIF(sum(st1.total_plan_time), 0.0) as total_plan_time1,
        NULLIF(sum(st1.blk_read_time), 0.0) as blk_read_time1,
        NULLIF(sum(st1.blk_write_time), 0.0) as blk_write_time1,
        NULLIF(sum(st1.trg_fn_total_time), 0.0) as trg_fn_total_time1,
        NULLIF(sum(st1.shared_gets), 0) as shared_gets1,
        NULLIF(sum(st1.local_gets), 0) as local_gets1,
        NULLIF(sum(st1.shared_blks_dirtied), 0) as shared_blks_dirtied1,
        NULLIF(sum(st1.local_blks_dirtied), 0) as local_blks_dirtied1,
        NULLIF(sum(st1.temp_blks_read), 0) as temp_blks_read1,
        NULLIF(sum(st1.temp_blks_written), 0) as temp_blks_written1,
        NULLIF(sum(st1.local_blks_read), 0) as local_blks_read1,
        NULLIF(sum(st1.local_blks_written), 0) as local_blks_written1,
        NULLIF(sum(st1.statements), 0) as statements1,
        NULLIF(sum(st1.wal_bytes), 0) as wal_bytes1,
        NULLIF(sum(st2.calls), 0) as calls2,
        NULLIF(sum(st2.total_exec_time), 0.0) as total_exec_time2,
        NULLIF(sum(st2.total_plan_time), 0.0) as total_plan_time2,
        NULLIF(sum(st2.blk_read_time), 0.0) as blk_read_time2,
        NULLIF(sum(st2.blk_write_time), 0.0) as blk_write_time2,
        NULLIF(sum(st2.trg_fn_total_time), 0.0) as trg_fn_total_time2,
        NULLIF(sum(st2.shared_gets), 0) as shared_gets2,
        NULLIF(sum(st2.local_gets), 0) as local_gets2,
        NULLIF(sum(st2.shared_blks_dirtied), 0) as shared_blks_dirtied2,
        NULLIF(sum(st2.local_blks_dirtied), 0) as local_blks_dirtied2,
        NULLIF(sum(st2.temp_blks_read), 0) as temp_blks_read2,
        NULLIF(sum(st2.temp_blks_written), 0) as temp_blks_written2,
        NULLIF(sum(st2.local_blks_read), 0) as local_blks_read2,
        NULLIF(sum(st2.local_blks_written), 0) as local_blks_written2,
        NULLIF(sum(st2.statements), 0) as statements2,
        NULLIF(sum(st2.wal_bytes), 0) as wal_bytes2
    FROM statements_stats(sserver_id,start1_id,end1_id,topn) st1
        FULL OUTER JOIN statements_stats(sserver_id,start2_id,end2_id,topn) st2 USING (datid)
    GROUP BY ROLLUP(COALESCE(st1.dbname,st2.dbname))
    ORDER BY COALESCE(st1.dbname,st2.dbname) NULLS LAST;

    r_result RECORD;
BEGIN
    -- Statements stats per database TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of query executions">Calls</th>'
            '{planning_times?time_hdr}'
            '<th colspan="2" title="Number of blocks fetched (hit + read)">Fetched (blk)</th>'
            '<th colspan="2" title="Number of blocks dirtied">Dirtied (blk)</th>'
            '<th colspan="2" title="Number of blocks, used in operations (like sorts and joins)">Temp (blk)</th>'
            '<th colspan="2" title="Number of blocks, used for temporary tables">Local (blk)</th>'
            '<th rowspan="2">Statements</th>'
            '{statement_wal_bytes?wal_bytes_hdr}'
          '</tr>'
          '<tr>'
            '{planning_times?plan_time_hdr}'
            '<th title="Time spent executing queries">Exec</th>'
            '<th title="Time spent reading blocks">Read</th>'   -- I/O time
            '<th title="Time spent writing blocks">Write</th>'
            '<th title="Time spent in trigger functions">Trg</th>'    -- Trigger functions time
            '<th>Shared</th>' -- Fetched (blk)
            '<th>Local</th>'
            '<th>Shared</th>' -- Dirtied (blk)
            '<th>Local</th>'
            '<th>Read</th>'   -- Work area  blocks
            '<th>Write</th>'
            '<th>Read</th>'   -- Local blocks
            '<th>Write</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stdb_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%1$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%2$s</td>'
          '{planning_times?plan_time_cell1}'
          '<td {value}>%4$s</td>'
          '<td {value}>%5$s</td>'
          '<td {value}>%6$s</td>'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '{statement_wal_bytes?wal_bytes_cell1}'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%18$s</td>'
          '{planning_times?plan_time_cell2}'
          '<td {value}>%20$s</td>'
          '<td {value}>%21$s</td>'
          '<td {value}>%22$s</td>'
          '<td {value}>%23$s</td>'
          '<td {value}>%24$s</td>'
          '<td {value}>%25$s</td>'
          '<td {value}>%26$s</td>'
          '<td {value}>%27$s</td>'
          '<td {value}>%28$s</td>'
          '<td {value}>%29$s</td>'
          '<td {value}>%30$s</td>'
          '<td {value}>%31$s</td>'
          '<td {value}>%32$s</td>'
          '{statement_wal_bytes?wal_bytes_cell2}'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      '!planning_times?time_hdr', -- Time header for stat_statements less then v1.8
        '<th colspan="4">Time (s)</th>',
      'planning_times?time_hdr', -- Time header for stat_statements v1.8 - added plan time field
        '<th colspan="5">Time (s)</th>',
      'planning_times?plan_time_hdr',
        '<th title="Time spent planning queries">Plan</th>',
      'planning_times?plan_time_cell1',
        '<td {value}>%3$s</td>',
      'planning_times?plan_time_cell2',
        '<td {value}>%19$s</td>',
      'statement_wal_bytes?wal_bytes_hdr',
        '<th rowspan="2">WAL size</th>',
      'statement_wal_bytes?wal_bytes_cell1',
        '<td {value}>%17$s</td>',
      'statement_wal_bytes?wal_bytes_cell2',
        '<td {value}>%33$s</td>'
    );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stdb_tpl'],
            r_result.dbname,
            r_result.calls1,
            round(CAST(r_result.total_plan_time1 AS numeric),2),
            round(CAST(r_result.total_exec_time1 AS numeric),2),
            round(CAST(r_result.blk_read_time1 AS numeric),2),
            round(CAST(r_result.blk_write_time1 AS numeric),2),
            round(CAST(r_result.trg_fn_total_time1 AS numeric),2),
            r_result.shared_gets1,
            r_result.local_gets1,
            r_result.shared_blks_dirtied1,
            r_result.local_blks_dirtied1,
            r_result.temp_blks_read1,
            r_result.temp_blks_written1,
            r_result.local_blks_read1,
            r_result.local_blks_written1,
            r_result.statements1,
            pg_size_pretty(r_result.wal_bytes1),
            r_result.calls2,
            round(CAST(r_result.total_plan_time2 AS numeric),2),
            round(CAST(r_result.total_exec_time2 AS numeric),2),
            round(CAST(r_result.blk_read_time2 AS numeric),2),
            round(CAST(r_result.blk_write_time2 AS numeric),2),
            round(CAST(r_result.trg_fn_total_time2 AS numeric),2),
            r_result.shared_gets2,
            r_result.local_gets2,
            r_result.shared_blks_dirtied2,
            r_result.local_blks_dirtied2,
            r_result.temp_blks_read2,
            r_result.temp_blks_written2,
            r_result.local_blks_read2,
            r_result.local_blks_written2,
            r_result.statements2,
            pg_size_pretty(r_result.wal_bytes2)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;
