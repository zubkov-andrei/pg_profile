/* ========= Reporting functions ========= */

/* ========= Cluster databases report functions ========= */
CREATE FUNCTION profile_checkavail_io_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have I/O times collected for report interval
  SELECT COALESCE(sum(blk_read_time), 0) + COALESCE(sum(blk_write_time), 0) > 0
  FROM sample_stat_database sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION dbstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id         integer,
    datid             oid,
    dbname            name,
    xact_commit       bigint,
    xact_rollback     bigint,
    blks_read         bigint,
    blks_hit          bigint,
    tup_returned      bigint,
    tup_fetched       bigint,
    tup_inserted      bigint,
    tup_updated       bigint,
    tup_deleted       bigint,
    temp_files        bigint,
    temp_bytes        bigint,
    datsize_delta     bigint,
    deadlocks         bigint,
    checksum_failures bigint,
    checksum_last_failure  timestamp with time zone,
    blk_read_time     double precision,
    blk_write_time    double precision
  )
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.datid AS datid,
        st.datname AS dbname,
        sum(xact_commit)::bigint AS xact_commit,
        sum(xact_rollback)::bigint AS xact_rollback,
        sum(blks_read)::bigint AS blks_read,
        sum(blks_hit)::bigint AS blks_hit,
        sum(tup_returned)::bigint AS tup_returned,
        sum(tup_fetched)::bigint AS tup_fetched,
        sum(tup_inserted)::bigint AS tup_inserted,
        sum(tup_updated)::bigint AS tup_updated,
        sum(tup_deleted)::bigint AS tup_deleted,
        sum(temp_files)::bigint AS temp_files,
        sum(temp_bytes)::bigint AS temp_bytes,
        sum(datsize_delta)::bigint AS datsize_delta,
        sum(deadlocks)::bigint AS deadlocks,
        sum(checksum_failures)::bigint AS checksum_failures,
        max(checksum_last_failure)::timestamp with time zone AS checksum_last_failure,
        sum(blk_read_time)/1000::double precision AS blk_read_time,
        sum(blk_write_time)/1000::double precision AS blk_write_time
    FROM sample_stat_database st
    WHERE st.server_id = sserver_id AND NOT datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.datid, st.datname
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_sessionstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there is table sizes collected in both bounds
  SELECT
    count(session_time) +
    count(active_time) +
    count(idle_in_transaction_time) +
    count(sessions) +
    count(sessions_abandoned) +
    count(sessions_fatal) +
    count(sessions_killed) > 0
  FROM sample_stat_database
  WHERE
    server_id = sserver_id
    AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_sessions(IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS TABLE(
    server_id         integer,
    datid             oid,
    dbname            name,
    session_time      double precision,
    active_time       double precision,
    idle_in_transaction_time  double precision,
    sessions            bigint,
    sessions_abandoned  bigint,
    sessions_fatal    bigint,
    sessions_killed   bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.datid AS datid,
        st.datname AS dbname,
        sum(session_time)::double precision AS xact_commit,
        sum(active_time)::double precision AS xact_rollback,
        sum(idle_in_transaction_time)::double precision AS blks_read,
        sum(sessions)::bigint AS blks_hit,
        sum(sessions_abandoned)::bigint AS tup_returned,
        sum(sessions_fatal)::bigint AS tup_fetched,
        sum(sessions_killed)::bigint AS tup_inserted
    FROM sample_stat_database st
    WHERE st.server_id = sserver_id AND NOT datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.datid, st.datname
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  datname       name,
  stats_reset   timestamp with time zone,
  sample_id       integer
)
SET search_path=@extschema@ AS $$
    SELECT
        st1.datname,
        st1.stats_reset,
        st1.sample_id
    FROM sample_stat_database st1
        LEFT JOIN sample_stat_database st0 ON
          (st0.server_id = st1.server_id AND st0.sample_id = st1.sample_id - 1 AND st0.datid = st1.datid)
    WHERE st1.server_id = sserver_id AND NOT st1.datistemplate AND st1.sample_id BETWEEN start_id + 1 AND end_id
      AND nullif(st1.stats_reset,st0.stats_reset) IS NOT NULL
    ORDER BY sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer) FOR
    SELECT
        datname,
        sample_id,
        stats_reset
    FROM dbstats_reset(sserver_id, start1_id, end1_id)
    ORDER BY stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Database</th>'
            '<th>Sample</th>'
            '<th>Reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl',
      '<tr>'
        '<td>%s</td>'
        '<td {value}>%s</td>'
        '<td {value}>%s</td>'
      '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
      (report_context #>> '{report_properties,start1_id}')::integer,
      (report_context #>> '{report_properties,end1_id}')::integer)
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['sample_tpl'],
            r_result.datname,
            r_result.sample_id,
            r_result.stats_reset
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_reset_diff_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer, start2_id integer, end2_id integer) FOR
    SELECT
        interval_num,
        datname,
        sample_id,
        stats_reset
    FROM
      (SELECT 1 AS interval_num, datname, sample_id, stats_reset
        FROM dbstats_reset(sserver_id, start1_id, end1_id)
      UNION ALL
      SELECT 2 AS interval_num, datname, sample_id, stats_reset
        FROM dbstats_reset(sserver_id, start2_id, end2_id)) AS samples
    ORDER BY interval_num, stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>I</th>'
            '<th>Database</th>'
            '<th>Sample</th>'
            '<th>Reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl1',
        '<tr {interval1}>'
          '<td {label} {title1}>1</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'sample_tpl2',
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
      CASE r_result.interval_num
        WHEN 1 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl1'],
              r_result.datname,
              r_result.sample_id,
              r_result.stats_reset
          );
        WHEN 2 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl2'],
              r_result.datname,
              r_result.sample_id,
              r_result.stats_reset
          );
        END CASE;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer) FOR
    SELECT
        COALESCE(st.dbname,'Total') as dbname,
        NULLIF(sum(st.xact_commit), 0) as xact_commit,
        NULLIF(sum(st.xact_rollback), 0) as xact_rollback,
        NULLIF(sum(st.blks_read), 0) as blks_read,
        NULLIF(sum(st.blks_hit), 0) as blks_hit,
        NULLIF(sum(st.tup_returned), 0) as tup_returned,
        NULLIF(sum(st.tup_fetched), 0) as tup_fetched,
        NULLIF(sum(st.tup_inserted), 0) as tup_inserted,
        NULLIF(sum(st.tup_updated), 0) as tup_updated,
        NULLIF(sum(st.tup_deleted), 0) as tup_deleted,
        NULLIF(sum(st.temp_files), 0) as temp_files,
        pg_size_pretty(NULLIF(sum(st.temp_bytes), 0)) AS temp_bytes,
        pg_size_pretty(NULLIF(sum(st_last.datsize), 0)) AS datsize,
        pg_size_pretty(NULLIF(sum(st.datsize_delta), 0)) AS datsize_delta,
        NULLIF(sum(st.deadlocks), 0) as deadlocks,
        (sum(st.blks_hit)*100/NULLIF(sum(st.blks_hit)+sum(st.blks_read),0))::double precision AS blks_hit_pct,
        NULLIF(sum(st.checksum_failures), 0) as checksum_failures,
        max(st.checksum_last_failure) as checksum_last_failure,
        NULLIF(sum(st.blk_read_time), 0) as blk_read_time,
        NULLIF(sum(st.blk_write_time), 0) as blk_write_time
    FROM dbstats(sserver_id, start1_id, end1_id) st
      LEFT OUTER JOIN sample_stat_database st_last ON
        (st_last.server_id = st.server_id AND st_last.datid = st.datid
          AND st_last.sample_id = end1_id)
    GROUP BY ROLLUP(st.dbname)
    ORDER BY st.dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th colspan="3">Transactions</th>'
            '{checksum_fail_detected?checksum_fail_hdr1}'
            '<th colspan="3">Block statistics</th>'
            '{io_times?io_times_hdr1}'
            '<th colspan="5">Tuples</th>'
            '<th colspan="2">Temp files</th>'
            '<th rowspan="2" title="Database size as is was at the moment of last sample in report interval">Size</th>'
            '<th rowspan="2" title="Database size increment during report interval">Growth</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of transactions in this database that have been committed">Commits</th>'
            '<th title="Number of transactions in this database that have been rolled back">Rollbacks</th>'
            '<th title="Number of deadlocks detected in this database">Deadlocks</th>'
            '{checksum_fail_detected?checksum_fail_hdr2}'
            '<th title="Buffer cache hit ratio">Hit(%)</th>'
            '<th title="Number of disk blocks read in this database">Read</th>'
            '<th title="Number of times disk blocks were found already in the buffer cache">Hit</th>'
            '{io_times?io_times_hdr2}'
            '<th title="Number of rows returned by queries in this database">Ret</th>'
            '<th title="Number of rows fetched by queries in this database">Fet</th>'
            '<th title="Number of rows inserted by queries in this database">Ins</th>'
            '<th title="Number of rows updated by queries in this database">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Total amount of data written to temporary files by queries in this database">Size</th>'
            '<th title="Number of temporary files created by queries in this database">Files</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr>'
          '<td>%1$s</td>'
          '<td {value}>%2$s</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '{checksum_fail_detected?checksum_fail_row}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '{io_times?io_times_row}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
        '</tr>',
      'checksum_fail_detected?checksum_fail_hdr1',
        '<th colspan="2">Checksums</th>',
      'checksum_fail_detected?checksum_fail_hdr2',
        '<th title="Number of block checksum failures detected">Failures</th>'
        '<th title="Last checksum filure detected">Last</th>',
      'checksum_fail_detected?checksum_fail_row',
        '<td {value}><strong>%5$s</strong></td>'
        '<td {value}><strong>%6$s</strong></td>',
      'io_times?io_times_hdr1',
        '<th colspan="2">Block I/O times</th>',
      'io_times?io_times_hdr2',
        '<th title="Time spent reading data file blocks by backends, in seconds">Read</th>'
        '<th title="Time spent writing data file blocks by backends, in seconds">Write</th>',
      'io_times?io_times_row',
        '<td {value}>%19$s</td>'
        '<td {value}>%20$s</td>'
      );
          -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
      (report_context #>> '{report_properties,start1_id}')::integer,
      (report_context #>> '{report_properties,end1_id}')::integer
    )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            r_result.xact_commit,
            r_result.xact_rollback,
            r_result.deadlocks,
            r_result.checksum_failures,
            r_result.checksum_last_failure::text,
            round(CAST(r_result.blks_hit_pct AS numeric),2),
            r_result.blks_read,
            r_result.blks_hit,
            r_result.tup_returned,
            r_result.tup_fetched,
            r_result.tup_inserted,
            r_result.tup_updated,
            r_result.tup_deleted,
            r_result.temp_bytes,
            r_result.temp_files,
            r_result.datsize,
            r_result.datsize_delta,
            round(CAST(r_result.blk_read_time AS numeric),2),
            round(CAST(r_result.blk_write_time AS numeric),2)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer)
    FOR
    SELECT
        COALESCE(COALESCE(dbs1.dbname,dbs2.dbname),'Total') AS dbname,
        NULLIF(sum(dbs1.xact_commit), 0) AS xact_commit1,
        NULLIF(sum(dbs1.xact_rollback), 0) AS xact_rollback1,
        NULLIF(sum(dbs1.blks_read), 0) AS blks_read1,
        NULLIF(sum(dbs1.blks_hit), 0) AS blks_hit1,
        NULLIF(sum(dbs1.tup_returned), 0) AS tup_returned1,
        NULLIF(sum(dbs1.tup_fetched), 0) AS tup_fetched1,
        NULLIF(sum(dbs1.tup_inserted), 0) AS tup_inserted1,
        NULLIF(sum(dbs1.tup_updated), 0) AS tup_updated1,
        NULLIF(sum(dbs1.tup_deleted), 0) AS tup_deleted1,
        NULLIF(sum(dbs1.temp_files), 0) AS temp_files1,
        pg_size_pretty(NULLIF(sum(dbs1.temp_bytes), 0)) AS temp_bytes1,
        pg_size_pretty(NULLIF(sum(st_last1.datsize), 0)) AS datsize1,
        pg_size_pretty(NULLIF(sum(dbs1.datsize_delta), 0)) AS datsize_delta1,
        NULLIF(sum(dbs1.deadlocks), 0) AS deadlocks1,
        (sum(dbs1.blks_hit)*100/NULLIF(sum(dbs1.blks_hit)+sum(dbs1.blks_read),0))::double precision AS blks_hit_pct1,
        NULLIF(sum(dbs1.checksum_failures), 0) as checksum_failures1,
        max(dbs1.checksum_last_failure) as checksum_last_failure1,
        NULLIF(sum(dbs1.blk_read_time), 0) as blk_read_time1,
        NULLIF(sum(dbs1.blk_write_time), 0) as blk_write_time1,
        NULLIF(sum(dbs2.xact_commit), 0) AS xact_commit2,
        NULLIF(sum(dbs2.xact_rollback), 0) AS xact_rollback2,
        NULLIF(sum(dbs2.blks_read), 0) AS blks_read2,
        NULLIF(sum(dbs2.blks_hit), 0) AS blks_hit2,
        NULLIF(sum(dbs2.tup_returned), 0) AS tup_returned2,
        NULLIF(sum(dbs2.tup_fetched), 0) AS tup_fetched2,
        NULLIF(sum(dbs2.tup_inserted), 0) AS tup_inserted2,
        NULLIF(sum(dbs2.tup_updated), 0) AS tup_updated2,
        NULLIF(sum(dbs2.tup_deleted), 0) AS tup_deleted2,
        NULLIF(sum(dbs2.temp_files), 0) AS temp_files2,
        pg_size_pretty(NULLIF(sum(dbs2.temp_bytes), 0)) AS temp_bytes2,
        pg_size_pretty(NULLIF(sum(st_last2.datsize), 0)) AS datsize2,
        pg_size_pretty(NULLIF(sum(dbs2.datsize_delta), 0)) AS datsize_delta2,
        NULLIF(sum(dbs2.deadlocks), 0) AS deadlocks2,
        (sum(dbs2.blks_hit)*100/NULLIF(sum(dbs2.blks_hit)+sum(dbs2.blks_read),0))::double precision AS blks_hit_pct2,
        NULLIF(sum(dbs2.checksum_failures), 0) as checksum_failures2,
        max(dbs2.checksum_last_failure) as checksum_last_failure2,
        NULLIF(sum(dbs2.blk_read_time), 0) as blk_read_time2,
        NULLIF(sum(dbs2.blk_write_time), 0) as blk_write_time2
    FROM dbstats(sserver_id,start1_id,end1_id) dbs1
      FULL OUTER JOIN dbstats(sserver_id,start2_id,end2_id) dbs2
        USING (server_id, datid)
      LEFT OUTER JOIN sample_stat_database st_last1 ON
        (st_last1.server_id = dbs1.server_id AND st_last1.datid = dbs1.datid AND st_last1.sample_id =
        end1_id)
      LEFT OUTER JOIN sample_stat_database st_last2 ON
        (st_last2.server_id = dbs2.server_id AND st_last2.datid = dbs2.datid AND st_last2.sample_id =
        end2_id)
    GROUP BY ROLLUP(COALESCE(dbs1.dbname,dbs2.dbname))
    ORDER BY COALESCE(dbs1.dbname,dbs2.dbname) NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="3">Transactions</th>'
            '{checksum_fail_detected?checksum_fail_hdr1}'
            '<th colspan="3">Block statistics</th>'
            '{io_times?io_times_hdr1}'
            '<th colspan="5">Tuples</th>'
            '<th colspan="2">Temp files</th>'
            '<th rowspan="2" title="Database size as is was at the moment of last sample in report interval">Size</th>'
            '<th rowspan="2" title="Database size increment during report interval">Growth</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of transactions in this database that have been committed">Commits</th>'
            '<th title="Number of transactions in this database that have been rolled back">Rollbacks</th>'
            '<th title="Number of deadlocks detected in this database">Deadlocks</th>'
            '{checksum_fail_detected?checksum_fail_hdr2}'
            '<th title="Buffer cache hit ratio">Hit(%)</th>'
            '<th title="Number of disk blocks read in this database">Read</th>'
            '<th title="Number of times disk blocks were found already in the buffer cache">Hit</th>'
            '{io_times?io_times_hdr2}'
            '<th title="Number of rows returned by queries in this database">Ret</th>'
            '<th title="Number of rows fetched by queries in this database">Fet</th>'
            '<th title="Number of rows inserted by queries in this database">Ins</th>'
            '<th title="Number of rows updated by queries in this database">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Total amount of data written to temporary files by queries in this database">Size</th>'
            '<th title="Number of temporary files created by queries in this database">Files</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%1$s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%2$s</td>'
          '<td {value}>%3$s</td>'
          '<td {value}>%4$s</td>'
          '{checksum_fail_detected?checksum_fail_row1}'
          '<td {value}>%7$s</td>'
          '<td {value}>%8$s</td>'
          '<td {value}>%9$s</td>'
          '{io_times?io_times_row1}'
          '<td {value}>%10$s</td>'
          '<td {value}>%11$s</td>'
          '<td {value}>%12$s</td>'
          '<td {value}>%13$s</td>'
          '<td {value}>%14$s</td>'
          '<td {value}>%15$s</td>'
          '<td {value}>%16$s</td>'
          '<td {value}>%17$s</td>'
          '<td {value}>%18$s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%19$s</td>'
          '<td {value}>%20$s</td>'
          '<td {value}>%21$s</td>'
          '{checksum_fail_detected?checksum_fail_row2}'
          '<td {value}>%24$s</td>'
          '<td {value}>%25$s</td>'
          '<td {value}>%26$s</td>'
          '{io_times?io_times_row2}'
          '<td {value}>%27$s</td>'
          '<td {value}>%28$s</td>'
          '<td {value}>%29$s</td>'
          '<td {value}>%30$s</td>'
          '<td {value}>%31$s</td>'
          '<td {value}>%32$s</td>'
          '<td {value}>%33$s</td>'
          '<td {value}>%34$s</td>'
          '<td {value}>%35$s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'checksum_fail_detected?checksum_fail_hdr1',
        '<th colspan="2">Checksums</th>',
      'checksum_fail_detected?checksum_fail_hdr2',
        '<th title="Number of block checksum failures detected">Failures</th>'
        '<th title="Last checksum filure detected">Last</th>',
      'checksum_fail_detected?checksum_fail_row1',
        '<td {value}><strong>%5$s</strong></td>'
        '<td {value}><strong>%6$s</strong></td>',
      'checksum_fail_detected?checksum_fail_row2',
        '<td {value}><strong>%22$s</strong></td>'
        '<td {value}><strong>%23$s</strong></td>',
      'io_times?io_times_hdr1',
        '<th colspan="2">Block I/O times</th>',
      'io_times?io_times_hdr2',
        '<th title="Time spent reading data file blocks by backends, in seconds">Read</th>'
        '<th title="Time spent writing data file blocks by backends, in seconds">Write</th>',
      'io_times?io_times_row1',
        '<td {value}>%36$s</td>'
        '<td {value}>%37$s</td>',
      'io_times?io_times_row2',
        '<td {value}>%38$s</td>'
        '<td {value}>%39$s</td>'
    );
    -- apply settings to templates

    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            r_result.xact_commit1,
            r_result.xact_rollback1,
            r_result.deadlocks1,
            r_result.checksum_failures1,
            r_result.checksum_last_failure1,
            round(CAST(r_result.blks_hit_pct1 AS numeric),2),
            r_result.blks_read1,
            r_result.blks_hit1,
            r_result.tup_returned1,
            r_result.tup_fetched1,
            r_result.tup_inserted1,
            r_result.tup_updated1,
            r_result.tup_deleted1,
            r_result.temp_bytes1,
            r_result.temp_files1,
            r_result.datsize1,
            r_result.datsize_delta1,
            r_result.xact_commit2,
            r_result.xact_rollback2,
            r_result.deadlocks2,
            r_result.checksum_failures2,
            r_result.checksum_last_failure2,
            round(CAST(r_result.blks_hit_pct2 AS numeric),2),
            r_result.blks_read2,
            r_result.blks_hit2,
            r_result.tup_returned2,
            r_result.tup_fetched2,
            r_result.tup_inserted2,
            r_result.tup_updated2,
            r_result.tup_deleted2,
            r_result.temp_bytes2,
            r_result.temp_files2,
            r_result.datsize2,
            r_result.datsize_delta2,
            round(CAST(r_result.blk_read_time1 AS numeric),2),
            round(CAST(r_result.blk_write_time1 AS numeric),2),
            round(CAST(r_result.blk_read_time2 AS numeric),2),
            round(CAST(r_result.blk_write_time2 AS numeric),2)
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_sessions_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer, topn integer) FOR
    SELECT
        COALESCE(st.dbname,'Total') as dbname,
        NULLIF(sum(st.session_time), 0) as session_time,
        NULLIF(sum(st.active_time), 0) as active_time,
        NULLIF(sum(st.idle_in_transaction_time), 0) as idle_in_transaction_time,
        NULLIF(sum(st.sessions), 0) as sessions,
        NULLIF(sum(st.sessions_abandoned), 0) as sessions_abandoned,
        NULLIF(sum(st.sessions_fatal), 0) as sessions_fatal,
        NULLIF(sum(st.sessions_killed), 0) as sessions_killed
    FROM dbstats_sessions(sserver_id,start1_id,end1_id,topn) st
      LEFT OUTER JOIN sample_stat_database st_last ON
        (st_last.server_id = st.server_id AND st_last.datid = st.datid AND st_last.sample_id =
        end1_id)
    GROUP BY ROLLUP(st.dbname)
    ORDER BY st.dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th colspan="3" title="Session timings for databases">Timings (s)</th>'
            '<th colspan="4" title="Session counts for databases">Sessions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by database sessions in this database (note that statistics are only updated when the state of a session changes, so if sessions have been idle for a long time, this idle time won''t be included)">Total</th>'
            '<th title="Time spent executing SQL statements in this database (this corresponds to the states active and fastpath function call in pg_stat_activity)">Active</th>'
            '<th title="Time spent idling while in a transaction in this database (this corresponds to the states idle in transaction and idle in transaction (aborted) in pg_stat_activity)">Idle(T)</th>'
            '<th title="Total number of sessions established to this database">Established</th>'
            '<th title="Number of database sessions to this database that were terminated because connection to the client was lost">Abondoned</th>'
            '<th title="Number of database sessions to this database that were terminated by fatal errors">Fatal</th>'
            '<th title="Number of database sessions to this database that were terminated by operator intervention">Killed</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr>'
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
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            round(CAST(r_result.session_time / 1000 AS numeric),2),
            round(CAST(r_result.active_time / 1000 AS numeric),2),
            round(CAST(r_result.idle_in_transaction_time / 1000 AS numeric),2),
            r_result.sessions,
            r_result.sessions_abandoned,
            r_result.sessions_fatal,
            r_result.sessions_killed
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION dbstats_sessions_diff_htbl(IN report_context jsonb, IN sserver_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer)
    FOR
    SELECT
        COALESCE(COALESCE(dbs1.dbname,dbs2.dbname),'Total') AS dbname,
        NULLIF(sum(dbs1.session_time), 0) as session_time1,
        NULLIF(sum(dbs1.active_time), 0) as active_time1,
        NULLIF(sum(dbs1.idle_in_transaction_time), 0) as idle_in_transaction_time1,
        NULLIF(sum(dbs1.sessions), 0) as sessions1,
        NULLIF(sum(dbs1.sessions_abandoned), 0) as sessions_abandoned1,
        NULLIF(sum(dbs1.sessions_fatal), 0) as sessions_fatal1,
        NULLIF(sum(dbs1.sessions_killed), 0) as sessions_killed1,
        NULLIF(sum(dbs2.session_time), 0) as session_time2,
        NULLIF(sum(dbs2.active_time), 0) as active_time2,
        NULLIF(sum(dbs2.idle_in_transaction_time), 0) as idle_in_transaction_time2,
        NULLIF(sum(dbs2.sessions), 0) as sessions2,
        NULLIF(sum(dbs2.sessions_abandoned), 0) as sessions_abandoned2,
        NULLIF(sum(dbs2.sessions_fatal), 0) as sessions_fatal2,
        NULLIF(sum(dbs2.sessions_killed), 0) as sessions_killed2
    FROM dbstats_sessions(sserver_id,start1_id,end1_id,topn) dbs1
      FULL OUTER JOIN dbstats_sessions(sserver_id,start2_id,end2_id,topn) dbs2
        USING (server_id, datid)
      LEFT OUTER JOIN sample_stat_database st_last1 ON
        (st_last1.server_id = dbs1.server_id AND st_last1.datid = dbs1.datid AND st_last1.sample_id =
        end1_id)
      LEFT OUTER JOIN sample_stat_database st_last2 ON
        (st_last2.server_id = dbs2.server_id AND st_last2.datid = dbs2.datid AND st_last2.sample_id =
        end2_id)
    GROUP BY ROLLUP(COALESCE(dbs1.dbname,dbs2.dbname))
    ORDER BY COALESCE(dbs1.dbname,dbs2.dbname) NULLS LAST;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">Database</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="3" title="Session timings for databases">Timings (s)</th>'
            '<th colspan="4" title="Session counts for databases">Sessions</th>'
          '</tr>'
          '<tr>'
            '<th title="Time spent by database sessions in this database (note that statistics are only updated when the state of a session changes, so if sessions have been idle for a long time, this idle time won''t be included)">Total</th>'
            '<th title="Time spent executing SQL statements in this database (this corresponds to the states active and fastpath function call in pg_stat_activity)">Active</th>'
            '<th title="Time spent idling while in a transaction in this database (this corresponds to the states idle in transaction and idle in transaction (aborted) in pg_stat_activity)">Idle(T)</th>'
            '<th title="Total number of sessions established to this database">Established</th>'
            '<th title="Number of database sessions to this database that were terminated because connection to the client was lost">Abondoned</th>'
            '<th title="Number of database sessions to this database that were terminated by fatal errors">Fatal</th>'
            '<th title="Number of database sessions to this database that were terminated by operator intervention">Killed</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'db_tpl',
        '<tr {interval1}>'
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

    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            round(CAST(r_result.session_time1 / 1000 AS numeric),2),
            round(CAST(r_result.active_time1 / 1000 AS numeric),2),
            round(CAST(r_result.idle_in_transaction_time1 / 1000 AS numeric),2),
            r_result.sessions1,
            r_result.sessions_abandoned1,
            r_result.sessions_fatal1,
            r_result.sessions_killed1,
            round(CAST(r_result.session_time2 / 1000 AS numeric),2),
            round(CAST(r_result.active_time2 / 1000 AS numeric),2),
            round(CAST(r_result.idle_in_transaction_time2 / 1000 AS numeric),2),
            r_result.sessions2,
            r_result.sessions_abandoned2,
            r_result.sessions_fatal2,
            r_result.sessions_killed2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
