/* ========= Cluster databases report functions ========= */
CREATE FUNCTION profile_checkavail_io_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS
$$
-- Check if we have I/O times collected for report interval
  SELECT COALESCE(sum(blk_read_time), 0) + COALESCE(sum(blk_write_time), 0) > 0
  FROM sample_stat_database sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_sessionstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS
$$
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

CREATE FUNCTION dbstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    datid                 oid,
    dbname                name,
    xact_commit           bigint,
    xact_rollback         bigint,
    blks_read             bigint,
    blks_hit              bigint,
    tup_returned          bigint,
    tup_fetched           bigint,
    tup_inserted          bigint,
    tup_updated           bigint,
    tup_deleted           bigint,
    temp_files            bigint,
    temp_bytes            bigint,
    datsize_delta         bigint,
    deadlocks             bigint,
    checksum_failures     bigint,
    checksum_last_failure timestamp with time zone,
    blk_read_time         double precision,
    blk_write_time        double precision,
    session_time          double precision,
    active_time           double precision,
    idle_in_transaction_time double precision,
    sessions              bigint,
    sessions_abandoned    bigint,
    sessions_fatal        bigint,
    sessions_killed       bigint,
    parallel_workers_to_launch  bigint,
    parallel_workers_launched   bigint,
    last_datsize          bigint
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
        sum(blk_write_time)/1000::double precision AS blk_write_time,
        sum(session_time)/1000::double precision AS session_time,
        sum(active_time)/1000::double precision AS active_time,
        sum(idle_in_transaction_time)/1000::double precision AS idle_in_transaction_time,
        sum(sessions)::bigint AS sessions,
        sum(sessions_abandoned)::bigint AS sessions_abandoned,
        sum(sessions_fatal)::bigint AS sessions_fatal,
        sum(sessions_killed)::bigint AS sessions_killed,
        sum(parallel_workers_to_launch)::bigint AS parallel_workers_to_launch,
        sum(parallel_workers_launched)::bigint AS parallel_workers_launched,
        max(datsize) FILTER (WHERE sample_id = end_id) AS last_datsize
    FROM sample_stat_database st
    WHERE st.server_id = sserver_id AND NOT datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.datid, st.datname
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid                 oid,
    dbname                name,
    xact_commit           numeric,
    xact_rollback         numeric,
    blks_read             numeric,
    blks_hit              numeric,
    tup_returned          numeric,
    tup_fetched           numeric,
    tup_inserted          numeric,
    tup_updated           numeric,
    tup_deleted           numeric,
    temp_files            numeric,
    temp_bytes            text,
    datsize               text,
    datsize_delta         text,
    deadlocks             numeric,
    blks_hit_pct          numeric,
    checksum_failures     numeric,
    checksum_last_failure text,
    blk_read_time         numeric,
    blk_write_time        numeric,
    session_time          numeric,
    active_time           numeric,
    idle_in_transaction_time numeric,
    sessions              numeric,
    sessions_abandoned    numeric,
    sessions_fatal        numeric,
    sessions_killed       numeric,
    parallel_workers_to_launch  numeric,
    parallel_workers_launched   numeric,
    -- ordering fields
    ord_db                integer
) AS $$
    SELECT
        st.datid AS datid,
        COALESCE(st.dbname,'Total') AS dbname,
        NULLIF(sum(st.xact_commit), 0) AS xact_commit,
        NULLIF(sum(st.xact_rollback), 0) AS xact_rollback,
        NULLIF(sum(st.blks_read), 0) AS blks_read,
        NULLIF(sum(st.blks_hit), 0) AS blks_hit,
        NULLIF(sum(st.tup_returned), 0) AS tup_returned,
        NULLIF(sum(st.tup_fetched), 0) AS tup_fetched,
        NULLIF(sum(st.tup_inserted), 0) AS tup_inserted,
        NULLIF(sum(st.tup_updated), 0) AS tup_updated,
        NULLIF(sum(st.tup_deleted), 0) AS tup_deleted,
        NULLIF(sum(st.temp_files), 0) AS temp_files,
        pg_size_pretty(NULLIF(sum(st.temp_bytes), 0)) AS temp_bytes,
        pg_size_pretty(NULLIF(sum(st.last_datsize), 0)) AS datsize,
        pg_size_pretty(NULLIF(sum(st.datsize_delta), 0)) AS datsize_delta,
        NULLIF(sum(st.deadlocks), 0) AS deadlocks,
        round(CAST((sum(st.blks_hit)*100/NULLIF(sum(st.blks_hit)+sum(st.blks_read),0)) AS numeric),2) AS blks_hit_pct,
        NULLIF(sum(st.checksum_failures), 0) AS checksum_failures,
        max(st.checksum_last_failure)::text AS checksum_last_failure,
        round(CAST(NULLIF(sum(st.blk_read_time), 0) AS numeric),2) AS blk_read_time,
        round(CAST(NULLIF(sum(st.blk_write_time), 0) AS numeric),2) AS blk_write_time,
        round(CAST(NULLIF(sum(st.session_time), 0) AS numeric),2) AS session_time,
        round(CAST(NULLIF(sum(st.active_time), 0) AS numeric),2) AS active_time,
        round(CAST(NULLIF(sum(st.idle_in_transaction_time), 0) AS numeric),2) AS idle_in_transaction_time,
        NULLIF(sum(st.sessions), 0) AS sessions,
        NULLIF(sum(st.sessions_abandoned), 0) AS sessions_abandoned,
        NULLIF(sum(st.sessions_fatal), 0) AS sessions_fatal,
        NULLIF(sum(st.sessions_killed), 0) AS sessions_killed,
        NULLIF(sum(st.parallel_workers_to_launch), 0) AS parallel_workers_to_launch,
        NULLIF(sum(st.parallel_workers_launched), 0) AS parallel_workers_launched,
        -- ordering fields
        row_number() OVER (ORDER BY st.dbname NULLS LAST)::integer AS ord_db
    FROM dbstats(sserver_id, start_id, end_id) st
    GROUP BY GROUPING SETS ((st.datid, st.dbname), ())
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_format_diff(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid                   oid,
    dbname                  name,
    xact_commit1            numeric,
    xact_rollback1          numeric,
    blks_read1              numeric,
    blks_hit1               numeric,
    tup_returned1           numeric,
    tup_fetched1            numeric,
    tup_inserted1           numeric,
    tup_updated1            numeric,
    tup_deleted1            numeric,
    temp_files1             numeric,
    temp_bytes1             text,
    datsize1                text,
    datsize_delta1          text,
    deadlocks1              numeric,
    blks_hit_pct1           numeric,
    checksum_failures1      numeric,
    checksum_last_failure1  text,
    blk_read_time1          numeric,
    blk_write_time1         numeric,
    session_time1           numeric,
    active_time1            numeric,
    idle_in_transaction_time1 numeric,
    sessions1               numeric,
    sessions_abandoned1     numeric,
    sessions_fatal1         numeric,
    sessions_killed1        numeric,
    parallel_workers_to_launch1 numeric,
    parallel_workers_launched1  numeric,
    xact_commit2            numeric,
    xact_rollback2          numeric,
    blks_read2              numeric,
    blks_hit2               numeric,
    tup_returned2           numeric,
    tup_fetched2            numeric,
    tup_inserted2           numeric,
    tup_updated2            numeric,
    tup_deleted2            numeric,
    temp_files2             numeric,
    temp_bytes2             text,
    datsize2                text,
    datsize_delta2          text,
    deadlocks2              numeric,
    blks_hit_pct2           numeric,
    checksum_failures2      numeric,
    checksum_last_failure2  text,
    blk_read_time2          numeric,
    blk_write_time2         numeric,
    session_time2           numeric,
    active_time2            numeric,
    idle_in_transaction_time2 numeric,
    sessions2               numeric,
    sessions_abandoned2     numeric,
    sessions_fatal2         numeric,
    sessions_killed2        numeric,
    parallel_workers_to_launch2 numeric,
    parallel_workers_launched2  numeric,
    -- ordering fields
    ord_db                  integer
) AS $$
    SELECT
        COALESCE(dbs1.datid,dbs2.datid) AS datid,
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
        pg_size_pretty(NULLIF(sum(dbs1.last_datsize), 0)) AS datsize1,
        pg_size_pretty(NULLIF(sum(dbs1.datsize_delta), 0)) AS datsize_delta1,
        NULLIF(sum(dbs1.deadlocks), 0) AS deadlocks1,
        round(CAST((sum(dbs1.blks_hit)*100/NULLIF(sum(dbs1.blks_hit)+sum(dbs1.blks_read),0))::double precision AS numeric),2) AS blks_hit_pct1,
        NULLIF(sum(dbs1.checksum_failures), 0) as checksum_failures1,
        max(dbs1.checksum_last_failure)::text as checksum_last_failure1,
        round(CAST(NULLIF(sum(dbs1.blk_read_time), 0) AS numeric),2) AS blk_read_time1,
        round(CAST(NULLIF(sum(dbs1.blk_write_time), 0) AS numeric),2) as blk_write_time1,
        round(CAST(NULLIF(sum(dbs1.session_time), 0) AS numeric),2) AS session_time1,
        round(CAST(NULLIF(sum(dbs1.active_time), 0) AS numeric),2) AS active_time1,
        round(CAST(NULLIF(sum(dbs1.idle_in_transaction_time), 0) AS numeric),2) AS idle_in_transaction_time1,
        NULLIF(sum(dbs1.sessions), 0) AS sessions1,
        NULLIF(sum(dbs1.sessions_abandoned), 0) AS sessions_abandoned1,
        NULLIF(sum(dbs1.sessions_fatal), 0) AS sessions_fatal1,
        NULLIF(sum(dbs1.sessions_killed), 0) AS sessions_killed1,
        NULLIF(sum(dbs1.parallel_workers_to_launch), 0) AS parallel_workers_to_launch1,
        NULLIF(sum(dbs1.parallel_workers_launched), 0) AS parallel_workers_launched1,
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
        pg_size_pretty(NULLIF(sum(dbs2.last_datsize), 0)) AS datsize2,
        pg_size_pretty(NULLIF(sum(dbs2.datsize_delta), 0)) AS datsize_delta2,
        NULLIF(sum(dbs2.deadlocks), 0) AS deadlocks2,
        round(CAST((sum(dbs2.blks_hit)*100/NULLIF(sum(dbs2.blks_hit)+sum(dbs2.blks_read),0))::double precision AS numeric),2) AS blks_hit_pct2,
        NULLIF(sum(dbs2.checksum_failures), 0) as checksum_failures2,
        max(dbs2.checksum_last_failure)::text as checksum_last_failure2,
        round(CAST(NULLIF(sum(dbs2.blk_read_time), 0) AS numeric),2) as blk_read_time2,
        round(CAST(NULLIF(sum(dbs2.blk_write_time), 0) AS numeric),2) as blk_write_time2,
        round(CAST(NULLIF(sum(dbs2.session_time), 0) AS numeric),2) AS session_time2,
        round(CAST(NULLIF(sum(dbs2.active_time), 0) AS numeric),2) AS active_time2,
        round(CAST(NULLIF(sum(dbs2.idle_in_transaction_time), 0) AS numeric),2) AS idle_in_transaction_time2,
        NULLIF(sum(dbs2.sessions), 0) AS sessions2,
        NULLIF(sum(dbs2.sessions_abandoned), 0) AS sessions_abandoned2,
        NULLIF(sum(dbs2.sessions_fatal), 0) AS sessions_fatal2,
        NULLIF(sum(dbs2.sessions_killed), 0) AS sessions_killed2,
        NULLIF(sum(dbs2.parallel_workers_to_launch), 0) AS parallel_workers_to_launch2,
        NULLIF(sum(dbs2.parallel_workers_launched), 0) AS parallel_workers_launched2,
        -- ordering fields
        row_number() OVER (ORDER BY COALESCE(dbs1.dbname,dbs2.dbname) NULLS LAST)::integer AS ord_db
    FROM dbstats(sserver_id,start1_id,end1_id) dbs1
      FULL OUTER JOIN dbstats(sserver_id,start2_id,end2_id) dbs2
        USING (server_id, datid)
    GROUP BY GROUPING SETS ((COALESCE(dbs1.datid,dbs2.datid), COALESCE(dbs1.dbname,dbs2.dbname)),
      ())
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    dbname      name,
    stats_reset timestamp with time zone,
    sample_id   integer
  )
SET search_path=@extschema@ AS
$$
  SELECT
    st.dbname,
    st.next_stats_reset AS stats_reset,
    st.next_sample_id AS sample_id
  FROM (
    SELECT
      st.sample_id,
      lead(st.sample_id) OVER w AS next_sample_id,
      st.datname AS dbname,
      st.stats_reset,
      lead(st.stats_reset) OVER w AS next_stats_reset
    FROM sample_stat_database st
    WHERE st.server_id = sserver_id AND NOT st.datistemplate AND st.sample_id BETWEEN start_id AND end_id
    WINDOW w AS (PARTITION BY st.datid ORDER BY st.sample_id)) st
  WHERE st.next_sample_id IS NOT NUll AND st.next_stats_reset IS DISTINCT FROM st.stats_reset
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_dbstats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS
$$
-- Check if database statistics were reset
    SELECT COUNT(*) > 0 FROM dbstats_reset(sserver_id, start_id, end_id);
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    dbname      name,
    stats_reset text,
    sample_id   integer
  )
    SET search_path=@extschema@
AS
$$
  SELECT
    dbname,
    stats_reset::text as stats_reset,
    sample_id
  FROM dbstats_reset(sserver_id, start_id, end_id)
  ORDER BY sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION dbstats_reset_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  interval_num integer,
  dbname       name,
  stats_reset  text,
  sample_id    integer
)
SET search_path=@extschema@
AS
$$
  SELECT
    interval_num,
    dbname,
    stats_reset::text as stats_reset,
    sample_id
  FROM
    (SELECT 1 AS interval_num, dbname, stats_reset, sample_id
      FROM dbstats_reset(sserver_id, start1_id, end1_id)
    UNION
    SELECT 2 AS interval_num, dbname, stats_reset, sample_id
      FROM dbstats_reset(sserver_id, start2_id, end2_id)) AS samples
  ORDER BY interval_num, sample_id ASC;
$$ LANGUAGE sql;
