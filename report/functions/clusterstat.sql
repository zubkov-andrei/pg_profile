/* ===== Cluster stats functions ===== */

CREATE FUNCTION cluster_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    checkpoints_timed     bigint,
    checkpoints_req       bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time  double precision,
    buffers_checkpoint    bigint,
    buffers_clean         bigint,
    buffers_backend       bigint,
    buffers_backend_fsync bigint,
    maxwritten_clean      bigint,
    buffers_alloc         bigint,
    wal_size              bigint,
    archived_count        bigint,
    failed_count          bigint,
    start_lsn             pg_lsn,
    end_lsn               pg_lsn,
    restartpoints_timed   bigint,
    restartpoints_req     bigint,
    restartpoints_done    bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        server_id,
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        buffers_backend,
        buffers_backend_fsync,
        maxwritten_clean,
        buffers_alloc,
        wal_size,
        archived_count,
        failed_count,
        start_lsn,
        end_lsn,
        restartpoints_timed,
        restartpoints_req,
        restartpoints_done
    FROM (
      SELECT
          st.server_id as server_id,
          sum(checkpoints_timed)::bigint as checkpoints_timed,
          sum(checkpoints_req)::bigint as checkpoints_req,
          sum(checkpoint_write_time)::double precision as checkpoint_write_time,
          sum(checkpoint_sync_time)::double precision as checkpoint_sync_time,
          sum(buffers_checkpoint)::bigint as buffers_checkpoint,
          sum(buffers_clean)::bigint as buffers_clean,
          sum(buffers_backend)::bigint as buffers_backend,
          sum(buffers_backend_fsync)::bigint as buffers_backend_fsync,
          sum(maxwritten_clean)::bigint as maxwritten_clean,
          sum(buffers_alloc)::bigint as buffers_alloc,
          sum(wal_size)::bigint as wal_size,
          sum(archived_count)::bigint as archived_count,
          sum(failed_count)::bigint as failed_count,
          sum(restartpoints_timed)::bigint as restartpoints_timed,
          sum(restartpoints_req)::bigint as restartpoints_req,
          sum(restartpoints_done)::bigint as restartpoints_done
      FROM sample_stat_cluster st
          LEFT OUTER JOIN sample_stat_archiver sa USING (server_id, sample_id)
      WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
      GROUP BY st.server_id
    ) clu JOIN (
      SELECT
        server_id,
        s.wal_lsn as start_lsn,
        e.wal_lsn as end_lsn
      FROM
        sample_stat_cluster s
        JOIN sample_stat_cluster e USING (server_id)
      WHERE
        (s.sample_id, e.sample_id, server_id) = (start_id, end_id, sserver_id)
    ) lsn USING (server_id)
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stats_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  checkpoints_timed     numeric,
  checkpoints_req       numeric,
  checkpoint_write_time numeric,
  checkpoint_sync_time  numeric,
  buffers_checkpoint    numeric,
  buffers_clean         numeric,
  buffers_backend       numeric,
  buffers_backend_fsync numeric,
  maxwritten_clean      numeric,
  buffers_alloc         numeric,
  wal_size              numeric,
  wal_size_pretty       text,
  archived_count        numeric,
  failed_count          numeric,
  start_lsn             text,
  end_lsn               text,
  restartpoints_timed   numeric,
  restartpoints_req     numeric,
  restartpoints_done    numeric
) SET search_path=@extschema@ AS $$
  SELECT
    NULLIF(checkpoints_timed, 0)::numeric,
    NULLIF(checkpoints_req, 0)::numeric,
    round(cast(NULLIF(checkpoint_write_time, 0.0)/1000 as numeric),2),
    round(cast(NULLIF(checkpoint_sync_time, 0.0)/1000 as numeric),2),
    NULLIF(buffers_checkpoint, 0)::numeric,
    NULLIF(buffers_clean, 0)::numeric,
    NULLIF(buffers_backend, 0)::numeric,
    NULLIF(buffers_backend_fsync, 0)::numeric,
    NULLIF(maxwritten_clean, 0)::numeric,
    NULLIF(buffers_alloc, 0)::numeric,
    NULLIF(wal_size, 0)::numeric,
    pg_size_pretty(NULLIF(wal_size, 0)),
    NULLIF(archived_count, 0)::numeric,
    NULLIF(failed_count, 0)::numeric,
    start_lsn::text AS start_lsn,
    end_lsn::text AS end_lsn,
    NULLIF(restartpoints_timed, 0)::numeric,
    NULLIF(restartpoints_req, 0)::numeric,
    NULLIF(restartpoints_done, 0)::numeric
  FROM cluster_stats(sserver_id, start_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stats_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  checkpoints_timed1     numeric,
  checkpoints_req1       numeric,
  checkpoint_write_time1 numeric,
  checkpoint_sync_time1  numeric,
  buffers_checkpoint1    numeric,
  buffers_clean1         numeric,
  buffers_backend1       numeric,
  buffers_backend_fsync1 numeric,
  maxwritten_clean1      numeric,
  buffers_alloc1         numeric,
  wal_size1              numeric,
  wal_size_pretty1       text,
  archived_count1        numeric,
  failed_count1          numeric,
  start_lsn1             text,
  end_lsn1               text,
  restartpoints_timed1   numeric,
  restartpoints_req1     numeric,
  restartpoints_done1    numeric,
  checkpoints_timed2     numeric,
  checkpoints_req2       numeric,
  checkpoint_write_time2 numeric,
  checkpoint_sync_time2  numeric,
  buffers_checkpoint2    numeric,
  buffers_clean2         numeric,
  buffers_backend2       numeric,
  buffers_backend_fsync2 numeric,
  maxwritten_clean2      numeric,
  buffers_alloc2         numeric,
  wal_size2              numeric,
  wal_size_pretty2       text,
  archived_count2        numeric,
  failed_count2          numeric,
  start_lsn2             text,
  end_lsn2               text,
  restartpoints_timed2   numeric,
  restartpoints_req2     numeric,
  restartpoints_done2    numeric
) SET search_path=@extschema@ AS $$
  SELECT
    NULLIF(st1.checkpoints_timed, 0)::numeric AS checkpoints_timed1,
    NULLIF(st1.checkpoints_req, 0)::numeric AS checkpoints_req1,
    round(cast(NULLIF(st1.checkpoint_write_time, 0.0)/1000 as numeric),2) as checkpoint_write_time1,
    round(cast(NULLIF(st1.checkpoint_sync_time, 0.0)/1000 as numeric),2) as checkpoint_sync_time1,
    NULLIF(st1.buffers_checkpoint, 0)::numeric AS buffers_checkpoint1,
    NULLIF(st1.buffers_clean, 0)::numeric AS buffers_clean1,
    NULLIF(st1.buffers_backend, 0)::numeric AS buffers_backend1,
    NULLIF(st1.buffers_backend_fsync, 0)::numeric AS buffers_backend_fsync1,
    NULLIF(st1.maxwritten_clean, 0)::numeric AS maxwritten_clean1,
    NULLIF(st1.buffers_alloc, 0)::numeric AS buffers_alloc1,
    NULLIF(st1.wal_size, 0)::numeric AS wal_size1,
    pg_size_pretty(NULLIF(st1.wal_size, 0)) AS wal_size_pretty1,
    NULLIF(st1.archived_count, 0)::numeric AS archived_count1,
    NULLIF(st1.failed_count, 0)::numeric AS failed_count1,
    st1.start_lsn::text AS start_lsn1,
    st1.end_lsn::text AS end_lsn1,
    NULLIF(st1.restartpoints_timed, 0)::numeric AS restartpoints_timed1,
    NULLIF(st1.restartpoints_req, 0)::numeric AS restartpoints_req1,
    NULLIF(st1.restartpoints_done, 0)::numeric AS restartpoints_done1,
    NULLIF(st2.checkpoints_timed, 0)::numeric AS checkpoints_timed2,
    NULLIF(st2.checkpoints_req, 0)::numeric AS checkpoints_req2,
    round(cast(NULLIF(st2.checkpoint_write_time, 0.0)/1000 as numeric),2) as checkpoint_write_time2,
    round(cast(NULLIF(st2.checkpoint_sync_time, 0.0)/1000 as numeric),2) as checkpoint_sync_time2,
    NULLIF(st2.buffers_checkpoint, 0)::numeric AS buffers_checkpoint2,
    NULLIF(st2.buffers_clean, 0)::numeric AS buffers_clean2,
    NULLIF(st2.buffers_backend, 0)::numeric AS buffers_backend2,
    NULLIF(st2.buffers_backend_fsync, 0)::numeric AS buffers_backend_fsync2,
    NULLIF(st2.maxwritten_clean, 0)::numeric AS maxwritten_clean2,
    NULLIF(st2.buffers_alloc, 0)::numeric AS buffers_alloc2,
    NULLIF(st2.wal_size, 0)::numeric AS wal_size2,
    pg_size_pretty(NULLIF(st2.wal_size, 0)) AS wal_size_pretty2,
    NULLIF(st2.archived_count, 0)::numeric AS archived_count2,
    NULLIF(st2.failed_count, 0)::numeric AS failed_count2,
    st2.start_lsn::text AS start_lsn2,
    st2.end_lsn::text AS end_lsn2,
    NULLIF(st2.restartpoints_timed, 0)::numeric AS restartpoints_timed2,
    NULLIF(st2.restartpoints_req, 0)::numeric AS restartpoints_req2,
    NULLIF(st2.restartpoints_done, 0)::numeric AS restartpoints_done2
  FROM cluster_stats(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN cluster_stats(sserver_id, start2_id, end2_id) st2 USING (server_id)
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    sample_id               integer,
    bgwriter_stats_reset    timestamp with time zone,
    archiver_stats_reset    timestamp with time zone,
    checkpoint_stats_reset  timestamp with time zone
)
SET search_path=@extschema@ AS $$
  SELECT
      clu1.sample_id as sample_id,
      nullif(clu1.stats_reset, clu0.stats_reset),
      nullif(sta1.stats_reset, sta0.stats_reset),
      nullif(clu1.checkpoint_stats_reset, clu0.checkpoint_stats_reset)
  FROM sample_stat_cluster clu1
      LEFT OUTER JOIN sample_stat_archiver sta1 USING (server_id,sample_id)
      JOIN sample_stat_cluster clu0 ON (clu1.server_id = clu0.server_id AND clu1.sample_id = clu0.sample_id + 1)
      LEFT OUTER JOIN sample_stat_archiver sta0 ON (sta1.server_id = sta0.server_id AND sta1.sample_id = sta0.sample_id + 1)
  WHERE clu1.server_id = sserver_id AND clu1.sample_id BETWEEN start_id + 1 AND end_id
    AND
      (clu0.stats_reset, clu0.checkpoint_stats_reset, sta0.stats_reset) IS DISTINCT FROM
      (clu1.stats_reset, clu1.checkpoint_stats_reset, sta1.stats_reset)
  ORDER BY clu1.sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_cluster_stats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS
$$
    -- Check if statistics were reset
    SELECT COUNT(*) > 0 FROM cluster_stats_reset(sserver_id, start_id, end_id);
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stats_reset_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  sample_id               integer,
  bgwriter_stats_reset    text,
  archiver_stats_reset    text,
  checkpoint_stats_reset  text
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    bgwriter_stats_reset::text,
    archiver_stats_reset::text,
    checkpoint_stats_reset::text
  FROM
    cluster_stats_reset(sserver_id, start_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stats_reset_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    interval_num            integer,
    sample_id               integer,
    bgwriter_stats_reset    text,
    archiver_stats_reset    text,
    checkpoint_stats_reset  text
  )
SET search_path=@extschema@
AS
$$
  SELECT
    interval_num,
    sample_id,
    bgwriter_stats_reset::text,
    archiver_stats_reset::text,
    checkpoint_stats_reset::text
  FROM
    (SELECT
      1 AS interval_num,
      sample_id,
      bgwriter_stats_reset,
      archiver_stats_reset,
      checkpoint_stats_reset
    FROM cluster_stats_reset(sserver_id, start1_id, end1_id)
    UNION
    SELECT
      2 AS interval_num,
      sample_id,
      bgwriter_stats_reset,
      archiver_stats_reset,
      checkpoint_stats_reset
    FROM cluster_stats_reset(sserver_id, start2_id, end2_id)) AS samples
  ORDER BY interval_num, sample_id ASC;
$$ LANGUAGE sql;
