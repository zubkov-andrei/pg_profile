/* ===== Cluster stats functions ===== */

CREATE FUNCTION cluster_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    checkpoints_timed     bigint,
    checkpoints_req       bigint,
    checkpoints_done      bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time  double precision,
    buffers_checkpoint    bigint,
    slru_checkpoint       bigint,
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
    st.server_id AS server_id,
    sum(checkpoints_timed) FILTER (WHERE st.sample_id > start_id)::bigint  AS checkpoints_timed,
    sum(checkpoints_req) FILTER (WHERE st.sample_id > start_id)::bigint AS checkpoints_req,
    sum(checkpoints_done) FILTER (WHERE st.sample_id > start_id)::bigint AS checkpoints_done,
    sum(checkpoint_write_time) FILTER (WHERE st.sample_id > start_id)::double precision AS checkpoint_write_time,
    sum(checkpoint_sync_time) FILTER (WHERE st.sample_id > start_id)::double precision AS checkpoint_sync_time,
    sum(buffers_checkpoint) FILTER (WHERE st.sample_id > start_id)::bigint AS buffers_checkpoint,
    sum(slru_checkpoint) FILTER (WHERE st.sample_id > start_id)::bigint AS slru_checkpoint,
    sum(buffers_clean) FILTER (WHERE st.sample_id > start_id)::bigint AS buffers_clean,
    sum(buffers_backend) FILTER (WHERE st.sample_id > start_id)::bigint AS buffers_backend,
    sum(buffers_backend_fsync) FILTER (WHERE st.sample_id > start_id)::bigint AS buffers_backend_fsync,
    sum(maxwritten_clean) FILTER (WHERE st.sample_id > start_id)::bigint AS maxwritten_clean,
    sum(buffers_alloc) FILTER (WHERE st.sample_id > start_id)::bigint AS buffers_alloc,
    sum(wal_size) FILTER (WHERE st.sample_id > start_id)::bigint AS wal_size,
    sum(sa.archived_count) FILTER (WHERE st.sample_id > start_id)::bigint AS archived_count,
    sum(sa.failed_count) FILTER (WHERE sa.sample_id > start_id)::bigint AS failed_count,
    max(st.wal_lsn::text) FILTER (WHERE st.sample_id = start_id)::pg_lsn AS start_lsn,
    max(st.wal_lsn::text) FILTER (WHERE st.sample_id = end_id)::pg_lsn AS end_lsn,
    sum(restartpoints_timed) FILTER (WHERE sa.sample_id > start_id)::bigint AS restartpoints_timed,
    sum(restartpoints_req) FILTER (WHERE st.sample_id > start_id)::bigint AS restartpoints_req,
    sum(restartpoints_done) FILTER (WHERE st.sample_id > start_id)::bigint AS restartpoints_done
  FROM sample_stat_cluster st
    LEFT OUTER JOIN sample_stat_archiver sa USING (server_id, sample_id)
  WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id AND end_id
  GROUP BY st.server_id
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stats_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  checkpoints_timed     numeric,
  checkpoints_req       numeric,
  checkpoints_done      numeric,
  checkpoint_write_time numeric,
  checkpoint_sync_time  numeric,
  buffers_checkpoint    numeric,
  slru_checkpoint       numeric,
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
    NULLIF(checkpoints_done, 0)::numeric,
    round(cast(NULLIF(checkpoint_write_time, 0.0)/1000 as numeric),2),
    round(cast(NULLIF(checkpoint_sync_time, 0.0)/1000 as numeric),2),
    NULLIF(buffers_checkpoint, 0)::numeric,
    NULLIF(slru_checkpoint, 0)::numeric,
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
  checkpoints_done1      numeric,
  checkpoint_write_time1 numeric,
  checkpoint_sync_time1  numeric,
  buffers_checkpoint1    numeric,
  slru_checkpoint1       numeric,
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
  checkpoints_done2      numeric,
  checkpoint_write_time2 numeric,
  checkpoint_sync_time2  numeric,
  buffers_checkpoint2    numeric,
  slru_checkpoint2       numeric,
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
    NULLIF(st1.checkpoints_done, 0)::numeric AS checkpoints_done1,
    round(cast(NULLIF(st1.checkpoint_write_time, 0.0)/1000 as numeric),2) as checkpoint_write_time1,
    round(cast(NULLIF(st1.checkpoint_sync_time, 0.0)/1000 as numeric),2) as checkpoint_sync_time1,
    NULLIF(st1.buffers_checkpoint, 0)::numeric AS buffers_checkpoint1,
    NULLIF(st1.slru_checkpoint, 0)::numeric AS slru_checkpoint1,
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
    NULLIF(st2.checkpoints_done, 0)::numeric AS checkpoints_done2,
    round(cast(NULLIF(st2.checkpoint_write_time, 0.0)/1000 as numeric),2) as checkpoint_write_time2,
    round(cast(NULLIF(st2.checkpoint_sync_time, 0.0)/1000 as numeric),2) as checkpoint_sync_time2,
    NULLIF(st2.buffers_checkpoint, 0)::numeric AS buffers_checkpoint2,
    NULLIF(st2.slru_checkpoint, 0)::numeric AS slru_checkpoint2,
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
    st.next_sample_id AS sample_id,
    NULLIF(st.next_bgwriter_stats_reset, st.bgwriter_stats_reset) AS bgwriter_stats_reset,
    NULLIF(st.next_archiver_stats_reset, st.archiver_stats_reset) AS archiver_stats_reset,
    NULLIF(st.next_checkpoint_stats_reset, st.checkpoint_stats_reset) AS checkpoint_stats_reset
  FROM (
    SELECT
      clu.sample_id,
      lead(clu.sample_id) OVER w as next_sample_id,
      clu.stats_reset as bgwriter_stats_reset,
      lead(clu.stats_reset) OVER w as next_bgwriter_stats_reset,
      sta.stats_reset as archiver_stats_reset,
      lead(sta.stats_reset) OVER w as next_archiver_stats_reset,
      clu.checkpoint_stats_reset as checkpoint_stats_reset,
      lead(clu.checkpoint_stats_reset) OVER w as next_checkpoint_stats_reset
    FROM sample_stat_cluster clu
      LEFT OUTER JOIN sample_stat_archiver sta USING (server_id,sample_id)
    WHERE clu.server_id = sserver_id AND clu.sample_id BETWEEN start_id AND end_id
    WINDOW w AS (ORDER BY clu.sample_id ASC)) st
    WHERE st.next_sample_id IS NOT NULL AND
      (st.bgwriter_stats_reset, st.archiver_stats_reset, st.checkpoint_stats_reset) IS DISTINCT FROM
      (st.next_bgwriter_stats_reset, st.next_archiver_stats_reset, st.next_checkpoint_stats_reset)
  ORDER BY st.sample_id ASC;
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
