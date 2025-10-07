CREATE FUNCTION calculate_wal_stats(IN sserver_id integer, IN ssample_id integer
) RETURNS void AS $$
-- Calc WAL stat diff
INSERT INTO sample_stat_wal(
  server_id,
  sample_id,
  wal_records,
  wal_fpi,
  wal_bytes,
  wal_buffers_full,
  wal_write,
  wal_sync,
  wal_write_time,
  wal_sync_time,
  stats_reset
)
SELECT
    cur.server_id,
    cur.sample_id,
    cur.wal_records - COALESCE(lst.wal_records,0),
    cur.wal_fpi - COALESCE(lst.wal_fpi,0),
    cur.wal_bytes - COALESCE(lst.wal_bytes,0),
    cur.wal_buffers_full - COALESCE(lst.wal_buffers_full,0),
    cur.wal_write - COALESCE(lst.wal_write,0),
    cur.wal_sync - COALESCE(lst.wal_sync,0),
    cur.wal_write_time - COALESCE(lst.wal_write_time,0),
    cur.wal_sync_time - COALESCE(lst.wal_sync_time,0),
    cur.stats_reset
FROM last_stat_wal cur
LEFT OUTER JOIN last_stat_wal lst ON
  (lst.server_id, lst.sample_id) = (sserver_id, ssample_id - 1)
  AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
WHERE (cur.server_id, cur.sample_id) = (sserver_id, ssample_id);

DELETE FROM last_stat_wal WHERE server_id = sserver_id AND sample_id != ssample_id;
$$ LANGUAGE sql;