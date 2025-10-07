CREATE FUNCTION calculate_archiver_stats(IN sserver_id integer, IN ssample_id integer
) RETURNS void AS $$
-- Calc stat archiver diff
INSERT INTO sample_stat_archiver(
  server_id,
  sample_id,
  archived_count,
  last_archived_wal,
  last_archived_time,
  failed_count,
  last_failed_wal,
  last_failed_time,
  stats_reset
)
SELECT
    cur.server_id,
    cur.sample_id,
    cur.archived_count - COALESCE(lst.archived_count,0),
    cur.last_archived_wal,
    cur.last_archived_time,
    cur.failed_count - COALESCE(lst.failed_count,0),
    cur.last_failed_wal,
    cur.last_failed_time,
    cur.stats_reset
FROM last_stat_archiver cur
LEFT OUTER JOIN last_stat_archiver lst ON
  (lst.server_id, lst.sample_id) =
  (cur.server_id, cur.sample_id - 1)
  AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
WHERE cur.sample_id = ssample_id AND cur.server_id = sserver_id;

DELETE FROM last_stat_archiver WHERE server_id = sserver_id AND sample_id != ssample_id;
$$ LANGUAGE sql;