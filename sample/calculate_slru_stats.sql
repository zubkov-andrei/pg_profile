CREATE FUNCTION calculate_slru_stats(IN sserver_id integer, IN ssample_id integer
) RETURNS void AS $$
-- Calc SLRU stat diff
INSERT INTO sample_stat_slru(
    server_id,
    sample_id,
    name,
    blks_zeroed,
    blks_hit,
    blks_read,
    blks_written,
    blks_exists,
    flushes,
    truncates,
    stats_reset
)
SELECT
    cur.server_id,
    cur.sample_id,
    cur.name,
    cur.blks_zeroed - COALESCE(lst.blks_zeroed, 0),
    cur.blks_hit - COALESCE(lst.blks_hit, 0),
    cur.blks_read - COALESCE(lst.blks_read, 0),
    cur.blks_written - COALESCE(lst.blks_written, 0),
    cur.blks_exists - COALESCE(lst.blks_exists, 0),
    cur.flushes - COALESCE(lst.flushes, 0),
    cur.truncates - COALESCE(lst.truncates, 0),
    cur.stats_reset
FROM last_stat_slru cur
LEFT OUTER JOIN last_stat_slru lst ON
  (lst.server_id, lst.sample_id, lst.name) =
  (sserver_id, ssample_id - 1, cur.name)
  AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
WHERE
  (cur.server_id, cur.sample_id) = (sserver_id, ssample_id) AND
  GREATEST(
    cur.blks_zeroed - COALESCE(lst.blks_zeroed, 0),
    cur.blks_hit - COALESCE(lst.blks_hit, 0),
    cur.blks_read - COALESCE(lst.blks_read, 0),
    cur.blks_written - COALESCE(lst.blks_written, 0),
    cur.blks_exists - COALESCE(lst.blks_exists, 0),
    cur.flushes - COALESCE(lst.flushes, 0),
    cur.truncates - COALESCE(lst.truncates, 0)
  ) > 0;

DELETE FROM last_stat_slru WHERE server_id = sserver_id AND sample_id != ssample_id;
$$ LANGUAGE sql;