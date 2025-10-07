CREATE FUNCTION calculate_cluster_stats(IN sserver_id integer, IN ssample_id integer
) RETURNS void AS $$
-- Calc stat cluster diff
INSERT INTO sample_stat_cluster(
  server_id,
  sample_id,
  checkpoints_timed,
  checkpoints_req,
  checkpoints_done,
  checkpoint_write_time,
  checkpoint_sync_time,
  buffers_checkpoint,
  slru_checkpoint,
  buffers_clean,
  maxwritten_clean,
  buffers_backend,
  buffers_backend_fsync,
  buffers_alloc,
  stats_reset,
  wal_size,
  wal_lsn,
  in_recovery,
  restartpoints_timed,
  restartpoints_req,
  restartpoints_done,
  checkpoint_stats_reset
)
SELECT
    cur.server_id,
    cur.sample_id,
    cur.checkpoints_timed - COALESCE(lstc.checkpoints_timed,0),
    cur.checkpoints_req - COALESCE(lstc.checkpoints_req,0),
    cur.checkpoints_done - COALESCE(lstc.checkpoints_done,0),
    cur.checkpoint_write_time - COALESCE(lstc.checkpoint_write_time,0),
    cur.checkpoint_sync_time - COALESCE(lstc.checkpoint_sync_time,0),
    cur.buffers_checkpoint - COALESCE(lstc.buffers_checkpoint,0),
    cur.slru_checkpoint - COALESCE(lstc.slru_checkpoint,0),
    cur.buffers_clean - COALESCE(lstb.buffers_clean,0),
    cur.maxwritten_clean - COALESCE(lstb.maxwritten_clean,0),
    cur.buffers_backend - COALESCE(lstb.buffers_backend,0),
    cur.buffers_backend_fsync - COALESCE(lstb.buffers_backend_fsync,0),
    cur.buffers_alloc - COALESCE(lstb.buffers_alloc,0),
    cur.stats_reset,
    cur.wal_size - COALESCE(lstb.wal_size,0),
    /* We will overwrite this value in case of stats reset
     * (see below)
     */
    cur.wal_lsn,
    cur.in_recovery,
    cur.restartpoints_timed - COALESCE(lstc.restartpoints_timed,0),
    cur.restartpoints_timed - COALESCE(lstc.restartpoints_timed,0),
    cur.restartpoints_timed - COALESCE(lstc.restartpoints_timed,0),
    cur.checkpoint_stats_reset
FROM last_stat_cluster cur
  LEFT OUTER JOIN last_stat_cluster lstb ON
    (lstb.server_id, lstb.sample_id) =
    (sserver_id, ssample_id - 1)
    AND cur.stats_reset IS NOT DISTINCT FROM lstb.stats_reset
  LEFT OUTER JOIN last_stat_cluster lstc ON
    (lstc.server_id, lstc.sample_id) =
    (sserver_id, ssample_id - 1)
    AND cur.checkpoint_stats_reset IS NOT DISTINCT FROM lstc.checkpoint_stats_reset
WHERE
  (cur.server_id, cur.sample_id) = (sserver_id, ssample_id);

/* wal_size is calculated since 0 to current value when stats reset happened
 * so, we need to update it
 */
UPDATE sample_stat_cluster ssc
SET wal_size = cur.wal_size - lst.wal_size
FROM last_stat_cluster cur
  JOIN last_stat_cluster lst ON
    (lst.server_id, lst.sample_id) =
    (sserver_id, ssample_id - 1)
WHERE
  (ssc.server_id, ssc.sample_id) = (sserver_id, ssample_id) AND
  (cur.server_id, cur.sample_id) = (sserver_id, ssample_id) AND
  cur.stats_reset IS DISTINCT FROM lst.stats_reset;

DELETE FROM last_stat_cluster WHERE server_id = sserver_id AND sample_id != ssample_id;
$$ LANGUAGE sql;