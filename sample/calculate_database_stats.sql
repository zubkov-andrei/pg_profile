CREATE FUNCTION calculate_database_stats(IN sserver_id integer, IN ssample_id integer
) RETURNS void AS $$
-- Calc stat_database diff
INSERT INTO sample_stat_database(
  server_id,
  sample_id,
  datid,
  datname,
  xact_commit,
  xact_rollback,
  blks_read,
  blks_hit,
  tup_returned,
  tup_fetched,
  tup_inserted,
  tup_updated,
  tup_deleted,
  conflicts,
  temp_files,
  temp_bytes,
  deadlocks,
  checksum_failures,
  checksum_last_failure,
  blk_read_time,
  blk_write_time,
  session_time,
  active_time,
  idle_in_transaction_time,
  sessions,
  sessions_abandoned,
  sessions_fatal,
  sessions_killed,
  parallel_workers_to_launch,
  parallel_workers_launched,
  stats_reset,
  datsize,
  datsize_delta,
  datistemplate
)
SELECT
    cur.server_id,
    cur.sample_id,
    cur.datid,
    cur.datname,
    cur.xact_commit - COALESCE(lst.xact_commit,0),
    cur.xact_rollback - COALESCE(lst.xact_rollback,0),
    cur.blks_read - COALESCE(lst.blks_read,0),
    cur.blks_hit - COALESCE(lst.blks_hit,0),
    cur.tup_returned - COALESCE(lst.tup_returned,0),
    cur.tup_fetched - COALESCE(lst.tup_fetched,0),
    cur.tup_inserted - COALESCE(lst.tup_inserted,0),
    cur.tup_updated - COALESCE(lst.tup_updated,0),
    cur.tup_deleted - COALESCE(lst.tup_deleted,0),
    cur.conflicts - COALESCE(lst.conflicts,0),
    cur.temp_files - COALESCE(lst.temp_files,0),
    cur.temp_bytes - COALESCE(lst.temp_bytes,0),
    cur.deadlocks - COALESCE(lst.deadlocks,0),
    cur.checksum_failures - COALESCE(lst.checksum_failures,0),
    cur.checksum_last_failure,
    cur.blk_read_time - COALESCE(lst.blk_read_time,0),
    cur.blk_write_time - COALESCE(lst.blk_write_time,0),
    cur.session_time - COALESCE(lst.session_time,0),
    cur.active_time - COALESCE(lst.active_time,0),
    cur.idle_in_transaction_time - COALESCE(lst.idle_in_transaction_time,0),
    cur.sessions - COALESCE(lst.sessions,0),
    cur.sessions_abandoned - COALESCE(lst.sessions_abandoned,0),
    cur.sessions_fatal - COALESCE(lst.sessions_fatal,0),
    cur.sessions_killed - COALESCE(lst.sessions_killed,0),
    cur.parallel_workers_to_launch - COALESCE(lst.parallel_workers_to_launch,0),
    cur.parallel_workers_launched - COALESCE(lst.parallel_workers_launched,0),
    cur.stats_reset,
    cur.datsize as datsize,
    cur.datsize - COALESCE(lst.datsize,0) as datsize_delta,
    cur.datistemplate
FROM last_stat_database cur
  LEFT OUTER JOIN last_stat_database lst ON
    (lst.server_id, lst.sample_id, lst.datid, lst.datname) =
    (sserver_id, ssample_id - 1, cur.datid, cur.datname)
    AND lst.stats_reset IS NOT DISTINCT FROM cur.stats_reset
WHERE
  (cur.server_id, cur.sample_id) = (sserver_id, ssample_id);

/*
* In case of statistics reset full database size, and checksum checksum_failures
* is incorrectly considered as increment by previous query.
* So, we need to update it with correct value
*/
UPDATE sample_stat_database sdb
SET
  datsize_delta = cur.datsize - lst.datsize,
  checksum_failures = cur.checksum_failures - lst.checksum_failures,
  checksum_last_failure = cur.checksum_last_failure
FROM
  last_stat_database cur
  JOIN last_stat_database lst ON
    (lst.server_id, lst.sample_id, lst.datid, lst.datname) =
    (sserver_id, ssample_id - 1, cur.datid, cur.datname)
WHERE cur.stats_reset IS DISTINCT FROM lst.stats_reset AND
  (cur.server_id, cur.sample_id) = (sserver_id, ssample_id) AND
  (sdb.server_id, sdb.sample_id, sdb.datid, sdb.datname) =
  (cur.server_id, cur.sample_id, cur.datid, cur.datname);
$$ LANGUAGE sql;
