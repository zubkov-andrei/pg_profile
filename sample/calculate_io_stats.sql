CREATE FUNCTION calculate_io_stats(IN sserver_id integer, IN ssample_id integer
) RETURNS void AS $$
-- Calc I/O stat diff
INSERT INTO sample_stat_io(
    server_id,
    sample_id,
    backend_type,
    object,
    context,
    reads,
    read_bytes,
    read_time,
    writes,
    write_bytes,
    write_time,
    writebacks,
    writeback_time,
    extends,
    extend_bytes,
    extend_time,
    op_bytes,
    hits,
    evictions,
    reuses,
    fsyncs,
    fsync_time,
    stats_reset
)
SELECT
    cur.server_id,
    cur.sample_id,
    cur.backend_type,
    cur.object,
    cur.context,
    cur.reads - COALESCE(lst.reads, 0),
    cur.read_bytes - COALESCE(lst.read_bytes, 0),
    cur.read_time - COALESCE(lst.read_time, 0),
    cur.writes - COALESCE(lst.writes, 0),
    cur.write_bytes - COALESCE(lst.write_bytes, 0),
    cur.write_time - COALESCE(lst.write_time, 0),
    cur.writebacks - COALESCE(lst.writebacks, 0),
    cur.writeback_time - COALESCE(lst.writeback_time, 0),
    cur.extends - COALESCE(lst.extends, 0),
    cur.extend_bytes - COALESCE(lst.extend_bytes, 0),
    cur.extend_time - COALESCE(lst.extend_time, 0),
    cur.op_bytes,
    cur.hits - COALESCE(lst.hits, 0),
    cur.evictions - COALESCE(lst.evictions, 0),
    cur.reuses - COALESCE(lst.reuses, 0),
    cur.fsyncs - COALESCE(lst.fsyncs, 0),
    cur.fsync_time - COALESCE(lst.fsync_time, 0),
    cur.stats_reset
FROM last_stat_io cur
LEFT OUTER JOIN last_stat_io lst ON
  (lst.server_id, lst.sample_id, lst.backend_type, lst.object, lst.context) =
  (sserver_id, ssample_id - 1, cur.backend_type, cur.object, cur.context)
  AND (cur.op_bytes,cur.stats_reset) IS NOT DISTINCT FROM (lst.op_bytes,lst.stats_reset)
WHERE
  (cur.server_id, cur.sample_id) = (sserver_id, ssample_id) AND
  GREATEST(
    cur.reads - COALESCE(lst.reads, 0),
    cur.writes - COALESCE(lst.writes, 0),
    cur.writebacks - COALESCE(lst.writebacks, 0),
    cur.extends - COALESCE(lst.extends, 0),
    cur.hits - COALESCE(lst.hits, 0),
    cur.evictions - COALESCE(lst.evictions, 0),
    cur.reuses - COALESCE(lst.reuses, 0),
    cur.fsyncs - COALESCE(lst.fsyncs, 0)
  ) > 0;

DELETE FROM last_stat_io WHERE server_id = sserver_id AND sample_id != ssample_id;
$$ LANGUAGE sql;