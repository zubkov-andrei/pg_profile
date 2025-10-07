CREATE FUNCTION calculate_tablespace_stats(IN sserver_id integer, IN ssample_id integer
) RETURNS void as $$
INSERT INTO tablespaces_list AS itl (
    server_id,
    last_sample_id,
    tablespaceid,
    tablespacename,
    tablespacepath
  )
SELECT
  cur.server_id,
  NULL,
  cur.tablespaceid,
  cur.tablespacename,
  cur.tablespacepath
FROM
  last_stat_tablespaces cur
WHERE
  (cur.server_id, cur.sample_id) = (sserver_id, ssample_id)
ON CONFLICT ON CONSTRAINT pk_tablespace_list DO
UPDATE SET
    (last_sample_id, tablespacename, tablespacepath) =
    (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath)
  WHERE
    (itl.last_sample_id, itl.tablespacename, itl.tablespacepath) IS DISTINCT FROM
    (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath);

-- Calculate diffs for tablespaces
INSERT INTO sample_stat_tablespaces(
  server_id,
  sample_id,
  tablespaceid,
  size,
  size_delta
)
SELECT
  cur.server_id as server_id,
  cur.sample_id as sample_id,
  cur.tablespaceid as tablespaceid,
  cur.size as size,
  cur.size - COALESCE(lst.size, 0) AS size_delta
FROM last_stat_tablespaces cur
  LEFT OUTER JOIN last_stat_tablespaces lst ON
    (lst.server_id, lst.sample_id, cur.tablespaceid) =
    (sserver_id, ssample_id - 1, lst.tablespaceid)
WHERE (cur.server_id, cur.sample_id) = (sserver_id, ssample_id);
$$ LANGUAGE sql;