/* ===== Tables stats functions ===== */

CREATE FUNCTION tablespace_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id integer,
    tablespaceid oid,
    tablespacename name,
    tablespacepath text,
    size_delta bigint,
    last_size bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.tablespaceid,
        st.tablespacename,
        st.tablespacepath,
        sum(st.size_delta)::bigint AS size_delta,
        max(st.size) FILTER (WHERE st.sample_id = end_id) AS last_size
    FROM v_sample_stat_tablespaces st
    WHERE st.server_id = sserver_id
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.tablespaceid, st.tablespacename, st.tablespacepath
$$ LANGUAGE sql;

CREATE FUNCTION tablespace_stats_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  tablespacename        text,
  tablespacepath        text,
  size                  text,
  size_delta            text
)
SET search_path=@extschema@ AS $$
  SELECT
      st.tablespacename::text,
      st.tablespacepath,
      pg_size_pretty(NULLIF(st.last_size, 0)) as size,
      pg_size_pretty(NULLIF(st.size_delta, 0)) as size_delta
  FROM tablespace_stats(sserver_id, start_id, end_id) st
  ORDER BY st.tablespacename ASC;
$$ LANGUAGE sql;

CREATE FUNCTION tablespace_stats_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  tablespacename        text,
  tablespacepath        text,
  size1                 text,
  size2                 text,
  size_delta1           text,
  size_delta2           text
)
SET search_path=@extschema@ AS $$
  SELECT
      COALESCE(stat1.tablespacename,stat2.tablespacename)::text AS tablespacename,
      COALESCE(stat1.tablespacepath,stat2.tablespacepath) AS tablespacepath,
      pg_size_pretty(NULLIF(stat1.last_size, 0)) as size1,
      pg_size_pretty(NULLIF(stat2.last_size, 0)) as size2,
      pg_size_pretty(NULLIF(stat1.size_delta, 0)) as size_delta1,
      pg_size_pretty(NULLIF(stat2.size_delta, 0)) as size_delta2
  FROM tablespace_stats(sserver_id,start1_id,end1_id) stat1
      FULL OUTER JOIN tablespace_stats(sserver_id,start2_id,end2_id) stat2
        USING (server_id,tablespaceid)
$$ LANGUAGE sql;
