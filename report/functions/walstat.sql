/* ===== Cluster stats functions ===== */
CREATE FUNCTION profile_checkavail_walstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there is table sizes collected in both bounds
  SELECT
    count(wal_bytes) > 0
  FROM sample_stat_wal
  WHERE
    server_id = sserver_id
    AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        sample_id        integer,
        wal_stats_reset  timestamp with time zone
)
SET search_path=@extschema@ AS $$
  SELECT
      ws1.sample_id as sample_id,
      nullif(ws1.stats_reset,ws0.stats_reset)
  FROM sample_stat_wal ws1
      JOIN sample_stat_wal ws0 ON (ws1.server_id = ws0.server_id AND ws1.sample_id = ws0.sample_id + 1)
  WHERE ws1.server_id = sserver_id AND ws1.sample_id BETWEEN start_id + 1 AND end_id
    AND
      nullif(ws1.stats_reset,ws0.stats_reset) IS NOT NULL
  ORDER BY ws1.sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_wal_stats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
    -- Check if wal statistics were reset
  SELECT count(*) > 0 FROM wal_stats_reset(sserver_id, start_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_reset_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  sample_id       integer,
  wal_stats_reset text
)
SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    wal_stats_reset::text
  FROM
    wal_stats_reset(sserver_id, start_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_reset_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  interval_num    integer,
  sample_id       integer,
  wal_stats_reset text
)
SET search_path=@extschema@ AS $$
  SELECT
    1 AS interval_num,
    sample_id,
    wal_stats_reset::text
  FROM
    wal_stats_reset(sserver_id, start1_id, end1_id)
  UNION
  SELECT
    2 AS interval_num,
    sample_id,
    wal_stats_reset::text
  FROM
    wal_stats_reset(sserver_id, start2_id, end2_id)
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  server_id           integer,
  wal_records         bigint,
  wal_fpi             bigint,
  wal_bytes           numeric,
  wal_buffers_full    bigint,
  wal_write           bigint,
  wal_sync            bigint,
  wal_write_time      double precision,
  wal_sync_time       double precision
)
SET search_path=@extschema@ AS $$
  SELECT
    st.server_id as server_id,
    sum(wal_records)::bigint as wal_records,
    sum(wal_fpi)::bigint as wal_fpi,
    sum(wal_bytes)::numeric as wal_bytes,
    sum(wal_buffers_full)::bigint as wal_buffers_full,
    sum(wal_write)::bigint as wal_write,
    sum(wal_sync)::bigint as wal_sync,
    sum(wal_write_time)::double precision as wal_write_time,
    sum(wal_sync_time)::double precision as wal_sync_time
  FROM sample_stat_wal st
  WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
  GROUP BY st.server_id
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_format(IN sserver_id integer, IN start_id integer, IN end_id integer,
  duration numeric)
RETURNS TABLE(
  wal_records       numeric,
  wal_fpi           numeric,
  wal_bytes         numeric,
  wal_bytes_text    text,
  wal_bytes_per_sec text,
  wal_buffers_full  numeric,
  wal_write         numeric,
  wal_write_per_sec numeric,
  wal_sync          numeric,
  wal_sync_per_sec  numeric,
  wal_write_time    numeric,
  wal_write_time_per_sec  text,
  wal_sync_time     numeric,
  wal_sync_time_per_sec   text
)
SET search_path=@extschema@ AS $$
  SELECT
    NULLIF(wal_records, 0),
    NULLIF(wal_fpi, 0),
    NULLIF(wal_bytes, 0),
    pg_size_pretty(NULLIF(wal_bytes, 0)),
    pg_size_pretty(round(NULLIF(wal_bytes, 0)/NULLIF(duration, 0))::bigint),
    NULLIF(wal_buffers_full, 0),
    NULLIF(wal_write, 0),
    round((NULLIF(wal_write, 0)/NULLIF(duration, 0))::numeric,2),
    NULLIF(wal_sync, 0),
    round((NULLIF(wal_sync, 0)/NULLIF(duration, 0))::numeric,2),
    round(cast(NULLIF(wal_write_time, 0)/1000 as numeric),2),
    round((NULLIF(wal_write_time, 0)/10/NULLIF(duration, 0))::numeric,2) || '%',
    round(cast(NULLIF(wal_sync_time, 0)/1000 as numeric),2),
    round((NULLIF(wal_sync_time, 0)/10/NULLIF(duration, 0))::numeric,2) || '%'
  FROM
    wal_stats(sserver_id, start_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer,
  duration1 numeric, duration2 numeric)
RETURNS TABLE(
  wal_records1       numeric,
  wal_fpi1           numeric,
  wal_bytes1         numeric,
  wal_bytes_text1    text,
  wal_bytes_per_sec1 text,
  wal_buffers_full1  numeric,
  wal_write1         numeric,
  wal_write_per_sec1 numeric,
  wal_sync1          numeric,
  wal_sync_per_sec1  numeric,
  wal_write_time1    numeric,
  wal_write_time_per_sec1  text,
  wal_sync_time1     numeric,
  wal_sync_time_per_sec1   text,

  wal_records2       numeric,
  wal_fpi2           numeric,
  wal_bytes2         numeric,
  wal_bytes_text2    text,
  wal_bytes_per_sec2 text,
  wal_buffers_full2  numeric,
  wal_write2         numeric,
  wal_write_per_sec2 numeric,
  wal_sync2          numeric,
  wal_sync_per_sec2  numeric,
  wal_write_time2    numeric,
  wal_write_time_per_sec2  text,
  wal_sync_time2     numeric,
  wal_sync_time_per_sec2   text
)
SET search_path=@extschema@ AS $$
  SELECT
    NULLIF(ws1.wal_records, 0),
    NULLIF(ws1.wal_fpi, 0),
    NULLIF(ws1.wal_bytes, 0),
    pg_size_pretty(NULLIF(ws1.wal_bytes, 0)),
    pg_size_pretty(round(NULLIF(ws1.wal_bytes, 0)/NULLIF(duration1, 0))::bigint),
    NULLIF(ws1.wal_buffers_full, 0),
    NULLIF(ws1.wal_write, 0),
    round((NULLIF(ws1.wal_write, 0)/NULLIF(duration1, 0))::numeric,2),
    NULLIF(ws1.wal_sync, 0),
    round((NULLIF(ws1.wal_sync, 0)/NULLIF(duration1, 0))::numeric,2),
    round(cast(NULLIF(ws1.wal_write_time, 0)/1000 as numeric),2),
    round((NULLIF(ws1.wal_write_time, 0)/10/NULLIF(duration1, 0))::numeric,2) || '%',
    round(cast(NULLIF(ws1.wal_sync_time, 0)/1000 as numeric),2),
    round((NULLIF(ws1.wal_sync_time, 0)/10/NULLIF(duration1, 0))::numeric,2) || '%',

    NULLIF(ws2.wal_records, 0),
    NULLIF(ws2.wal_fpi, 0),
    NULLIF(ws2.wal_bytes, 0),
    pg_size_pretty(NULLIF(ws2.wal_bytes, 0)),
    pg_size_pretty(round(NULLIF(ws2.wal_bytes, 0)/NULLIF(duration2, 0))::bigint),
    NULLIF(ws2.wal_buffers_full, 0),
    NULLIF(ws2.wal_write, 0),
    round((NULLIF(ws2.wal_write, 0)/NULLIF(duration2, 0))::numeric,2),
    NULLIF(ws2.wal_sync, 0),
    round((NULLIF(ws2.wal_sync, 0)/NULLIF(duration2, 0))::numeric,2),
    round(cast(NULLIF(ws2.wal_write_time, 0)/1000 as numeric),2),
    round((NULLIF(ws2.wal_write_time, 0)/10/NULLIF(duration2, 0))::numeric,2) || '%',
    round(cast(NULLIF(ws2.wal_sync_time, 0)/1000 as numeric),2),
    round((NULLIF(ws2.wal_sync_time, 0)/10/NULLIF(duration2, 0))::numeric,2) || '%'
  FROM
    wal_stats(sserver_id, start1_id, end1_id) ws1
    CROSS JOIN
    wal_stats(sserver_id, start2_id, end2_id) ws2
$$ LANGUAGE sql;
