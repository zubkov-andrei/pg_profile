/* Total statistics functions used for normalization */

CREATE FUNCTION report_smpl_duration(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    duration     interval
) STABLE SET search_path=@extschema@ AS $$
    SELECT e.sample_time - s.sample_time AS duration
    FROM samples s, samples e
    WHERE
      (s.server_id, s.sample_id) = (sserver_id, start_id) AND
      (e.server_id, e.sample_id) = (sserver_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION total_bgwriter(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id               integer,
    written                 numeric,
    buffers_backend         numeric,
    wal_size                numeric
) STABLE ROWS 1 SET search_path=@extschema@ AS $$
    SELECT
      server_id,
      sum(buffers_checkpoint) + sum(buffers_clean) +
        sum(coalesce(
          NULLIF(buffers_backend, 0),
          io_buffers_backend
          )
        ) AS written,
      sum(coalesce(
          NULLIF(buffers_backend, 0),
          io_buffers_backend
          )
        ) AS buffers_backend,
      sum(wal_size) AS wal_size
    FROM sample_stat_cluster LEFT JOIN
      (
      /*
        since 17 there is no buffers_backend in pg_stat_bgwriter
        So we'll get it from pg_stat_io data
      */
        SELECT
          sample_id,
          sum(writes) FILTER (WHERE
            backend_type IN ('parallel worker', 'client backend'))::bigint AS io_buffers_backend
        FROM sample_stat_io
        WHERE
          server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
        GROUP BY sample_id
      ) io USING (sample_id)
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY server_id
$$ LANGUAGE sql;

CREATE FUNCTION total_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id               integer,
    total_time              double precision,
    shared_blk_read_time    double precision,
    shared_blk_write_time   double precision,
    local_blk_read_time     double precision,
    local_blk_write_time    double precision,
    shared_blks_hit         numeric,
    shared_blks_read        numeric,
    shared_blks_dirtied     numeric,
    temp_blks_read          numeric,
    temp_blks_written       numeric,
    temp_blk_read_time      double precision,
    temp_blk_write_time     double precision,
    local_blks_read         numeric,
    local_blks_written      numeric,
    calls                   numeric,
    plans                   numeric
) STABLE ROWS 1 SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        COALESCE(sum(total_plan_time), 0.0) + sum(total_exec_time) AS total_time,
        sum(shared_blk_read_time) AS shared_blk_read_time,
        sum(shared_blk_write_time) AS shared_blk_write_time,
        sum(local_blk_read_time) AS local_blk_read_time,
        sum(local_blk_write_time) AS local_blk_write_time,
        sum(shared_blks_hit) AS shared_blks_hit,
        sum(shared_blks_read) AS shared_blks_read,
        sum(shared_blks_dirtied) AS shared_blks_dirtied,
        sum(temp_blks_read) AS temp_blks_read,
        sum(temp_blks_written) AS temp_blks_written,
        sum(temp_blk_read_time) AS temp_blk_read_time,
        sum(temp_blk_write_time) AS temp_blk_write_time,
        sum(local_blks_read) AS local_blks_read,
        sum(local_blks_written) AS local_blks_written,
        sum(calls) AS calls,
        sum(plans) AS plans
    FROM sample_statements_total st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id
$$ LANGUAGE sql;

CREATE FUNCTION total_kcache_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id               integer,
    user_time               double precision,
    system_time             double precision,
    reads                   numeric,
    writes                  numeric
) STABLE ROWS 1 SET search_path=@extschema@ AS $$
    SELECT
        sserver_id,
        COALESCE(sum(exec_user_time), 0.0) + COALESCE(sum(plan_user_time), 0.0) AS user_time,
        COALESCE(sum(exec_system_time), 0.0) + COALESCE(sum(plan_system_time), 0.0)  AS system_time,
        COALESCE(sum(exec_reads), 0) + COALESCE(sum(plan_reads), 0) AS reads,
        COALESCE(sum(exec_writes), 0) + COALESCE(sum(plan_writes), 0) AS writes
    FROM sample_kcache_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION total_dbstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id       integer,
    blks_read       bigint,
    blks_hit        bigint,
    blk_read_time   double precision,
    blk_write_time  double precision
) STABLE ROWS 1 SET search_path=@extschema@ AS $$
  SELECT
    sserver_id,
    COALESCE(sum(blks_read)::bigint, 0),
    COALESCE(sum(blks_hit)::bigint, 0),
    COALESCE(sum(blk_read_time), 0),
    COALESCE(sum(blk_write_time), 0)
  FROM
    sample_stat_database
  WHERE
    server_id = sserver_id AND
    sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;
