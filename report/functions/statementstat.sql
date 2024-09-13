/* ===== Statements stats functions ===== */
CREATE FUNCTION profile_checkavail_top_temp(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
    -- Check if top_temp is available
    SELECT COUNT(*) > 0
    FROM sample_statements_total st
    WHERE
        server_id = sserver_id AND
        sample_id BETWEEN start_id + 1 AND end_id AND
        COALESCE(st.temp_blks_read, 0) + COALESCE(st.temp_blks_written, 0) +
        COALESCE(st.local_blks_read, 0) + COALESCE(st.local_blks_written, 0) > 0
$$ LANGUAGE sql;

CREATE FUNCTION top_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id               integer,
    datid                   oid,
    dbname                  name,
    userid                  oid,
    username                name,
    queryid                 bigint,
    toplevel                boolean,
    plans                   bigint,
    plans_pct               float,
    calls                   bigint,
    calls_pct               float,
    total_time              double precision,
    total_time_pct          double precision,
    total_plan_time         double precision,
    plan_time_pct           float,
    total_exec_time         double precision,
    total_exec_time_pct     float,
    exec_time_pct           float,
    min_exec_time           double precision,
    max_exec_time           double precision,
    mean_exec_time          double precision,
    stddev_exec_time        numeric,
    min_plan_time           double precision,
    max_plan_time           double precision,
    mean_plan_time          double precision,
    stddev_plan_time        numeric,
    rows                    bigint,
    shared_blks_hit         bigint,
    shared_hit_pct          float,
    shared_blks_read        bigint,
    read_pct                float,
    shared_blks_fetched     bigint,
    shared_blks_fetched_pct float,
    shared_blks_dirtied     bigint,
    dirtied_pct             float,
    shared_blks_written     bigint,
    tot_written_pct         float,
    backend_written_pct     float,
    local_blks_hit          bigint,
    local_hit_pct           float,
    local_blks_read         bigint,
    local_blks_fetched      bigint,
    local_blks_dirtied      bigint,
    local_blks_written      bigint,
    temp_blks_read          bigint,
    temp_blks_written       bigint,
    shared_blk_read_time    double precision,
    shared_blk_write_time   double precision,
    local_blk_read_time     double precision,
    local_blk_write_time    double precision,
    temp_blk_read_time      double precision,
    temp_blk_write_time     double precision,
    io_time                 double precision,
    io_time_pct             float,
    temp_read_total_pct     float,
    temp_write_total_pct    float,
    temp_io_time_pct        float,
    local_read_total_pct    float,
    local_write_total_pct   float,
    wal_records             bigint,
    wal_fpi                 bigint,
    wal_bytes               numeric,
    wal_bytes_pct           float,
    user_time               double precision,
    system_time             double precision,
    reads                   bigint,
    writes                  bigint,
    jit_functions           bigint,
    jit_generation_time     double precision,
    jit_inlining_count      bigint,
    jit_inlining_time       double precision,
    jit_optimization_count  bigint,
    jit_optimization_time   double precision,
    jit_emission_count      bigint,
    jit_emission_time       double precision,
    jit_deform_count        bigint,
    jit_deform_time         double precision,
    stmt_alloc              bigint,
    stmt_cover              double precision
) SET search_path=@extschema@ AS $$
    WITH
      tot AS (
        SELECT
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
      ),
      totbgwr AS (
        SELECT
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
      ),
    r_samples AS (
      SELECT e.sample_time - s.sample_time AS duration
      FROM samples s, samples e
      WHERE
        (s.server_id, s.sample_id) = (sserver_id, start_id) AND
        (e.server_id, e.sample_id) = (sserver_id, end_id)
    )
    SELECT
        st.server_id as server_id,
        st.datid as datid,
        sample_db.datname as dbname,
        st.userid as userid,
        rl.username as username,
        st.queryid as queryid,
        st.toplevel as toplevel,
        sum(st.plans)::bigint as plans,
        (sum(st.plans)*100/NULLIF(min(tot.plans), 0))::float as plans_pct,
        sum(st.calls)::bigint as calls,
        (sum(st.calls)*100/NULLIF(min(tot.calls), 0))::float as calls_pct,
        (sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0))/1000 as total_time,
        (sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0))*100/NULLIF(min(tot.total_time), 0) as total_time_pct,
        sum(st.total_plan_time)/1000::double precision as total_plan_time,
        sum(st.total_plan_time)*100/NULLIF(sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0), 0) as plan_time_pct,
        sum(st.total_exec_time)/1000::double precision as total_exec_time,
        sum(st.total_exec_time)*100/NULLIF(min(tot.total_time), 0) as total_exec_time_pct,
        sum(st.total_exec_time)*100/NULLIF(sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0), 0) as exec_time_pct,
        min(st.min_exec_time) as min_exec_time,
        max(st.max_exec_time) as max_exec_time,
        sum(st.total_exec_time)/NULLIF(sum(st.calls), 0) as mean_exec_time,
        CASE
          WHEN sum(st.calls) = 1 THEN 0
          WHEN sum(st.sum_exec_time_sq) * sum(st.calls) -
            pow(sum(st.total_exec_time::numeric), 2) > 0 THEN
              sqrt(
                (sum(st.sum_exec_time_sq) * sum(st.calls) - pow(sum(st.total_exec_time::numeric), 2)) /
                NULLIF(pow(sum(st.calls), 2), 0)
              )
          ELSE NULL
        END as stddev_exec_time,
        min(st.min_plan_time) as min_plan_time,
        max(st.max_plan_time) as max_plan_time,
        sum(st.total_plan_time)/NULLIF(sum(st.plans), 0) as mean_plan_time,
        CASE
          WHEN sum(st.plans) = 1 THEN 0
          WHEN sum(st.sum_plan_time_sq) * sum(st.plans) -
            pow(sum(st.total_plan_time::numeric), 2) > 0 THEN
              sqrt(
                (sum(st.sum_plan_time_sq) * sum(st.plans) - pow(sum(st.total_plan_time::numeric), 2)) /
                NULLIF(pow(sum(st.plans), 2), 0)
              )
          ELSE NULL
        END as stddev_plan_time,
        sum(st.rows)::bigint as rows,
        sum(st.shared_blks_hit)::bigint as shared_blks_hit,
        (sum(st.shared_blks_hit) * 100 / NULLIF(sum(st.shared_blks_hit) + sum(st.shared_blks_read), 0))::float as shared_hit_pct,
        sum(st.shared_blks_read)::bigint as shared_blks_read,
        (sum(st.shared_blks_read) * 100 / NULLIF(min(tot.shared_blks_read), 0))::float as read_pct,
        (sum(st.shared_blks_hit) + sum(st.shared_blks_read))::bigint as shared_blks_fetched,
        ((sum(st.shared_blks_hit) + sum(st.shared_blks_read)) * 100 / NULLIF(min(tot.shared_blks_hit) + min(tot.shared_blks_read), 0))::float as shared_blks_fetched_pct,
        sum(st.shared_blks_dirtied)::bigint as shared_blks_dirtied,
        (sum(st.shared_blks_dirtied) * 100 / NULLIF(min(tot.shared_blks_dirtied), 0))::float as dirtied_pct,
        sum(st.shared_blks_written)::bigint as shared_blks_written,
        (sum(st.shared_blks_written) * 100 / NULLIF(min(totbgwr.written), 0))::float as tot_written_pct,
        (sum(st.shared_blks_written) * 100 / NULLIF(min(totbgwr.buffers_backend), 0))::float as backend_written_pct,
        sum(st.local_blks_hit)::bigint as local_blks_hit,
        (sum(st.local_blks_hit) * 100 / NULLIF(sum(st.local_blks_hit) + sum(st.local_blks_read),0))::float as local_hit_pct,
        sum(st.local_blks_read)::bigint as local_blks_read,
        (sum(st.local_blks_hit) + sum(st.local_blks_read))::bigint as local_blks_fetched,
        sum(st.local_blks_dirtied)::bigint as local_blks_dirtied,
        sum(st.local_blks_written)::bigint as local_blks_written,
        sum(st.temp_blks_read)::bigint as temp_blks_read,
        sum(st.temp_blks_written)::bigint as temp_blks_written,
        sum(st.shared_blk_read_time)/1000::double precision as shared_blk_read_time,
        sum(st.shared_blk_write_time)/1000::double precision as shared_blk_write_time,
        sum(st.local_blk_read_time)/1000::double precision as local_blk_read_time,
        sum(st.local_blk_write_time)/1000::double precision as local_blk_write_time,
        sum(st.temp_blk_read_time)/1000::double precision as temp_blk_read_time,
        sum(st.temp_blk_write_time)/1000::double precision as temp_blk_write_time,
        (sum(st.shared_blk_read_time) + sum(st.shared_blk_write_time) +
          sum(st.local_blk_read_time) + sum(st.local_blk_write_time)
          )/1000::double precision as io_time,
        (sum(st.shared_blk_read_time) + sum(st.shared_blk_write_time) +
          sum(st.local_blk_read_time) + sum(st.local_blk_write_time)) * 100 /
          NULLIF(min(tot.shared_blk_read_time) + min(tot.shared_blk_write_time) +
            min(tot.local_blk_read_time) + min(tot.local_blk_write_time), 0) as io_time_pct,
        (sum(st.temp_blks_read) * 100 / NULLIF(min(tot.temp_blks_read), 0))::float as temp_read_total_pct,
        (sum(st.temp_blks_written) * 100 / NULLIF(min(tot.temp_blks_written), 0))::float as temp_write_total_pct,
        (sum(st.temp_blk_read_time) + sum(st.temp_blk_write_time)) * 100 /
          NULLIF(min(tot.temp_blk_read_time) + min(tot.temp_blk_write_time),0) as temp_io_time_pct,
        (sum(st.local_blks_read) * 100 / NULLIF(min(tot.local_blks_read), 0))::float as local_read_total_pct,
        (sum(st.local_blks_written) * 100 / NULLIF(min(tot.local_blks_written), 0))::float as local_write_total_pct,
        sum(st.wal_records)::bigint as wal_records,
        sum(st.wal_fpi)::bigint as wal_fpi,
        sum(st.wal_bytes) as wal_bytes,
        (sum(st.wal_bytes) * 100 / NULLIF(min(totbgwr.wal_size), 0))::float wal_bytes_pct,
        -- kcache stats
        COALESCE(sum(kc.exec_user_time), 0.0) + COALESCE(sum(kc.plan_user_time), 0.0) as user_time,
        COALESCE(sum(kc.exec_system_time), 0.0) + COALESCE(sum(kc.plan_system_time), 0.0) as system_time,
        (COALESCE(sum(kc.exec_reads), 0) + COALESCE(sum(kc.plan_reads), 0))::bigint as reads,
        (COALESCE(sum(kc.exec_writes), 0) + COALESCE(sum(kc.plan_writes), 0))::bigint as writes,
        sum(st.jit_functions)::bigint AS jit_functions,
        sum(st.jit_generation_time)/1000::double precision AS jit_generation_time,
        sum(st.jit_inlining_count)::bigint AS jit_inlining_count,
        sum(st.jit_inlining_time)/1000::double precision AS jit_inlining_time,
        sum(st.jit_optimization_count)::bigint AS jit_optimization_count,
        sum(st.jit_optimization_time)/1000::double precision AS jit_optimization_time,
        sum(st.jit_emission_count)::bigint AS jit_emission_count,
        sum(st.jit_emission_time)/1000::double precision AS jit_emission_time,
        sum(st.jit_deform_count)::bigint AS jit_deform_count,
        sum(st.jit_deform_time)/1000::double precision AS jit_deform_time,
        count(DISTINCT st.stats_since) AS stmt_alloc,
        100 - extract(epoch from sum(CASE
            WHEN st.stats_since <= s_prev.sample_time THEN '0'::interval
            ELSE st.stats_since - s_prev.sample_time
          END)) * 100 / -- Possibly lost stats time intervel
          extract(epoch from min(r_samples.duration)) AS stmt_cover
    FROM sample_statements st
        -- User name
        JOIN roles_list rl USING (server_id, userid)
        -- Database name
        JOIN sample_stat_database sample_db
        USING (server_id, sample_id, datid)
        -- kcache join
        LEFT OUTER JOIN sample_kcache kc USING(server_id, sample_id, userid, datid, queryid, toplevel)
        -- Total stats
        CROSS JOIN tot CROSS JOIN totbgwr
        -- Prev sample is needed to calc stat coverage
        JOIN samples s_prev ON (s_prev.server_id, s_prev.sample_id) = (sserver_id, st.sample_id - 1)
        CROSS JOIN r_samples
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY
      st.server_id,
      st.datid,
      sample_db.datname,
      st.userid,
      rl.username,
      st.queryid,
      st.toplevel
$$ LANGUAGE sql;

CREATE FUNCTION top_statements_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid                   oid,
    dbname                  name,
    userid                  oid,
    username                name,
    queryid                 bigint,
    hexqueryid              text,
    toplevel                boolean,
    hashed_ids              text,
    plans                   bigint,
    plans_pct               numeric,
    calls                   bigint,
    calls_pct               numeric,
    total_time              numeric,
    total_time_pct          numeric,
    total_plan_time         numeric,
    plan_time_pct           numeric,
    total_exec_time         numeric,
    total_exec_time_pct     numeric,
    exec_time_pct           numeric,
    min_exec_time           numeric,
    max_exec_time           numeric,
    mean_exec_time          numeric,
    stddev_exec_time        numeric,
    min_plan_time           numeric,
    max_plan_time           numeric,
    mean_plan_time          numeric,
    stddev_plan_time        numeric,
    rows                    bigint,
    shared_blks_hit         bigint,
    shared_hit_pct          numeric,
    shared_blks_read        bigint,
    read_pct                numeric,
    shared_blks_fetched     bigint,
    shared_blks_fetched_pct numeric,
    shared_blks_dirtied     bigint,
    dirtied_pct             numeric,
    shared_blks_written     bigint,
    tot_written_pct         numeric,
    backend_written_pct     numeric,
    local_blks_hit          bigint,
    local_hit_pct           numeric,
    local_blks_read         bigint,
    local_blks_fetched      bigint,
    local_blks_dirtied      bigint,
    local_blks_written      bigint,
    temp_blks_read          bigint,
    temp_blks_written       bigint,
    shared_blk_read_time    numeric,
    shared_blk_write_time   numeric,
    local_blk_read_time    numeric,
    local_blk_write_time   numeric,
    temp_blk_read_time      numeric,
    temp_blk_write_time     numeric,
    io_time                 numeric,
    io_time_pct             numeric,
    temp_read_total_pct     numeric,
    temp_write_total_pct    numeric,
    temp_io_time_pct        numeric,
    local_read_total_pct    numeric,
    local_write_total_pct   numeric,
    wal_records             bigint,
    wal_fpi                 bigint,
    wal_bytes               numeric,
    wal_bytes_fmt           text,
    wal_bytes_pct           numeric,
    user_time               numeric,
    system_time             numeric,
    reads                   bigint,
    writes                  bigint,
    jit_total_time          numeric,
    jit_functions           bigint,
    jit_generation_time     numeric,
    jit_inlining_count      bigint,
    jit_inlining_time       numeric,
    jit_optimization_count  bigint,
    jit_optimization_time   numeric,
    jit_emission_count      bigint,
    jit_emission_time       numeric,
    jit_deform_count        bigint,
    jit_deform_time         numeric,
    stmt_alloc              bigint,
    stmt_cover              numeric,
    sum_tmp_blks            bigint,
    sum_jit_time            numeric,
    ord_total_time          integer,
    ord_plan_time           integer,
    ord_exec_time           integer,
    ord_mean_exec_time      integer,
    ord_calls               integer,
    ord_io_time             integer,
    ord_temp_io_time        integer,
    ord_shared_blocks_fetched integer,
    ord_shared_blocks_read  integer,
    ord_shared_blocks_dirt  integer,
    ord_shared_blocks_written integer,
    ord_wal                 integer,
    ord_temp                integer,
    ord_jit                 integer
)
SET search_path=@extschema@ AS $$
  SELECT
    st.datid,
    st.dbname,
    st.userid,
    st.username,
    st.queryid,
    to_hex(st.queryid) AS hexqueryid,
    st.toplevel,
    left(encode(sha224(convert_to(
      st.userid::text || st.datid::text || st.queryid::text,
      'UTF8')
    ), 'hex'), 10) AS hashed_ids,
    NULLIF(st.plans, 0),
    round(CAST(NULLIF(st.plans_pct, 0.0) AS numeric), 2),
    NULLIF(st.calls, 0),
    round(CAST(NULLIF(st.calls_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.total_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.total_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.total_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.plan_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.total_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.total_exec_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.exec_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.min_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.max_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.mean_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.stddev_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.min_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.max_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.mean_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.stddev_plan_time, 0.0) AS numeric), 2),
    NULLIF(st.rows, 0),
    NULLIF(st.shared_blks_hit, 0),
    round(CAST(NULLIF(st.shared_hit_pct, 0.0) AS numeric), 2),
    NULLIF(st.shared_blks_read, 0),
    round(CAST(NULLIF(st.read_pct, 0.0) AS numeric), 2),
    NULLIF(st.shared_blks_fetched, 0),
    round(CAST(NULLIF(st.shared_blks_fetched_pct, 0.0) AS numeric), 2),
    NULLIF(st.shared_blks_dirtied, 0),
    round(CAST(NULLIF(st.dirtied_pct, 0.0) AS numeric), 2),
    NULLIF(st.shared_blks_written, 0),
    round(CAST(NULLIF(st.tot_written_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.backend_written_pct, 0.0) AS numeric), 2),
    NULLIF(st.local_blks_hit, 0),
    round(CAST(NULLIF(st.local_hit_pct, 0.0) AS numeric), 2),
    NULLIF(st.local_blks_read, 0),
    NULLIF(st.local_blks_fetched, 0),
    NULLIF(st.local_blks_dirtied, 0),
    NULLIF(st.local_blks_written, 0),
    NULLIF(st.temp_blks_read, 0),
    NULLIF(st.temp_blks_written, 0),
    round(CAST(NULLIF(st.shared_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.shared_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.local_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.local_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.temp_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.temp_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.io_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.io_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.temp_read_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.temp_write_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.temp_io_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.local_read_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.local_write_total_pct, 0.0) AS numeric), 2),
    NULLIF(st.wal_records, 0),
    NULLIF(st.wal_fpi, 0),
    NULLIF(st.wal_bytes, 0),
    pg_size_pretty(NULLIF(st.wal_bytes, 0)),
    round(CAST(NULLIF(st.wal_bytes_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.system_time, 0.0) AS numeric), 2),
    NULLIF(st.reads, 0),
    NULLIF(st.writes, 0),
    round(NULLIF(CAST(st.jit_generation_time + st.jit_inlining_time +
      st.jit_optimization_time + st.jit_emission_time +
      st.jit_deform_time AS numeric), 0), 2) AS jit_total_time,
    NULLIF(st.jit_functions, 0),
    round(CAST(NULLIF(st.jit_generation_time, 0.0) AS numeric), 2),
    NULLIF(st.jit_inlining_count, 0),
    round(CAST(NULLIF(st.jit_inlining_time, 0.0) AS numeric), 2),
    NULLIF(st.jit_optimization_count, 0),
    round(CAST(NULLIF(st.jit_optimization_time, 0.0) AS numeric), 2),
    NULLIF(st.jit_emission_count, 0),
    round(CAST(NULLIF(st.jit_emission_time, 0.0) AS numeric), 2),
    NULLIF(st.jit_deform_count, 0),
    round(CAST(NULLIF(st.jit_deform_time, 0.0) AS numeric), 2),
    NULLIF(st.stmt_alloc, 0),
    round(CAST(NULLIF(st.stmt_cover, 0.0) AS numeric)),
    COALESCE(st.temp_blks_read, 0) +
        COALESCE(st.temp_blks_written, 0) +
        COALESCE(st.local_blks_read, 0) +
        COALESCE(st.local_blks_written, 0) AS sum_tmp_blks,
    (st.jit_generation_time + st.jit_inlining_time +
        st.jit_optimization_time + st.jit_emission_time)::numeric AS sum_jit_time,
    row_number() OVER (ORDER BY st.total_time DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer AS ord_total_time,
    CASE WHEN st.total_plan_time > 0 THEN
      row_number() OVER (ORDER BY st.total_plan_time DESC NULLS LAST,
        st.total_exec_time DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_plan_time,
    CASE WHEN st.total_exec_time > 0 THEN
      row_number() OVER (ORDER BY st.total_exec_time DESC NULLS LAST,
        st.total_time DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_exec_time,
    CASE WHEN st.mean_exec_time > 0 THEN
      row_number() OVER (ORDER BY st.mean_exec_time DESC NULLS LAST,
        st.total_time DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_mean_exec_time,
    CASE WHEN st.calls > 0 THEN
      row_number() OVER (ORDER BY st.calls DESC NULLS LAST,
        st.total_time DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_calls,
    CASE WHEN st.io_time > 0 THEN
      row_number() OVER (ORDER BY st.io_time DESC NULLS LAST,
        st.total_time DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_io_time,
    CASE WHEN COALESCE(st.temp_blk_read_time, 0.0) + COALESCE(st.temp_blk_write_time, 0.0) > 0 THEN
      row_number() OVER (ORDER BY COALESCE(st.temp_blk_read_time, 0.0) + COALESCE(st.temp_blk_write_time, 0.0)
        DESC NULLS LAST,
          st.total_time DESC NULLS LAST,
          st.datid,
          st.userid,
          st.queryid,
          st.toplevel)::integer
    ELSE NULL END AS ord_temp_io_time,
    CASE WHEN st.shared_blks_fetched > 0 THEN
      row_number() OVER (ORDER BY st.shared_blks_fetched DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_shared_blocks_fetched,
    CASE WHEN st.shared_blks_read > 0 THEN
      row_number() OVER (ORDER BY st.shared_blks_read DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_shared_blocks_read,
    CASE WHEN st.shared_blks_dirtied > 0 THEN
      row_number() OVER (ORDER BY st.shared_blks_dirtied DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_shared_blocks_dirt,
    CASE WHEN st.shared_blks_written > 0 THEN
      row_number() OVER (ORDER BY st.shared_blks_written DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_shared_blocks_written,
    CASE WHEN st.wal_bytes > 0 THEN
      row_number() OVER (ORDER BY st.wal_bytes DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_wal,
    CASE WHEN COALESCE(st.temp_blks_read, 0) +
        COALESCE(st.temp_blks_written, 0) +
        COALESCE(st.local_blks_read, 0) +
        COALESCE(st.local_blks_written, 0) > 0 THEN
      row_number() OVER (ORDER BY COALESCE(st.temp_blks_read, 0) +
          COALESCE(st.temp_blks_written, 0) +
          COALESCE(st.local_blks_read, 0) +
          COALESCE(st.local_blks_written, 0) DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_temp,
    CASE WHEN
        st.jit_generation_time + st.jit_inlining_time +
        st.jit_optimization_time + st.jit_emission_time > 0 THEN
      row_number() OVER (ORDER BY st.jit_generation_time +
        st.jit_inlining_time + st.jit_optimization_time +
        st.jit_emission_time DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_jit
  FROM
    top_statements(sserver_id, start_id, end_id) st
$$ LANGUAGE sql;

CREATE FUNCTION top_statements_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid                    oid,
    dbname                   name,
    userid                   oid,
    username                 name,
    queryid                  bigint,
    hexqueryid               text,
    toplevel                 boolean,
    hashed_ids               text,
    -- First Interval
    plans1                   bigint,
    plans_pct1               numeric,
    calls1                   bigint,
    calls_pct1               numeric,
    total_time1              numeric,
    total_time_pct1          numeric,
    total_plan_time1         numeric,
    plan_time_pct1           numeric,
    total_exec_time1         numeric,
    total_exec_time_pct1     numeric,
    exec_time_pct1           numeric,
    min_exec_time1           numeric,
    max_exec_time1           numeric,
    mean_exec_time1          numeric,
    stddev_exec_time1        numeric,
    min_plan_time1           numeric,
    max_plan_time1           numeric,
    mean_plan_time1          numeric,
    stddev_plan_time1        numeric,
    rows1                    bigint,
    shared_blks_hit1         bigint,
    shared_hit_pct1          numeric,
    shared_blks_read1        bigint,
    read_pct1                numeric,
    shared_blks_fetched1     bigint,
    shared_blks_fetched_pct1 numeric,
    shared_blks_dirtied1     bigint,
    dirtied_pct1             numeric,
    shared_blks_written1     bigint,
    tot_written_pct1         numeric,
    backend_written_pct1     numeric,
    local_blks_hit1          bigint,
    local_hit_pct1           numeric,
    local_blks_read1         bigint,
    local_blks_fetched1      bigint,
    local_blks_dirtied1      bigint,
    local_blks_written1      bigint,
    temp_blks_read1          bigint,
    temp_blks_written1       bigint,
    shared_blk_read_time1    numeric,
    shared_blk_write_time1   numeric,
    local_blk_read_time1    numeric,
    local_blk_write_time1   numeric,
    temp_blk_read_time1      numeric,
    temp_blk_write_time1     numeric,
    io_time1                 numeric,
    io_time_pct1             numeric,
    temp_read_total_pct1     numeric,
    temp_write_total_pct1    numeric,
    temp_io_time_pct1        numeric,
    local_read_total_pct1    numeric,
    local_write_total_pct1   numeric,
    wal_records1             bigint,
    wal_fpi1                 bigint,
    wal_bytes1               numeric,
    wal_bytes_fmt1           text,
    wal_bytes_pct1           numeric,
    user_time1               numeric,
    system_time1             numeric,
    reads1                   bigint,
    writes1                  bigint,
    jit_total_time1          numeric,
    jit_functions1           bigint,
    jit_generation_time1     numeric,
    jit_inlining_count1      bigint,
    jit_inlining_time1       numeric,
    jit_optimization_count1  bigint,
    jit_optimization_time1   numeric,
    jit_emission_count1      bigint,
    jit_emission_time1       numeric,
    jit_deform_count1        bigint,
    jit_deform_time1         numeric,
    stmt_alloc1              bigint,
    stmt_cover1              numeric,
    --Second Interval
    plans2                   bigint,
    plans_pct2               numeric,
    calls2                   bigint,
    calls_pct2               numeric,
    total_time2              numeric,
    total_time_pct2          numeric,
    total_plan_time2         numeric,
    plan_time_pct2           numeric,
    total_exec_time2         numeric,
    total_exec_time_pct2     numeric,
    exec_time_pct2           numeric,
    min_exec_time2           numeric,
    max_exec_time2           numeric,
    mean_exec_time2          numeric,
    stddev_exec_time2        numeric,
    min_plan_time2           numeric,
    max_plan_time2           numeric,
    mean_plan_time2          numeric,
    stddev_plan_time2        numeric,
    rows2                    bigint,
    shared_blks_hit2         bigint,
    shared_hit_pct2          numeric,
    shared_blks_read2        bigint,
    read_pct2                numeric,
    shared_blks_fetched2     bigint,
    shared_blks_fetched_pct2 numeric,
    shared_blks_dirtied2     bigint,
    dirtied_pct2             numeric,
    shared_blks_written2     bigint,
    tot_written_pct2         numeric,
    backend_written_pct2     numeric,
    local_blks_hit2          bigint,
    local_hit_pct2           numeric,
    local_blks_read2         bigint,
    local_blks_fetched2      bigint,
    local_blks_dirtied2      bigint,
    local_blks_written2      bigint,
    temp_blks_read2          bigint,
    temp_blks_written2       bigint,
    shared_blk_read_time2    numeric,
    shared_blk_write_time2   numeric,
    local_blk_read_time2     numeric,
    local_blk_write_time2    numeric,
    temp_blk_read_time2      numeric,
    temp_blk_write_time2     numeric,
    io_time2                 numeric,
    io_time_pct2             numeric,
    temp_read_total_pct2     numeric,
    temp_write_total_pct2    numeric,
    temp_io_time_pct2        numeric,
    local_read_total_pct2    numeric,
    local_write_total_pct2   numeric,
    wal_records2             bigint,
    wal_fpi2                 bigint,
    wal_bytes2               numeric,
    wal_bytes_fmt2           text,
    wal_bytes_pct2           numeric,
    user_time2               numeric,
    system_time2             numeric,
    reads2                   bigint,
    writes2                  bigint,
    jit_total_time2          numeric,
    jit_functions2           bigint,
    jit_generation_time2     numeric,
    jit_inlining_count2      bigint,
    jit_inlining_time2       numeric,
    jit_optimization_count2  bigint,
    jit_optimization_time2   numeric,
    jit_emission_count2      bigint,
    jit_emission_time2       numeric,
    jit_deform_count2        bigint,
    jit_deform_time2         numeric,
    stmt_alloc2              bigint,
    stmt_cover2              numeric,
    -- Filter and ordering fields
    sum_tmp_blks             bigint,
    sum_jit_time             numeric,
    ord_total_time           integer,
    ord_plan_time            integer,
    ord_exec_time            integer,
    ord_mean_exec_time       integer,
    ord_calls                integer,
    ord_io_time              integer,
    ord_temp_io_time         integer,
    ord_shared_blocks_fetched integer,
    ord_shared_blocks_read   integer,
    ord_shared_blocks_dirt   integer,
    ord_shared_blocks_written integer,
    ord_wal                  integer,
    ord_temp                 integer,
    ord_jit                  integer
)
SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(st1.datid,st2.datid) as datid,
    COALESCE(st1.dbname,st2.dbname) as dbname,
    COALESCE(st1.userid,st2.userid) as userid,
    COALESCE(st1.username,st2.username) as username,
    COALESCE(st1.queryid,st2.queryid) AS queryid,
    to_hex(COALESCE(st1.queryid,st2.queryid)) as hexqueryid,
    COALESCE(st1.toplevel,st2.toplevel) as toplevel,
    left(encode(sha224(convert_to(
      COALESCE(st1.userid,st2.userid)::text ||
      COALESCE(st1.datid,st2.datid)::text ||
      COALESCE(st1.queryid,st2.queryid)::text,
      'UTF8')
    ), 'hex'), 10) AS hashed_ids,
    -- First Interval
    NULLIF(st1.plans, 0),
    round(CAST(NULLIF(st1.plans_pct, 0.0) AS numeric), 2),
    NULLIF(st1.calls, 0),
    round(CAST(NULLIF(st1.calls_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.total_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.total_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.total_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.plan_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.total_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.total_exec_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.exec_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.min_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.max_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.mean_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.stddev_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.min_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.max_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.mean_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.stddev_plan_time, 0.0) AS numeric), 2),
    NULLIF(st1.rows, 0),
    NULLIF(st1.shared_blks_hit, 0),
    round(CAST(NULLIF(st1.shared_hit_pct, 0.0) AS numeric), 2),
    NULLIF(st1.shared_blks_read, 0),
    round(CAST(NULLIF(st1.read_pct, 0.0) AS numeric), 2),
    NULLIF(st1.shared_blks_fetched, 0),
    round(CAST(NULLIF(st1.shared_blks_fetched_pct, 0.0) AS numeric), 2),
    NULLIF(st1.shared_blks_dirtied, 0),
    round(CAST(NULLIF(st1.dirtied_pct, 0.0) AS numeric), 2),
    NULLIF(st1.shared_blks_written, 0),
    round(CAST(NULLIF(st1.tot_written_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.backend_written_pct, 0.0) AS numeric), 2),
    NULLIF(st1.local_blks_hit, 0),
    round(CAST(NULLIF(st1.local_hit_pct, 0.0) AS numeric), 2),
    NULLIF(st1.local_blks_read, 0),
    NULLIF(st1.local_blks_fetched, 0),
    NULLIF(st1.local_blks_dirtied, 0),
    NULLIF(st1.local_blks_written, 0),
    NULLIF(st1.temp_blks_read, 0),
    NULLIF(st1.temp_blks_written, 0),
    round(CAST(NULLIF(st1.shared_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.shared_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.local_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.local_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.temp_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.temp_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.io_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.io_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.temp_read_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.temp_write_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.temp_io_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.local_read_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.local_write_total_pct, 0.0) AS numeric), 2),
    NULLIF(st1.wal_records, 0),
    NULLIF(st1.wal_fpi, 0),
    NULLIF(st1.wal_bytes, 0),
    pg_size_pretty(NULLIF(st1.wal_bytes, 0)),
    round(CAST(NULLIF(st1.wal_bytes_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.system_time, 0.0) AS numeric), 2),
    NULLIF(st1.reads, 0),
    NULLIF(st1.writes, 0),
    round(NULLIF(CAST(st1.jit_generation_time + st1.jit_inlining_time +
      st1.jit_optimization_time + st1.jit_emission_time +
      st1.jit_deform_time AS numeric), 0), 2),
    NULLIF(st1.jit_functions, 0),
    round(CAST(NULLIF(st1.jit_generation_time, 0.0) AS numeric), 2),
    NULLIF(st1.jit_inlining_count, 0),
    round(CAST(NULLIF(st1.jit_inlining_time, 0.0) AS numeric), 2),
    NULLIF(st1.jit_optimization_count, 0),
    round(CAST(NULLIF(st1.jit_optimization_time, 0.0) AS numeric), 2),
    NULLIF(st1.jit_emission_count, 0),
    round(CAST(NULLIF(st1.jit_emission_time, 0.0) AS numeric), 2),
    NULLIF(st1.jit_deform_count, 0),
    round(CAST(NULLIF(st1.jit_deform_time, 0.0) AS numeric), 2),
    NULLIF(st1.stmt_alloc, 0),
    round(CAST(NULLIF(st1.stmt_cover, 0.0) AS numeric)),
    -- Second Interval
    NULLIF(st2.plans, 0),
    round(CAST(NULLIF(st2.plans_pct, 0.0) AS numeric), 2),
    NULLIF(st2.calls, 0),
    round(CAST(NULLIF(st2.calls_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.total_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.total_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.total_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.plan_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.total_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.total_exec_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.exec_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.min_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.max_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.mean_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.stddev_exec_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.min_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.max_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.mean_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.stddev_plan_time, 0.0) AS numeric), 2),
    NULLIF(st2.rows, 0),
    NULLIF(st2.shared_blks_hit, 0),
    round(CAST(NULLIF(st2.shared_hit_pct, 0.0) AS numeric), 2),
    NULLIF(st2.shared_blks_read, 0),
    round(CAST(NULLIF(st2.read_pct, 0.0) AS numeric), 2),
    NULLIF(st2.shared_blks_fetched, 0),
    round(CAST(NULLIF(st2.shared_blks_fetched_pct, 0.0) AS numeric), 2),
    NULLIF(st2.shared_blks_dirtied, 0),
    round(CAST(NULLIF(st2.dirtied_pct, 0.0) AS numeric), 2),
    NULLIF(st2.shared_blks_written, 0),
    round(CAST(NULLIF(st2.tot_written_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.backend_written_pct, 0.0) AS numeric), 2),
    NULLIF(st2.local_blks_hit, 0),
    round(CAST(NULLIF(st2.local_hit_pct, 0.0) AS numeric), 2),
    NULLIF(st2.local_blks_read, 0),
    NULLIF(st2.local_blks_fetched, 0),
    NULLIF(st2.local_blks_dirtied, 0),
    NULLIF(st2.local_blks_written, 0),
    NULLIF(st2.temp_blks_read, 0),
    NULLIF(st2.temp_blks_written, 0),
    round(CAST(NULLIF(st2.shared_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.shared_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.local_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.local_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.temp_blk_read_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.temp_blk_write_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.io_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.io_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.temp_read_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.temp_write_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.temp_io_time_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.local_read_total_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.local_write_total_pct, 0.0) AS numeric), 2),
    NULLIF(st2.wal_records, 0),
    NULLIF(st2.wal_fpi, 0),
    NULLIF(st2.wal_bytes, 0),
    pg_size_pretty(NULLIF(st2.wal_bytes, 0)),
    round(CAST(NULLIF(st2.wal_bytes_pct, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.system_time, 0.0) AS numeric), 2),
    NULLIF(st2.reads, 0),
    NULLIF(st2.writes, 0),
    round(NULLIF(CAST(st2.jit_generation_time + st2.jit_inlining_time +
      st2.jit_optimization_time + st2.jit_emission_time +
      st2.jit_deform_time AS numeric), 0), 2),
    NULLIF(st2.jit_functions, 0),
    round(CAST(NULLIF(st2.jit_generation_time, 0.0) AS numeric), 2),
    NULLIF(st2.jit_inlining_count, 0),
    round(CAST(NULLIF(st2.jit_inlining_time, 0.0) AS numeric), 2),
    NULLIF(st2.jit_optimization_count, 0),
    round(CAST(NULLIF(st2.jit_optimization_time, 0.0) AS numeric), 2),
    NULLIF(st2.jit_emission_count, 0),
    round(CAST(NULLIF(st2.jit_emission_time, 0.0) AS numeric), 2),
    NULLIF(st2.jit_deform_count, 0),
    round(CAST(NULLIF(st2.jit_deform_time, 0.0) AS numeric), 2),
    NULLIF(st2.stmt_alloc, 0),
    round(CAST(NULLIF(st2.stmt_cover, 0.0) AS numeric)),
    -- Filter and ordering fields
    COALESCE(st1.temp_blks_read, 0) +
        COALESCE(st1.temp_blks_written, 0) +
        COALESCE(st1.local_blks_read, 0) +
        COALESCE(st1.local_blks_written, 0) +
        COALESCE(st2.temp_blks_read, 0) +
        COALESCE(st2.temp_blks_written, 0) +
        COALESCE(st2.local_blks_read, 0) +
        COALESCE(st2.local_blks_written, 0) AS sum_tmp_blks,
    (st1.jit_generation_time + st1.jit_inlining_time +
        st1.jit_optimization_time + st1.jit_emission_time +
        st2.jit_generation_time + st2.jit_inlining_time +
        st2.jit_optimization_time + st2.jit_emission_time)::numeric AS sum_jit_time,
    row_number() OVER (ORDER BY COALESCE(st1.total_time, 0.0) +
      COALESCE(st2.total_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer AS ord_total_time,

    CASE WHEN COALESCE(st1.total_plan_time, 0.0) +
        COALESCE(st2.total_plan_time, 0.0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.total_plan_time, 0.0) +
      COALESCE(st2.total_plan_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.total_exec_time, 0.0) +
      COALESCE(st2.total_exec_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_plan_time,

    CASE WHEN COALESCE(st1.total_exec_time, 0.0) +
        COALESCE(st2.total_exec_time, 0.0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.total_exec_time, 0.0) +
       COALESCE(st2.total_exec_time, 0.0) DESC NULLS LAST,
       COALESCE(st1.total_time, 0.0) +
       COALESCE(st2.total_time, 0.0) DESC NULLS LAST,
       COALESCE(st1.datid,st2.datid),
       COALESCE(st1.userid,st2.userid),
       COALESCE(st1.queryid,st2.queryid),
       COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_exec_time,

    CASE WHEN COALESCE(st1.mean_exec_time, 0.0) +
        COALESCE(st2.mean_exec_time, 0.0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.mean_exec_time, 0.0) +
       COALESCE(st2.mean_exec_time, 0.0) DESC NULLS LAST,
       COALESCE(st1.total_time, 0.0) +
       COALESCE(st2.total_time, 0.0) DESC NULLS LAST,
       COALESCE(st1.datid,st2.datid),
       COALESCE(st1.userid,st2.userid),
       COALESCE(st1.queryid,st2.queryid),
       COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_mean_exec_time,

    CASE WHEN COALESCE(st1.calls, 0) +
        COALESCE(st2.calls, 0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.calls, 0) +
      COALESCE(st2.calls, 0) DESC NULLS LAST,
      COALESCE(st1.total_time, 0.0) +
      COALESCE(st2.total_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_calls,

    CASE WHEN COALESCE(st1.io_time, 0.0) +
        COALESCE(st2.io_time, 0.0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.io_time, 0.0) +
      COALESCE(st2.io_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.total_time, 0.0) +
      COALESCE(st2.total_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_io_time,

    CASE WHEN COALESCE(st1.temp_blk_read_time, 0.0) + COALESCE(st2.temp_blk_read_time, 0.0) +
        COALESCE(st1.temp_blk_write_time, 0.0) + COALESCE(st2.temp_blk_write_time, 0.0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.temp_blk_read_time, 0.0) + COALESCE(st2.temp_blk_read_time, 0.0) +
        COALESCE(st1.temp_blk_write_time, 0.0) + COALESCE(st2.temp_blk_write_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.total_time, 0.0) +
      COALESCE(st2.total_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_temp_io_time,

    CASE WHEN COALESCE(st1.shared_blks_fetched, 0) +
        COALESCE(st2.shared_blks_fetched, 0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.shared_blks_fetched, 0) +
      COALESCE(st2.shared_blks_fetched, 0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_shared_blocks_fetched,

    CASE WHEN COALESCE(st1.shared_blks_read, 0) +
        COALESCE(st2.shared_blks_read, 0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.shared_blks_read, 0) +
      COALESCE(st2.shared_blks_read, 0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_shared_blocks_read,

    CASE WHEN COALESCE(st1.shared_blks_dirtied, 0) +
        COALESCE(st2.shared_blks_dirtied, 0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.shared_blks_dirtied, 0) +
      COALESCE(st2.shared_blks_dirtied, 0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_shared_blocks_dirt,

    CASE WHEN COALESCE(st1.shared_blks_written, 0) +
        COALESCE(st2.shared_blks_written, 0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.shared_blks_written, 0) +
      COALESCE(st2.shared_blks_written, 0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_shared_blocks_written,

    CASE WHEN COALESCE(st1.wal_bytes, 0) +
        COALESCE(st2.wal_bytes, 0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.wal_bytes, 0) +
      COALESCE(st2.wal_bytes, 0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_wal,

    CASE WHEN COALESCE(st1.temp_blks_read, 0) +
        COALESCE(st1.temp_blks_written, 0) +
        COALESCE(st1.local_blks_read, 0) +
        COALESCE(st1.local_blks_written, 0) +
        COALESCE(st2.temp_blks_read, 0) +
        COALESCE(st2.temp_blks_written, 0) +
        COALESCE(st2.local_blks_read, 0) +
        COALESCE(st2.local_blks_written, 0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.temp_blks_read, 0) +
      COALESCE(st1.temp_blks_written, 0) +
      COALESCE(st1.local_blks_read, 0) +
      COALESCE(st1.local_blks_written, 0) +
      COALESCE(st2.temp_blks_read, 0) +
      COALESCE(st2.temp_blks_written, 0) +
      COALESCE(st2.local_blks_read, 0) +
      COALESCE(st2.local_blks_written, 0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_temp,

    CASE WHEN
        COALESCE(st1.jit_generation_time + st1.jit_inlining_time +
        st1.jit_optimization_time + st1.jit_emission_time, 0.0) +
        COALESCE(st2.jit_generation_time + st2.jit_inlining_time +
        st2.jit_optimization_time + st2.jit_emission_time, 0.0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.jit_generation_time + st1.jit_inlining_time +
      st1.jit_optimization_time + st1.jit_emission_time, 0.0) +
      COALESCE(st2.jit_generation_time + st2.jit_inlining_time +
      st2.jit_optimization_time + st2.jit_emission_time, 0.0) DESC NULLS LAST,
      COALESCE(st1.datid,st2.datid),
      COALESCE(st1.userid,st2.userid),
      COALESCE(st1.queryid,st2.queryid),
      COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_jit
  FROM top_statements(sserver_id, start1_id, end1_id) st1
      FULL OUTER JOIN top_statements(sserver_id, start2_id, end2_id) st2 USING
        (server_id, datid, userid, queryid, toplevel)
$$ LANGUAGE sql;

CREATE FUNCTION report_queries_format(IN report_context jsonb, IN sserver_id integer, IN queries_list jsonb,
  IN start1_id integer, IN end1_id integer, IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  hexqueryid  text,
  query_texts jsonb
)
SET search_path=@extschema@ AS $$
DECLARE
    c_queries CURSOR(lim integer)
    FOR
    SELECT
      queryid,
      ord,
      rowspan,
      replace(query, '<', '&lt;') AS query
    FROM (
      SELECT
      queryid,
      row_number() OVER (PARTITION BY queryid
        ORDER BY
          last_sample_id DESC NULLS FIRST,
          queryid_md5 DESC NULLS FIRST
        ) ord,
      -- Calculate a value for statement rowspan attribute
      least(count(*) OVER (PARTITION BY queryid),lim) rowspan,
      query
      FROM (
        SELECT DISTINCT
          server_id,
          queryid,
          queryid_md5
        FROM
          jsonb_to_recordset(queries_list) ql(
            userid   bigint,
            datid    bigint,
            queryid  bigint
          )
          JOIN sample_statements ss USING (datid, userid, queryid)
        WHERE
          ss.server_id = sserver_id
          AND (
            sample_id BETWEEN start1_id AND end1_id
            OR sample_id BETWEEN start2_id AND end2_id
          )
      ) queryids
      JOIN stmt_list USING (server_id, queryid_md5)
      WHERE query IS NOT NULL
    ) ord_stmt_v
    WHERE ord <= lim
    ORDER BY
      queryid ASC,
      ord ASC;

    qr_result         RECORD;
    qlen_limit        integer;
    lim               CONSTANT integer := 3;
BEGIN
    IF NOT has_column_privilege('stmt_list', 'query', 'SELECT') THEN
      -- Return empty set when permissions denied to see query text
      hexqueryid := '';
      query_texts := '[]'::jsonb;
      query_texts := jsonb_insert(query_texts, '{-1}',
        to_jsonb('You must be a member of pg_read_all_stats to access query texts'::text));
      RETURN NEXT;
      RETURN;
    END IF;
    qlen_limit := (report_context #>> '{report_properties,max_query_length}')::integer;
    FOR qr_result IN c_queries(lim)
    LOOP
        IF qr_result.ord = 1 THEN
          hexqueryid := to_hex(qr_result.queryid);
          query_texts := '[]'::jsonb;
        END IF;
        query_texts := jsonb_insert(query_texts, '{-1}',
          to_jsonb(left(qr_result.query, qlen_limit)));
        IF qr_result.ord = qr_result.rowspan THEN
          RETURN NEXT;
        END IF;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
