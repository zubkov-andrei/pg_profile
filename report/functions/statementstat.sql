/* ===== Statements stats functions ===== */
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
    calls                   bigint,
    total_time              double precision,
    total_plan_time         double precision,
    total_exec_time         double precision,
    min_exec_time           double precision,
    max_exec_time           double precision,
    sum_exec_time_sq        numeric,
    min_plan_time           double precision,
    max_plan_time           double precision,
    sum_plan_time_sq        numeric,
    rows                    bigint,
    shared_blks_hit         bigint,
    shared_blks_read        bigint,
    shared_blks_dirtied     bigint,
    shared_blks_written     bigint,
    local_blks_hit          bigint,
    local_blks_read         bigint,
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
    wal_records             bigint,
    wal_fpi                 bigint,
    wal_bytes               numeric,
    wal_buffers_full        numeric,
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
    parallel_workers_to_launch  bigint,
    parallel_workers_launched   bigint,
    generic_plan_calls  bigint,
    custom_plan_calls   bigint,
    skipped_time            double precision
) SET search_path=@extschema@ AS $$
    SELECT
        st.server_id as server_id,
        st.datid as datid,
        sample_db.datname as dbname,
        st.userid as userid,
        rl.username as username,
        st.queryid as queryid,
        st.toplevel as toplevel,
        sum(st.plans)::bigint as plans,
        sum(st.calls)::bigint as calls,
        (sum(st.total_exec_time) + COALESCE(sum(st.total_plan_time), 0.0))/1000 as total_time,
        sum(st.total_plan_time)::double precision as total_plan_time,
        sum(st.total_exec_time)::double precision as total_exec_time,
        min(st.min_exec_time) as min_exec_time,
        max(st.max_exec_time) as max_exec_time,
        sum(st.sum_exec_time_sq) as sum_exec_time_sq,
        min(st.min_plan_time) as min_plan_time,
        max(st.max_plan_time) as max_plan_time,
        sum(st.sum_plan_time_sq) as sum_plan_time_sq,
        sum(st.rows)::bigint as rows,
        sum(st.shared_blks_hit)::bigint as shared_blks_hit,
        sum(st.shared_blks_read)::bigint as shared_blks_read,
        sum(st.shared_blks_dirtied)::bigint as shared_blks_dirtied,
        sum(st.shared_blks_written)::bigint as shared_blks_written,
        sum(st.local_blks_hit)::bigint as local_blks_hit,
        sum(st.local_blks_read)::bigint as local_blks_read,
        sum(st.local_blks_dirtied)::bigint as local_blks_dirtied,
        sum(st.local_blks_written)::bigint as local_blks_written,
        sum(st.temp_blks_read)::bigint as temp_blks_read,
        sum(st.temp_blks_written)::bigint as temp_blks_written,
        sum(st.shared_blk_read_time) as shared_blk_read_time,
        sum(st.shared_blk_write_time) as shared_blk_write_time,
        sum(st.local_blk_read_time) as local_blk_read_time,
        sum(st.local_blk_write_time) as local_blk_write_time,
        sum(st.temp_blk_read_time) as temp_blk_read_time,
        sum(st.temp_blk_write_time) as temp_blk_write_time,
        coalesce(sum(st.shared_blk_read_time), 0) +
          coalesce(sum(st.shared_blk_write_time), 0) +
          coalesce(sum(st.local_blk_read_time), 0) +
          coalesce(sum(st.local_blk_write_time), 0)
          as io_time,
        sum(st.wal_records)::bigint as wal_records,
        sum(st.wal_fpi)::bigint as wal_fpi,
        sum(st.wal_bytes) as wal_bytes,
        sum(st.wal_buffers_full) as wal_buffers_full,
        -- kcache stats
        COALESCE(sum(kc.exec_user_time), 0.0) + COALESCE(sum(kc.plan_user_time), 0.0) as user_time,
        COALESCE(sum(kc.exec_system_time), 0.0) + COALESCE(sum(kc.plan_system_time), 0.0) as system_time,
        (COALESCE(sum(kc.exec_reads), 0) + COALESCE(sum(kc.plan_reads), 0))::bigint as reads,
        (COALESCE(sum(kc.exec_writes), 0) + COALESCE(sum(kc.plan_writes), 0))::bigint as writes,
        sum(st.jit_functions)::bigint AS jit_functions,
        sum(st.jit_generation_time) AS jit_generation_time,
        sum(st.jit_inlining_count)::bigint AS jit_inlining_count,
        sum(st.jit_inlining_time) AS jit_inlining_time,
        sum(st.jit_optimization_count)::bigint AS jit_optimization_count,
        sum(st.jit_optimization_time) AS jit_optimization_time,
        sum(st.jit_emission_count)::bigint AS jit_emission_count,
        sum(st.jit_emission_time) AS jit_emission_time,
        sum(st.jit_deform_count)::bigint AS jit_deform_count,
        sum(st.jit_deform_time)/1000::double precision AS jit_deform_time,
        sum(st.parallel_workers_to_launch)::bigint AS parallel_workers_to_launch,
        sum(st.parallel_workers_launched)::bigint AS parallel_workers_launched,
        sum(st.generic_plan_calls)::bigint AS generic_plan_calls,
        sum(st.custom_plan_calls)::bigint AS custom_plan_calls,
        extract(epoch from sum(CASE
            WHEN st.stats_since <= s_prev.sample_time THEN '0'::interval
            ELSE st.stats_since - s_prev.sample_time
          END)) AS skipped_time
    FROM sample_statements st
        -- User name
        JOIN roles_list rl USING (server_id, userid)
        -- Database name
        JOIN sample_stat_database sample_db
        USING (server_id, sample_id, datid)
        -- kcache join
        LEFT OUTER JOIN sample_kcache kc USING(server_id, sample_id, userid, datid, queryid, toplevel)
        -- Prev sample is needed to calc stat coverage
        JOIN samples s_prev ON (s_prev.server_id, s_prev.sample_id) = (sserver_id, st.sample_id - 1)
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
    queryid                 text,
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
    wal_buffers_full        numeric,
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
    parallel_workers_to_launch  bigint,
    parallel_workers_launched   bigint,
    generic_plan_calls      bigint,
    custom_plan_calls       bigint,
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
    ord_jit                 integer,
    ord_wrkrs_cnt           integer
)
SET search_path=@extschema@ AS $$
  SELECT
    st.datid,
    st.dbname,
    st.userid,
    st.username,
    st.queryid::text,
    to_hex(st.queryid) AS hexqueryid,
    st.toplevel,
    left(encode(sha224(convert_to(
      st.userid::text || st.datid::text || st.queryid::text,
      'UTF8')
    ), 'hex'), 10) AS hashed_ids,
    NULLIF(st.plans, 0),
    round(CAST(NULLIF(st.plans * 100 / NULLIF(tot.plans, 0), 0.0) AS numeric), 2) as plans_pct,
    NULLIF(st.calls, 0),
    round(CAST(NULLIF(st.calls * 100 / NULLIF(tot.calls, 0), 0.0) AS numeric), 2) as calls_pct,
    round(CAST(NULLIF(st.total_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(
      (st.total_exec_time + COALESCE(st.total_plan_time, 0)) * 100 /
      NULLIF(tot.total_time, 0), 0.0) AS numeric), 2) as total_time_pct,
    round(CAST(NULLIF(st.total_plan_time / 1000, 0.0) AS numeric), 2) as total_plan_time,
    round(CAST(NULLIF(st.total_plan_time * 100 /
      NULLIF(st.total_exec_time + COALESCE(st.total_plan_time, 0), 0), 0.0) AS numeric), 2)
      as plan_time_pct,
    round(CAST(NULLIF(st.total_exec_time / 1000, 0.0) AS numeric), 2) as total_exec_time,
    round(CAST(NULLIF(st.total_exec_time * 100 /
      NULLIF(tot.total_time, 0), 0.0) AS numeric), 2) as total_exec_time_pct,
    round(CAST(NULLIF(st.total_exec_time * 100 /
      NULLIF(st.total_exec_time + COALESCE(st.total_plan_time, 0.0), 0), 0.0) AS numeric), 2)
      as exec_time_pct,
    round(CAST(NULLIF(st.min_exec_time, 0.0) AS numeric), 2) as min_exec_time,
    round(CAST(NULLIF(st.max_exec_time, 0.0) AS numeric), 2) as max_exec_time,
    round(CAST(NULLIF(st.total_exec_time / NULLIF(st.calls, 0), 0.0) AS numeric), 2) as mean_exec_time,
    CASE
      WHEN st.calls = 1 THEN 0
      WHEN st.sum_exec_time_sq * st.calls -
        pow(st.total_exec_time, 2) > 0 THEN
          round(sqrt(
            (st.sum_exec_time_sq * st.calls - pow(st.total_exec_time, 2)) /
            NULLIF(pow(st.calls, 2), 0)
          )::numeric, 2)
      ELSE NULL
    END as stddev_exec_time,
    round(CAST(NULLIF(st.min_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.max_plan_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.total_plan_time / NULLIF(st.plans, 0), 0.0) AS numeric), 2),
    CASE
      WHEN st.plans = 1 THEN 0
      WHEN st.sum_plan_time_sq * st.plans -
        pow(st.total_plan_time, 2) > 0 THEN
          round(sqrt(
            (st.sum_plan_time_sq * st.plans - pow(st.total_plan_time, 2)) /
            NULLIF(pow(st.plans, 2), 0)
          )::numeric, 2)
      ELSE NULL
    END as stddev_plan_time,
    NULLIF(st.rows, 0) as rows,
    NULLIF(st.shared_blks_hit, 0),
    round(CAST(NULLIF(
      (st.shared_blks_hit * 100 /
        NULLIF(st.shared_blks_hit + st.shared_blks_read, 0)),
      0.0) AS numeric), 2) as shared_hit_pct,
    NULLIF(st.shared_blks_read, 0) as shared_blks_read,
    round(CAST(NULLIF(st.shared_blks_read * 100 /
      NULLIF(tot.shared_blks_read, 0), 0.0) AS numeric), 2) as read_pct,
    NULLIF(st.shared_blks_hit + st.shared_blks_read, 0) as shared_blks_fetched,
    round(CAST(NULLIF((st.shared_blks_hit + st.shared_blks_read) * 100 /
      NULLIF(tot.shared_blks_hit + tot.shared_blks_read, 0), 0.0) AS numeric), 2)
       as shared_blks_fetched_pct,
    NULLIF(st.shared_blks_dirtied, 0) as shared_blks_dirtied,
    round(CAST(NULLIF(st.shared_blks_dirtied * 100 /
      NULLIF(tot.shared_blks_dirtied, 0), 0.0) AS numeric), 2) as dirtied_pct,
    NULLIF(st.shared_blks_written, 0) as shared_blks_written,
    round(CAST(NULLIF(st.shared_blks_written * 100 /
      NULLIF(totbgwr.written, 0), 0.0) AS numeric), 2) as tot_written_pct,
    round(CAST(NULLIF(st.shared_blks_written * 100 /
      NULLIF(totbgwr.buffers_backend, 0), 0.0) AS numeric), 2) as backend_written_pct,
    NULLIF(st.local_blks_hit, 0) as local_blks_hit,
    round(CAST(NULLIF(st.local_blks_hit * 100 /
      NULLIF(st.local_blks_hit + st.local_blks_read,0), 0.0) AS numeric), 2)
       as local_hit_pct,
    NULLIF(st.local_blks_read, 0) as local_blks_read,
    NULLIF(st.local_blks_hit + st.local_blks_read, 0) as local_blks_fetched,
    NULLIF(st.local_blks_dirtied, 0) as local_blks_dirtied,
    NULLIF(st.local_blks_written, 0) as local_blks_written,
    NULLIF(st.temp_blks_read, 0) as temp_blks_read,
    NULLIF(st.temp_blks_written, 0) as temp_blks_written,
    round(CAST(NULLIF(st.shared_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as shared_blk_read_time,
    round(CAST(NULLIF(st.shared_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as shared_blk_write_time,
    round(CAST(NULLIF(st.local_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as local_blk_read_time,
    round(CAST(NULLIF(st.local_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as local_blk_write_time,
    round(CAST(NULLIF(st.temp_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as temp_blk_read_time,
    round(CAST(NULLIF(st.temp_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as temp_blk_write_time,
    round(CAST(NULLIF(st.io_time, 0.0) AS numeric) / 1000, 2)
      as io_time,
    round(CAST(NULLIF(st.io_time * 100 /
      NULLIF(tot.shared_blk_read_time + tot.shared_blk_write_time +
      tot.local_blk_read_time + tot.local_blk_write_time, 0), 0.0) AS numeric), 2)
        as io_time_pct,
    round(CAST(NULLIF(st.temp_blks_read * 100 /
      NULLIF(tot.temp_blks_read, 0), 0.0) AS numeric), 2) as temp_read_total_pct,
    round(CAST(NULLIF(st.temp_blks_written * 100 /
      NULLIF(tot.temp_blks_written, 0), 0.0) AS numeric), 2) as temp_write_total_pct,
    round(CAST(NULLIF((coalesce(st.temp_blk_read_time, 0) +
      coalesce(st.temp_blk_write_time, 0)) * 100 /
      NULLIF(tot.temp_blk_read_time + tot.temp_blk_write_time, 0), 0.0) AS numeric), 2)
      as temp_io_time_pct,
    round(CAST(NULLIF(st.local_blks_read * 100 /
      NULLIF(tot.local_blks_read, 0), 0.0) AS numeric), 2) as local_read_total_pct,
    round(CAST(NULLIF(st.local_blks_written * 100 /
      NULLIF(tot.local_blks_written, 0), 0.0) AS numeric), 2) as local_write_total_pct,
    NULLIF(st.wal_records, 0) as wal_records,
    NULLIF(st.wal_fpi, 0) as wal_fpi,
    NULLIF(st.wal_bytes, 0) as wal_bytes,
    pg_size_pretty(NULLIF(st.wal_bytes, 0)) as wal_bytes_fmt,
    round(CAST(NULLIF(st.wal_bytes * 100 /
      NULLIF(totbgwr.wal_size, 0), 0.0) AS numeric), 2) as wal_bytes_pct,
    NULLIF(st.wal_buffers_full, 0) as wal_buffers_full,
    round(CAST(NULLIF(st.user_time, 0.0) AS numeric), 2) as user_time,
    round(CAST(NULLIF(st.system_time, 0.0) AS numeric), 2) as system_time,
    NULLIF(st.reads, 0) as reads,
    NULLIF(st.writes, 0) as writes,
    round(NULLIF(CAST(st.jit_generation_time + st.jit_inlining_time +
      st.jit_optimization_time + st.jit_emission_time +
      st.jit_deform_time AS numeric) / 1000, 0), 2) as jit_total_time,
    NULLIF(st.jit_functions, 0) as jit_functions,
    round(CAST(NULLIF(st.jit_generation_time, 0.0) AS numeric) / 1000, 2) as jit_generation_time,
    NULLIF(st.jit_inlining_count, 0) as jit_inlining_count,
    round(CAST(NULLIF(st.jit_inlining_time, 0.0) AS numeric) / 1000, 2) as jit_inlining_time,
    NULLIF(st.jit_optimization_count, 0) as jit_optimization_count,
    round(CAST(NULLIF(st.jit_optimization_time, 0.0) AS numeric) / 1000, 2) as jit_optimization_time,
    NULLIF(st.jit_emission_count, 0) as jit_emission_count,
    round(CAST(NULLIF(st.jit_emission_time, 0.0) AS numeric) / 1000, 2) as jit_emission_time,
    NULLIF(st.jit_deform_count, 0) as jit_deform_count,
    round(CAST(NULLIF(st.jit_deform_time, 0.0) AS numeric) / 1000, 2) as jit_deform_time,
    NULLIF(st.parallel_workers_to_launch, 0) as parallel_workers_to_launch,
    NULLIF(st.parallel_workers_launched, 0) as parallel_workers_launched,
    NULLIF(st.generic_plan_calls, 0),
    NULLIF(st.custom_plan_calls, 0),
    round(CAST(NULLIF(100 - st.skipped_time * 100 /
      extract(epoch from r_samples_dur.duration), 0.0) AS numeric))
      as stmt_cover,
    COALESCE(st.temp_blks_read, 0) +
        COALESCE(st.temp_blks_written, 0) +
        COALESCE(st.local_blks_read, 0) +
        COALESCE(st.local_blks_written, 0) AS sum_tmp_blks,
    (st.jit_generation_time + st.jit_inlining_time +
        st.jit_optimization_time + st.jit_emission_time +
        st.jit_deform_time)::numeric AS sum_jit_time,
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
    CASE WHEN st.total_exec_time / NULLIF(st.calls, 0) > 0 THEN
      row_number() OVER (ORDER BY st.total_exec_time / NULLIF(st.calls, 0) DESC NULLS LAST,
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
    CASE WHEN st.shared_blks_hit + st.shared_blks_read > 0 THEN
      row_number() OVER (ORDER BY st.shared_blks_hit + st.shared_blks_read DESC NULLS LAST,
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
        st.jit_optimization_time + st.jit_emission_time +
        st.jit_deform_time > 0 THEN
      row_number() OVER (ORDER BY st.jit_generation_time +
        st.jit_inlining_time + st.jit_optimization_time +
        st.jit_emission_time + st.jit_deform_time DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_jit,
    CASE WHEN
        st.parallel_workers_to_launch + st.parallel_workers_launched > 0 THEN
      row_number() OVER (ORDER BY st.parallel_workers_to_launch +
        st.parallel_workers_launched DESC NULLS LAST,
        st.datid,
        st.userid,
        st.queryid,
        st.toplevel)::integer
    ELSE NULL END AS ord_wrkrs_cnt
  FROM
    top_statements(sserver_id, start_id, end_id) st
    CROSS JOIN total_statements(sserver_id, start_id, end_id) tot
    CROSS JOIN total_bgwriter(sserver_id, start_id, end_id) totbgwr
    CROSS JOIN report_smpl_duration(sserver_id, start_id, end_id) r_samples_dur
$$ LANGUAGE sql;

CREATE FUNCTION top_statements_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid                    oid,
    dbname                   name,
    userid                   oid,
    username                 name,
    queryid                  text,
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
    wal_buffers_full1        numeric,
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
    parallel_workers_to_launch1 bigint,
    parallel_workers_launched1  bigint,
    generic_plan_calls1      bigint,
    custom_plan_calls1       bigint,
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
    wal_buffers_full2        numeric,
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
    parallel_workers_to_launch2 bigint,
    parallel_workers_launched2  bigint,
    generic_plan_calls2      bigint,
    custom_plan_calls2       bigint,
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
    ord_jit                  integer,
    ord_wrkrs_cnt            integer
)
SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(st1.datid,st2.datid) as datid,
    COALESCE(st1.dbname,st2.dbname) as dbname,
    COALESCE(st1.userid,st2.userid) as userid,
    COALESCE(st1.username,st2.username) as username,
    COALESCE(st1.queryid,st2.queryid)::text AS queryid,
    to_hex(COALESCE(st1.queryid,st2.queryid)) as hexqueryid,
    COALESCE(st1.toplevel,st2.toplevel) as toplevel,
    left(encode(sha224(convert_to(
      COALESCE(st1.userid,st2.userid)::text ||
      COALESCE(st1.datid,st2.datid)::text ||
      COALESCE(st1.queryid,st2.queryid)::text,
      'UTF8')
    ), 'hex'), 10) AS hashed_ids,
    -- First Interval
    NULLIF(st1.plans, 0) as plans1,
    round(CAST(NULLIF(st1.plans * 100 / NULLIF(tot1.plans, 0), 0.0) AS numeric), 2) as plans_pct1,
    NULLIF(st1.calls, 0) as calls1,
    round(CAST(NULLIF(st1.calls * 100 / NULLIF(tot1.calls, 0), 0.0) AS numeric), 2) as calls_pct1,
    round(CAST(NULLIF(st1.total_time, 0.0) AS numeric), 2) as total_time1,
    round(CAST(NULLIF(
      (st1.total_exec_time + COALESCE(st1.total_plan_time, 0)) * 100 /
      NULLIF(tot1.total_time, 0), 0.0) AS numeric), 2) as total_time_pct1,
    round(CAST(NULLIF(st1.total_plan_time / 1000, 0.0) AS numeric), 2) as total_plan_time1,
    round(CAST(NULLIF(st1.total_plan_time * 100 /
      NULLIF(st1.total_exec_time + COALESCE(st1.total_plan_time, 0), 0), 0.0) AS numeric), 2)
      as plan_time_pct1,
    round(CAST(NULLIF(st1.total_exec_time / 1000, 0.0) AS numeric), 2) as total_exec_time1,
    round(CAST(NULLIF(st1.total_exec_time * 100 /
      NULLIF(tot1.total_time, 0), 0.0) AS numeric), 2) as total_exec_time_pct1,
    round(CAST(NULLIF(st1.total_exec_time * 100 /
      NULLIF(st1.total_exec_time + COALESCE(st1.total_plan_time, 0.0), 0), 0.0) AS numeric), 2)
      as exec_time_pct1,
    round(CAST(NULLIF(st1.min_exec_time, 0.0) AS numeric), 2) as min_exec_time1,
    round(CAST(NULLIF(st1.max_exec_time, 0.0) AS numeric), 2) as max_exec_time1,
    round(CAST(NULLIF(st1.total_exec_time / NULLIF(st1.calls, 0), 0.0) AS numeric), 2)
      as mean_exec_time1,
    CASE
      WHEN st1.calls = 1 THEN 0
      WHEN st1.sum_exec_time_sq * st1.calls -
        pow(st1.total_exec_time, 2) > 0 THEN
          round(sqrt(
            (st1.sum_exec_time_sq * st1.calls - pow(st1.total_exec_time, 2)) /
            NULLIF(pow(st1.calls, 2), 0)
          )::numeric, 2)
      ELSE NULL
    END as stddev_exec_time1,
    round(CAST(NULLIF(st1.min_plan_time, 0.0) AS numeric), 2) as min_plan_time1,
    round(CAST(NULLIF(st1.max_plan_time, 0.0) AS numeric), 2) as max_plan_time1,
    round(CAST(NULLIF(st1.total_plan_time / NULLIF(st1.plans, 0), 0.0) AS numeric), 2)
      as mean_plan_time1,
    CASE
      WHEN st1.plans = 1 THEN 0
      WHEN st1.sum_plan_time_sq * st1.plans -
        pow(st1.total_plan_time, 2) > 0 THEN
          round(sqrt(
            (st1.sum_plan_time_sq * st1.plans - pow(st1.total_plan_time, 2)) /
            NULLIF(pow(st1.plans, 2), 0)
          )::numeric, 2)
      ELSE NULL
    END as stddev_plan_time1,
    NULLIF(st1.rows, 0) as rows1,
    NULLIF(st1.shared_blks_hit, 0) as shared_blks_hit1,
    round(CAST(NULLIF(
      (st1.shared_blks_hit * 100 /
        NULLIF(st1.shared_blks_hit + st1.shared_blks_read, 0)),
      0.0) AS numeric), 2) as shared_hit_pct1,
    NULLIF(st1.shared_blks_read, 0) as shared_blks_read1,
    round(CAST(NULLIF(st1.shared_blks_read * 100 /
      NULLIF(tot1.shared_blks_read, 0), 0.0) AS numeric), 2) as read_pct1,
    NULLIF(st1.shared_blks_hit + st1.shared_blks_read, 0) as shared_blks_fetched1,
    round(CAST(NULLIF((st1.shared_blks_hit + st1.shared_blks_read) * 100 /
      NULLIF(tot1.shared_blks_hit + tot1.shared_blks_read, 0), 0.0) AS numeric), 2)
       as shared_blks_fetched_pct1,
    NULLIF(st1.shared_blks_dirtied, 0) as shared_blks_dirtied1,
    round(CAST(NULLIF(st1.shared_blks_dirtied * 100 /
      NULLIF(tot1.shared_blks_dirtied, 0), 0.0) AS numeric), 2) as dirtied_pct1,
    NULLIF(st1.shared_blks_written, 0) as shared_blks_written1,
    round(CAST(NULLIF(st1.shared_blks_written * 100 /
      NULLIF(totbgwr1.written, 0), 0.0) AS numeric), 2) as tot_written_pct1,
    round(CAST(NULLIF(st1.shared_blks_written * 100 /
      NULLIF(totbgwr1.buffers_backend, 0), 0.0) AS numeric), 2) as backend_written_pct1,
    NULLIF(st1.local_blks_hit, 0) as local_blks_hit1,
    round(CAST(NULLIF(st1.local_blks_hit * 100 /
      NULLIF(st1.local_blks_hit + st1.local_blks_read,0), 0.0) AS numeric), 2)
       as local_hit_pct1,
    NULLIF(st1.local_blks_read, 0) as local_blks_read1,
    NULLIF(st1.local_blks_hit + st1.local_blks_read, 0) as local_blks_fetched1,
    NULLIF(st1.local_blks_dirtied, 0) as local_blks_dirtied1,
    NULLIF(st1.local_blks_written, 0) as local_blks_written1,
    NULLIF(st1.temp_blks_read, 0) as temp_blks_read1,
    NULLIF(st1.temp_blks_written, 0) as temp_blks_written1,
    round(CAST(NULLIF(st1.shared_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as shared_blk_read_time1,
    round(CAST(NULLIF(st1.shared_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as shared_blk_write_time1,
    round(CAST(NULLIF(st1.local_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as local_blk_read_time1,
    round(CAST(NULLIF(st1.local_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as local_blk_write_time1,
    round(CAST(NULLIF(st1.temp_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as temp_blk_read_time1,
    round(CAST(NULLIF(st1.temp_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as temp_blk_write_time1,
    round(CAST(NULLIF(st1.io_time, 0.0) AS numeric) / 1000, 2)
      as io_time1,
    round(CAST(NULLIF(st1.io_time * 100 /
      NULLIF(tot1.shared_blk_read_time + tot1.shared_blk_write_time +
      tot1.local_blk_read_time + tot1.local_blk_write_time, 0), 0.0) AS numeric), 2)
        as io_time_pct1,
    round(CAST(NULLIF(st1.temp_blks_read * 100 /
      NULLIF(tot1.temp_blks_read, 0), 0.0) AS numeric), 2) as temp_read_total_pct1,
    round(CAST(NULLIF(st1.temp_blks_written * 100 /
      NULLIF(tot1.temp_blks_written, 0), 0.0) AS numeric), 2) as temp_write_total_pct1,
    round(CAST(NULLIF((coalesce(st1.temp_blk_read_time, 0) +
      coalesce(st1.temp_blk_write_time, 0)) * 100 /
      NULLIF(tot1.temp_blk_read_time + tot1.temp_blk_write_time, 0), 0.0) AS numeric), 2)
      as temp_io_time_pct1,
    round(CAST(NULLIF(st1.local_blks_read * 100 /
      NULLIF(tot1.local_blks_read, 0), 0.0) AS numeric), 2) as local_read_total_pct1,
    round(CAST(NULLIF(st1.local_blks_written * 100 /
      NULLIF(tot1.local_blks_written, 0), 0.0) AS numeric), 2) as local_write_total_pct1,
    NULLIF(st1.wal_records, 0) as wal_records1,
    NULLIF(st1.wal_fpi, 0) as wal_fpi1,
    NULLIF(st1.wal_bytes, 0) as wal_bytes1,
    pg_size_pretty(NULLIF(st1.wal_bytes, 0)) as wal_bytes_fmt1,
    round(CAST(NULLIF(st1.wal_bytes * 100 /
      NULLIF(totbgwr1.wal_size, 0), 0.0) AS numeric), 2) as wal_bytes_pct1,
    NULLIF(st1.wal_buffers_full, 0) as wal_buffers_full1,
    round(CAST(NULLIF(st1.user_time, 0.0) AS numeric), 2) as user_time1,
    round(CAST(NULLIF(st1.system_time, 0.0) AS numeric), 2) as system_time1,
    NULLIF(st1.reads, 0) as reads1,
    NULLIF(st1.writes, 0) as writes1,
    round(NULLIF(CAST(st1.jit_generation_time + st1.jit_inlining_time +
      st1.jit_optimization_time + st1.jit_emission_time +
      st1.jit_deform_time AS numeric) / 1000, 0), 2) as jit_total_time1,
    NULLIF(st1.jit_functions, 0) as jit_functions1,
    round(CAST(NULLIF(st1.jit_generation_time, 0.0) AS numeric) / 1000, 2) as jit_generation_time1,
    NULLIF(st1.jit_inlining_count, 0) as jit_inlining_count1,
    round(CAST(NULLIF(st1.jit_inlining_time, 0.0) AS numeric) / 1000, 2) as jit_inlining_time1,
    NULLIF(st1.jit_optimization_count, 0) as jit_optimization_count1,
    round(CAST(NULLIF(st1.jit_optimization_time, 0.0) AS numeric) / 1000, 2) as jit_optimization_time1,
    NULLIF(st1.jit_emission_count, 0) as jit_emission_count1,
    round(CAST(NULLIF(st1.jit_emission_time, 0.0) AS numeric) / 1000, 2) as jit_emission_time1,
    NULLIF(st1.jit_deform_count, 0) as jit_deform_count1,
    round(CAST(NULLIF(st1.jit_deform_time, 0.0) AS numeric) / 1000, 2) as jit_deform_time1,
    NULLIF(st1.parallel_workers_to_launch, 0) as parallel_workers_to_launch1,
    NULLIF(st1.parallel_workers_launched, 0) as parallel_workers_to_launch1,
    NULLIF(st1.generic_plan_calls, 0) as generic_plan_calls1,
    NULLIF(st1.custom_plan_calls, 0) as custom_plan_calls1,
    round(CAST(NULLIF(100 - st1.skipped_time * 100 /
      extract(epoch from r_samples_dur1.duration), 0.0) AS numeric))
      as stmt_cover1,

    -- Second Interval
    NULLIF(st2.plans, 0) as plans2,
    round(CAST(NULLIF(st2.plans * 100 / NULLIF(tot2.plans, 0), 0.0) AS numeric), 2) as plans_pct2,
    NULLIF(st2.calls, 0) as calls2,
    round(CAST(NULLIF(st2.calls * 100 / NULLIF(tot2.calls, 0), 0.0) AS numeric), 2) as calls_pct2,
    round(CAST(NULLIF(st2.total_time, 0.0) AS numeric), 2) as total_time2,
    round(CAST(NULLIF(
      (st2.total_exec_time + COALESCE(st2.total_plan_time, 0)) * 100 /
      NULLIF(tot2.total_time, 0), 0.0) AS numeric), 2) as total_time_pct2,
    round(CAST(NULLIF(st2.total_plan_time / 1000, 0.0) AS numeric), 2) as total_plan_time2,
    round(CAST(NULLIF(st2.total_plan_time * 100 /
      NULLIF(st2.total_exec_time + COALESCE(st2.total_plan_time, 0), 0), 0.0) AS numeric), 2)
      as plan_time_pct2,
    round(CAST(NULLIF(st2.total_exec_time / 1000, 0.0) AS numeric), 2) as total_exec_time2,
    round(CAST(NULLIF(st2.total_exec_time * 100 /
      NULLIF(tot2.total_time, 0), 0.0) AS numeric), 2) as total_exec_time_pct2,
    round(CAST(NULLIF(st2.total_exec_time * 100 /
      NULLIF(st2.total_exec_time + COALESCE(st2.total_plan_time, 0.0), 0), 0.0) AS numeric), 2)
      as exec_time_pct2,
    round(CAST(NULLIF(st2.min_exec_time, 0.0) AS numeric), 2) as min_exec_time2,
    round(CAST(NULLIF(st2.max_exec_time, 0.0) AS numeric), 2) as max_exec_time2,
    round(CAST(NULLIF(st2.total_exec_time / NULLIF(st2.calls, 0), 0.0) AS numeric), 2)
      as mean_exec_time2,
    CASE
      WHEN st2.calls = 1 THEN 0
      WHEN st2.sum_exec_time_sq * st2.calls -
        pow(st2.total_exec_time, 2) > 0 THEN
          round(sqrt(
            (st2.sum_exec_time_sq * st2.calls - pow(st2.total_exec_time, 2)) /
            NULLIF(pow(st2.calls, 2), 0)
          )::numeric, 2)
      ELSE NULL
    END as stddev_exec_time2,
    round(CAST(NULLIF(st2.min_plan_time, 0.0) AS numeric), 2) as min_plan_time2,
    round(CAST(NULLIF(st2.max_plan_time, 0.0) AS numeric), 2) as max_plan_time2,
    round(CAST(NULLIF(st2.total_plan_time / NULLIF(st2.plans, 0), 0.0) AS numeric), 2)
      as mean_plan_time2,
    CASE
      WHEN st2.plans = 1 THEN 0
      WHEN st2.sum_plan_time_sq * st2.plans -
        pow(st2.total_plan_time, 2) > 0 THEN
          round(sqrt(
            (st2.sum_plan_time_sq * st2.plans - pow(st2.total_plan_time, 2)) /
            NULLIF(pow(st2.plans, 2), 0)
          )::numeric, 2)
      ELSE NULL
    END as stddev_plan_time2,
    NULLIF(st2.rows, 0) as rows2,
    NULLIF(st2.shared_blks_hit, 0) as shared_blks_hit2,
    round(CAST(NULLIF(
      (st2.shared_blks_hit * 100 /
        NULLIF(st2.shared_blks_hit + st2.shared_blks_read, 0)),
      0.0) AS numeric), 2) as shared_hit_pct2,
    NULLIF(st2.shared_blks_read, 0) as shared_blks_read2,
    round(CAST(NULLIF(st2.shared_blks_read * 100 /
      NULLIF(tot2.shared_blks_read, 0), 0.0) AS numeric), 2) as read_pct2,
    NULLIF(st2.shared_blks_hit + st2.shared_blks_read, 0) as shared_blks_fetched2,
    round(CAST(NULLIF((st2.shared_blks_hit + st2.shared_blks_read) * 100 /
      NULLIF(tot2.shared_blks_hit + tot2.shared_blks_read, 0), 0.0) AS numeric), 2)
       as shared_blks_fetched_pct2,
    NULLIF(st2.shared_blks_dirtied, 0) as shared_blks_dirtied2,
    round(CAST(NULLIF(st2.shared_blks_dirtied * 100 /
      NULLIF(tot2.shared_blks_dirtied, 0), 0.0) AS numeric), 2) as dirtied_pct2,
    NULLIF(st2.shared_blks_written, 0) as shared_blks_written2,
    round(CAST(NULLIF(st2.shared_blks_written * 100 /
      NULLIF(totbgwr2.written, 0), 0.0) AS numeric), 2) as tot_written_pct2,
    round(CAST(NULLIF(st2.shared_blks_written * 100 /
      NULLIF(totbgwr2.buffers_backend, 0), 0.0) AS numeric), 2) as backend_written_pct2,
    NULLIF(st2.local_blks_hit, 0) as local_blks_hit2,
    round(CAST(NULLIF(st2.local_blks_hit * 100 /
      NULLIF(st2.local_blks_hit + st2.local_blks_read,0), 0.0) AS numeric), 2)
       as local_hit_pct2,
    NULLIF(st2.local_blks_read, 0) as local_blks_read2,
    NULLIF(st2.local_blks_hit + st2.local_blks_read, 0) as local_blks_fetched2,
    NULLIF(st2.local_blks_dirtied, 0) as local_blks_dirtied2,
    NULLIF(st2.local_blks_written, 0) as local_blks_written2,
    NULLIF(st2.temp_blks_read, 0) as temp_blks_read2,
    NULLIF(st2.temp_blks_written, 0) as temp_blks_written2,
    round(CAST(NULLIF(st2.shared_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as shared_blk_read_time2,
    round(CAST(NULLIF(st2.shared_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as shared_blk_write_time2,
    round(CAST(NULLIF(st2.local_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as local_blk_read_time2,
    round(CAST(NULLIF(st2.local_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as local_blk_write_time2,
    round(CAST(NULLIF(st2.temp_blk_read_time, 0.0) AS numeric) / 1000, 2)
      as temp_blk_read_time2,
    round(CAST(NULLIF(st2.temp_blk_write_time, 0.0) AS numeric) / 1000, 2)
      as temp_blk_write_time2,
    round(CAST(NULLIF(st2.io_time, 0.0) AS numeric) / 1000, 2)
      as io_time2,
    round(CAST(NULLIF(st2.io_time * 100 /
      NULLIF(tot2.shared_blk_read_time + tot2.shared_blk_write_time +
      tot2.local_blk_read_time + tot2.local_blk_write_time, 0), 0.0) AS numeric), 2)
        as io_time_pct2,
    round(CAST(NULLIF(st2.temp_blks_read * 100 /
      NULLIF(tot2.temp_blks_read, 0), 0.0) AS numeric), 2) as temp_read_total_pct2,
    round(CAST(NULLIF(st2.temp_blks_written * 100 /
      NULLIF(tot2.temp_blks_written, 0), 0.0) AS numeric), 2) as temp_write_total_pct2,
    round(CAST(NULLIF((coalesce(st2.temp_blk_read_time, 0) +
      coalesce(st2.temp_blk_write_time, 0)) * 100 /
      NULLIF(tot2.temp_blk_read_time + tot2.temp_blk_write_time, 0), 0.0) AS numeric), 2)
      as temp_io_time_pct2,
    round(CAST(NULLIF(st2.local_blks_read * 100 /
      NULLIF(tot2.local_blks_read, 0), 0.0) AS numeric), 2) as local_read_total_pct2,
    round(CAST(NULLIF(st2.local_blks_written * 100 /
      NULLIF(tot2.local_blks_written, 0), 0.0) AS numeric), 2) as local_write_total_pct2,
    NULLIF(st2.wal_records, 0) as wal_records2,
    NULLIF(st2.wal_fpi, 0) as wal_fpi2,
    NULLIF(st2.wal_bytes, 0) as wal_bytes2,
    pg_size_pretty(NULLIF(st2.wal_bytes, 0)) as wal_bytes_fmt2,
    round(CAST(NULLIF(st2.wal_bytes * 100 /
      NULLIF(totbgwr2.wal_size, 0), 0.0) AS numeric), 2) as wal_bytes_pct2,
    NULLIF(st2.wal_buffers_full, 0) as wal_buffers_full2,
    round(CAST(NULLIF(st2.user_time, 0.0) AS numeric), 2) as user_time2,
    round(CAST(NULLIF(st2.system_time, 0.0) AS numeric), 2) as system_time2,
    NULLIF(st2.reads, 0) as reads2,
    NULLIF(st2.writes, 0) as writes2,
    round(NULLIF(CAST(st2.jit_generation_time + st2.jit_inlining_time +
      st2.jit_optimization_time + st2.jit_emission_time +
      st2.jit_deform_time AS numeric) / 1000, 0), 2) as jit_total_time2,
    NULLIF(st2.jit_functions, 0) as jit_functions2,
    round(CAST(NULLIF(st2.jit_generation_time, 0.0) AS numeric) / 1000, 2) as jit_generation_time2,
    NULLIF(st2.jit_inlining_count, 0) as jit_inlining_count2,
    round(CAST(NULLIF(st2.jit_inlining_time, 0.0) AS numeric) / 1000, 2) as jit_inlining_time2,
    NULLIF(st2.jit_optimization_count, 0) as jit_optimization_count2,
    round(CAST(NULLIF(st2.jit_optimization_time, 0.0) AS numeric) / 1000, 2) as jit_optimization_time2,
    NULLIF(st2.jit_emission_count, 0) as jit_emission_count2,
    round(CAST(NULLIF(st2.jit_emission_time, 0.0) AS numeric) / 1000, 2) as jit_emission_time2,
    NULLIF(st2.jit_deform_count, 0) as jit_deform_count2,
    round(CAST(NULLIF(st2.jit_deform_time, 0.0) AS numeric) / 1000, 2) as jit_deform_time2,
    NULLIF(st2.parallel_workers_to_launch, 0) as parallel_workers_to_launch2,
    NULLIF(st2.parallel_workers_launched, 0) as parallel_workers_launched2,
    NULLIF(st2.generic_plan_calls, 0) as generic_plan_calls2,
    NULLIF(st2.custom_plan_calls, 0) as custom_plan_calls2,
    round(CAST(NULLIF(100 - st2.skipped_time * 100 /
      extract(epoch from r_samples_dur2.duration), 0.0) AS numeric))
      as stmt_cover2,
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
        st1.jit_deform_count +
        st2.jit_generation_time + st2.jit_inlining_time +
        st2.jit_optimization_time + st2.jit_emission_time +
        st2.jit_deform_count)::numeric AS sum_jit_time,

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

    CASE WHEN COALESCE(st1.total_exec_time / NULLIF(st1.calls, 0), 0.0) +
        COALESCE(st2.total_exec_time / NULLIF(st2.calls, 0), 0.0) > 0 THEN
    row_number() OVER (ORDER BY
       COALESCE(st1.total_exec_time / NULLIF(st1.calls, 0), 0.0) +
       COALESCE(st2.total_exec_time / NULLIF(st2.calls, 0), 0.0) DESC NULLS LAST,
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

    CASE WHEN COALESCE(st1.shared_blks_hit + st1.shared_blks_read, 0) +
        COALESCE(st2.shared_blks_hit + st2.shared_blks_read, 0) > 0 THEN
    row_number() OVER (ORDER BY COALESCE(st1.shared_blks_hit + st1.shared_blks_read, 0) +
      COALESCE(st2.shared_blks_hit + st2.shared_blks_read, 0) DESC NULLS LAST,
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
    ELSE NULL END AS ord_jit,
    CASE WHEN
        st1.parallel_workers_to_launch + st1.parallel_workers_launched +
        st2.parallel_workers_to_launch + st2.parallel_workers_launched > 0 THEN
      row_number() OVER (ORDER BY st1.parallel_workers_to_launch + st1.parallel_workers_launched +
        st2.parallel_workers_to_launch + st2.parallel_workers_launched DESC NULLS LAST,
        COALESCE(st1.datid,st2.datid),
        COALESCE(st1.userid,st2.userid),
        COALESCE(st1.queryid,st2.queryid),
        COALESCE(st1.toplevel,st2.toplevel))::integer
    ELSE NULL END AS ord_wrkrs_cnt
  FROM
    top_statements(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN
    top_statements(sserver_id, start2_id, end2_id) st2 USING
        (server_id, datid, userid, queryid, toplevel)
    CROSS JOIN total_statements(sserver_id, start1_id, end1_id) tot1
    CROSS JOIN total_bgwriter(sserver_id, start1_id, end1_id) totbgwr1
    CROSS JOIN report_smpl_duration(sserver_id, start1_id, end1_id) r_samples_dur1
    CROSS JOIN total_statements(sserver_id, start2_id, end2_id) tot2
    CROSS JOIN total_bgwriter(sserver_id, start2_id, end2_id) totbgwr2
    CROSS JOIN report_smpl_duration(sserver_id, start2_id, end2_id) r_samples_dur2
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
