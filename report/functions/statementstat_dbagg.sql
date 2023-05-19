/* ========= Check available statement stats for report ========= */

CREATE FUNCTION profile_checkavail_statstatements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there was available pg_stat_statements statistics for report interval
  SELECT count(sn.sample_id) = count(st.sample_id)
  FROM samples sn LEFT OUTER JOIN sample_statements_total st USING (server_id, sample_id)
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_planning_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(total_plan_time), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_stmt_wal_bytes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have statement wal sizes collected for report interval
  SELECT COALESCE(sum(wal_bytes), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_statements_jit_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
    SELECT COALESCE(sum(jit_functions + jit_inlining_count + jit_optimization_count + jit_emission_count), 0) > 0
    FROM sample_statements_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_statements_temp_io_times(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(temp_blk_read_time), 0) + COALESCE(sum(temp_blk_write_time), 0) > 0
  FROM sample_statements_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;
/* ========= Statement stats functions ========= */

CREATE FUNCTION statements_dbstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        dbname              name,
        datid               oid,
        calls               bigint,
        plans               bigint,
        total_exec_time     double precision,
        total_plan_time     double precision,
        blk_read_time       double precision,
        blk_write_time      double precision,
        trg_fn_total_time   double precision,
        shared_gets         bigint,
        local_gets          bigint,
        shared_blks_dirtied bigint,
        local_blks_dirtied  bigint,
        temp_blks_read      bigint,
        temp_blks_written   bigint,
        temp_blk_read_time  double precision,
        temp_blk_write_time double precision,
        local_blks_read     bigint,
        local_blks_written  bigint,
        statements          bigint,
        wal_bytes           bigint,
        jit_functions       bigint,
        jit_generation_time double precision,
        jit_inlining_count  bigint,
        jit_inlining_time   double precision,
        jit_optimization_count  bigint,
        jit_optimization_time   double precision,
        jit_emission_count  bigint,
        jit_emission_time   double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        sample_db.datname AS dbname,
        sample_db.datid AS datid,
        sum(st.calls)::bigint AS calls,
        sum(st.plans)::bigint AS plans,
        sum(st.total_exec_time)/1000::double precision AS total_exec_time,
        sum(st.total_plan_time)/1000::double precision AS total_plan_time,
        sum(st.blk_read_time)/1000::double precision AS blk_read_time,
        sum(st.blk_write_time)/1000::double precision AS blk_write_time,
        (sum(trg.total_time)/1000)::double precision AS trg_fn_total_time,
        sum(st.shared_blks_hit)::bigint + sum(st.shared_blks_read)::bigint AS shared_gets,
        sum(st.local_blks_hit)::bigint + sum(st.local_blks_read)::bigint AS local_gets,
        sum(st.shared_blks_dirtied)::bigint AS shared_blks_dirtied,
        sum(st.local_blks_dirtied)::bigint AS local_blks_dirtied,
        sum(st.temp_blks_read)::bigint AS temp_blks_read,
        sum(st.temp_blks_written)::bigint AS temp_blks_written,
        sum(st.temp_blk_read_time)/1000::double precision AS temp_blk_read_time,
        sum(st.temp_blk_write_time)/1000::double precision AS temp_blk_write_time,
        sum(st.local_blks_read)::bigint AS local_blks_read,
        sum(st.local_blks_written)::bigint AS local_blks_written,
        sum(st.statements)::bigint AS statements,
        sum(st.wal_bytes)::bigint AS wal_bytes,
        sum(st.jit_functions)::bigint AS jit_functions,
        sum(st.jit_generation_time)/1000::double precision AS jit_generation_time,
        sum(st.jit_inlining_count)::bigint AS jit_inlining_count,
        sum(st.jit_inlining_time)/1000::double precision AS jit_inlining_time,
        sum(st.jit_optimization_count)::bigint AS jit_optimization_count,
        sum(st.jit_optimization_time)/1000::double precision AS jit_optimization_time,
        sum(st.jit_emission_count)::bigint AS jit_emission_count,
        sum(st.jit_emission_time)/1000::double precision AS jit_emission_time
    FROM sample_statements_total st
        LEFT OUTER JOIN sample_stat_user_func_total trg
          ON (st.server_id = trg.server_id AND st.sample_id = trg.sample_id AND st.datid = trg.datid AND trg.trg_fn)
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY sample_db.datname, sample_db.datid;
$$ LANGUAGE sql;

CREATE FUNCTION statements_dbstats_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    dbname              name,
    calls               numeric,
    plans               numeric,
    total_exec_time     numeric,
    total_plan_time     numeric,
    blk_read_time       numeric,
    blk_write_time      numeric,
    trg_fn_total_time   numeric,
    shared_gets         numeric,
    local_gets          numeric,
    shared_blks_dirtied numeric,
    local_blks_dirtied  numeric,
    temp_blks_read      numeric,
    temp_blks_written   numeric,
    temp_blk_read_time  numeric,
    temp_blk_write_time numeric,
    local_blks_read     numeric,
    local_blks_written  numeric,
    statements          numeric,
    wal_bytes           numeric,
    wal_bytes_fmt       text,
    jit_functions       numeric,
    jit_generation_time numeric,
    jit_inlining_count  numeric,
    jit_inlining_time   numeric,
    jit_optimization_count  numeric,
    jit_optimization_time   numeric,
    jit_emission_count  numeric,
    jit_emission_time   numeric,
    -- ordering fields
    ord_db              integer
) AS $$
  SELECT
    datid,
    COALESCE(dbname,'Total') AS dbname,
    NULLIF(sum(calls), 0) AS calls,
    NULLIF(sum(plans), 0) AS plans,
    round(CAST(NULLIF(sum(total_exec_time), 0.0) AS numeric),2) AS total_exec_time,
    round(CAST(NULLIF(sum(total_plan_time), 0.0) AS numeric),2) AS total_plan_time,
    round(CAST(NULLIF(sum(blk_read_time), 0.0) AS numeric),2) AS blk_read_time,
    round(CAST(NULLIF(sum(blk_write_time), 0.0) AS numeric),2) AS blk_write_time,
    round(CAST(NULLIF(sum(trg_fn_total_time), 0.0) AS numeric),2) AS trg_fn_total_time,
    NULLIF(sum(shared_gets), 0) AS shared_gets,
    NULLIF(sum(local_gets), 0) AS local_gets,
    NULLIF(sum(shared_blks_dirtied), 0) AS shared_blks_dirtied,
    NULLIF(sum(local_blks_dirtied), 0) AS local_blks_dirtied,
    NULLIF(sum(temp_blks_read), 0) AS temp_blks_read,
    NULLIF(sum(temp_blks_written), 0) AS temp_blks_written,
    round(CAST(NULLIF(sum(temp_blk_read_time), 0.0) AS numeric),2) AS temp_blk_read_time,
    round(CAST(NULLIF(sum(temp_blk_write_time), 0.0) AS numeric),2) AS temp_blk_write_time,
    NULLIF(sum(local_blks_read), 0) AS local_blks_read,
    NULLIF(sum(local_blks_written), 0) AS local_blks_written,
    NULLIF(sum(statements), 0) AS statements,
    sum(wal_bytes) AS wal_bytes,
    pg_size_pretty(NULLIF(sum(wal_bytes), 0)) AS wal_bytes_fmt,
    NULLIF(sum(jit_functions), 0) AS jit_functions,
    round(CAST(NULLIF(sum(jit_generation_time), 0.0) AS numeric),2) AS jit_generation_time,
    NULLIF(sum(jit_inlining_count), 0) AS jit_inlining_count,
    round(CAST(NULLIF(sum(jit_inlining_time), 0.0) AS numeric),2) AS jit_inlining_time,
    NULLIF(sum(jit_optimization_count), 0) AS jit_optimization_count,
    round(CAST(NULLIF(sum(jit_optimization_time), 0.0) AS numeric),2) AS jit_optimization_time,
    NULLIF(sum(jit_emission_count), 0) AS jit_emission_count,
    round(CAST(NULLIF(sum(jit_emission_time), 0.0) AS numeric),2) AS jit_emission_time,
    -- ordering fields
    row_number() OVER (ORDER BY dbname NULLS LAST)::integer AS ord_db
  FROM statements_dbstats(sserver_id, start_id, end_id)
  GROUP BY GROUPING SETS ((datid, dbname), ())
$$ LANGUAGE sql;

CREATE FUNCTION statements_dbstats_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid                oid,
    dbname               name,
    calls1               numeric,
    plans1               numeric,
    total_exec_time1     numeric,
    total_plan_time1     numeric,
    blk_read_time1       numeric,
    blk_write_time1      numeric,
    trg_fn_total_time1   numeric,
    shared_gets1         numeric,
    local_gets1          numeric,
    shared_blks_dirtied1 numeric,
    local_blks_dirtied1  numeric,
    temp_blks_read1      numeric,
    temp_blks_written1   numeric,
    temp_blk_read_time1  numeric,
    temp_blk_write_time1 numeric,
    local_blks_read1     numeric,
    local_blks_written1  numeric,
    statements1          numeric,
    wal_bytes1           numeric,
    wal_bytes_fmt1       text,
    jit_functions1       numeric,
    jit_generation_time1 numeric,
    jit_inlining_count1  numeric,
    jit_inlining_time1   numeric,
    jit_optimization_count1  numeric,
    jit_optimization_time1   numeric,
    jit_emission_count1  numeric,
    jit_emission_time1   numeric,
    calls2               numeric,
    plans2               numeric,
    total_exec_time2     numeric,
    total_plan_time2     numeric,
    blk_read_time2       numeric,
    blk_write_time2      numeric,
    trg_fn_total_time2   numeric,
    shared_gets2         numeric,
    local_gets2          numeric,
    shared_blks_dirtied2 numeric,
    local_blks_dirtied2  numeric,
    temp_blks_read2      numeric,
    temp_blks_written2   numeric,
    temp_blk_read_time2  numeric,
    temp_blk_write_time2 numeric,
    local_blks_read2     numeric,
    local_blks_written2  numeric,
    statements2          numeric,
    wal_bytes2           numeric,
    wal_bytes_fmt2       text,
    jit_functions2       numeric,
    jit_generation_time2 numeric,
    jit_inlining_count2  numeric,
    jit_inlining_time2   numeric,
    jit_optimization_count2  numeric,
    jit_optimization_time2   numeric,
    jit_emission_count2  numeric,
    jit_emission_time2   numeric,
    -- ordering fields
    ord_db              integer
) AS $$
  SELECT
    COALESCE(st1.datid,st2.datid) AS datid,
    COALESCE(COALESCE(st1.dbname,st2.dbname),'Total') AS dbname,
    NULLIF(sum(st1.calls), 0) AS calls1,
    NULLIF(sum(st1.plans), 0) AS plans1,
    round(CAST(NULLIF(sum(st1.total_exec_time), 0.0) AS numeric),2) AS total_exec_time1,
    round(CAST(NULLIF(sum(st1.total_plan_time), 0.0) AS numeric),2) AS total_plan_time1,
    round(CAST(NULLIF(sum(st1.blk_read_time), 0.0) AS numeric),2) AS blk_read_time1,
    round(CAST(NULLIF(sum(st1.blk_write_time), 0.0) AS numeric),2) AS blk_write_time1,
    round(CAST(NULLIF(sum(st1.trg_fn_total_time), 0.0) AS numeric),2) AS trg_fn_total_time1,
    NULLIF(sum(st1.shared_gets), 0) AS shared_gets1,
    NULLIF(sum(st1.local_gets), 0) AS local_gets1,
    NULLIF(sum(st1.shared_blks_dirtied), 0) AS shared_blks_dirtied1,
    NULLIF(sum(st1.local_blks_dirtied), 0) AS local_blks_dirtied1,
    NULLIF(sum(st1.temp_blks_read), 0) AS temp_blks_read1,
    NULLIF(sum(st1.temp_blks_written), 0) AS temp_blks_written1,
    round(CAST(NULLIF(sum(st1.temp_blk_read_time), 0.0) AS numeric),2) AS temp_blk_read_time1,
    round(CAST(NULLIF(sum(st1.temp_blk_write_time), 0.0) AS numeric),2) AS temp_blk_write_time1,
    NULLIF(sum(st1.local_blks_read), 0) AS local_blks_read1,
    NULLIF(sum(st1.local_blks_written), 0) AS local_blks_written1,
    NULLIF(sum(st1.statements), 0) AS statements1,
    sum(st1.wal_bytes) AS wal_bytes1,
    pg_size_pretty(NULLIF(sum(st1.wal_bytes), 0)) AS wal_bytes_fmt1,
    NULLIF(sum(st1.jit_functions), 0) AS jit_functions1,
    round(CAST(NULLIF(sum(st1.jit_generation_time), 0.0) AS numeric),2) AS jit_generation_time1,
    NULLIF(sum(st1.jit_inlining_count), 0) AS jit_inlining_count1,
    round(CAST(NULLIF(sum(st1.jit_inlining_time), 0.0) AS numeric),2) AS jit_inlining_time1,
    NULLIF(sum(st1.jit_optimization_count), 0) AS jit_optimization_count1,
    round(CAST(NULLIF(sum(st1.jit_optimization_time), 0.0) AS numeric),2) AS jit_optimization_time1,
    NULLIF(sum(st1.jit_emission_count), 0) AS jit_emission_count1,
    round(CAST(NULLIF(sum(st1.jit_emission_time), 0.0) AS numeric),2) AS jit_emission_time1,
    NULLIF(sum(st2.calls), 0) AS calls2,
    NULLIF(sum(st2.plans), 0) AS plans2,
    round(CAST(NULLIF(sum(st2.total_exec_time), 0.0) AS numeric),2) AS total_exec_time2,
    round(CAST(NULLIF(sum(st2.total_plan_time), 0.0) AS numeric),2) AS total_plan_time2,
    round(CAST(NULLIF(sum(st2.blk_read_time), 0.0) AS numeric),2) AS blk_read_time2,
    round(CAST(NULLIF(sum(st2.blk_write_time), 0.0) AS numeric),2) AS blk_write_time2,
    round(CAST(NULLIF(sum(st2.trg_fn_total_time), 0.0) AS numeric),2) AS trg_fn_total_time2,
    NULLIF(sum(st2.shared_gets), 0) AS shared_gets2,
    NULLIF(sum(st2.local_gets), 0) AS local_gets2,
    NULLIF(sum(st2.shared_blks_dirtied), 0) AS shared_blks_dirtied2,
    NULLIF(sum(st2.local_blks_dirtied), 0) AS local_blks_dirtied2,
    NULLIF(sum(st2.temp_blks_read), 0) AS temp_blks_read2,
    NULLIF(sum(st2.temp_blks_written), 0) AS temp_blks_written2,
    round(CAST(NULLIF(sum(st2.temp_blk_read_time), 0.0) AS numeric),2) AS temp_blk_read_time2,
    round(CAST(NULLIF(sum(st2.temp_blk_write_time), 0.0) AS numeric),2) AS temp_blk_write_time2,
    NULLIF(sum(st2.local_blks_read), 0) AS local_blks_read2,
    NULLIF(sum(st2.local_blks_written), 0) AS local_blks_written2,
    NULLIF(sum(st2.statements), 0) AS statements2,
    sum(st2.wal_bytes) AS wal_bytes2,
    pg_size_pretty(NULLIF(sum(st2.wal_bytes), 0)) AS wal_bytes_fmt2,
    NULLIF(sum(st2.jit_functions), 0) AS jit_functions2,
    round(CAST(NULLIF(sum(st2.jit_generation_time), 0.0) AS numeric),2) AS jit_generation_time2,
    NULLIF(sum(st2.jit_inlining_count), 0) AS jit_inlining_count2,
    round(CAST(NULLIF(sum(st2.jit_inlining_time), 0.0) AS numeric),2) AS jit_inlining_time2,
    NULLIF(sum(st2.jit_optimization_count), 0) AS jit_optimization_count2,
    round(CAST(NULLIF(sum(st2.jit_optimization_time), 0.0) AS numeric),2) AS jit_optimization_time2,
    NULLIF(sum(st2.jit_emission_count), 0) AS jit_emission_count2,
    round(CAST(NULLIF(sum(st2.jit_emission_time), 0.0) AS numeric),2) AS jit_emission_time2,
    -- ordering fields
    row_number() OVER (ORDER BY COALESCE(st1.dbname,st2.dbname) NULLS LAST)::integer AS ord_db
  FROM statements_dbstats(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN statements_dbstats(sserver_id, start2_id, end2_id) st2 USING (datid)
  GROUP BY GROUPING SETS ((COALESCE(st1.datid,st2.datid), COALESCE(st1.dbname,st2.dbname)),
    ())
$$ LANGUAGE sql;
