CREATE FUNCTION cluster_stat_io(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    backend_type          text,
    object                text,
    context               text,
    reads                 bigint,
    read_bytes            bigint,
    read_time             double precision,
    writes                bigint,
    write_bytes           bigint,
    write_time            double precision,
    writebacks            bigint,
    writeback_bytes       bigint,
    writeback_time        double precision,
    extends               bigint,
    extend_bytes          bigint,
    extend_time           double precision,
    hits                  bigint,
    evictions             bigint,
    reuses                bigint,
    fsyncs                bigint,
    fsync_time            double precision,
    total_io_time         double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.backend_type AS backend_type,
        st.object AS object,
        st.context AS context,
        SUM(reads)::bigint AS reads,
        SUM(reads * op_bytes)::bigint AS read_bytes,
        SUM(read_time)::double precision AS read_time,
        SUM(writes)::bigint AS writes,
        SUM(writes * op_bytes)::bigint AS write_bytes,
        SUM(write_time)::double precision AS write_time,
        SUM(writebacks)::bigint AS writebacks,
        SUM(writebacks * op_bytes)::bigint AS writeback_bytes,
        SUM(writeback_time)::double precision AS writeback_time,
        SUM(extends)::bigint AS extends,
        SUM(extends * op_bytes)::bigint AS extend_bytes,
        SUM(extend_time)::double precision AS extend_time,
        SUM(hits)::bigint AS hits,
        SUM(evictions)::bigint AS evictions,
        SUM(reuses)::bigint AS reuses,
        SUM(fsyncs)::bigint AS fsyncs,
        SUM(fsync_time)::double precision AS fsync_time,
        SUM(
          COALESCE(read_time, 0.0) +
          COALESCE(write_time, 0.0) +
          COALESCE(writeback_time, 0.0) +
          COALESCE(extend_time, 0.0) +
          COALESCE(fsync_time, 0.0)
        )::double precision AS total_io_time
    FROM sample_stat_io st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.backend_type, st.object, st.context
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_io_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    backend_type          text,
    object                text,
    context               text,

    reads                 bigint,
    read_sz               text,
    read_time             numeric,
    writes                bigint,
    write_sz              text,
    write_time            numeric,
    writebacks            bigint,
    writeback_sz          text,
    writeback_time        numeric,
    extends               bigint,
    extend_sz             text,
    extend_time           numeric,
    hits                  bigint,
    evictions             bigint,
    reuses                bigint,
    fsyncs                bigint,
    fsync_time            double precision
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(backend_type, 'Total'),
    COALESCE(object, '*'),
    COALESCE(context, '*'),

    NULLIF(SUM(reads), 0) AS reads,
    pg_size_pretty(NULLIF(SUM(read_bytes), 0)) AS read_sz,
    ROUND(CAST(NULLIF(SUM(read_time), 0.0) / 1000 AS numeric),2) AS read_time,
    NULLIF(SUM(writes), 0) AS writes,
    pg_size_pretty(NULLIF(SUM(write_bytes), 0)) AS write_sz,
    ROUND(CAST(NULLIF(SUM(write_time), 0.0) / 1000 AS numeric),2) AS write_time,
    NULLIF(SUM(writebacks), 0) AS writebacks,
    pg_size_pretty(NULLIF(SUM(writeback_bytes), 0)) AS writeback_sz,
    ROUND(CAST(NULLIF(SUM(writeback_time), 0.0) / 1000 AS numeric),2) AS writeback_time,
    NULLIF(SUM(extends), 0) AS extends,
    pg_size_pretty(NULLIF(SUM(extend_bytes), 0)) AS extend_sz,
    ROUND(CAST(NULLIF(SUM(extend_time), 0.0) / 1000 AS numeric),2) AS extend_time,
    NULLIF(SUM(hits), 0) AS hits,
    NULLIF(SUM(evictions), 0) AS evictions,
    NULLIF(SUM(reuses), 0) AS reuses,
    NULLIF(SUM(fsyncs), 0) AS fsyncs,
    ROUND(CAST(NULLIF(SUM(fsync_time), 0.0) / 1000 AS numeric),2) AS fsync_time

  FROM cluster_stat_io(sserver_id, start_id, end_id)
  GROUP BY ROLLUP(object, backend_type, context)
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_io_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    backend_type          text,
    object                text,
    context               text,

    reads1                bigint,
    read_sz1              text,
    read_time1            numeric,
    writes1               bigint,
    write_sz1             text,
    write_time1           numeric,
    writebacks1           bigint,
    writeback_sz1         text,
    writeback_time1       numeric,
    extends1              bigint,
    extend_sz1            text,
    extend_time1          numeric,
    hits1                 bigint,
    evictions1            bigint,
    reuses1               bigint,
    fsyncs1               bigint,
    fsync_time1           double precision,

    reads2                bigint,
    read_sz2              text,
    read_time2            numeric,
    writes2               bigint,
    write_sz2             text,
    write_time2           numeric,
    writebacks2           bigint,
    writeback_sz2         text,
    writeback_time2       numeric,
    extends2              bigint,
    extend_sz2            text,
    extend_time2          numeric,
    hits2                 bigint,
    evictions2            bigint,
    reuses2               bigint,
    fsyncs2               bigint,
    fsync_time2           double precision
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(backend_type, 'Total'),
    COALESCE(object, '*'),
    COALESCE(context, '*'),

    NULLIF(SUM(st1.reads), 0) AS reads1,
    pg_size_pretty(NULLIF(SUM(st1.read_bytes), 0)) AS read_sz1,
    ROUND(CAST(NULLIF(SUM(st1.read_time), 0.0) / 1000 AS numeric),2) AS read_time1,
    NULLIF(SUM(st1.writes), 0) AS writes1,
    pg_size_pretty(NULLIF(SUM(st1.write_bytes), 0)) AS write_sz1,
    ROUND(CAST(NULLIF(SUM(st1.write_time), 0.0) / 1000 AS numeric),2) AS write_time1,
    NULLIF(SUM(st1.writebacks), 0) AS writebacks1,
    pg_size_pretty(NULLIF(SUM(st1.writeback_bytes), 0)) AS writeback_sz1,
    ROUND(CAST(NULLIF(SUM(st1.writeback_time), 0.0) / 1000 AS numeric),2) AS writeback_time1,
    NULLIF(SUM(st1.extends), 0) AS extends1,
    pg_size_pretty(NULLIF(SUM(st1.extend_bytes), 0)) AS extend_sz1,
    ROUND(CAST(NULLIF(SUM(st1.extend_time), 0.0) / 1000 AS numeric),2) AS extend_time1,
    NULLIF(SUM(st1.hits), 0) AS hits1,
    NULLIF(SUM(st1.evictions), 0) AS evictions1,
    NULLIF(SUM(st1.reuses), 0) AS reuses1,
    NULLIF(SUM(st1.fsyncs), 0) AS fsyncs1,
    ROUND(CAST(NULLIF(SUM(st1.fsync_time), 0.0) / 1000 AS numeric),2) AS fsync_time1,

    NULLIF(SUM(st2.reads), 0) AS reads2,
    pg_size_pretty(NULLIF(SUM(st2.read_bytes), 0)) AS read_sz2,
    ROUND(CAST(NULLIF(SUM(st2.read_time), 0.0) / 1000 AS numeric),2) AS read_time2,
    NULLIF(SUM(st2.writes), 0) AS writes2,
    pg_size_pretty(NULLIF(SUM(st2.write_bytes), 0)) AS write_sz2,
    ROUND(CAST(NULLIF(SUM(st2.write_time), 0.0) / 1000 AS numeric),2) AS write_time2,
    NULLIF(SUM(st2.writebacks), 0) AS writebacks2,
    pg_size_pretty(NULLIF(SUM(st2.writeback_bytes), 0)) AS writeback_sz2,
    ROUND(CAST(NULLIF(SUM(st2.writeback_time), 0.0) / 1000 AS numeric),2) AS writeback_time2,
    NULLIF(SUM(st2.extends), 0) AS extends2,
    pg_size_pretty(NULLIF(SUM(st2.extend_bytes), 0)) AS extend_sz2,
    ROUND(CAST(NULLIF(SUM(st2.extend_time), 0.0) / 1000 AS numeric),2) AS extend_time2,
    NULLIF(SUM(st2.hits), 0) AS hits2,
    NULLIF(SUM(st2.evictions), 0) AS evictions2,
    NULLIF(SUM(st2.reuses), 0) AS reuses2,
    NULLIF(SUM(st2.fsyncs), 0) AS fsyncs2,
    ROUND(CAST(NULLIF(SUM(st2.fsync_time), 0.0) / 1000 AS numeric),2) AS fsync_time2
    
  FROM cluster_stat_io(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN cluster_stat_io(sserver_id, start2_id, end2_id) st2
    USING (server_id, backend_type, object, context)
  GROUP BY ROLLUP(object, backend_type, context)
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_io_resets(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id     integer,
    sample_id     integer,
    backend_type  text,
    object        text,
    context       text,
    stats_reset   timestamp with time zone
)
SET search_path=@extschema@ AS $$
    WITH first_val AS (
      SELECT backend_type, object, context, stats_reset
      FROM sample_stat_io st JOIN (
         SELECT backend_type, object, context, MIN(sample_id) AS sample_id
         FROM sample_stat_io
         WHERE server_id = sserver_id AND
           sample_id BETWEEN start_id AND end_id
         GROUP BY backend_type, object, context
       ) f USING (backend_type, object, context, sample_id)
     WHERE st.server_id = sserver_id
    )
    SELECT
      server_id,
      min(sample_id),
      backend_type,
      object,
      context,
      st.stats_reset
    FROM sample_stat_io st JOIN first_val USING (backend_type, object, context)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
      AND st.stats_reset IS DISTINCT FROM first_val.stats_reset
    GROUP BY server_id, backend_type, object, context, st.stats_reset
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_io_reset_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    sample_id     integer,
    backend_type  text,
    object        text,
    context       text,
    stats_reset   timestamp with time zone
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    backend_type,
    object,
    context,
    stats_reset
  FROM cluster_stat_io_resets(sserver_id, start_id, end_id)
  ORDER BY sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_io_reset_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer, IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    sample_id     integer,
    backend_type  text,
    object        text,
    context       text,
    stats_reset   timestamp with time zone
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    backend_type,
    object,
    context,
    stats_reset
  FROM (
    SELECT
      sample_id,
      backend_type,
      object,
      context,
      stats_reset
    FROM cluster_stat_io_resets(sserver_id, start1_id, end1_id)
    UNION
    SELECT
      sample_id,
      backend_type,
      object,
      context,
      stats_reset
    FROM cluster_stat_io_resets(sserver_id, start2_id, end2_id)
    ) st
  ORDER BY sample_id ASC
$$ LANGUAGE sql;
