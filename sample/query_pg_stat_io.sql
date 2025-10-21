CREATE FUNCTION query_pg_stat_io(IN server_properties jsonb, IN sserver_id integer, IN ssample_id integer
) RETURNS jsonb AS $$
declare
    server_query text;
    pg_version int := (get_sp_setting(server_properties, 'server_version_num')).reset_val::integer;
begin
    server_properties := log_sample_timings(server_properties, 'query pg_stat_io', 'start');
    -- pg_stat_io data
    CASE
      WHEN pg_version >= 180000 THEN
        server_query := 'SELECT '
          'backend_type,'
          'object,'
          'pg_stat_io.context,'
          'reads,'
          'read_bytes,'
          'read_time,'
          'writes,'
          'write_bytes,'
          'write_time,'
          'writebacks,'
          'writeback_time,'
          'extends,'
          'extend_bytes,'
          'extend_time,'
          'ps.setting::integer AS op_bytes,'
          'hits,'
          'evictions,'
          'reuses,'
          'fsyncs,'
          'fsync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_io '
          'JOIN pg_catalog.pg_settings ps ON name = ''block_size'' '
          'WHERE greatest('
              'reads,'
              'writes,'
              'writebacks,'
              'extends,'
              'hits,'
              'evictions,'
              'reuses,'
              'fsyncs'
            ') > 0'
          ;
      WHEN pg_version >= 160000 THEN
        server_query := 'SELECT '
          'backend_type,'
          'object,'
          'context,'
          'reads,'
          'NULL as read_bytes,'
          'read_time,'
          'writes,'
          'NULL as write_bytes,'
          'write_time,'
          'writebacks,'
          'writeback_time,'
          'extends,'
          'NULL as extend_bytes,'
          'extend_time,'
          'op_bytes,'
          'hits,'
          'evictions,'
          'reuses,'
          'fsyncs,'
          'fsync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_io '
          'WHERE greatest('
              'reads,'
              'writes,'
              'writebacks,'
              'extends,'
              'hits,'
              'evictions,'
              'reuses,'
              'fsyncs'
            ') > 0'
          ;
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_io (
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
        sserver_id,
        ssample_id,
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
      FROM dblink('server_connection',server_query) AS rs (
        backend_type      text,
        object            text,
        context           text,
        reads             bigint,
        read_bytes        numeric,
        read_time         double precision,
        writes            bigint,
        write_bytes       numeric,
        write_time        double precision,
        writebacks        bigint,
        writeback_time    double precision,
        extends           bigint,
        extend_bytes      numeric,
        extend_time       double precision,
        op_bytes          bigint,
        hits              bigint,
        evictions         bigint,
        reuses            bigint,
        fsyncs            bigint,
        fsync_time        double precision,
        stats_reset       timestamp with time zone
      );
    END IF;
    server_properties := log_sample_timings(server_properties, 'query pg_stat_io', 'end');
    return server_properties;
end;
$$ LANGUAGE plpgsql;
