CREATE FUNCTION query_pg_stat_wal(IN server_properties jsonb, IN sserver_id integer, IN ssample_id integer
) RETURNS jsonb AS $$
declare
    server_query text;
    pg_version int := (get_sp_setting(server_properties, 'server_version_num')).reset_val::integer;
begin
    server_properties := log_sample_timings(server_properties, 'query pg_stat_wal', 'start');
    -- pg_stat_wal data
    CASE
      WHEN pg_version >= 180000 THEN
        server_query := 'SELECT '
          'wal.wal_records,'
          'wal.wal_fpi,'
          'wal.wal_bytes,'
          'wal.wal_buffers_full,'
          'NULL as wal_write,'
          'NULL as wal_sync,'
          'NULL as wal_write_time,'
          'NULL as wal_sync_time,'
          'wal.stats_reset '
          'FROM pg_catalog.pg_stat_wal wal';
      WHEN pg_version >= 140000 THEN
        server_query := 'SELECT '
          'wal_records,'
          'wal_fpi,'
          'wal_bytes,'
          'wal_buffers_full,'
          'wal_write,'
          'wal_sync,'
          'wal_write_time,'
          'wal_sync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_wal';
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_wal (
        server_id,
        sample_id,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time,
        stats_reset
      )
      SELECT
        sserver_id,
        ssample_id,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        wal_buffers_full    bigint,
        wal_write           bigint,
        wal_sync            bigint,
        wal_write_time      double precision,
        wal_sync_time       double precision,
        stats_reset         timestamp with time zone);
    END IF;
    server_properties := log_sample_timings(server_properties, 'query pg_stat_wal', 'end');
    return server_properties;
end;
$$ LANGUAGE plpgsql;
