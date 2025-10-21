CREATE FUNCTION query_pg_stat_slru(IN server_properties jsonb, IN sserver_id integer, IN ssample_id integer
) RETURNS jsonb AS $$
declare
    server_query text;
    pg_version int := (get_sp_setting(server_properties, 'server_version_num')).reset_val::integer;
begin
    server_properties := log_sample_timings(server_properties, 'query pg_stat_slru', 'start');
    -- pg_stat_slru data
    CASE
      WHEN pg_version >= 130000 THEN
        server_query := 'SELECT '
          'name,'
          'blks_zeroed,'
          'blks_hit,'
          'blks_read,'
          'blks_written,'
          'blks_exists,'
          'flushes,'
          'truncates,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_slru '
          'WHERE greatest('
              'blks_zeroed,'
              'blks_hit,'
              'blks_read,'
              'blks_written,'
              'blks_exists,'
              'flushes,'
              'truncates'
            ') > 0'
          ;
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_slru (
        server_id,
        sample_id,
        name,
        blks_zeroed,
        blks_hit,
        blks_read,
        blks_written,
        blks_exists,
        flushes,
        truncates,
        stats_reset
      )
      SELECT
        sserver_id,
        ssample_id,
        name,
        blks_zeroed,
        blks_hit,
        blks_read,
        blks_written,
        blks_exists,
        flushes,
        truncates,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        name          text,
        blks_zeroed   bigint,
        blks_hit      bigint,
        blks_read     bigint,
        blks_written  bigint,
        blks_exists   bigint,
        flushes       bigint,
        truncates     bigint,
        stats_reset   timestamp with time zone
      );
    END IF;
    server_properties := log_sample_timings(server_properties, 'query pg_stat_slru', 'end');
    return server_properties;
end;
$$ LANGUAGE plpgsql;
