CREATE FUNCTION query_pg_stat_lock(IN server_properties jsonb, IN sserver_id integer, IN ssample_id integer
) RETURNS jsonb AS $$
declare
    server_query text;
    pg_version int := (get_sp_setting(server_properties, 'server_version_num')).reset_val::integer;
begin
    server_properties := log_sample_timings(server_properties, 'query pg_stat_lock', 'start');
    -- pg_stat_lock data
    CASE
      WHEN pg_version >= 190000 THEN
        server_query := 'SELECT '
          'locktype,'
          'waits,'
          'wait_time,'
          'fastpath_exceeded,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_lock '
          'WHERE greatest('
              'waits,'
              'wait_time,'
              'fastpath_exceeded'
            ') > 0'
          ;
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_lock (
        server_id,
        sample_id,
        locktype,
        waits,
        wait_time,
        fastpath_exceeded,
        stats_reset
      )
      SELECT
        sserver_id,
        ssample_id,
        locktype,
        waits,
        round(wait_time)::bigint,
        fastpath_exceeded,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        locktype            text,
        waits               bigint,
        wait_time           double precision,
        fastpath_exceeded   bigint,
        stats_reset         timestamp with time zone
      );
    END IF;
    server_properties := log_sample_timings(server_properties, 'query pg_stat_lock', 'end');
    return server_properties;
end;
$$ LANGUAGE plpgsql;
