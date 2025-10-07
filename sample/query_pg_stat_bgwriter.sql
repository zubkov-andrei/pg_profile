CREATE FUNCTION query_pg_stat_bgwriter(IN server_properties jsonb, IN sserver_id integer, IN ssample_id integer
) RETURNS jsonb AS $$
declare
    server_query text;
    pg_version int := (get_sp_setting(server_properties, 'server_version_num')).reset_val::integer;
begin
    server_properties := log_sample_timings(server_properties, 'query pg_stat_bgwriter', 'start');
    -- pg_stat_bgwriter data
    CASE
      WHEN pg_version < 100000 THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'NULL as checkpoints_done,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'NULL as slru_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
          'THEN pg_catalog.pg_xlog_location_diff(pg_catalog.pg_last_xlog_replay_location(),''0/00000000'') '
          'ELSE pg_catalog.pg_xlog_location_diff(pg_catalog.pg_current_xlog_location(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
          'THEN pg_catalog.pg_last_xlog_replay_location() '
          'ELSE pg_catalog.pg_current_xlog_location() '
          'END AS wal_lsn,'
          'pg_is_in_recovery() AS in_recovery,'
          'NULL AS restartpoints_timed,'
          'NULL AS restartpoints_req,'
          'NULL AS restartpoints_done,'
          'stats_reset as checkpoint_stats_reset '
          'FROM pg_catalog.pg_stat_bgwriter';
      WHEN pg_version < 170000 THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'NULL as checkpoints_done,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'NULL as slru_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(),''0/00000000'') '
            'ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_last_wal_replay_lsn() '
            'ELSE pg_catalog.pg_current_wal_lsn() '
          'END AS wal_lsn,'
          'pg_catalog.pg_is_in_recovery() as in_recovery, '
          'NULL AS restartpoints_timed,'
          'NULL AS restartpoints_req,'
          'NULL AS restartpoints_done,'
          'stats_reset as checkpoint_stats_reset '
        'FROM pg_catalog.pg_stat_bgwriter';
      WHEN pg_version < 180000 THEN
        server_query := 'SELECT '
          'c.num_timed as checkpoints_timed,'
          'c.num_requested as checkpoints_req,'
          'NULL as checkpoints_done,'
          'c.write_time as checkpoint_write_time,'
          'c.sync_time as checkpoint_sync_time,'
          'c.buffers_written as buffers_checkpoint,'
          'NULL as slru_checkpoint,'
          'b.buffers_clean as buffers_clean,'
          'b.maxwritten_clean as maxwritten_clean,'
          'NULL as buffers_backend,'
          'NULL as buffers_backend_fsync,'
          'b.buffers_alloc,'
          'b.stats_reset as stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(),''0/00000000'') '
            'ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery()'
            'THEN pg_catalog.pg_last_wal_replay_lsn()'
            'ELSE pg_catalog.pg_current_wal_lsn()'
          'END AS wal_lsn,'
          'pg_catalog.pg_is_in_recovery() as in_recovery,'
          'c.restartpoints_timed,'
          'c.restartpoints_req,'
          'c.restartpoints_done,'
          'c.stats_reset as checkpoint_stats_reset '
        'FROM '
          'pg_catalog.pg_stat_checkpointer c CROSS JOIN '
          'pg_catalog.pg_stat_bgwriter b';
      WHEN pg_version >= 180000 THEN
        server_query := 'SELECT '
          'c.num_timed as checkpoints_timed,'
          'c.num_requested as checkpoints_req,'
          'c.num_done as checkpoints_done,'
          'c.write_time as checkpoint_write_time,'
          'c.sync_time as checkpoint_sync_time,'
          'c.buffers_written as buffers_checkpoint,'
          'c.slru_written as slru_checkpoint,'
          'b.buffers_clean as buffers_clean,'
          'b.maxwritten_clean as maxwritten_clean,'
          'NULL as buffers_backend,'
          'NULL as buffers_backend_fsync,'
          'b.buffers_alloc,'
          'b.stats_reset as stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(),''0/00000000'') '
            'ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery()'
            'THEN pg_catalog.pg_last_wal_replay_lsn()'
            'ELSE pg_catalog.pg_current_wal_lsn()'
          'END AS wal_lsn,'
          'pg_catalog.pg_is_in_recovery() as in_recovery,'
          'c.restartpoints_timed,'
          'c.restartpoints_req,'
          'c.restartpoints_done,'
          'c.stats_reset as checkpoint_stats_reset '
        'FROM '
          'pg_catalog.pg_stat_checkpointer c CROSS JOIN '
          'pg_catalog.pg_stat_bgwriter b';
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_cluster (
        server_id,
        sample_id,
        checkpoints_timed,
        checkpoints_req,
        checkpoints_done,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        slru_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset,
        wal_size,
        wal_lsn,
        in_recovery,
        restartpoints_timed,
        restartpoints_req,
        restartpoints_done,
        checkpoint_stats_reset)
      SELECT
        sserver_id,
        ssample_id,
        checkpoints_timed,
        checkpoints_req,
        checkpoints_done,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        slru_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset,
        wal_size,
        wal_lsn,
        in_recovery,
        restartpoints_timed,
        restartpoints_req,
        restartpoints_done,
        checkpoint_stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        checkpoints_timed bigint,
        checkpoints_req bigint,
        checkpoints_done bigint,
        checkpoint_write_time double precision,
        checkpoint_sync_time double precision,
        buffers_checkpoint bigint,
        slru_checkpoint bigint,
        buffers_clean bigint,
        maxwritten_clean bigint,
        buffers_backend bigint,
        buffers_backend_fsync bigint,
        buffers_alloc bigint,
        stats_reset timestamp with time zone,
        wal_size bigint,
        wal_lsn pg_lsn,
        in_recovery boolean,
        restartpoints_timed bigint,
        restartpoints_req bigint,
        restartpoints_done bigint,
        checkpoint_stats_reset timestamp with time zone);
    END IF;
    server_properties := log_sample_timings(server_properties, 'query pg_stat_bgwriter', 'end');
    return server_properties;
end;
$$ LANGUAGE plpgsql;