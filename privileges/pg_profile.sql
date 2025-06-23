--<extension_start>
GRANT USAGE ON SCHEMA @extschema@ TO public;
--<extension_end>
GRANT SELECT ON sample_stat_cluster TO public;
GRANT SELECT ON sample_stat_slru TO public;
GRANT SELECT ON sample_stat_wal TO public;
GRANT SELECT ON sample_stat_io TO public;
GRANT SELECT ON sample_stat_archiver TO public;
GRANT SELECT ON indexes_list TO public;
GRANT SELECT ON sample_stat_indexes TO public;
GRANT SELECT ON sample_stat_indexes_total TO public;
GRANT SELECT ON tablespaces_list TO public;
GRANT SELECT ON sample_stat_tablespaces TO public;
GRANT SELECT ON tables_list TO public;
GRANT SELECT ON sample_stat_tables TO public;
GRANT SELECT ON sample_stat_tables_total TO public;
GRANT SELECT ON sample_settings TO public;
GRANT SELECT ON funcs_list TO public;
GRANT SELECT ON sample_stat_user_functions TO public;
GRANT SELECT ON sample_stat_user_func_total TO public;
GRANT SELECT ON sample_stat_database TO public;
GRANT SELECT ON sample_statements TO public;
GRANT SELECT ON sample_statements_total TO public;
GRANT SELECT ON sample_kcache TO public;
GRANT SELECT ON sample_kcache_total TO public;
GRANT SELECT ON roles_list TO public;
GRANT SELECT ON wait_sampling_total TO public;
GRANT SELECT (server_id, server_name, server_description, server_created, db_exclude,
  enabled, max_sample_age, last_sample_id, size_smp_wnd_start, size_smp_wnd_dur, size_smp_interval)
  ON servers TO public;
GRANT SELECT ON samples TO public;
GRANT SELECT ON baselines TO public;
GRANT SELECT ON bl_samples TO public;
GRANT SELECT ON report_static TO public;
GRANT SELECT ON report TO public;
GRANT SELECT ON report_struct TO public;
GRANT SELECT ON extension_versions TO public;
GRANT SELECT ON table_storage_parameters TO public;
GRANT SELECT ON index_storage_parameters TO public;
GRANT SELECT ON v_sample_stat_indexes TO public;
GRANT SELECT ON v_sample_stat_tablespaces TO public;
GRANT SELECT ON v_sample_timings TO public;
GRANT SELECT ON v_sample_stat_tables TO public;
GRANT SELECT ON v_sample_settings TO public;
GRANT SELECT ON v_sample_stat_user_functions TO public;
GRANT SELECT ON v_extension_versions TO public;
GRANT SELECT ON v_table_storage_parameters TO public;
GRANT SELECT ON v_index_storage_parameters TO public;

-- pg_read_all_stats can see the query texts
GRANT SELECT ON stmt_list TO pg_read_all_stats;
