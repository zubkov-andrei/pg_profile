/^CREATE FUNCTION collect_pg_stat_statements_stats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION stat_activity_states_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION top_rusage_statements_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION top_rusage_statements_format_diff(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION top_statements_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION top_statements_format_diff(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION get_report_context(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION statements_dbstats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION statements_dbstats_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION statements_dbstats_format_diff(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION delete_samples(IN server_id integer.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION delete_samples(integer\, integer\, integer.*$/,/';$/p

/^CREATE FUNCTION import_section_data_profile(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION drop_server(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION drop_server(.*';$/p
/^CREATE FUNCTION get_report_datasets(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION collect_obj_stats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION sample_dbobj_delta(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION table_storage_parameters_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION index_storage_parameters_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p

/^CREATE FUNCTION init_sample(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION take_sample(IN sserver_id integer.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION take_sample(IN sserver_id integer.*$/,/';$/p
/^CREATE FUNCTION take_sample(IN server name.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION take_sample(IN server name.*$/,/';$/p
/^CREATE FUNCTION take_sample_subset(IN sets_cnt integer.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION take_sample_subset(IN sets_cnt integer.*$/,/';$/p

/^CREATE FUNCTION settings_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION settings_format_diff(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p

/^CREATE FUNCTION extension_versions_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION get_report(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION get_report(.*$/,/';$/p
/^CREATE FUNCTION get_report_latest(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION get_report_latest(.*';$/p
/^CREATE FUNCTION get_diffreport(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION get_diffreport(.*$/,/';$/p

/^CREATE FUNCTION import_data(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION import_data(.*$/,/';$/p
/^CREATE FUNCTION export_data(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION export_data(.*$/,/';$/p
/^CREATE FUNCTION cluster_stats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION cluster_stats_reset(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION dbstats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION dbstats_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION dbstats_format_diff(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION dbstats_reset(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION cluster_stat_io_resets(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION cluster_stat_slru_resets(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION tablespace_stats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION tablespace_stats_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION tablespace_stats_format_diff(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p

/^CREATE FUNCTION set_server_setting(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION show_server_settings(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION set_server_size_sampling(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION set_server_size_sampling(.*$/,/';$/p
/^CREATE FUNCTION show_servers_size_sampling(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION show_servers_size_sampling(.*$/,/';$/p
/^CREATE FUNCTION top_indexes(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p

/^CREATE FUNCTION top_statements(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
