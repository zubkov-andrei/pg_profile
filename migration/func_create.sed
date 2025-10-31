/^CREATE FUNCTION top_tables(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION top_toasts(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION collect_obj_stats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION sample_dbobj_delta(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION get_report_context(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION import_section_data_profile(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION import_section_data_subsample(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION import_data(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION import_data(.*$/,/';$/p
/^CREATE FUNCTION init_sample(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION log_sample_timings(.*$/,/'log event to sample_timings';$/p
/^CREATE FUNCTION take_sample(IN sserver_id integer.*$/,/'Statistics sample creation function (by server_id)';$/p
/^CREATE FUNCTION take_sample(IN server name.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION take_sample(IN server name.*$/,/';$/p
/^CREATE FUNCTION take_sample_subset(IN sets_cnt integer.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION take_sample_subset(IN sets_cnt integer.*$/,/';$/p
/^CREATE[[:space:]]\+FUNCTION[[:space:]]\+calculate_.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE[[:space:]]\+FUNCTION[[:space:]]\+collect_tablespace_stats(.*$/,/\$\w*\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE[[:space:]]\+FUNCTION[[:space:]]\+collect_database_stats(.*$/,/\$\w*\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE[[:space:]]\+FUNCTION[[:space:]]\+delete_obsolete_samples(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE[[:space:]]\+FUNCTION[[:space:]]\+get_sp_setting(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*IMMUTABLE[[:space:]]*;[[:space:]]*$/p
/^CREATE[[:space:]]\+FUNCTION[[:space:]]\+query_pg_stat_.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION take_subsample(IN sserver_id integer.*$/,/'Take a sub-sample for a server by server_id';$/p
/^CREATE FUNCTION export_data(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION export_data(.*$/,/';$/p