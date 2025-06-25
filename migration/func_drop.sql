DROP FUNCTION collect_pg_stat_statements_stats;
DROP FUNCTION stat_activity_states_format(integer, integer, integer, integer, integer);
DROP FUNCTION stat_activity_states_format(integer, integer, integer);
DROP FUNCTION top_rusage_statements_format;
DROP FUNCTION top_rusage_statements_format_diff;
DROP FUNCTION top_statements_format;
DROP FUNCTION top_statements_format_diff;
DROP FUNCTION get_report_context;
DROP FUNCTION statements_dbstats;
DROP FUNCTION statements_dbstats_format;
DROP FUNCTION statements_dbstats_format_diff;
DROP FUNCTION delete_samples(integer, integer, integer);

DROP FUNCTION import_section_data_profile;
DROP FUNCTION drop_server;
DROP FUNCTION get_report_datasets;
DROP FUNCTION collect_obj_stats;
DROP FUNCTION sample_dbobj_delta;

DROP FUNCTION init_sample;
DROP FUNCTION take_sample(integer, boolean);
DROP FUNCTION take_sample(name, boolean);
DROP FUNCTION take_sample_subset(integer, integer);
DROP FUNCTION settings_format;
DROP FUNCTION settings_format_diff;

DROP FUNCTION extension_versions_format;
DROP FUNCTION get_report(integer, integer, integer, text, boolean, name[]);
DROP FUNCTION get_report(name, integer, integer, text, boolean, name[]);
DROP FUNCTION get_report(integer, integer, text, boolean, name[]);
DROP FUNCTION get_report(integer, tstzrange, text, boolean, name[]);
DROP FUNCTION get_report(name, tstzrange, text, boolean, name[]);
DROP FUNCTION get_report(tstzrange, text, boolean, name[]);
DROP FUNCTION get_report(name, varchar, text, boolean, name[]);
DROP FUNCTION get_report(varchar, text, boolean, name[]);
DROP FUNCTION get_report_latest;
DROP FUNCTION get_diffreport(integer, integer, integer, integer, integer, text, boolean, name[]);
DROP FUNCTION get_diffreport(name, integer, integer, integer, integer, text, boolean, name[]);
DROP FUNCTION get_diffreport(integer, integer, integer, integer, text, boolean, name[]);
DROP FUNCTION get_diffreport(name, varchar, varchar, text, boolean, name[]);
DROP FUNCTION get_diffreport(varchar, varchar, text, boolean, name[]);
DROP FUNCTION get_diffreport(name, varchar, integer, integer, text, boolean, name[]);
DROP FUNCTION get_diffreport(varchar, integer, integer, text, boolean, name[]);
DROP FUNCTION get_diffreport(name, integer, integer, varchar, text, boolean, name[]);
DROP FUNCTION get_diffreport(integer, integer, varchar, text, boolean, name[]);
DROP FUNCTION get_diffreport(name, tstzrange, tstzrange, text, boolean, name[]);
DROP FUNCTION get_diffreport(name, varchar, tstzrange, text, boolean, name[]);
DROP FUNCTION get_diffreport(name, tstzrange, varchar, text, boolean, name[]);

DROP FUNCTION import_data;
DROP FUNCTION export_data;
DROP FUNCTION cluster_stats;
DROP FUNCTION cluster_stats_reset;
DROP FUNCTION dbstats;
DROP FUNCTION dbstats_format;
DROP FUNCTION dbstats_format_diff;
DROP FUNCTION dbstats_reset;
DROP FUNCTION cluster_stat_io_resets;
DROP FUNCTION cluster_stat_slru_resets;
DROP FUNCTION tablespace_stats;
DROP FUNCTION tablespace_stats_format;
DROP FUNCTION tablespace_stats_format_diff;

DROP FUNCTION set_server_size_sampling;
DROP FUNCTION show_servers_size_sampling;
DROP FUNCTION top_indexes;

DROP FUNCTION top_statements;
