DROP FUNCTION collect_subsamples;
DROP FUNCTION create_server(name, text, boolean, integer, text);
DROP FUNCTION get_report_context;
DROP FUNCTION import_section_data_subsample;
DROP FUNCTION init_sample;
DROP FUNCTION sample_dbobj_delta;
DROP FUNCTION set_server_subsampling;
DROP FUNCTION settings_format;
DROP FUNCTION settings_format_diff;
DROP FUNCTION stat_activity_states;
DROP FUNCTION take_sample(integer, boolean);
DROP FUNCTION take_subsample(integer, jsonb);
DROP FUNCTION take_subsample_subset;

DROP FUNCTION collect_obj_stats;
DROP FUNCTION import_section_data_profile;
DROP FUNCTION report_active_queries_format;
DROP FUNCTION report_queries_format;
DROP FUNCTION take_sample_subset;
DROP FUNCTION get_connstr;
DROP FUNCTION collect_pg_stat_statements_stats;
DROP FUNCTION export_data;
DROP FUNCTION top_rusage_statements_format;
DROP FUNCTION top_rusage_statements_format_diff;
DROP FUNCTION top_statements_format;
DROP FUNCTION top_statements_format_diff;

DROP FUNCTION cluster_stats;
DROP FUNCTION cluster_stats_format;
DROP FUNCTION cluster_stats_format_diff;
DROP FUNCTION cluster_stats_reset;
DROP FUNCTION cluster_stats_reset_format;
DROP FUNCTION cluster_stats_reset_format_diff;
DROP FUNCTION top_statements;

DROP FUNCTION save_pg_stat_statements;
DROP FUNCTION statements_dbstats;
DROP FUNCTION statements_dbstats_format;
DROP FUNCTION statements_dbstats_format_diff;

DROP FUNCTION stat_activity_states_format(integer, integer, integer);
DROP FUNCTION stat_activity_states_format(integer, integer, integer, integer, integer);
DROP FUNCTION mark_pg_stat_statements;
DROP FUNCTION sections_jsonb;
