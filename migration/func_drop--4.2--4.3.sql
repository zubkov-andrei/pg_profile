DROP FUNCTION report_queries_format(sserver_id integer, queries_list jsonb, start1_id integer, end1_id integer, start2_id integer, end2_id integer);
DROP FUNCTION collect_obj_stats;
DROP FUNCTION collect_pg_stat_statements_stats;
DROP FUNCTION dbstats_reset;
DROP FUNCTION get_report_context;
DROP FUNCTION dbstats_reset_format;
DROP FUNCTION dbstats_reset_format_diff;
DROP FUNCTION drop_server;
DROP FUNCTION get_report_datasets;
DROP FUNCTION init_sample;
DROP FUNCTION sample_dbobj_delta;
DROP FUNCTION sections_jsonb;
DROP FUNCTION show_servers;
DROP FUNCTION take_sample(sserver_id integer, skip_sizes boolean);
DROP FUNCTION top_io_tables_format;
DROP FUNCTION top_io_tables_format_diff;
DROP FUNCTION top_tables;
DROP FUNCTION top_tables_format;
DROP FUNCTION top_tables_format_diff;
DROP FUNCTION top_toasts;
DROP FUNCTION save_pg_stat_statements;
