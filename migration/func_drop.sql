DROP FUNCTION delete_samples(integer, integer, integer);
DROP FUNCTION collect_obj_stats;
DROP FUNCTION get_report_context;
DROP FUNCTION get_report_datasets;
DROP FUNCTION import_section_data_profile;
DROP FUNCTION sample_dbobj_delta;
DROP FUNCTION stat_activity_states;
DROP FUNCTION collect_pg_stat_statements_stats;
DROP FUNCTION drop_server;
DROP FUNCTION stat_activity_states_format(integer, integer, integer, integer, integer);
DROP FUNCTION stat_activity_states_format(integer, integer, integer);
DROP FUNCTION statements_dbstats;
DROP FUNCTION import_data;
DROP FUNCTION collect_pg_wait_sampling_stats_11;

DROP FUNCTION get_report(integer, integer, integer, text, boolean);
DROP FUNCTION get_report(name, integer, integer, text, boolean);
DROP FUNCTION get_report(integer, integer, text, boolean);
DROP FUNCTION get_report(integer, tstzrange, text, boolean);
DROP FUNCTION get_report(name, tstzrange, text, boolean);
DROP FUNCTION get_report(tstzrange, text, boolean);
DROP FUNCTION get_report(name, varchar, text, boolean);
DROP FUNCTION get_report(varchar, text, boolean);
DROP FUNCTION get_report_latest(name);

DROP FUNCTION get_diffreport(integer, integer, integer, integer, integer, text, boolean);
DROP FUNCTION get_diffreport(name, integer, integer, integer, integer, text, boolean);
DROP FUNCTION get_diffreport(integer, integer, integer, integer, text, boolean);
DROP FUNCTION get_diffreport(name, varchar, varchar, text, boolean);
DROP FUNCTION get_diffreport(varchar, varchar, text, boolean);
DROP FUNCTION get_diffreport(name, varchar, integer, integer, text, boolean);
DROP FUNCTION get_diffreport(varchar, integer, integer, text, boolean);
DROP FUNCTION get_diffreport(name, integer, integer, varchar, text, boolean);
DROP FUNCTION get_diffreport(integer, integer, varchar, text, boolean);
DROP FUNCTION get_diffreport(name, tstzrange, tstzrange, text, boolean);
DROP FUNCTION get_diffreport(name, varchar, tstzrange, text, boolean);
DROP FUNCTION get_diffreport(name, tstzrange, varchar, text, boolean);

DROP FUNCTION take_sample(integer, boolean);
