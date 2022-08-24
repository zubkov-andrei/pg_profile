/^CREATE FUNCTION get_connstr(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION jsonb_replace(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION profile_checkavail_io_times(.*$/,/\$\$ LANGUAGE sql;$/p
/^CREATE FUNCTION dbstats(.*$/,/\$\$ LANGUAGE sql;$/p
/^CREATE FUNCTION top_indexes(.*$/,/\$\$ LANGUAGE sql;$/p
/^CREATE FUNCTION profile_checkavail_wait_sampling_total(.*$/,/\$\$ LANGUAGE sql;$/p
/^CREATE FUNCTION wait_sampling_total_stats(.*$/,/\$\$ LANGUAGE sql;$/p
/^CREATE FUNCTION report_queries(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer.*$/,/';$/p
/^CREATE FUNCTION get_diffreport(IN sserver_id integer, IN start1_id integer, IN end1_id integer.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION get_diffreport(IN sserver_id integer, IN start1_id integer, IN end1_id integer.*$/,/';$/p
/^CREATE FUNCTION get_report_template(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION init_report_temp_tables(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION cleanup_report_temp_tables(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION template_populate_sections(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION collect_pg_wait_sampling_stats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION collect_pg_wait_sampling_stats_11(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION collect_obj_stats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION .\+_htbl(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
