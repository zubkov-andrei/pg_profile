/^CREATE FUNCTION cluster_stat_io_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION cluster_stat_slru_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION wal_stats_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION get_report_datasets(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION import_section_data_profile(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION import_data(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION import_data.*$/,/';$/p
/^CREATE FUNCTION drop_server(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION drop_server.*';$/p
/^CREATE FUNCTION report_queries_format(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION sections_jsonb(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
