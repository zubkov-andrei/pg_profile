/^CREATE FUNCTION collect_obj_stats(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION delete_samples(IN server_id integer, IN start_id integer.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION delete_samples(integer.*$/,/';$/p
/^CREATE FUNCTION drop_server(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^COMMENT ON FUNCTION drop_server.*';$/p
/^CREATE FUNCTION import_section_data_profile(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION sample_dbobj_delta(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION top_indexes(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
/^CREATE FUNCTION top_io_indexes(.*$/,/\$\$[[:space:]]*LANGUAGE[[:space:]]\+\(plpg\)\?sql[[:space:]]*;[[:space:]]*$/p
