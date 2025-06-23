/* == Testing server management functions == */
SELECT profile.create_server('srvtest','dbname=postgres host=localhost port=5432', TRUE, NULL, 'Server description 1');
SELECT server_id, server_name, server_description, db_exclude,
  enabled, connstr, max_sample_age, last_sample_id
FROM profile.servers WHERE server_name != 'local';
SELECT profile.rename_server('srvtest','srvtestrenamed');
SELECT profile.set_server_connstr('srvtestrenamed','dbname=postgres host=localhost port=5433');
SELECT profile.set_server_description('srvtestrenamed','Server description 2');
SELECT profile.set_server_db_exclude('srvtestrenamed',ARRAY['db1','db2','db3']);
SELECT profile.set_server_max_sample_age('srvtestrenamed',3);
-- settings validation test
SELECT profile.set_server_setting('srvtestrenamed','name_failure','test');
SELECT profile.set_server_setting('srvtestrenamed','collect_vacuum_stats','value_failure');
SELECT profile.set_server_setting('srvtestrenamed','collect_vacuum_stats','on');
SELECT srv_settings::text FROM profile.servers ORDER BY server_id;
SELECT * FROM profile.show_server_settings('srvtestrenamed');
SELECT profile.set_server_setting('srvtestrenamed','collect_vacuum_stats');
SELECT * FROM profile.show_server_settings('srvtestrenamed');
SELECT server_id, server_name, server_description, db_exclude,
  enabled, connstr, max_sample_age, last_sample_id
FROM profile.servers WHERE server_name != 'local';
SELECT profile.disable_server('srvtestrenamed');
SELECT server_id, server_name, server_description, db_exclude,
  enabled, connstr, max_sample_age, last_sample_id
FROM profile.servers WHERE server_name != 'local';
SELECT profile.enable_server('srvtestrenamed');
SELECT server_id, server_name, server_description, db_exclude,
  enabled, connstr, max_sample_age, last_sample_id
FROM profile.servers WHERE server_name != 'local';
SELECT * FROM profile.show_servers() where server_name != 'local';
SELECT * FROM profile.drop_server('srvtestrenamed');
