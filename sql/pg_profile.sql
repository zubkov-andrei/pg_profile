CREATE SCHEMA IF NOT EXISTS profile;
CREATE SCHEMA IF NOT EXISTS dblink;
CREATE SCHEMA IF NOT EXISTS statements;
CREATE EXTENSION dblink SCHEMA dblink;
CREATE EXTENSION pg_stat_statements SCHEMA statements;
CREATE EXTENSION pg_profile SCHEMA profile;

/* == Testing server management functions == */
SELECT profile.create_server('srvtest','dbname=postgres host=localhost port=5432');
SELECT * FROM servers;
SELECT profile.rename_server('srvtest','srvtestrenamed');
SELECT profile.set_server_connstr('srvtestrenamed','dbname=postgres host=localhost port=5433');
SELECT profile.set_server_db_exclude('srvtestrenamed',ARRAY['db1','db2','db3']);
SELECT profile.set_server_max_sample_age('srvtestrenamed',3);
SELECT * FROM servers;
SELECT profile.disable_server('srvtestrenamed');
SELECT * FROM servers;
SELECT profile.enable_server('srvtestrenamed');
SELECT * FROM servers;
SELECT * FROM profile.drop_server('srvtestrenamed');
/* Testing sample creation */
SELECT * FROM profile.take_sample();
SELECT * FROM profile.take_sample();
/* Testing report */
SELECT count(1) FROM profile.get_report(1,2);
/* Testing diffreport */
SELECT * FROM profile.take_sample();
SELECT count(1) FROM profile.get_diffreport(1,2,2,3);
SELECT * FROM profile.take_sample();
SELECT * FROM profile.take_sample();
UPDATE profile.samples SET sample_time = now() - '4 days'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 1;
UPDATE profile.samples SET sample_time = now() - '3 days'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 2;
UPDATE profile.samples SET sample_time = now() - '2 days'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 3;
UPDATE profile.samples SET sample_time = now() - '1 days'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 4;
UPDATE profile.samples SET sample_time = now() - '23 hours'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 5;
SELECT * FROM profile.take_sample();
SELECT count(*) FROM profile.samples WHERE sample_time < now() - '1 days'::interval;
SELECT * FROM profile.set_server_max_sample_age('local',1);
/* Testing baseline creation */
SELECT * FROM profile.create_baseline('testline1',2,4);
SELECT * FROM profile.create_baseline('testline2',2,4);
SELECT count(*) FROM baselines;
SELECT * FROM profile.keep_baseline('testline2',-1);
/* Testing baseline show */
SELECT count(*) FROM profile.show_baselines();
/* Testing baseline deletion */
SELECT * FROM profile.take_sample();
SELECT count(*) FROM baselines;
/* Testing samples retention override with baseline */
SELECT count(*) FROM profile.samples WHERE sample_time < now() - '1 days'::interval;
SELECT * FROM profile.drop_baseline('testline1');
/* Testing samples deletion after baseline removed */
SELECT * FROM profile.take_sample();
SELECT count(*) FROM profile.samples WHERE sample_time < now() - '1 days'::interval;
/* Testing drop server with data */
SELECT * FROM profile.drop_server('local');
DROP EXTENSION pg_profile;
DROP EXTENSION IF EXISTS pg_stat_statements;
DROP EXTENSION IF EXISTS dblink;
DROP SCHEMA profile;
DROP SCHEMA dblink;
DROP SCHEMA statements;
