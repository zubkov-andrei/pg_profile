/* Drop test objects */
DROP TABLE profile.grow_table;
DROP FUNCTION profile.dummy_func();
DROP FUNCTION profile.grow_table_trg_f();
/* Testing drop server with data */
SELECT * FROM profile.drop_server('local');
DROP EXTENSION pg_profile;
DROP EXTENSION IF EXISTS pg_stat_statements;
DROP EXTENSION IF EXISTS dblink;
DROP SCHEMA profile;
DROP SCHEMA dblink;
DROP SCHEMA statements;
