SET track_functions TO 'all';
/* === Initialize some structures === */
DROP TABLE IF EXISTS profile.grow_table;
CREATE TABLE profile.grow_table (
  id          SERIAL PRIMARY KEY,
  short_str   varchar(50),
  long_str    text
);

CREATE INDEX IF NOT EXISTS ix_grow_table ON profile.grow_table(short_str);

CREATE OR REPLACE FUNCTION profile.dummy_func() RETURNS VOID AS $$
BEGIN
  PERFORM pg_sleep(0.5);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION profile.grow_table_trg_f() RETURNS trigger AS
$$
BEGIN
  PERFORM pg_sleep(0.1);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER grow_table_trg
BEFORE INSERT OR UPDATE ON profile.grow_table FOR EACH ROW
EXECUTE PROCEDURE profile.grow_table_trg_f();

/* Testing sample creation */
SELECT server,result FROM profile.take_sample();
/* Perform some load */
INSERT INTO profile.grow_table (short_str,long_str)
SELECT array_to_string(array
  (select
  substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
  trunc(random() * 62)::integer + 1, 1)
  FROM   generate_series(1, 40)), ''
) as arr1,
array_to_string(array
  (select
  substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
  trunc(random() * 62)::integer + 1, 1)
  FROM   generate_series(1, 8000)), ''
)
FROM generate_series(1,20);
SELECT * FROM profile.dummy_func();
/* Taking next sample */
SELECT server,result FROM profile.take_sample();

/* Check collected data */
SELECT
  n_tup_ins,
  n_live_tup
FROM profile.sample_stat_tables st
  JOIN profile.tables_list tl USING (server_id,datid,relid)
WHERE
  tl.relname = 'grow_table' AND tl.schemaname = 'profile'
  AND st.sample_id = 2;

SELECT
  n_tup_ins,
  n_live_tup
FROM
  profile.sample_stat_tables st
  JOIN profile.tables_list tl ON
    (st.server_id = tl.server_id AND st.datid = tl.datid
    AND st.relid = tl.reltoastrelid)
WHERE
  tl.relname = 'grow_table' AND tl.schemaname = 'profile'
  AND st.sample_id = 2;

SELECT
  calls,
  total_time > 0 tt,
  self_time > 0 st,
  trg_fn
FROM
  profile.sample_stat_user_functions f
  JOIN profile.funcs_list fl USING (server_id,datid,funcid)
WHERE
  schemaname = 'profile' AND funcname IN ('grow_table_trg_f', 'dummy_func')
  AND sample_id = 2
ORDER BY funcname;

/* Testing report */
SELECT count(1) FROM profile.get_report(1,2);
/* Testing diffreport */
SELECT server,result FROM profile.take_sample();
SELECT count(1) FROM profile.get_diffreport(1,2,2,3);
/* Test server system identifier changing */
BEGIN;
UPDATE profile.sample_settings
SET reset_val = reset_val::bigint + 1
WHERE name = 'system_identifier';
SELECT server,result != 'OK' FROM profile.take_sample();
ROLLBACK;
/* Test size collection sampling settings */
INSERT INTO profile.grow_table (short_str,long_str)
SELECT array_to_string(array
  (select
  substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
  trunc(random() * 62)::integer + 1, 1)
  FROM   generate_series(1, 40)), ''
) as arr1,
array_to_string(array
  (select
  substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
  trunc(random() * 62)::integer + 1, 1)
  FROM   generate_series(1, 8000)), ''
)
FROM generate_series(1,5);
SELECT profile.set_server_size_sampling('local',current_time - interval '10 minute',interval '30 minute',interval '2 minute');
SELECT server_name,window_duration,sample_interval FROM profile.show_servers_size_sampling();
SELECT server,result FROM profile.take_sample();
SELECT sample, sizes_collected FROM profile.show_samples() WHERE NOT sizes_collected;
SELECT strpos(profile.get_report(2,3),'growing') > 0;
SELECT strpos(profile.get_report(3,4),'growing') > 0;
SELECT profile.set_server_size_sampling('local',null,null,null);
SELECT server,result FROM profile.take_sample();
SELECT strpos(profile.get_report(3,4,null,true),'growing') > 0;
DROP TABLE profile.grow_table;
DROP FUNCTION profile.dummy_func();
DROP FUNCTION profile.grow_table_trg_f();
