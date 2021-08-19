SET track_functions TO 'all';
SET client_min_messages = WARNING;
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
-- (sample 1)
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
-- (sample 2)
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
-- (sample 3)
SELECT server,result FROM profile.take_sample();
SELECT count(1) FROM profile.get_diffreport(1,2,2,3);
/* Test server system identifier changing */
BEGIN;
UPDATE profile.sample_settings
SET reset_val = reset_val::bigint + 1
WHERE name = 'system_identifier';
-- (sample 4)
SELECT server,result != 'OK' FROM profile.take_sample();
ROLLBACK;
