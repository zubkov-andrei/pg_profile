UPDATE profile.samples
SET sample_time = now() - (5 - sample_id) * '1 day'::interval - '10 minutes'::interval
WHERE sample_id <= 5;

ALTER TABLE profile.test_rel_storage_params1 RESET (autovacuum_vacuum_threshold,fillfactor,autovacuum_enabled);
ALTER INDEX profile.ix_test_rel_storage_params_id1 RESET (fillfactor);
ALTER INDEX profile.ix_test_rel_storage_params_val1 RESET (fillfactor);

SELECT server,result FROM profile.take_sample();

SELECT relname, t.last_sample_id, reloptions
FROM profile.table_storage_parameters t JOIN profile.tables_list
  USING (server_id, relid)
WHERE relname IN ('test_rel_storage_params1','test_rel_storage_params2') ORDER BY relid, last_sample_id;
SELECT relname, indexrelname, i.last_sample_id, reloptions
FROM profile.index_storage_parameters i JOIN profile.tables_list
  USING (server_id, relid)
  JOIN profile.indexes_list USING (server_id, relid, indexrelid)
WHERE relname IN ('test_rel_storage_params1','test_rel_storage_params2') ORDER BY relid, indexrelid, last_sample_id;

BEGIN;
  SELECT profile.delete_samples();
  SELECT sample FROM profile.show_samples() ORDER BY sample;
ROLLBACK;
SELECT count(*) FROM profile.samples WHERE sample_time < now() - '1 days'::interval;
SELECT * FROM profile.set_server_max_sample_age('local',1);
/* Testing baseline creation */
SELECT * FROM profile.create_baseline('testline1',2,4);
BEGIN;
  SELECT profile.delete_samples('local',tstzrange(
      (SELECT sample_time FROM profile.samples WHERE sample_id = 1),
      (SELECT sample_time FROM profile.samples WHERE sample_id = 5),
      '[]'
    )
  );
  SELECT sample FROM profile.show_samples() ORDER BY sample;
ROLLBACK;
BEGIN;
  SELECT profile.delete_samples(tstzrange(
      (SELECT sample_time FROM profile.samples WHERE sample_id = 1),
      (SELECT sample_time FROM profile.samples WHERE sample_id = 5),
      '[]'
    )
  );
  SELECT sample FROM profile.show_samples() ORDER BY sample;
ROLLBACK;
SELECT * FROM profile.create_baseline('testline2',2,4);
SELECT count(*) FROM profile.baselines;
SELECT * FROM profile.keep_baseline('testline2',-1);
/* Testing baseline show */
SELECT baseline, min_sample, max_sample, keep_until_time IS NULL
FROM profile.show_baselines()
ORDER BY baseline;
/* Testing baseline deletion */
SELECT server,result FROM profile.take_sample();
SELECT count(*) FROM profile.baselines;
/* Testing samples retention override with baseline */
SELECT count(*) FROM profile.samples WHERE sample_time < now() - '1 days'::interval;
SELECT * FROM profile.drop_baseline('testline1');
/* Testing samples deletion after baseline removed */
SELECT server,result FROM profile.take_sample();
SELECT count(*) FROM profile.samples WHERE sample_time < now() - '1 days'::interval;
