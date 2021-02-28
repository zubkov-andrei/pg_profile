UPDATE profile.samples SET sample_time = now() - '4 days'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 1;
UPDATE profile.samples SET sample_time = now() - '3 days'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 2;
UPDATE profile.samples SET sample_time = now() - '2 days'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 3;
UPDATE profile.samples SET sample_time = now() - '1 days'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 4;
UPDATE profile.samples SET sample_time = now() - '23 hours'::interval - '10 minutes'::interval WHERE server_id = 1 AND sample_id = 5;
SELECT server,result FROM profile.take_sample();
SELECT count(*) FROM profile.samples WHERE sample_time < now() - '1 days'::interval;
SELECT * FROM profile.set_server_max_sample_age('local',1);
/* Testing baseline creation */
SELECT * FROM profile.create_baseline('testline1',2,4);
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
