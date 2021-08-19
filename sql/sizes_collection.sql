SET client_min_messages = WARNING;
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
/* Test rare relation sizes collection */
SELECT profile.set_server_size_sampling('local',current_time - interval '10 minute',interval '30 minute',interval '2 minute');
SELECT server_name,window_duration,sample_interval,limited_collection FROM profile.show_servers_size_sampling();
-- vacuum a table to test index collection size
VACUUM profile.grow_table;
-- statistics collector emits a new report once in 500 ms by default
SELECT pg_sleep(0.6);
-- (sample 4)
SELECT server,result FROM profile.take_sample();
-- restrict limited sizes collection
SELECT profile.set_server_size_sampling('local',current_time - interval '10 minute',interval '30 minute',interval '2 minute', false);
VACUUM profile.grow_table;
-- statistics collector emits a new report once in 500 ms by default
SELECT pg_sleep(0.6);
-- (sample 5)
SELECT server,result FROM profile.take_sample();
SELECT sample, sizes_collected FROM profile.show_samples() WHERE NOT sizes_collected;
SELECT strpos(profile.get_report(2,3),'growing') > 0;
SELECT strpos(profile.get_report(3,4),'growing') > 0;
-- check index size collection
SELECT sample_id, tl.schemaname, relname, indexrelname, vacuum_count, autovacuum_count, ist.relsize > 0 AS size_collected
FROM profile.sample_stat_indexes ist
  JOIN profile.indexes_list il USING (server_id, datid, indexrelid)
  JOIN profile.tables_list tl USING (server_id, datid, relid)
  JOIN profile.sample_stat_tables USING (server_id, sample_id, datid, relid)
WHERE (server_id, tl.schemaname, relname) = (1, 'profile', 'grow_table')
  AND sample_id BETWEEN 4 AND 5
ORDER BY 1,2,3,4;
-- check table size collection
SELECT sample_id, tl.schemaname, relname, vacuum_count, autovacuum_count, st.relsize > 0 AS size_collected
FROM profile.sample_stat_tables st
  JOIN profile.tables_list tl USING (server_id, datid, relid)
WHERE (server_id, tl.schemaname, relname) = (1, 'profile', 'grow_table')
  AND sample_id BETWEEN 4 AND 5
ORDER BY 1,2,3,4;
-- Disable rare sizes collection
SELECT profile.set_server_size_sampling('local',null,null,null);
SELECT server,result FROM profile.take_sample();
SELECT strpos(profile.get_report(5,6,null,true),'growing') > 0;