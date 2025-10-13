SET client_min_messages = WARNING;
SET pg_profile.relsize_collect_mode = 'on';
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
/* Enable scheduled relation sizes collection at server*/
SELECT profile.set_server_size_sampling('local',current_time - interval '10 minute',interval '30 minute',interval '2 minute','schedule');
-- check show_servers_size_sampling()
SELECT server_name,window_duration,sample_interval,collect_mode FROM profile.show_servers_size_sampling();
-- (sample 4)
SELECT server,result FROM profile.take_sample();
-- Disable relation sizes collection at server
SELECT profile.set_server_size_sampling('local',null,null,null,'failure');
SELECT profile.set_server_size_sampling('local',null,null,null,'off');
-- (sample 5)
SELECT server,result FROM profile.take_sample();
-- Enable relation sizes collection at server
SELECT profile.set_server_size_sampling('local',null,null,null,'on');
-- (sample 6)
SELECT server,result FROM profile.take_sample();
-- Reset relation sizes collection mode at server
SELECT profile.set_server_size_sampling('local',null,null,null,null);
-- Enable relation sizes collection configuration parameter
SET pg_profile.relsize_collect_mode = 'on';
-- (sample 7)
SELECT server,result FROM profile.take_sample();
-- Disable relation sizes collection configuration parameter
SET pg_profile.relsize_collect_mode = 'off';
-- (sample 8)
SELECT server,result FROM profile.take_sample();
-- check show_samples()
SELECT sample, sizes_collected FROM profile.show_samples() WHERE NOT sizes_collected;
-- check tables sizes collection
SELECT
  sample_id,
  count(relsize) > 0 as relsize,
  count(relsize_diff) > 0 as relsize_diff,
  count(relpages_bytes) > 0 as relpages,
  count(relpages_bytes_diff) > 0 as relpages_diff
FROM profile.sample_stat_tables GROUP BY sample_id
ORDER BY sample_id;
-- check indexes sizes collection
SELECT
  sample_id,
  count(relsize) > 0 as relsize,
  count(relsize_diff) > 0 as relsize_diff,
  count(relpages_bytes) > 0 as relpages,
  count(relpages_bytes_diff) > 0 as relpages_diff
FROM profile.sample_stat_indexes GROUP BY sample_id
ORDER BY sample_id;
