INSERT INTO import_queries_version_order VALUES
('pg_profile','4.11','pg_profile','4.10')
;

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;

-- PWR-238
truncate table sample_timings;
drop VIEW v_sample_timings;
alter table sample_timings drop CONSTRAINT pk_sample_timings;
alter table sample_timings drop time_spent;
alter table sample_timings add exec_point text;
alter table sample_timings add event_ts timestamp;
alter table sample_timings add CONSTRAINT pk_sample_timings PRIMARY KEY (server_id, sample_id, event, exec_point);
CREATE VIEW v_sample_timings AS
SELECT
  srv.server_name,
  smp.sample_id,
  smp.sample_time,
  tm.event as sampling_event,
  tm.exec_point,
  tm.event_ts
FROM
  sample_timings tm
  JOIN servers srv USING (server_id)
  JOIN samples smp USING (server_id, sample_id);
COMMENT ON VIEW v_sample_timings IS 'Sample taking time statistics with server names and sample times';
GRANT SELECT ON v_sample_timings TO public;