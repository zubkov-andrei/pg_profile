INSERT INTO import_queries_version_order VALUES
('pg_profile','4.7','pg_profile','4.6')
;

COMMENT ON FUNCTION delete_samples(name, tstzrange) IS
  'Manually deletes server samples for provided server name and time interval';
COMMENT ON FUNCTION delete_samples(tstzrange) IS
  'Manually deletes server samples for time interval on local server';
COMMENT ON FUNCTION delete_samples(name, integer, integer) IS
  'Manually deletes server samples for provided server name. By default deletes all samples';

ALTER TABLE sample_act_backend_state
  ADD COLUMN query_start timestamp with time zone,
  ADD CONSTRAINT fk_bk_state_statement
    FOREIGN KEY (server_id, sample_id, pid, query_start)
    REFERENCES sample_act_statement (server_id, sample_id, pid, query_start)
    ON DELETE CASCADE;

CREATE INDEX ix_bk_state_statements ON
  sample_act_backend_state(server_id, sample_id, pid, query_start);

UPDATE sample_act_backend_state usstate
SET query_start = stmt.query_start
FROM
  sample_act_backend_state sstate
  JOIN
  sample_act_statement stmt
  ON (sstate.server_id, sstate.sample_id, sstate.pid) =
    (stmt.server_id, stmt.sample_id, stmt.pid)
    AND sstate.state_change BETWEEN stmt.query_start AND stmt.stmt_last_ts
WHERE
  (usstate.server_id, usstate.sample_id, usstate.pid, usstate.state_change) =
  (sstate.server_id, sstate.sample_id, sstate.pid, sstate.state_change)
;

ALTER TABLE sample_act_statement
  ADD COLUMN xact_start timestamp with time zone,
  ADD CONSTRAINT fk_act_stmt_xact FOREIGN KEY (server_id, sample_id, pid, xact_start)
    REFERENCES sample_act_xact (server_id, sample_id, pid, xact_start)
    ON DELETE CASCADE;

UPDATE sample_act_statement usas
  SET xact_start = sabs.xact_start
FROM sample_act_statement sas JOIN sample_act_backend_state sabs
  USING (server_id, sample_id, pid, state_change)
WHERE (usas.server_id, usas.sample_id, usas.pid, usas.query_start) =
  (sas.server_id, sas.sample_id, sas.pid, sas.query_start);

ALTER TABLE sample_act_statement
  DROP CONSTRAINT fk_act_stmt_bk_state,
  DROP COLUMN state_change;

CREATE INDEX ix_act_stmt_xact ON sample_act_statement(server_id, sample_id, pid, xact_start);

ALTER TABLE server_subsample
  ALTER COLUMN subsample_enabled DROP NOT NULL;

ALTER TABLE last_stat_cluster
  ADD COLUMN restartpoints_timed        bigint,
  ADD COLUMN restartpoints_req          bigint,
  ADD COLUMN restartpoints_done         bigint,
  ADD COLUMN checkpoint_stats_reset     timestamp with time zone
;

ALTER TABLE sample_stat_cluster
  ADD COLUMN restartpoints_timed        bigint,
  ADD COLUMN restartpoints_req          bigint,
  ADD COLUMN restartpoints_done         bigint,
  ADD COLUMN checkpoint_stats_reset     timestamp with time zone
;

ALTER TABLE sample_statements_total
  ADD COLUMN mean_max_plan_time  double precision,
  ADD COLUMN mean_max_exec_time  double precision,
  ADD COLUMN mean_min_plan_time  double precision,
  ADD COLUMN mean_min_exec_time  double precision
;

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;

ALTER TABLE sample_act_xact
    ALTER COLUMN backend_xid TYPE text USING backend_xid::text;
ALTER TABLE sample_act_backend_state
    ALTER COLUMN backend_xmin TYPE text USING backend_xmin::text;
ALTER TABLE last_stat_activity
    ALTER COLUMN backend_xid TYPE text USING backend_xid::text,
    ALTER COLUMN backend_xmin TYPE text USING backend_xmin::text;

ALTER TABLE sample_statements
  ALTER COLUMN stddev_plan_time TYPE numeric USING
    CASE
      WHEN plans = 0 THEN 0
      WHEN plans = 1 THEN
        pow(total_plan_time::numeric, 2)
      ELSE
        pow(stddev_plan_time::numeric, 2) * plans +
        pow(mean_plan_time::numeric, 2) * plans
    END,
  ALTER COLUMN stddev_exec_time TYPE numeric USING
    CASE
      WHEN calls = 0 THEN 0
      WHEN calls = 1 THEN
        pow(total_exec_time::numeric, 2)
      ELSE
        pow(stddev_exec_time::numeric, 2) * calls +
        pow(mean_exec_time::numeric, 2) * calls
    END,
  ADD COLUMN local_blk_read_time double precision,
  ADD COLUMN local_blk_write_time  double precision,
  ADD COLUMN jit_deform_count    bigint,
  ADD COLUMN jit_deform_time     double precision,
  ADD COLUMN stats_since         timestamp with time zone,
  ADD COLUMN minmax_stats_since  timestamp with time zone
;
ALTER TABLE sample_statements
  RENAME COLUMN stddev_plan_time TO sum_plan_time_sq;
ALTER TABLE sample_statements
  RENAME COLUMN stddev_exec_time TO sum_exec_time_sq;
ALTER TABLE sample_statements
  RENAME COLUMN blk_read_time TO shared_blk_read_time;
ALTER TABLE sample_statements
  RENAME COLUMN blk_write_time TO shared_blk_write_time;

ALTER TABLE last_stat_statements
  ADD COLUMN local_blk_read_time double precision,
  ADD COLUMN local_blk_write_time  double precision,
  ADD COLUMN jit_deform_count    bigint,
  ADD COLUMN jit_deform_time     double precision,
  ADD COLUMN stats_since         timestamp with time zone,
  ADD COLUMN minmax_stats_since  timestamp with time zone
;
ALTER TABLE last_stat_statements
  RENAME COLUMN blk_read_time TO shared_blk_read_time;
ALTER TABLE last_stat_statements
  RENAME COLUMN blk_write_time TO shared_blk_write_time;

ALTER TABLE sample_statements_total
  ADD COLUMN local_blk_read_time double precision,
  ADD COLUMN local_blk_write_time  double precision,
  ADD COLUMN jit_deform_count    bigint,
  ADD COLUMN jit_deform_time     double precision
;

ALTER TABLE sample_statements_total
  RENAME COLUMN blk_read_time TO shared_blk_read_time;
ALTER TABLE sample_statements_total
  RENAME COLUMN blk_write_time TO shared_blk_write_time;

ALTER TABLE report_struct DROP COLUMN href;

ALTER TABLE last_stat_kcache
  ADD COLUMN stats_since         timestamp with time zone;
ALTER TABLE sample_kcache
  ADD COLUMN stats_since         timestamp with time zone;
