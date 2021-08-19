ALTER TABLE servers
  ADD COLUMN sizes_limited       boolean DEFAULT true;

ALTER TABLE sample_stat_database
  ADD COLUMN session_time        double precision,
  ADD COLUMN active_time         double precision,
  ADD COLUMN idle_in_transaction_time  double precision,
  ADD COLUMN sessions            bigint,
  ADD COLUMN sessions_abandoned  bigint,
  ADD COLUMN sessions_fatal      bigint,
  ADD COLUMN sessions_killed     bigint
;

ALTER TABLE last_stat_database
  ADD COLUMN session_time        double precision,
  ADD COLUMN active_time         double precision,
  ADD COLUMN idle_in_transaction_time  double precision,
  ADD COLUMN sessions            bigint,
  ADD COLUMN sessions_abandoned  bigint,
  ADD COLUMN sessions_fatal      bigint,
  ADD COLUMN sessions_killed     bigint
;

ALTER TABLE sample_statements
  ADD COLUMN toplevel            boolean
;

DROP INDEX IF EXISTS ix_tables_list_reltoast;

CREATE TABLE sample_stat_wal
(
    server_id           integer,
    sample_id           integer,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    wal_buffers_full    bigint,
    wal_write           bigint,
    wal_sync            bigint,
    wal_write_time      double precision,
    wal_sync_time       double precision,
    stats_reset         timestamp with time zone,
    CONSTRAINT fk_statwal_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_wal PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_cluster IS 'Sample WAL statistics table';

CREATE TABLE last_stat_wal AS SELECT * FROM sample_stat_wal WHERE false;
ALTER TABLE last_stat_wal ADD CONSTRAINT fk_last_stat_wal_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_wal IS 'Last WAL sample data for calculating diffs in next sample';

/* ===== import queries update ===== */
INSERT INTO import_queries_version_order VALUES
  ('pg_profile','0.3.4','pg_profile','0.3.3');

/* ===== temporary table usage optimisation ===== */
UPDATE import_queries
set query = replace(query,
'JOIN jsonb_to_recordset($1) AS srv_map ( imp_srv_id    integer, local_srv_id  integer )',
'JOIN tmp_srv_map srv_map');

UPDATE import_queries
set query = replace(query,
'JOIN jsonb_to_recordset($1) AS srv_map ( imp_srv_id   integer, local_srv_id integer )',
'JOIN tmp_srv_map srv_map');

UPDATE import_queries
set query = replace(query,
'AND imp.section_id = $2',
'AND imp.section_id = $1');

/* ===== New fields in sample_stat_databse ===== */
UPDATE import_queries SET
  query = 'INSERT INTO sample_stat_database (server_id,sample_id,datid,datname,'
    'xact_commit,xact_rollback,blks_read,blks_hit,tup_returned,tup_fetched,'
    'tup_inserted,tup_updated,tup_deleted,conflicts,temp_files,temp_bytes,'
    'deadlocks,blk_read_time,blk_write_time,stats_reset,datsize,'
    'datsize_delta,datistemplate,session_time,active_time,'
    'idle_in_transaction_time,sessions,sessions_abandoned,sessions_fatal,'
    'sessions_killed)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.datname, '
    'dt.xact_commit, '
    'dt.xact_rollback, '
    'dt.blks_read, '
    'dt.blks_hit, '
    'dt.tup_returned, '
    'dt.tup_fetched, '
    'dt.tup_inserted, '
    'dt.tup_updated, '
    'dt.tup_deleted, '
    'dt.conflicts, '
    'dt.temp_files, '
    'dt.temp_bytes, '
    'dt.deadlocks, '
    'dt.blk_read_time, '
    'dt.blk_write_time, '
    'dt.stats_reset, '
    'dt.datsize, '
    'dt.datsize_delta, '
    'dt.datistemplate, '
    'dt.session_time, '
    'dt.active_time, '
    'dt.idle_in_transaction_time, '
    'dt.sessions, '
    'dt.sessions_abandoned, '
    'dt.sessions_fatal, '
    'dt.sessions_killed '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id       integer, '
        'sample_id       integer, '
        'datid           oid, '
        'datname         name, '
        'xact_commit     bigint, '
        'xact_rollback   bigint, '
        'blks_read       bigint, '
        'blks_hit        bigint, '
        'tup_returned    bigint, '
        'tup_fetched     bigint, '
        'tup_inserted    bigint, '
        'tup_updated     bigint, '
        'tup_deleted     bigint, '
        'conflicts       bigint, '
        'temp_files      bigint, '
        'temp_bytes      bigint, '
        'deadlocks       bigint, '
        'blk_read_time   double precision, '
        'blk_write_time  double precision, '
        'stats_reset     timestamp with time zone, '
        'datsize         bigint, '
        'datsize_delta   bigint, '
        'datistemplate   boolean, '
        'session_time    double precision, '
        'active_time     double precision, '
        'idle_in_transaction_time  double precision, '
        'sessions        bigint, '
        'sessions_abandoned  bigint, '
        'sessions_fatal      bigint, '
        'sessions_killed     bigint'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_database ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'sample_stat_database');

UPDATE import_queries SET
  query = 'INSERT INTO sample_statements (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,'
    'stddev_plan_time,calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,'
    'stddev_exec_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,'
    'shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,'
    'local_blks_written,temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,'
    'wal_records,wal_fpi,wal_bytes,toplevel'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.queryid, '
    'dt.queryid_md5, '
    'dt.plans, '
    'dt.total_plan_time, '
    'dt.min_plan_time, '
    'dt.max_plan_time, '
    'dt.mean_plan_time, '
    'dt.stddev_plan_time, '
    'dt.calls, '
    'dt.total_exec_time, '
    'dt.min_exec_time, '
    'dt.max_exec_time, '
    'dt.mean_exec_time, '
    'dt.stddev_exec_time, '
    'dt.rows, '
    'dt.shared_blks_hit, '
    'dt.shared_blks_read, '
    'dt.shared_blks_dirtied, '
    'dt.shared_blks_written, '
    'dt.local_blks_hit, '
    'dt.local_blks_read, '
    'dt.local_blks_dirtied, '
    'dt.local_blks_written, '
    'dt.temp_blks_read, '
    'dt.temp_blks_written, '
    'dt.blk_read_time, '
    'dt.blk_write_time, '
    'dt.wal_records, '
    'dt.wal_fpi, '
    'dt.wal_bytes, '
    'dt.toplevel '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'userid               oid, '
        'datid                oid, '
        'queryid              bigint, '
        'queryid_md5          character(32), '
        'plans                bigint, '
        'total_plan_time      double precision, '
        'min_plan_time        double precision, '
        'max_plan_time        double precision, '
        'mean_plan_time       double precision, '
        'stddev_plan_time     double precision, '
        'calls                bigint, '
        'total_exec_time      double precision, '
        'min_exec_time        double precision, '
        'max_exec_time        double precision, '
        'mean_exec_time       double precision, '
        'stddev_exec_time     double precision, '
        'rows                 bigint, '
        'shared_blks_hit      bigint, '
        'shared_blks_read     bigint, '
        'shared_blks_dirtied  bigint, '
        'shared_blks_written  bigint, '
        'local_blks_hit       bigint, '
        'local_blks_read      bigint, '
        'local_blks_dirtied   bigint, '
        'local_blks_written   bigint, '
        'temp_blks_read       bigint, '
        'temp_blks_written    bigint, '
        'blk_read_time        double precision, '
        'blk_write_time       double precision, '
        'wal_records          bigint, '
        'wal_fpi              bigint, '
        'wal_bytes            numeric, '
        'toplevel             boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_statements ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid '
      'AND ld.userid = dt.userid AND ld.queryid = dt.queryid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.2', 1, 'sample_statements');
/* ===== V0.3.4 ===== */
INSERT INTO import_queries VALUES
('pg_profile','0.3.4', 1,'sample_stat_wal',
  'INSERT INTO sample_stat_wal (server_id,sample_id,wal_records,'
    'wal_fpi,wal_bytes,wal_buffers_full,wal_write,wal_sync,'
    'wal_write_time,wal_sync_time,stats_reset)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.wal_records, '
    'dt.wal_fpi, '
    'dt.wal_bytes, '
    'dt.wal_buffers_full, '
    'dt.wal_write, '
    'dt.wal_sync, '
    'dt.wal_write_time, '
    'dt.wal_sync_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id              integer, '
        'sample_id              integer, '
        'wal_records         bigint, '
        'wal_fpi             bigint, '
        'wal_bytes           numeric, '
        'wal_buffers_full    bigint, '
        'wal_write           bigint, '
        'wal_sync            bigint, '
        'wal_write_time      double precision, '
        'wal_sync_time       double precision, '
        'stats_reset            timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_wal ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.4', 1,'last_stat_wal',
  'INSERT INTO last_stat_wal (server_id,sample_id,wal_records,'
    'wal_fpi,wal_bytes,wal_buffers_full,wal_write,wal_sync,'
    'wal_write_time,wal_sync_time,stats_reset)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.wal_records, '
    'dt.wal_fpi, '
    'dt.wal_bytes, '
    'dt.wal_buffers_full, '
    'dt.wal_write, '
    'dt.wal_sync, '
    'dt.wal_write_time, '
    'dt.wal_sync_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id              integer, '
        'sample_id              integer, '
        'wal_records         bigint, '
        'wal_fpi             bigint, '
        'wal_bytes           numeric, '
        'wal_buffers_full    bigint, '
        'wal_write           bigint, '
        'wal_sync            bigint, '
        'wal_write_time      double precision, '
        'wal_sync_time       double precision, '
        'stats_reset            timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_wal ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
);

/*
* Bugfix in collected data - in case of postgres upgrade pg_profile could save more
* than one system_identifier record for a server. This
* behavior makes it impossible to load exported dump.
*/
DELETE FROM sample_settings sett
USING (
  SELECT server_id, min(first_seen) AS first_seen, setting_scope, name
  FROM sample_settings
  WHERE setting_scope = 2 AND name = 'system_identifier'
  GROUP BY server_id, setting_scope, name
) first_sysid
WHERE
  (sett.server_id, sett.setting_scope, sett.name) =
  (first_sysid.server_id, first_sysid.setting_scope, first_sysid.name)
  AND sett.first_seen > first_sysid.first_seen;
-- Unique index on system_identifier to ensure there is no versions
-- as they are affecting export/import functionality
CREATE UNIQUE INDEX uk_sample_settings_sysid ON
  sample_settings (server_id,name) WHERE name='system_identifier';
