ALTER TABLE sample_statements
  DROP CONSTRAINT fk_statements_roles,
  ADD CONSTRAINT fk_statements_roles FOREIGN KEY (server_id, userid)
    REFERENCES roles_list (server_id, userid)
    ON DELETE NO ACTION ON UPDATE CASCADE
    DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE indexes_list
  DROP CONSTRAINT fk_indexes_tables,
  ADD CONSTRAINT fk_indexes_tables FOREIGN KEY (server_id, datid, relid)
    REFERENCES tables_list(server_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE sample_kcache
  DROP CONSTRAINT fk_kcache_stmt_list,
  ADD CONSTRAINT fk_kcache_stmt_list FOREIGN KEY (server_id,queryid_md5)
    REFERENCES stmt_list (server_id,queryid_md5)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE;

INSERT INTO import_queries_version_order VALUES
('pg_profile','4.1','pg_profile','4.0')
;

-- last_* tables partitioning. Rename first
ALTER TABLE last_stat_database RENAME TO old_last_stat_database;
ALTER TABLE last_stat_tablespaces RENAME TO old_last_stat_tablespaces;
ALTER TABLE last_stat_tables RENAME TO old_last_stat_tables;
ALTER TABLE last_stat_indexes RENAME TO old_last_stat_indexes;
ALTER TABLE last_stat_user_functions RENAME TO old_last_stat_user_functions;
ALTER TABLE last_stat_statements RENAME TO old_last_stat_statements;
ALTER TABLE last_stat_kcache RENAME TO old_last_stat_kcache;

-- Create partitioned tables
CREATE TABLE last_stat_database (LIKE sample_stat_database)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_database IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_tablespaces (LIKE v_sample_stat_tablespaces)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_tablespaces IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_tables(
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    schemaname          name,
    relname             name,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_live_tup          bigint,
    n_dead_tup          bigint,
    n_mod_since_analyze bigint,
    n_ins_since_vacuum  bigint,
    last_vacuum         timestamp with time zone,
    last_autovacuum     timestamp with time zone,
    last_analyze        timestamp with time zone,
    last_autoanalyze    timestamp with time zone,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    heap_blks_read      bigint,
    heap_blks_hit       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    toast_blks_read     bigint,
    toast_blks_hit      bigint,
    tidx_blks_read      bigint,
    tidx_blks_hit       bigint,
    relsize             bigint,
    relsize_diff        bigint,
    tablespaceid        oid,
    reltoastrelid       oid,
    relkind             char(1),
    in_sample           boolean NOT NULL DEFAULT false,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint
)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_tables IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_indexes (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid NOT NULL,
    indexrelid          oid,
    schemaname          name,
    relname             name,
    indexrelname        name,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize             bigint,
    relsize_diff        bigint,
    tablespaceid        oid NOT NULL,
    indisunique         bool,
    in_sample           boolean NOT NULL DEFAULT false,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint
)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_indexes IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_user_functions (LIKE v_sample_stat_user_functions, in_sample boolean NOT NULL DEFAULT false)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_user_functions IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE last_stat_statements (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    username            name,
    datid               oid,
    queryid             bigint,
    queryid_md5         char(32),
    plans               bigint,
    total_plan_time     double precision,
    min_plan_time       double precision,
    max_plan_time       double precision,
    mean_plan_time      double precision,
    stddev_plan_time    double precision,
    calls               bigint,
    total_exec_time     double precision,
    min_exec_time       double precision,
    max_exec_time       double precision,
    mean_exec_time      double precision,
    stddev_exec_time    double precision,
    rows                bigint,
    shared_blks_hit     bigint,
    shared_blks_read    bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit      bigint,
    local_blks_read     bigint,
    local_blks_dirtied  bigint,
    local_blks_written  bigint,
    temp_blks_read      bigint,
    temp_blks_written   bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    toplevel            boolean,
    in_sample           boolean DEFAULT false,
    jit_functions       bigint,
    jit_generation_time double precision,
    jit_inlining_count  bigint,
    jit_inlining_time   double precision,
    jit_optimization_count  bigint,
    jit_optimization_time   double precision,
    jit_emission_count  bigint,
    jit_emission_time   double precision
)
PARTITION BY LIST (server_id);

CREATE TABLE last_stat_kcache (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    datid               oid,
    toplevel            boolean DEFAULT true,
    queryid             bigint,
    plan_user_time      double precision, --  User CPU time used
    plan_system_time    double precision, --  System CPU time used
    plan_minflts         bigint, -- Number of page reclaims (soft page faults)
    plan_majflts         bigint, -- Number of page faults (hard page faults)
    plan_nswaps         bigint, -- Number of swaps
    plan_reads          bigint, -- Number of bytes read by the filesystem layer
    plan_writes         bigint, -- Number of bytes written by the filesystem layer
    plan_msgsnds        bigint, -- Number of IPC messages sent
    plan_msgrcvs        bigint, -- Number of IPC messages received
    plan_nsignals       bigint, -- Number of signals received
    plan_nvcsws         bigint, -- Number of voluntary context switches
    plan_nivcsws        bigint,
    exec_user_time      double precision, --  User CPU time used
    exec_system_time    double precision, --  System CPU time used
    exec_minflts         bigint, -- Number of page reclaims (soft page faults)
    exec_majflts         bigint, -- Number of page faults (hard page faults)
    exec_nswaps         bigint, -- Number of swaps
    exec_reads          bigint, -- Number of bytes read by the filesystem layer
    exec_writes         bigint, -- Number of bytes written by the filesystem layer
    exec_msgsnds        bigint, -- Number of IPC messages sent
    exec_msgrcvs        bigint, -- Number of IPC messages received
    exec_nsignals       bigint, -- Number of signals received
    exec_nvcsws         bigint, -- Number of voluntary context switches
    exec_nivcsws        bigint
)
PARTITION BY LIST (server_id);

-- Create sections for servers
SELECT create_server_partitions(server_id)
FROM servers;

-- Relload the contents of last_* tables
INSERT INTO last_stat_database
SELECT lst.*
FROM
  old_last_stat_database lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_tablespaces
SELECT lst.*
FROM
  old_last_stat_tablespaces lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_tables
SELECT lst.*
FROM
  old_last_stat_tables lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_indexes
SELECT lst.*
FROM
  old_last_stat_indexes lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_user_functions
SELECT lst.*
FROM
  old_last_stat_user_functions lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_statements
SELECT lst.*, 0, 0, 0, 0, 0, 0, 0, 0
FROM
  old_last_stat_statements lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

INSERT INTO last_stat_kcache
SELECT lst.*
FROM
  old_last_stat_kcache lst
  JOIN servers srv ON (lst.server_id, lst.sample_id) = (srv.server_id, srv.last_sample_id);

-- Remove old tables
DROP TABLE old_last_stat_database CASCADE;
DROP TABLE old_last_stat_tablespaces CASCADE;
DROP TABLE old_last_stat_tables CASCADE;
DROP TABLE old_last_stat_indexes CASCADE;
DROP TABLE old_last_stat_user_functions CASCADE;
DROP TABLE old_last_stat_statements CASCADE;
DROP TABLE old_last_stat_kcache CASCADE;

ALTER TABLE sample_statements
  ADD COLUMN jit_functions       bigint,
  ADD COLUMN jit_generation_time double precision,
  ADD COLUMN jit_inlining_count  bigint,
  ADD COLUMN jit_inlining_time   double precision,
  ADD COLUMN jit_optimization_count  bigint,
  ADD COLUMN jit_optimization_time   double precision,
  ADD COLUMN jit_emission_count  bigint,
  ADD COLUMN jit_emission_time   double precision
;

ALTER TABLE sample_statements_total
  ADD COLUMN jit_functions       bigint,
  ADD COLUMN jit_generation_time double precision,
  ADD COLUMN jit_inlining_count  bigint,
  ADD COLUMN jit_inlining_time   double precision,
  ADD COLUMN jit_optimization_count  bigint,
  ADD COLUMN jit_optimization_time   double precision,
  ADD COLUMN jit_emission_count  bigint,
  ADD COLUMN jit_emission_time   double precision
;

-- Import queries update
UPDATE import_queries SET
  query = 'INSERT INTO sample_statements_total (server_id,sample_id,datid,plans,total_plan_time,'
    'calls,total_exec_time,rows,shared_blks_hit,shared_blks_read,'
    'shared_blks_dirtied,shared_blks_written,local_blks_hit,local_blks_read,'
    'local_blks_dirtied,local_blks_written,temp_blks_read,temp_blks_written,'
    'blk_read_time,blk_write_time,wal_records,wal_fpi,wal_bytes,statements'
    ',jit_functions,jit_generation_time,jit_inlining_count,jit_inlining_time,'
    'jit_optimization_count,jit_optimization_time,jit_emission_count,jit_emission_time'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.plans, '
    'dt.total_plan_time, '
    'dt.calls, '
    'dt.total_exec_time, '
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
    'dt.statements, '
    'dt.jit_functions, '
    'dt.jit_generation_time, '
    'dt.jit_inlining_count, '
    'dt.jit_inlining_time, '
    'dt.jit_optimization_count, '
    'dt.jit_optimization_time, '
    'dt.jit_emission_count, '
    'dt.jit_emission_time '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'datid                oid, '
        'plans                bigint, '
        'total_plan_time      double precision, '
        'calls                bigint, '
        'total_exec_time      double precision, '
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
        'statements           bigint, '
        'jit_functions        bigint, '
        'jit_generation_time  double precision, '
        'jit_inlining_count   bigint, '
        'jit_inlining_time    double precision, '
        'jit_optimization_count  bigint, '
        'jit_optimization_time   double precision, '
        'jit_emission_count   bigint, '
        'jit_emission_time    double precision'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_statements_total ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('0.3.1', 1, 'sample_statements_total');

UPDATE import_queries SET
  query = 'INSERT INTO last_stat_statements (server_id,sample_id,userid,username,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,'
    'stddev_plan_time,calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,'
    'stddev_exec_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,'
    'shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,'
    'local_blks_written,temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,'
    'wal_records,wal_fpi,wal_bytes,toplevel,in_sample'
    ',jit_functions,jit_generation_time,jit_inlining_count,jit_inlining_time,'
    'jit_optimization_count,jit_optimization_time,jit_emission_count,jit_emission_time'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.username, '
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
    'dt.toplevel, '
    'dt.in_sample, '
    'dt.jit_functions, '
    'dt.jit_generation_time, '
    'dt.jit_inlining_count, '
    'dt.jit_inlining_time, '
    'dt.jit_optimization_count, '
    'dt.jit_optimization_time, '
    'dt.jit_emission_count, '
    'dt.jit_emission_time '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'userid               oid, '
        'username             name, '
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
        'toplevel             boolean, '
        'in_sample            boolean, '
        'jit_functions        bigint, '
        'jit_generation_time  double precision, '
        'jit_inlining_count   bigint, '
        'jit_inlining_time    double precision, '
        'jit_optimization_count  bigint, '
        'jit_optimization_time   double precision, '
        'jit_emission_count   bigint, '
        'jit_emission_time    double precision'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_statements ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, dt.toplevel) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('4.0', 1, 'last_stat_statements');

UPDATE import_queries SET
  query = 'INSERT INTO sample_statements (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,'
    'stddev_plan_time,calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,'
    'stddev_exec_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,'
    'shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,'
    'local_blks_written,temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,'
    'wal_records,wal_fpi,wal_bytes,toplevel'
    ',jit_functions,jit_generation_time,jit_inlining_count,jit_inlining_time,'
    'jit_optimization_count,jit_optimization_time,jit_emission_count,jit_emission_time'
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
    'dt.toplevel, '
    'dt.jit_functions, '
    'dt.jit_generation_time, '
    'dt.jit_inlining_count, '
    'dt.jit_inlining_time, '
    'dt.jit_optimization_count, '
    'dt.jit_optimization_time, '
    'dt.jit_emission_count, '
    'dt.jit_emission_time '
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
        'toplevel             boolean, '
        'jit_functions        bigint, '
        'jit_generation_time  double precision, '
        'jit_inlining_count   bigint, '
        'jit_inlining_time    double precision, '
        'jit_optimization_count  bigint, '
        'jit_optimization_time   double precision, '
        'jit_emission_count   bigint, '
        'jit_emission_time    double precision'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_statements ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, dt.toplevel) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('4.0', 1, 'sample_statements');

INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', 'dbagg_jit_stats_htbl', 'dbagg_jit_stat', NULL),
(2, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', 'dbagg_jit_stats_diff_htbl', 'dbagg_jit_stat', NULL),
(1, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', 'top_jit_htbl', 'top_jit', NULL),
(2, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', 'top_jit_diff_htbl', 'top_jit', NULL)
;
