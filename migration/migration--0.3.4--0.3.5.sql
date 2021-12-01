/* ===== import queries update ===== */
INSERT INTO import_queries_version_order VALUES
  ('pg_profile','0.3.5','pg_profile','0.3.4');

INSERT INTO roles_list (server_id, userid, username)
SELECT DISTINCT server_id, userid, 'unknown'
FROM sample_statements;

ALTER TABLE last_stat_tables
  ADD COLUMN in_sample    boolean NOT NULL DEFAULT false,
  ADD COLUMN relpages_bytes      bigint,
  ADD COLUMN relpages_bytes_diff bigint
;

ALTER TABLE last_stat_indexes
  ADD COLUMN in_sample    boolean NOT NULL DEFAULT false,
  ADD COLUMN relpages_bytes      bigint,
  ADD COLUMN relpages_bytes_diff bigint,
  ALTER COLUMN tablespaceid SET NOT NULL
;

ALTER TABLE last_stat_user_functions
  ADD COLUMN in_sample    boolean NOT NULL DEFAULT false
;

/* ===== Fix Comment issue ===== */
COMMENT ON TABLE sample_stat_cluster IS 'Sample cluster statistics table (fields from pg_stat_bgwriter, etc.)';
COMMENT ON TABLE sample_stat_wal IS 'Sample WAL statistics table';

/* ===== Constraints ===== */
ALTER TABLE sample_stat_indexes
  ALTER COLUMN tablespaceid SET NOT NULL,
  ADD COLUMN relpages_bytes      bigint,
  ADD COLUMN relpages_bytes_diff bigint
;

ALTER TABLE sample_stat_tables
  ALTER COLUMN tablespaceid SET NOT NULL,
  ADD COLUMN relpages_bytes      bigint,
  ADD COLUMN relpages_bytes_diff bigint
;

ALTER TABLE sample_statements
  ADD CONSTRAINT fk_statements_roles FOREIGN KEY (server_id, userid)
    REFERENCES roles_list (server_id, userid)
;

/* Drop tables */
DROP TABLE sample_stat_indexes_failures;
DROP TABLE sample_stat_tables_failures;

ALTER TABLE servers DROP COLUMN sizes_limited;

DROP VIEW v_sample_stat_indexes;
CREATE VIEW v_sample_stat_indexes AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
        tl.schemaname,
        tl.relname,
        il.indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        indisunique,
        relpages_bytes,
        relpages_bytes_diff
    FROM
        sample_stat_indexes s
        JOIN indexes_list il USING (datid, indexrelid, server_id)
        JOIN tables_list tl USING (datid, relid, server_id);
COMMENT ON VIEW v_sample_stat_indexes IS 'Reconstructed stats view with table and index names and schemas';

DROP VIEW v_sample_stat_tables;
CREATE VIEW v_sample_stat_tables AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze,
        n_ins_since_vacuum,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count,
        heap_blks_read,
        heap_blks_hit,
        idx_blks_read,
        idx_blks_hit,
        toast_blks_read,
        toast_blks_hit,
        tidx_blks_read,
        tidx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        reltoastrelid,
        relkind,
        relpages_bytes,
        relpages_bytes_diff
    FROM sample_stat_tables JOIN tables_list USING (server_id, datid, relid);
COMMENT ON VIEW v_sample_stat_tables IS 'Tables stats view with table names and schemas';

DROP VIEW v_sample_stat_user_functions;
CREATE VIEW v_sample_stat_user_functions AS
    SELECT
        server_id,
        sample_id,
        datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        calls,
        total_time,
        self_time,
        trg_fn
    FROM sample_stat_user_functions JOIN funcs_list USING (server_id, datid, funcid);
COMMENT ON VIEW v_sample_stat_user_functions IS 'Reconstructed stats view with function names and schemas';

/* ===== New fields in last_stat_tables ===== */
UPDATE import_queries SET
  query = 'INSERT INTO last_stat_tables (server_id,sample_id,datid,relid,schemaname,relname,'
    'seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,'
    'n_tup_hot_upd,n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,'
    'last_vacuum,last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,'
    'autovacuum_count,analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,'
    'idx_blks_read,idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,'
    'tidx_blks_hit,relsize,relsize_diff,tablespaceid,reltoastrelid,relkind,in_sample)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.schemaname, '
    'dt.relname, '
    'dt.seq_scan, '
    'dt.seq_tup_read, '
    'dt.idx_scan, '
    'dt.idx_tup_fetch, '
    'dt.n_tup_ins, '
    'dt.n_tup_upd, '
    'dt.n_tup_del, '
    'dt.n_tup_hot_upd, '
    'dt.n_live_tup, '
    'dt.n_dead_tup, '
    'dt.n_mod_since_analyze, '
    'dt.n_ins_since_vacuum, '
    'dt.last_vacuum, '
    'dt.last_autovacuum, '
    'dt.last_analyze, '
    'dt.last_autoanalyze, '
    'dt.vacuum_count, '
    'dt.autovacuum_count, '
    'dt.analyze_count, '
    'dt.autoanalyze_count, '
    'dt.heap_blks_read, '
    'dt.heap_blks_hit, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.toast_blks_read, '
    'dt.toast_blks_hit, '
    'dt.tidx_blks_read, '
    'dt.tidx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.tablespaceid, '
    'dt.reltoastrelid, '
    'dt.relkind, '
    'COALESCE(dt.in_sample, false) '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'datid                oid, '
        'relid                oid, '
        'schemaname           name, '
        'relname              name, '
        'seq_scan             bigint, '
        'seq_tup_read         bigint, '
        'idx_scan             bigint, '
        'idx_tup_fetch        bigint, '
        'n_tup_ins            bigint, '
        'n_tup_upd            bigint, '
        'n_tup_del            bigint, '
        'n_tup_hot_upd        bigint, '
        'n_live_tup           bigint, '
        'n_dead_tup           bigint, '
        'n_mod_since_analyze  bigint, '
        'n_ins_since_vacuum   bigint, '
        'last_vacuum          timestamp with time zone, '
        'last_autovacuum      timestamp with time zone, '
        'last_analyze         timestamp with time zone, '
        'last_autoanalyze     timestamp with time zone, '
        'vacuum_count         bigint, '
        'autovacuum_count     bigint, '
        'analyze_count        bigint, '
        'autoanalyze_count    bigint, '
        'heap_blks_read       bigint, '
        'heap_blks_hit        bigint, '
        'idx_blks_read        bigint, '
        'idx_blks_hit         bigint, '
        'toast_blks_read      bigint, '
        'toast_blks_hit       bigint, '
        'tidx_blks_read       bigint, '
        'tidx_blks_hit        bigint, '
        'relsize              bigint, '
        'relsize_diff         bigint, '
        'tablespaceid         oid, '
        'reltoastrelid        oid, '
        'relkind              character(1), '
        'in_sample            boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'last_stat_tables');

/* ===== New fields in last_stat_indexes ===== */
UPDATE import_queries SET
  query = 'INSERT INTO last_stat_indexes (server_id,sample_id,datid,relid,indexrelid,'
    'schemaname,relname,indexrelname,idx_scan,idx_tup_read,idx_tup_fetch,'
    'idx_blks_read,idx_blks_hit,relsize,relsize_diff,tablespaceid,indisunique,'
    'in_sample)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.indexrelid, '
    'dt.schemaname, '
    'dt.relname, '
    'dt.indexrelname, '
    'dt.idx_scan, '
    'dt.idx_tup_read, '
    'dt.idx_tup_fetch, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.tablespaceid, '
    'dt.indisunique, '
    'COALESCE(dt.in_sample, false) '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'datid          oid, '
        'relid          oid, '
        'indexrelid     oid, '
        'schemaname     name, '
        'relname        name, '
        'indexrelname   name, '
        'idx_scan       bigint, '
        'idx_tup_read   bigint, '
        'idx_tup_fetch  bigint, '
        'idx_blks_read  bigint, '
        'idx_blks_hit   bigint, '
        'relsize        bigint, '
        'relsize_diff   bigint, '
        'tablespaceid   oid, '
        'indisunique    boolean, '
        'in_sample      boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id '
      'AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'last_stat_indexes');

/* ===== New fields in last_stat_user_functions ===== */
UPDATE import_queries SET
  query = 'INSERT INTO last_stat_user_functions (server_id,sample_id,datid,funcid,schemaname,'
    'funcname,funcargs,calls,total_time,self_time,trg_fn,in_sample)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.funcid, '
    'dt.schemaname, '
    'dt.funcname, '
    'dt.funcargs, '
    'dt.calls, '
    'dt.total_time, '
    'dt.self_time, '
    'dt.trg_fn, '
    'COALESCE(dt.in_sample, false) '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id   integer, '
        'sample_id   integer, '
        'datid       oid, '
        'funcid      oid, '
        'schemaname  name, '
        'funcname    name, '
        'funcargs    text, '
        'calls       bigint, '
        'total_time  double precision, '
        'self_time   double precision, '
        'trg_fn      boolean, '
        'in_sample   boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_user_functions ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.funcid = dt.funcid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'last_stat_user_functions');

/* ===== Bugfix in last_stat_wal ===== */
UPDATE import_queries SET
  query = 'INSERT INTO last_stat_wal (server_id,sample_id,wal_records,'
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
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.4', 1,'last_stat_wal');

 /*
  * Support import from pg_profile 0.3.5
  */
-- roles
INSERT INTO import_queries VALUES
('pg_profile','0.3.5', 1,'roles_list',
  'INSERT INTO roles_list (server_id,userid,username)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.userid, '
    'dt.username '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id  integer, '
        'userid     oid, '
        'username   name '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN roles_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.userid = dt.userid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
);

UPDATE import_queries SET query = 'SELECT ''%1$s'' as imp WHERE -1 = $1'
WHERE relname IN ('sample_stat_indexes_failures', 'sample_stat_tables_failures');

UPDATE import_queries SET exec_order = 2 WHERE relname = 'sample_statements';
INSERT INTO import_queries VALUES
('pg_profile','0.3.2', 1,'sample_statements',
  'INSERT INTO roles_list (server_id,userid,username'
    ')'
  'SELECT DISTINCT '
    'srv_map.local_srv_id, '
    'dt.userid, '
    '''_unknown_'' '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'userid               oid '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN roles_list ld ON '
      '(ld.server_id = srv_map.local_srv_id '
      'AND ld.userid = dt.userid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','0.3.1', 1,'sample_statements',
  'INSERT INTO roles_list (server_id,userid,username'
    ')'
  'SELECT DISTINCT '
    'srv_map.local_srv_id, '
    'dt.userid, '
    '''_unknown_'' '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'userid               oid '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN roles_list ld ON '
      '(ld.server_id = srv_map.local_srv_id '
      'AND ld.userid = dt.userid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
)
;
