INSERT INTO import_queries_version_order VALUES
('pg_profile','4.3','pg_profile','4.2')
;
CREATE TABLE sample_stat_slru
(
    server_id     integer,
    sample_id     integer,
    name          text,
    blks_zeroed   bigint,
    blks_hit      bigint,
    blks_read     bigint,
    blks_written  bigint,
    blks_exists   bigint,
    flushes       bigint,
    truncates     bigint,
    stats_reset   timestamp with time zone,
    CONSTRAINT pk_sample_stat_slru PRIMARY KEY (server_id, sample_id, name),
    CONSTRAINT fk_sample_stat_slru_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_slru IS 'Sample SLRU statistics table (fields from pg_stat_slru)';

CREATE TABLE last_stat_slru (LIKE sample_stat_slru);
ALTER TABLE last_stat_slru ADD CONSTRAINT pk_last_stat_slru_samples
  PRIMARY KEY (server_id, sample_id, name);
ALTER TABLE last_stat_slru ADD CONSTRAINT fk_last_stat_slru_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_slru IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_io
(
    server_id                   integer,
    sample_id                   integer,
    backend_type                text,
    object                      text,
    context                     text,
    reads                       bigint,
    read_time                   double precision,
    writes                      bigint,
    write_time                  double precision,
    writebacks                  bigint,
    writeback_time              double precision,
    extends                     bigint,
    extend_time                 double precision,
    op_bytes                    bigint,
    hits                        bigint,
    evictions                   bigint,
    reuses                      bigint,
    fsyncs                      bigint,
    fsync_time                  double precision,
    stats_reset                 timestamp with time zone,
    CONSTRAINT pk_sample_stat_io PRIMARY KEY (server_id, sample_id, backend_type, object, context),
    CONSTRAINT fk_sample_stat_io_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_io IS 'Sample IO statistics table (fields from pg_stat_io)';

CREATE TABLE last_stat_io (LIKE sample_stat_io);
ALTER TABLE last_stat_io ADD CONSTRAINT pk_last_stat_io_samples
  PRIMARY KEY (server_id, sample_id, backend_type, object, context);
ALTER TABLE last_stat_io ADD CONSTRAINT fk_last_stat_io_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_io IS 'Last sample data for calculating diffs in next sample';

ALTER TABLE sample_stat_indexes
  ADD COLUMN last_idx_scan       timestamp with time zone;
ALTER TABLE last_stat_indexes
  ADD COLUMN last_idx_scan       timestamp with time zone;

ALTER TABLE sample_stat_tables
  ADD COLUMN last_seq_scan       timestamp with time zone,
  ADD COLUMN last_idx_scan       timestamp with time zone,
  ADD COLUMN n_tup_newpage_upd   bigint;

ALTER TABLE sample_stat_tables_total
  ADD COLUMN n_tup_newpage_upd   bigint;

ALTER TABLE last_stat_tables
  ADD COLUMN last_seq_scan       timestamp with time zone,
  ADD COLUMN last_idx_scan       timestamp with time zone,
  ADD COLUMN n_tup_newpage_upd   bigint;

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
        relpages_bytes_diff,
        last_idx_scan
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
        tablespacename,
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
        relpages_bytes_diff,
        last_seq_scan,
        last_idx_scan,
        n_tup_newpage_upd
    FROM sample_stat_tables
      JOIN tables_list USING (server_id, datid, relid)
      JOIN tablespaces_list tl USING (server_id, tablespaceid);
COMMENT ON VIEW v_sample_stat_tables IS 'Tables stats view with table names and schemas';

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;

UPDATE import_queries
SET query = 'INSERT INTO last_stat_indexes (server_id,sample_id,datid,relid,indexrelid,'
    'schemaname,relname,indexrelname,idx_scan,idx_tup_read,idx_tup_fetch,'
    'idx_blks_read,idx_blks_hit,relsize,relsize_diff,tablespaceid,indisunique,'
    'in_sample,relpages_bytes,relpages_bytes_diff,last_idx_scan)'
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
    'COALESCE(dt.in_sample, false), '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff, '
    'dt.last_idx_scan '
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
        'in_sample      boolean, '
        'relpages_bytes bigint, '
        'relpages_bytes_diff bigint, '
        'last_idx_scan  timestamp with time zone'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id '
      'AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'last_stat_indexes');

UPDATE import_queries
SET query = 'INSERT INTO last_stat_tables (server_id,sample_id,datid,relid,schemaname,relname,'
    'seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,'
    'n_tup_hot_upd,n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,'
    'last_vacuum,last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,'
    'autovacuum_count,analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,'
    'idx_blks_read,idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,'
    'tidx_blks_hit,relsize,relsize_diff,tablespaceid,reltoastrelid,relkind,in_sample,'
    'relpages_bytes,relpages_bytes_diff,last_seq_scan,last_idx_scan,n_tup_newpage_upd)'
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
    'COALESCE(dt.in_sample, false), '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff, '
    'dt.last_seq_scan, '
    'dt.last_idx_scan, '
    'dt.n_tup_newpage_upd '
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
        'in_sample            boolean, '
        'relpages_bytes       bigint, '
        'relpages_bytes_diff  bigint, '
        'last_seq_scan        timestamp with time zone,'
        'last_idx_scan        timestamp with time zone,'
        'n_tup_newpage_upd    bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'last_stat_tables');

UPDATE import_queries
SET query = 'INSERT INTO sample_stat_indexes (server_id,sample_id,datid,indexrelid,tablespaceid,'
    'idx_scan,idx_tup_read,idx_tup_fetch,idx_blks_read,idx_blks_hit,relsize,'
    'relsize_diff,indisunique,relpages_bytes,relpages_bytes_diff,last_idx_scan)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.indexrelid, '
    'dt.tablespaceid, '
    'dt.idx_scan, '
    'dt.idx_tup_read, '
    'dt.idx_tup_fetch, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.indisunique, '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff, '
    'dt.last_idx_scan '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'datid          oid, '
        'indexrelid     oid, '
        'tablespaceid   oid, '
        'idx_scan       bigint, '
        'idx_tup_read   bigint, '
        'idx_tup_fetch  bigint, '
        'idx_blks_read  bigint, '
        'idx_blks_hit   bigint, '
        'relsize        bigint, '
        'relsize_diff   bigint, '
        'indisunique    boolean, '
        'relpages_bytes bigint, '
        'relpages_bytes_diff bigint, '
        'last_idx_scan  timestamp with time zone'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'sample_stat_indexes');

UPDATE import_queries
SET query = 'INSERT INTO sample_stat_tables (server_id,sample_id,datid,relid,tablespaceid,seq_scan,'
    'seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,n_tup_hot_upd,'
    'n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,last_vacuum,'
    'last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,autovacuum_count,'
    'analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,idx_blks_read,'
    'idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,tidx_blks_hit,'
    'relsize,relsize_diff,relpages_bytes,relpages_bytes_diff,last_seq_scan,'
    'last_idx_scan,n_tup_newpage_upd)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.tablespaceid, '
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
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff, '
    'dt.last_seq_scan, '
    'dt.last_idx_scan, '
    'dt.n_tup_newpage_upd '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'datid                oid, '
        'relid                oid, '
        'tablespaceid         oid, '
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
        'relpages_bytes       bigint, '
        'relpages_bytes_diff  bigint, '
        'last_seq_scan        timestamp with time zone,'
        'last_idx_scan        timestamp with time zone,'
        'n_tup_newpage_upd    bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'sample_stat_tables');

UPDATE import_queries
SET query = 'INSERT INTO sample_stat_tables_total (server_id,sample_id,datid,tablespaceid,relkind,'
    'seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,'
    'n_tup_hot_upd,vacuum_count,autovacuum_count,analyze_count,autoanalyze_count,'
    'heap_blks_read,heap_blks_hit,idx_blks_read,idx_blks_hit,toast_blks_read,'
    'toast_blks_hit,tidx_blks_read,tidx_blks_hit,relsize_diff,n_tup_newpage_upd)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.tablespaceid, '
    'dt.relkind, '
    'dt.seq_scan, '
    'dt.seq_tup_read, '
    'dt.idx_scan, '
    'dt.idx_tup_fetch, '
    'dt.n_tup_ins, '
    'dt.n_tup_upd, '
    'dt.n_tup_del, '
    'dt.n_tup_hot_upd, '
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
    'dt.relsize_diff, '
    'dt.n_tup_newpage_upd '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id          integer, '
        'sample_id          integer, '
        'datid              oid, '
        'tablespaceid       oid, '
        'relkind            character(1), '
        'seq_scan           bigint, '
        'seq_tup_read       bigint, '
        'idx_scan           bigint, '
        'idx_tup_fetch      bigint, '
        'n_tup_ins          bigint, '
        'n_tup_upd          bigint, '
        'n_tup_del          bigint, '
        'n_tup_hot_upd      bigint, '
        'vacuum_count       bigint, '
        'autovacuum_count   bigint, '
        'analyze_count      bigint, '
        'autoanalyze_count  bigint, '
        'heap_blks_read     bigint, '
        'heap_blks_hit      bigint, '
        'idx_blks_read      bigint, '
        'idx_blks_hit       bigint, '
        'toast_blks_read    bigint, '
        'toast_blks_hit     bigint, '
        'tidx_blks_read     bigint, '
        'tidx_blks_hit      bigint, '
        'relsize_diff       bigint, '
        'n_tup_newpage_upd  bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_tables_total ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'sample_stat_tables_total');

INSERT INTO import_queries VALUES
('pg_profile','4.3', 1,'sample_stat_io',
  'INSERT INTO sample_stat_io (server_id,sample_id,backend_type,object,context,reads,'
    'read_time,writes,write_time,writebacks,writeback_time,extends,extend_time,'
    'op_bytes,hits,evictions,reuses,fsyncs,fsync_time,stats_reset'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.backend_type, '
    'dt.object, '
    'dt.context, '
    'dt.reads, '
    'dt.read_time, '
    'dt.writes, '
    'dt.write_time, '
    'dt.writebacks, '
    'dt.writeback_time, '
    'dt.extends, '
    'dt.extend_time, '
    'dt.op_bytes, '
    'dt.hits, '
    'dt.evictions, '
    'dt.reuses, '
    'dt.fsyncs, '
    'dt.fsync_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id                   integer, '
        'sample_id                   integer, '
        'backend_type                text, '
        'object                      text, '
        'context                     text, '
        'reads                       bigint, '
        'read_time                   double precision, '
        'writes                      bigint, '
        'write_time                  double precision, '
        'writebacks                  bigint, '
        'writeback_time              double precision, '
        'extends                     bigint, '
        'extend_time                 double precision, '
        'op_bytes                    bigint, '
        'hits                        bigint, '
        'evictions                   bigint, '
        'reuses                      bigint, '
        'fsyncs                      bigint, '
        'fsync_time                  double precision, '
        'stats_reset                 timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_io ld ON '
      '(ld.server_id, ld.sample_id, ld.backend_type, ld.object, ld.context) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.backend_type, dt.object, dt.context) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.3', 1,'last_stat_io',
  'INSERT INTO last_stat_io (server_id,sample_id,backend_type,object,context,reads,'
    'read_time,writes,write_time,writebacks,writeback_time,extends,extend_time,'
    'op_bytes,hits,evictions,reuses,fsyncs,fsync_time,stats_reset'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.backend_type, '
    'dt.object, '
    'dt.context, '
    'dt.reads, '
    'dt.read_time, '
    'dt.writes, '
    'dt.write_time, '
    'dt.writebacks, '
    'dt.writeback_time, '
    'dt.extends, '
    'dt.extend_time, '
    'dt.op_bytes, '
    'dt.hits, '
    'dt.evictions, '
    'dt.reuses, '
    'dt.fsyncs, '
    'dt.fsync_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id                   integer, '
        'sample_id                   integer, '
        'backend_type                text, '
        'object                      text, '
        'context                     text, '
        'reads                       bigint, '
        'read_time                   double precision, '
        'writes                      bigint, '
        'write_time                  double precision, '
        'writebacks                  bigint, '
        'writeback_time              double precision, '
        'extends                     bigint, '
        'extend_time                 double precision, '
        'op_bytes                    bigint, '
        'hits                        bigint, '
        'evictions                   bigint, '
        'reuses                      bigint, '
        'fsyncs                      bigint, '
        'fsync_time                  double precision, '
        'stats_reset                 timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_io ld ON '
      '(ld.server_id, ld.sample_id, ld.backend_type, ld.object, ld.context) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.backend_type, dt.object, dt.context) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.3', 1,'sample_stat_slru',
  'INSERT INTO sample_stat_slru (server_id,sample_id,name,blks_zeroed,'
    'blks_hit,blks_read,blks_written,blks_exists,flushes,truncates,'
    'stats_reset'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.name, '
    'dt.blks_zeroed, '
    'dt.blks_hit, '
    'dt.blks_read, '
    'dt.blks_written, '
    'dt.blks_exists, '
    'dt.flushes, '
    'dt.truncates, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'name           text, '
        'blks_zeroed    bigint, '
        'blks_hit       bigint, '
        'blks_read      bigint, '
        'blks_written   bigint, '
        'blks_exists    bigint, '
        'flushes        bigint, '
        'truncates      bigint, '
        'stats_reset    timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_slru ld ON '
      '(ld.server_id, ld.sample_id, ld.name) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.name) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.3', 1,'last_stat_slru',
  'INSERT INTO last_stat_slru (server_id,sample_id,name,blks_zeroed,'
    'blks_hit,blks_read,blks_written,blks_exists,flushes,truncates,'
    'stats_reset'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.name, '
    'dt.blks_zeroed, '
    'dt.blks_hit, '
    'dt.blks_read, '
    'dt.blks_written, '
    'dt.blks_exists, '
    'dt.flushes, '
    'dt.truncates, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'name           text, '
        'blks_zeroed    bigint, '
        'blks_hit       bigint, '
        'blks_read      bigint, '
        'blks_written   bigint, '
        'blks_exists    bigint, '
        'flushes        bigint, '
        'truncates      bigint, '
        'stats_reset    timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_slru ld ON '
      '(ld.server_id, ld.sample_id, ld.name) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.name) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
)
;
