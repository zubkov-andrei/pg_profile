ALTER TABLE indexes_list
  ADD COLUMN last_sample_id  integer,
  ADD CONSTRAINT fk_indexes_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE;
CREATE INDEX ix_indexes_list_smp ON indexes_list(server_id, last_sample_id);
CREATE INDEX ix_indexes_list_rel ON indexes_list(server_id, datid, relid);

ALTER TABLE last_stat_indexes
  DROP CONSTRAINT fk_last_stat_indexes_dat,
  ADD CONSTRAINT fk_last_stat_indexes_dat FOREIGN KEY (server_id, sample_id, datid)
  REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT;

ALTER TABLE tables_list
  ADD COLUMN last_sample_id      integer,
  ADD CONSTRAINT fk_tables_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE;
CREATE INDEX ix_tables_list_samples ON tables_list(server_id, last_sample_id);

ALTER TABLE last_stat_tables
  DROP CONSTRAINT fk_last_stat_tablespaces,
  DROP CONSTRAINT fk_last_stat_tables_dat,
  ADD CONSTRAINT fk_last_stat_tables_dat
  FOREIGN KEY (server_id, sample_id, datid)
  REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT;

ALTER TABLE funcs_list
  ADD COLUMN last_sample_id  integer,
  ADD CONSTRAINT fk_funcs_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE;
CREATE INDEX ix_funcs_list_samples ON funcs_list (server_id, last_sample_id);

ALTER TABLE last_stat_user_functions
  ADD CONSTRAINT pk_last_stat_user_functions PRIMARY KEY (server_id, sample_id, datid, funcid),
  DROP CONSTRAINT fk_last_stat_user_functions_dat,
  ADD CONSTRAINT fk_last_stat_user_functions_dat
  FOREIGN KEY (server_id, sample_id, datid)
  REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT;

ALTER TABLE roles_list
  ADD COLUMN last_sample_id  integer,
  ADD CONSTRAINT fk_roles_list_smp FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE CASCADE;
CREATE INDEX ix_roles_list_smp ON roles_list(server_id, last_sample_id);

ALTER TABLE sample_kcache
  DROP CONSTRAINT fk_kcache_stmt_list,
  ADD CONSTRAINT fk_kcache_stmt_list FOREIGN KEY (server_id,queryid_md5)
      REFERENCES stmt_list (server_id,queryid_md5) ON DELETE CASCADE ON UPDATE CASCADE;
CREATE INDEX ix_sample_kcache_sl ON sample_kcache(server_id,queryid_md5);

ALTER TABLE sample_settings
  DROP CONSTRAINT pk_sample_settings,
  ADD CONSTRAINT pk_sample_settings PRIMARY KEY (server_id, setting_scope, name, first_seen);

CREATE INDEX ix_sample_stat_indexes_il ON sample_stat_indexes(server_id, datid, indexrelid);
CREATE INDEX ix_sample_stat_indexes_ts ON sample_stat_indexes(server_id, sample_id, tablespaceid);

CREATE INDEX ix_sample_stat_indexes_total_ts ON sample_stat_indexes_total(server_id, sample_id, tablespaceid);

CREATE INDEX is_sample_stat_tables_ts ON sample_stat_tables(server_id, sample_id, tablespaceid);
CREATE INDEX ix_sample_stat_tables_rel ON sample_stat_tables(server_id, datid, relid);

CREATE INDEX ix_sample_stat_tables_total_ts ON sample_stat_tables_total(server_id, sample_id, tablespaceid);

ALTER TABLE sample_stat_tablespaces
  DROP CONSTRAINT fk_st_tablespaces_tablespaces,
  ADD CONSTRAINT fk_st_tablespaces_tablespaces FOREIGN KEY (server_id, tablespaceid)
        REFERENCES tablespaces_list(server_id, tablespaceid) ON DELETE CASCADE ON UPDATE CASCADE;

CREATE INDEX ix_sample_stat_tablespaces_ts ON sample_stat_tablespaces(server_id, tablespaceid);

CREATE INDEX ix_sample_stat_user_functions_fl ON sample_stat_user_functions(server_id, datid, funcid);

CREATE INDEX ix_sample_stmts_rol ON sample_statements (server_id, userid);
DROP INDEX ix_sample_stmts_qid;
CREATE INDEX ix_sample_stmts_qid ON sample_statements (server_id,queryid_md5);

ALTER TABLE stmt_list
  ADD COLUMN last_sample_id integer,
  ADD CONSTRAINT fk_stmt_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE;
CREATE INDEX ix_stmt_list_smp ON stmt_list(server_id, last_sample_id);

ALTER TABLE tablespaces_list
  ADD COLUMN last_sample_id      integer,
  ADD CONSTRAINT fk_tablespaces_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE;
CREATE INDEX ix_tablespaces_list_smp ON tablespaces_list(server_id, last_sample_id);

INSERT INTO import_queries_version_order VALUES
  ('pg_profile','3.9','pg_profile','3.8');

UPDATE import_queries SET query =
  'INSERT INTO funcs_list (server_id,last_sample_id,datid,funcid,schemaname,'
    'funcname,funcargs)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.datid, '
    'dt.funcid, '
    'dt.schemaname, '
    'dt.funcname, '
    'dt.funcargs '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'datid          oid, '
        'funcid         oid, '
        'schemaname     name, '
        'funcname       name, '
        'funcargs       text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN funcs_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.datid = dt.datid AND ld.funcid = dt.funcid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'funcs_list')
;

UPDATE import_queries SET query =
  'INSERT INTO indexes_list (server_id,last_sample_id,datid,indexrelid,relid,'
    'schemaname,indexrelname)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.datid, '
    'dt.indexrelid, '
    'dt.relid, '
    'dt.schemaname, '
    'dt.indexrelname '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'datid          oid, '
        'indexrelid     oid, '
        'relid          oid, '
        'schemaname     name, '
        'indexrelname   name '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN indexes_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'indexes_list')
;

UPDATE import_queries SET query =
  'INSERT INTO tables_list (server_id,last_sample_id,datid,relid,relkind,'
    'reltoastrelid,schemaname,relname)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.relkind, '
    'dt.reltoastrelid, '
    'dt.schemaname, '
    'dt.relname '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'datid          oid, '
        'relid          oid, '
        'relkind        character(1), '
        'reltoastrelid  oid, '
        'schemaname     name, '
        'relname        name '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN tables_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'tables_list')
;

UPDATE import_queries SET query =
  'INSERT INTO sample_stat_indexes (server_id,sample_id,datid,indexrelid,tablespaceid,'
    'idx_scan,idx_tup_read,idx_tup_fetch,idx_blks_read,idx_blks_hit,relsize,'
    'relsize_diff,indisunique,relpages_bytes,relpages_bytes_diff)'
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
    'dt.relpages_bytes_diff '
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
        'relpages_bytes_diff bigint'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'sample_stat_indexes')
;

UPDATE import_queries SET query =
  'INSERT INTO last_stat_tables (server_id,sample_id,datid,relid,schemaname,relname,'
    'seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,'
    'n_tup_hot_upd,n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,'
    'last_vacuum,last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,'
    'autovacuum_count,analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,'
    'idx_blks_read,idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,'
    'tidx_blks_hit,relsize,relsize_diff,tablespaceid,reltoastrelid,relkind,in_sample,'
    'relpages_bytes, relpages_bytes_diff)'
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
    'dt.relpages_bytes_diff '
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
        'relpages_bytes_diff  bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'last_stat_tables')
;

UPDATE import_queries SET query =
  'INSERT INTO last_stat_indexes (server_id,sample_id,datid,relid,indexrelid,'
    'schemaname,relname,indexrelname,idx_scan,idx_tup_read,idx_tup_fetch,'
    'idx_blks_read,idx_blks_hit,relsize,relsize_diff,tablespaceid,indisunique,'
    'in_sample,relpages_bytes,relpages_bytes_diff)'
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
    'dt.relpages_bytes_diff '
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
        'relpages_bytes_diff bigint'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id '
      'AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'last_stat_indexes')
;

UPDATE import_queries SET query =
  'INSERT INTO sample_stat_tables (server_id,sample_id,datid,relid,tablespaceid,seq_scan,'
    'seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,n_tup_hot_upd,'
    'n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,last_vacuum,'
    'last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,autovacuum_count,'
    'analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,idx_blks_read,'
    'idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,tidx_blks_hit,'
    'relsize,relsize_diff,relpages_bytes,relpages_bytes_diff)'
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
    'dt.relpages_bytes_diff '
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
        'relpages_bytes_diff  bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE (extension, from_version, exec_order, relname) =
  ('pg_profile','0.3.1', 1,'sample_stat_tables')
;

INSERT INTO import_queries VALUES
('pg_profile', '0.3.1', 2, 'sample_stat_tablespaces',
  'UPDATE tablespaces_list tl SET last_sample_id = tsl.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, tablespaceid '
    'FROM sample_stat_tablespaces '
    'GROUP BY server_id, tablespaceid'
    ') tsl '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (tl.server_id, tl.tablespaceid) = (tsl.server_id, tsl.tablespaceid) '
    'AND tl.last_sample_id IS NULL '
    'AND tsl.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.1', 2, 'sample_stat_tables',
  'UPDATE tables_list tl SET last_sample_id = isl.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, datid, relid '
    'FROM sample_stat_tables '
    'GROUP BY server_id, datid, relid'
    ') isl '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (tl.server_id, tl.datid, tl.relid) = (isl.server_id, isl.datid, isl.relid) '
    'AND tl.last_sample_id IS NULL '
    'AND isl.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.1', 2, 'sample_stat_indexes',
  'UPDATE indexes_list il SET last_sample_id = isl.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, datid, indexrelid '
    'FROM sample_stat_indexes '
    'GROUP BY server_id, datid, indexrelid'
    ') isl '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (il.server_id, il.datid, il.indexrelid) = (isl.server_id, isl.datid, isl.indexrelid) '
    'AND il.last_sample_id IS NULL '
    'AND isl.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.1', 2, 'sample_stat_user_functions',
  'UPDATE funcs_list fl SET last_sample_id = isl.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, datid, funcid '
    'FROM sample_stat_user_functions '
    'GROUP BY server_id, datid, funcid'
    ') isl '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (fl.server_id, fl.datid, fl.funcid) = (isl.server_id, isl.datid, isl.funcid) '
    'AND fl.last_sample_id IS NULL '
    'AND isl.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.2', 3, 'sample_statements',
  'UPDATE stmt_list sl SET last_sample_id = qid_smp.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, queryid_md5 '
    'FROM sample_statements '
    'GROUP BY server_id, queryid_md5'
    ') qid_smp '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (sl.server_id, sl.queryid_md5) = (qid_smp.server_id, qid_smp.queryid_md5) '
    'AND sl.last_sample_id IS NULL '
    'AND qid_smp.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.1', 3, 'sample_statements',
  'UPDATE stmt_list sl SET last_sample_id = qid_smp.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, queryid_md5 '
    'FROM sample_statements '
    'GROUP BY server_id, queryid_md5'
    ') qid_smp '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (sl.server_id, sl.queryid_md5) = (qid_smp.server_id, qid_smp.queryid_md5) '
    'AND sl.last_sample_id IS NULL '
    'AND qid_smp.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.1', 4, 'sample_statements',
  'UPDATE roles_list rl SET last_sample_id = r_smp.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, userid '
    'FROM sample_statements '
    'GROUP BY server_id, userid'
    ') r_smp '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (rl.server_id, rl.userid) = (r_smp.server_id, r_smp.userid) '
    'AND rl.last_sample_id IS NULL '
    'AND r_smp.last_sample_id != ms.max_server_id'
),
('pg_profile', '0.3.2', 4, 'sample_statements',
  'UPDATE roles_list rl SET last_sample_id = r_smp.last_sample_id '
  'FROM ('
    'SELECT server_id, max(sample_id) AS last_sample_id, userid '
    'FROM sample_statements '
    'GROUP BY server_id, userid'
    ') r_smp '
    'JOIN (SELECT server_id, max(sample_id) AS max_server_id FROM samples GROUP BY server_id) ms '
    'USING (server_id) '
  'WHERE (rl.server_id, rl.userid) = (r_smp.server_id, r_smp.userid) '
    'AND rl.last_sample_id IS NULL '
    'AND r_smp.last_sample_id != ms.max_server_id'
)
;

INSERT INTO import_queries VALUES
('pg_profile','3.9', 1,'stmt_list',
  'INSERT INTO stmt_list (server_id,last_sample_id,queryid_md5,query)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.queryid_md5, '
    'dt.query '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'queryid_md5    character(32), '
        'query          text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN stmt_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.queryid_md5 = dt.queryid_md5) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','3.9', 1,'tablespaces_list',
  'INSERT INTO tablespaces_list (server_id,last_sample_id,tablespaceid,tablespacename,tablespacepath)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.tablespaceid, '
    'dt.tablespacename, '
    'dt.tablespacepath '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'tablespaceid   oid, '
        'tablespacename name, '
        'tablespacepath text '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN tablespaces_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','3.9', 1,'roles_list',
  'INSERT INTO roles_list (server_id,last_sample_id,userid,username)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.last_sample_id, '
    'dt.userid, '
    'dt.username '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'last_sample_id integer, '
        'userid         oid, '
        'username       name '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN roles_list ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.userid = dt.userid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
)
;
