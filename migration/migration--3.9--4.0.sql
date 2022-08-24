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
    CONSTRAINT pk_last_stat_satements PRIMARY KEY (server_id, sample_id, userid, datid, queryid, toplevel)
);

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
    exec_nivcsws        bigint,
    CONSTRAINT pk_last_stat_kcache PRIMARY KEY (server_id, sample_id, datid, userid, queryid, toplevel),
    CONSTRAINT fk_last_kcache_stmts FOREIGN KEY
      (server_id, sample_id, datid, userid, queryid, toplevel) REFERENCES
      last_stat_statements(server_id, sample_id, datid, userid, queryid, toplevel)
      ON DELETE CASCADE
);

ALTER TABLE sample_kcache
  DROP CONSTRAINT fk_kcache_st,
  DROP CONSTRAINT pk_sample_kcache_n;

ALTER TABLE sample_statements
  DROP CONSTRAINT pk_sample_statements_n;

UPDATE sample_statements SET toplevel = true
WHERE toplevel IS NULL;

ALTER TABLE sample_statements
  ADD CONSTRAINT pk_sample_statements_n PRIMARY KEY (server_id, sample_id, datid, userid, queryid, toplevel);

ALTER TABLE sample_kcache
  ADD COLUMN toplevel   boolean;

UPDATE sample_kcache SET toplevel = true
WHERE toplevel IS NULL;

ALTER TABLE sample_kcache
  ADD CONSTRAINT pk_sample_kcache_n PRIMARY KEY (server_id, sample_id, datid, userid, queryid, toplevel),
  ADD CONSTRAINT fk_kcache_st FOREIGN KEY (server_id, sample_id, datid, userid, queryid, toplevel)
      REFERENCES sample_statements(server_id, sample_id, datid, userid, queryid, toplevel) ON DELETE CASCADE;

ALTER TABLE sample_stat_indexes
  DROP CONSTRAINT fk_stat_indexes_indexes,
  ADD CONSTRAINT fk_stat_indexes_indexes FOREIGN KEY (server_id, datid, indexrelid)
      REFERENCES indexes_list(server_id, datid, indexrelid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE
;

ALTER TABLE sample_stat_tables
  DROP CONSTRAINT fk_st_tables_tables,
  ADD CONSTRAINT fk_st_tables_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE
;

ALTER TABLE tables_list
  DROP CONSTRAINT fk_toast_table,
  ADD CONSTRAINT fk_toast_table FOREIGN KEY (server_id, datid, reltoastrelid)
      REFERENCES tables_list (server_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE
;

ALTER TABLE sample_stat_tablespaces
  DROP CONSTRAINT fk_st_tablespaces_tablespaces,
  ADD CONSTRAINT fk_st_tablespaces_tablespaces FOREIGN KEY (server_id, tablespaceid)
    REFERENCES tablespaces_list(server_id, tablespaceid)
    ON DELETE NO ACTION ON UPDATE CASCADE
    DEFERRABLE INITIALLY IMMEDIATE
;

ALTER TABLE sample_stat_user_functions
  DROP CONSTRAINT fk_user_functions_functions,
  ADD CONSTRAINT fk_user_functions_functions FOREIGN KEY (server_id, datid, funcid)
      REFERENCES funcs_list (server_id, datid, funcid)
      ON DELETE NO ACTION
      DEFERRABLE INITIALLY IMMEDIATE
;

ALTER TABLE sample_statements
  DROP CONSTRAINT fk_stmt_list,
  ADD CONSTRAINT fk_stmt_list FOREIGN KEY (server_id,queryid_md5)
      REFERENCES stmt_list (server_id,queryid_md5)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
;

INSERT INTO import_queries_version_order VALUES
('pg_profile','4.0','pg_profile','3.9')
;

UPDATE import_queries SET
  query = 'INSERT INTO funcs_list (server_id,last_sample_id,datid,funcid,schemaname,'
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
      '(ld.server_id, ld.datid, ld.funcid, ld.last_sample_id, ld.schemaname, ld.funcname, ld.funcargs) IS NOT DISTINCT FROM '
      '(srv_map.local_srv_id, dt.datid, dt.funcid, dt.last_sample_id, dt.schemaname, dt.funcname, dt.funcargs) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
  'ON CONFLICT ON CONSTRAINT pk_funcs_list DO '
  'UPDATE SET (last_sample_id, schemaname, funcname, funcargs) = '
    '(EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.funcname, EXCLUDED.funcargs) '
WHERE
  (from_version, exec_order, relname) = ('0.3.1', 1, 'funcs_list');

UPDATE import_queries SET
  query = 'INSERT INTO indexes_list (server_id,last_sample_id,datid,indexrelid,relid,'
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
      '(ld.server_id, ld.datid, ld.indexrelid, ld.last_sample_id, ld.schemaname, ld.indexrelname) IS NOT DISTINCT FROM '
      '(srv_map.local_srv_id, dt.datid, dt.indexrelid, dt.last_sample_id, dt.schemaname, dt.indexrelname)'
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
  'ON CONFLICT ON CONSTRAINT pk_indexes_list DO '
  'UPDATE SET (last_sample_id, schemaname, indexrelname) = '
    ' (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.indexrelname)'
WHERE
  (from_version, exec_order, relname) = ('0.3.1', 1, 'indexes_list');

UPDATE import_queries SET
  query = 'INSERT INTO sample_kcache (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plan_user_time,plan_system_time,plan_minflts,plan_majflts,'
    'plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,'
    'plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,'
    'exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,'
    'exec_nsignals,exec_nvcsws,exec_nivcsws,toplevel'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.queryid, '
    'q_map.queryid_md5_new, '
    'dt.plan_user_time, '
    'dt.plan_system_time, '
    'dt.plan_minflts, '
    'dt.plan_majflts, '
    'dt.plan_nswaps, '
    'dt.plan_reads, '
    'dt.plan_writes, '
    'dt.plan_msgsnds, '
    'dt.plan_msgrcvs, '
    'dt.plan_nsignals, '
    'dt.plan_nvcsws, '
    'dt.plan_nivcsws, '
    'dt.exec_user_time, '
    'dt.exec_system_time, '
    'dt.exec_minflts, '
    'dt.exec_majflts, '
    'dt.exec_nswaps, '
    'dt.exec_reads, '
    'dt.exec_writes, '
    'dt.exec_msgsnds, '
    'dt.exec_msgrcvs, '
    'dt.exec_nsignals, '
    'dt.exec_nvcsws, '
    'dt.exec_nivcsws, '
    'COALESCE(dt.toplevel, true) AS toplevel '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id         integer, '
        'sample_id         integer, '
        'userid            oid, '
        'datid             oid, '
        'queryid           bigint, '
        'queryid_md5       character(10), '
        'plan_user_time    double precision, '
        'plan_system_time  double precision, '
        'plan_minflts      bigint, '
        'plan_majflts      bigint, '
        'plan_nswaps       bigint, '
        'plan_reads        bigint, '
        'plan_writes       bigint, '
        'plan_msgsnds      bigint, '
        'plan_msgrcvs      bigint, '
        'plan_nsignals     bigint, '
        'plan_nvcsws       bigint, '
        'plan_nivcsws      bigint, '
        'exec_user_time    double precision, '
        'exec_system_time  double precision, '
        'exec_minflts      bigint, '
        'exec_majflts      bigint, '
        'exec_nswaps       bigint, '
        'exec_reads        bigint, '
        'exec_writes       bigint, '
        'exec_msgsnds      bigint, '
        'exec_msgrcvs      bigint, '
        'exec_nsignals     bigint, '
        'exec_nvcsws       bigint, '
        'exec_nivcsws      bigint, '
        'toplevel          boolean'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'JOIN queryid_map q_map ON (srv_map.local_srv_id, dt.queryid_md5) = (q_map.server_id, q_map.queryid_md5_old) '
    'LEFT OUTER JOIN sample_kcache ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, '
      'COALECSE(dt.toplevel, true)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('0.3.1', 1, 'sample_kcache');

UPDATE import_queries SET
  query = 'INSERT INTO tables_list (server_id,last_sample_id,datid,relid,relkind,'
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
      '(ld.server_id, ld.datid, ld.relid, ld.last_sample_id, ld.schemaname, ld.relname) IS NOT DISTINCT FROM '
      '(srv_map.local_srv_id, dt.datid, dt.relid, dt.last_sample_id, dt.schemaname, dt.relname) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
  'ON CONFLICT ON CONSTRAINT pk_tables_list DO '
  'UPDATE SET (last_sample_id, schemaname, relname) = '
    '(EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.relname)'
WHERE
  (from_version, exec_order, relname) = ('0.3.1', 1, 'tables_list');

UPDATE import_queries SET
  query = 'INSERT INTO sample_statements (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,stddev_plan_time,'
    'calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,stddev_exec_time,'
    'rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,shared_blks_written,'
    'local_blks_hit,local_blks_read,local_blks_dirtied,local_blks_written,'
    'temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,wal_records,'
    'wal_fpi,wal_bytes,toplevel'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.queryid, '
    'q_map.queryid_md5_new, '
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
    'COALESCE(dt.toplevel, true) AS toplevel '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'userid               oid, '
        'datid                oid, '
        'queryid              bigint, '
        'queryid_md5          character(10), '
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
        'toplevel             boolean'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'JOIN queryid_map q_map ON (srv_map.local_srv_id, dt.queryid_md5) = (q_map.server_id, q_map.queryid_md5_old) '
    'LEFT OUTER JOIN sample_statements ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, '
      'COALESCE(dt.toplevel, true)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('0.3.1', 2, 'sample_statements');

UPDATE import_queries SET
  query = 'INSERT INTO sample_kcache (server_id,sample_id,userid,datid,queryid,queryid_md5,'
    'plan_user_time,plan_system_time,plan_minflts,plan_majflts,'
    'plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,'
    'plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,'
    'exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,'
    'exec_nsignals,exec_nvcsws,exec_nivcsws,toplevel'
    ')'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.queryid, '
    'dt.queryid_md5, '
    'dt.plan_user_time, '
    'dt.plan_system_time, '
    'dt.plan_minflts, '
    'dt.plan_majflts, '
    'dt.plan_nswaps, '
    'dt.plan_reads, '
    'dt.plan_writes, '
    'dt.plan_msgsnds, '
    'dt.plan_msgrcvs, '
    'dt.plan_nsignals, '
    'dt.plan_nvcsws, '
    'dt.plan_nivcsws, '
    'dt.exec_user_time, '
    'dt.exec_system_time, '
    'dt.exec_minflts, '
    'dt.exec_majflts, '
    'dt.exec_nswaps, '
    'dt.exec_reads, '
    'dt.exec_writes, '
    'dt.exec_msgsnds, '
    'dt.exec_msgrcvs, '
    'dt.exec_nsignals, '
    'dt.exec_nvcsws, '
    'dt.exec_nivcsws, '
    'coalesce(dt.toplevel, true) '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id         integer, '
        'sample_id         integer, '
        'userid            oid, '
        'datid             oid, '
        'queryid           bigint, '
        'queryid_md5       character(32), '
        'plan_user_time    double precision, '
        'plan_system_time  double precision, '
        'plan_minflts      bigint, '
        'plan_majflts      bigint, '
        'plan_nswaps       bigint, '
        'plan_reads        bigint, '
        'plan_writes       bigint, '
        'plan_msgsnds      bigint, '
        'plan_msgrcvs      bigint, '
        'plan_nsignals     bigint, '
        'plan_nvcsws       bigint, '
        'plan_nivcsws      bigint, '
        'exec_user_time    double precision, '
        'exec_system_time  double precision, '
        'exec_minflts      bigint, '
        'exec_majflts      bigint, '
        'exec_nswaps       bigint, '
        'exec_reads        bigint, '
        'exec_writes       bigint, '
        'exec_msgsnds      bigint, '
        'exec_msgrcvs      bigint, '
        'exec_nsignals     bigint, '
        'exec_nvcsws       bigint, '
        'exec_nivcsws      bigint, '
        'toplevel          boolean '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_kcache ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, coalesce(dt.toplevel, true)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('0.3.2', 1, 'sample_kcache');

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
    'coalesce(dt.toplevel, true) '
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
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, coalesce(dt.toplevel, true)) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (from_version, exec_order, relname) = ('0.3.2', 2, 'sample_statements');

UPDATE import_queries SET
  query = 'INSERT INTO stmt_list (server_id,last_sample_id,queryid_md5,query)'
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
      '(ld.server_id, ld.queryid_md5, ld.last_sample_id) IS NOT DISTINCT FROM'
      '(srv_map.local_srv_id, dt.queryid_md5, dt.last_sample_id) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
  'ON CONFLICT ON CONSTRAINT pk_stmt_list '
  'DO UPDATE SET last_sample_id = EXCLUDED.last_sample_id'
WHERE
  (from_version, exec_order, relname) = ('3.9', 1, 'stmt_list');

 /*
  * Support import from pg_profile 4.0
  */
INSERT INTO import_queries VALUES
('pg_profile','4.0', 1,'last_stat_kcache',
  'INSERT INTO last_stat_kcache (server_id,sample_id,userid,datid,toplevel,queryid,'
    'plan_user_time,plan_system_time,plan_minflts,plan_majflts,'
    'plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,'
    'plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,'
    'exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,'
    'exec_nsignals,exec_nvcsws,exec_nivcsws'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.userid, '
    'dt.datid, '
    'dt.toplevel, '
    'dt.queryid, '
    'dt.plan_user_time, '
    'dt.plan_system_time, '
    'dt.plan_minflts, '
    'dt.plan_majflts, '
    'dt.plan_nswaps, '
    'dt.plan_reads, '
    'dt.plan_writes, '
    'dt.plan_msgsnds, '
    'dt.plan_msgrcvs, '
    'dt.plan_nsignals, '
    'dt.plan_nvcsws, '
    'dt.plan_nivcsws, '
    'dt.exec_user_time, '
    'dt.exec_system_time, '
    'dt.exec_minflts, '
    'dt.exec_majflts, '
    'dt.exec_nswaps, '
    'dt.exec_reads, '
    'dt.exec_writes, '
    'dt.exec_msgsnds, '
    'dt.exec_msgrcvs, '
    'dt.exec_nsignals, '
    'dt.exec_nvcsws, '
    'dt.exec_nivcsws '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id         integer, '
        'sample_id         integer, '
        'userid            oid, '
        'datid             oid, '
        'toplevel          boolean, '
        'queryid           bigint, '
        'plan_user_time    double precision, '
        'plan_system_time  double precision, '
        'plan_minflts      bigint, '
        'plan_majflts      bigint, '
        'plan_nswaps       bigint, '
        'plan_reads        bigint, '
        'plan_writes       bigint, '
        'plan_msgsnds      bigint, '
        'plan_msgrcvs      bigint, '
        'plan_nsignals     bigint, '
        'plan_nvcsws       bigint, '
        'plan_nivcsws      bigint, '
        'exec_user_time    double precision, '
        'exec_system_time  double precision, '
        'exec_minflts      bigint, '
        'exec_majflts      bigint, '
        'exec_nswaps       bigint, '
        'exec_reads        bigint, '
        'exec_writes       bigint, '
        'exec_msgsnds      bigint, '
        'exec_msgrcvs      bigint, '
        'exec_nsignals     bigint, '
        'exec_nvcsws       bigint, '
        'exec_nivcsws      bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_kcache ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, dt.toplevel) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.0', 1,'sample_statements',
  'INSERT INTO sample_statements (server_id,sample_id,userid,datid,queryid,queryid_md5,'
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
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, dt.toplevel) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.0', 1,'last_stat_statements',
  'INSERT INTO last_stat_statements (server_id,sample_id,userid,username,datid,queryid,queryid_md5,'
    'plans,total_plan_time,min_plan_time,max_plan_time,mean_plan_time,'
    'stddev_plan_time,calls,total_exec_time,min_exec_time,max_exec_time,mean_exec_time,'
    'stddev_exec_time,rows,shared_blks_hit,shared_blks_read,shared_blks_dirtied,'
    'shared_blks_written,local_blks_hit,local_blks_read,local_blks_dirtied,'
    'local_blks_written,temp_blks_read,temp_blks_written,blk_read_time,blk_write_time,'
    'wal_records,wal_fpi,wal_bytes,toplevel,in_sample'
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
    'dt.in_sample '
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
        'in_sample            boolean'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_statements ld ON '
      '(ld.server_id, ld.sample_id, ld.datid, ld.userid, ld.queryid, ld.toplevel) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.datid, dt.userid, dt.queryid, dt.toplevel) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
)
;
