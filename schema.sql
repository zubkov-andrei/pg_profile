
/* ========= Tables ========= */
CREATE TABLE nodes (
    node_id         SERIAL PRIMARY KEY,
    node_name       name UNIQUE NOT NULL,
    db_exclude      name[] DEFAULT NULL,
    enabled         boolean DEFAULT TRUE,
    connstr         text,
    retention       integer NULL,
    last_snap_id    integer DEFAULT 0 NOT NULL
);
COMMENT ON TABLE nodes IS 'Monitored nodes (Postgres clusters) list';

INSERT INTO nodes (node_name,enabled,connstr) VALUES ('local',true,'dbname='||current_database()||' port='||current_setting('port'));

CREATE TABLE snapshots (
    node_id integer NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    snap_id integer NOT NULL,
    snap_time timestamp (0) with time zone,
    CONSTRAINT pk_snapshots PRIMARY KEY (node_id, snap_id)
);

CREATE INDEX ix_snap_time ON snapshots(node_id, snap_time);
COMMENT ON TABLE snapshots IS 'Snapshot times list';

CREATE TABLE snap_settings (
    node_id             integer,
    first_seen          timestamp (0) with time zone,
    setting_scope       smallint, -- Scope of setting. Currently may be 1 for pg_settings and 2 for other adm functions (like version)
    name                text,
    setting             text,
    reset_val           text,
    boot_val            text,
    unit                text,
    sourcefile          text,
    sourceline          integer,
    pending_restart     boolean,
    CONSTRAINT pk_snap_settings PRIMARY KEY (node_id, first_seen, setting_scope, name),
    CONSTRAINT fk_snap_settings_nodes FOREIGN KEY (node_id)
      REFERENCES nodes(node_id) ON DELETE CASCADE
);
COMMENT ON TABLE snap_settings IS 'pg_settings values changes detected at time of snapshot';

CREATE OR REPLACE VIEW v_snap_settings AS
  SELECT
    node_id,
    snap_id,
    first_seen,
    setting_scope,
    name,
    setting,
    reset_val,
    boot_val,
    unit,
    sourcefile,
    sourceline,
    pending_restart
  FROM snapshots s
    JOIN snap_settings ss USING (node_id)
    JOIN LATERAL
      (SELECT node_id, name, max(first_seen) as first_seen
        FROM snap_settings WHERE node_id = s.node_id AND first_seen <= s.snap_time
        GROUP BY node_id, name) lst
      USING (node_id, name, first_seen);

COMMENT ON VIEW v_snap_settings IS 'Provides postgres settings for snapshots';

CREATE TABLE baselines (
    node_id integer NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    bl_id SERIAL,
    bl_name varchar (25) NOT NULL,
    keep_until timestamp (0) with time zone,
    CONSTRAINT pk_baselines PRIMARY KEY (node_id, bl_id),
    CONSTRAINT uk_baselines UNIQUE (node_id,bl_name)
);
COMMENT ON TABLE baselines IS 'Baselines list';

CREATE TABLE bl_snaps (
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    bl_id integer NOT NULL,
    CONSTRAINT fk_bl_snaps_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE RESTRICT,
    CONSTRAINT fk_bl_snaps_baselines FOREIGN KEY (node_id, bl_id) REFERENCES baselines(node_id, bl_id) ON DELETE CASCADE,
    CONSTRAINT bl_snaps_pk PRIMARY KEY (node_id, bl_id, snap_id)
);
CREATE INDEX ix_bl_snaps_blid ON bl_snaps(bl_id);
COMMENT ON TABLE bl_snaps IS 'Snapshots in baselines';

CREATE TABLE stmt_list(
    queryid_md5    char(10) PRIMARY KEY,
    query          text
);
COMMENT ON TABLE stmt_list IS 'Statements, captured in snapshots';

CREATE TABLE snap_stat_database
(
    node_id             integer,
    snap_id             integer,
    datid               oid,
    datname             name NOT NULL,
    xact_commit         bigint NOT NULL,
    xact_rollback       bigint NOT NULL,
    blks_read           bigint NOT NULL,
    blks_hit            bigint NOT NULL,
    tup_returned        bigint NOT NULL,
    tup_fetched         bigint NOT NULL,
    tup_inserted        bigint NOT NULL,
    tup_updated         bigint NOT NULL,
    tup_deleted         bigint NOT NULL,
    conflicts           bigint NOT NULL,
    temp_files          bigint NOT NULL,
    temp_bytes          bigint NOT NULL,
    deadlocks           bigint NOT NULL,
    blk_read_time       double precision NOT NULL,
    blk_write_time      double precision NOT NULL,
    stats_reset         timestamp with time zone,
    datsize             bigint NOT NULL,
    datsize_delta       bigint NOT NULL,
    CONSTRAINT fk_statdb_snapshots FOREIGN KEY (node_id, snap_id)
      REFERENCES snapshots (node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_database PRIMARY KEY (node_id,snap_id,datid)
);
COMMENT ON TABLE snap_stat_database IS 'Snapshot database statistics table (fields from pg_stat_database)';

CREATE TABLE last_stat_database AS SELECT * FROM snap_stat_database WHERE 0=1;
ALTER TABLE last_stat_database  ADD CONSTRAINT pk_last_stat_database PRIMARY KEY (node_id, snap_id, datid);
ALTER TABLE last_stat_database ADD CONSTRAINT fk_last_stat_database_snapshots
  FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_database IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_statements (
    node_id             integer,
    snap_id             integer,
    userid              oid,
    datid               oid,
    queryid             bigint,
    queryid_md5         char(10) REFERENCES stmt_list (queryid_md5) ON DELETE RESTRICT ON UPDATE CASCADE,
    calls               bigint NOT NULL,
    total_time          double precision NOT NULL,
    min_time            double precision NOT NULL,
    max_time            double precision NOT NULL,
    mean_time           double precision NOT NULL,
    stddev_time         double precision NOT NULL,
    rows                bigint NOT NULL,
    shared_blks_hit     bigint NOT NULL,
    shared_blks_read    bigint NOT NULL,
    shared_blks_dirtied bigint NOT NULL,
    shared_blks_written bigint NOT NULL,
    local_blks_hit      bigint NOT NULL,
    local_blks_read     bigint NOT NULL,
    local_blks_dirtied  bigint NOT NULL,
    local_blks_written  bigint NOT NULL,
    temp_blks_read      bigint NOT NULL,
    temp_blks_written   bigint NOT NULL,
    blk_read_time       double precision NOT NULL,
    blk_write_time      double precision NOT NULL,
    CONSTRAINT pk_snap_statements_n PRIMARY KEY (node_id,snap_id,datid,userid,queryid),
    CONSTRAINT fk_statments_dat FOREIGN KEY (node_id, snap_id, datid)
      REFERENCES snap_stat_database(node_id, snap_id, datid) ON DELETE CASCADE
);
CREATE INDEX ix_snap_stmts_qid ON snap_statements (queryid_md5);
COMMENT ON TABLE snap_statements IS 'Snapshot statement statistics table (fields from pg_stat_statements)';

CREATE VIEW v_snap_statements AS
SELECT
    st.node_id as node_id,
    st.snap_id as snap_id,
    st.userid as userid,
    st.datid as datid,
    st.queryid as queryid,
    queryid_md5 as queryid_md5,
    st.calls as calls,
    st.total_time as total_time,
    st.min_time as min_time,
    st.max_time as max_time,
    st.mean_time as mean_time,
    st.stddev_time as stddev_time,
    st.rows as rows,
    st.shared_blks_hit as shared_blks_hit,
    st.shared_blks_read as shared_blks_read,
    st.shared_blks_dirtied as shared_blks_dirtied,
    st.shared_blks_written as shared_blks_written,
    st.local_blks_hit as local_blks_hit,
    st.local_blks_read as local_blks_read,
    st.local_blks_dirtied as local_blks_dirtied,
    st.local_blks_written as local_blks_written,
    st.temp_blks_read as temp_blks_read,
    st.temp_blks_written as temp_blks_written,
    st.blk_read_time as blk_read_time,
    st.blk_write_time as blk_write_time,
    l.query as query
FROM
    snap_statements st
    JOIN stmt_list l USING (queryid_md5);

CREATE TABLE snap_statements_total (
    node_id             integer,
    snap_id             integer,
    datid               oid,
    calls               bigint NOT NULL,
    total_time          double precision NOT NULL,
    rows                bigint NOT NULL,
    shared_blks_hit     bigint NOT NULL,
    shared_blks_read    bigint NOT NULL,
    shared_blks_dirtied bigint NOT NULL,
    shared_blks_written bigint NOT NULL,
    local_blks_hit      bigint NOT NULL,
    local_blks_read     bigint NOT NULL,
    local_blks_dirtied  bigint NOT NULL,
    local_blks_written  bigint NOT NULL,
    temp_blks_read      bigint NOT NULL,
    temp_blks_written   bigint NOT NULL,
    blk_read_time       double precision NOT NULL,
    blk_write_time      double precision NOT NULL,
    statements          bigint NOT NULL,
    CONSTRAINT pk_snap_statements_total PRIMARY KEY (node_id, snap_id, datid),
    CONSTRAINT fk_statments_t_dat FOREIGN KEY (node_id, snap_id, datid)
      REFERENCES snap_stat_database(node_id, snap_id, datid) ON DELETE CASCADE
);
COMMENT ON TABLE snap_statements_total IS 'Aggregated stats for snapshot, based on pg_stat_statements';

CREATE TABLE tablespaces_list(
    node_id             integer REFERENCES nodes(node_id) ON DELETE CASCADE,
    tablespaceid        oid,
    tablespacename      name NOT NULL,
    tablespacepath      text NOT NULL, -- cannot be changed without changing oid
    CONSTRAINT pk_tablespace_list PRIMARY KEY (node_id, tablespaceid)
);
COMMENT ON TABLE tablespaces_list IS 'Tablespaces, captured in snapshots';

CREATE TABLE snap_stat_tablespaces
(
    node_id             integer,
    snap_id             integer,
    tablespaceid        oid,
    size             bigint NOT NULL,
    size_delta        bigint NOT NULL,
    CONSTRAINT fk_stattbs_snapshots FOREIGN KEY (node_id, snap_id)
        REFERENCES snapshots (node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT fk_st_tablespaces_tablespaces FOREIGN KEY (node_id, tablespaceid)
        REFERENCES tablespaces_list(node_id, tablespaceid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT pk_snap_stat_tablespaces PRIMARY KEY (node_id,snap_id,tablespaceid)
);
COMMENT ON TABLE snap_stat_tablespaces IS 'Snapshot tablespaces statistics (fields from pg_tablespace)';

CREATE VIEW v_snap_stat_tablespaces AS
    SELECT
        node_id,
        snap_id,
        tablespaceid,
        tablespacename,
        tablespacepath,
        size,
        size_delta
    FROM snap_stat_tablespaces JOIN tablespaces_list USING (node_id, tablespaceid);
COMMENT ON VIEW v_snap_stat_tablespaces IS 'Tablespaces stats view with tablespace names';

CREATE TABLE last_stat_tablespaces AS SELECT * FROM v_snap_stat_tablespaces WHERE 0=1;
ALTER TABLE last_stat_tablespaces ADD CONSTRAINT pk_last_stat_tablespaces PRIMARY KEY (node_id, snap_id, tablespaceid);
ALTER TABLE last_stat_tablespaces ADD CONSTRAINT fk_last_stat_tablespaces_snapshots
  FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_tablespaces IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE tables_list(
    node_id             integer REFERENCES nodes(node_id) ON DELETE CASCADE,
    datid               oid,
    relid               oid,
    relkind             char(1) NOT NULL,
    reltoastrelid       oid,
    schemaname          name NOT NULL,
    relname             name NOT NULL,
    CONSTRAINT pk_tables_list PRIMARY KEY (node_id, datid, relid),
    CONSTRAINT fk_toast_table FOREIGN KEY (node_id, datid, reltoastrelid)
      REFERENCES tables_list (node_id, datid, relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT uk_toast_table UNIQUE (node_id, datid, reltoastrelid)
);
CREATE UNIQUE INDEX ix_tables_list_reltoast ON tables_list(node_id, datid, reltoastrelid);
COMMENT ON TABLE tables_list IS 'Table names and scheams, captured in snapshots';

CREATE TABLE snap_stat_tables (
    node_id             integer,
    snap_id             integer,
    datid               oid,
    relid               oid,
    tablespaceid        oid,
    seq_scan            bigint NOT NULL,
    seq_tup_read        bigint NOT NULL,
    idx_scan            bigint NOT NULL,
    idx_tup_fetch       bigint NOT NULL,
    n_tup_ins           bigint NOT NULL,
    n_tup_upd           bigint NOT NULL,
    n_tup_del           bigint NOT NULL,
    n_tup_hot_upd       bigint NOT NULL,
    n_live_tup          bigint NOT NULL,
    n_dead_tup          bigint NOT NULL,
    n_mod_since_analyze bigint NOT NULL,
    last_vacuum         timestamp with time zone,
    last_autovacuum     timestamp with time zone,
    last_analyze        timestamp with time zone,
    last_autoanalyze    timestamp with time zone,
    vacuum_count        bigint NOT NULL,
    autovacuum_count    bigint NOT NULL,
    analyze_count       bigint NOT NULL,
    autoanalyze_count   bigint NOT NULL,
    heap_blks_read      bigint NOT NULL,
    heap_blks_hit       bigint NOT NULL,
    idx_blks_read       bigint NOT NULL,
    idx_blks_hit        bigint NOT NULL,
    toast_blks_read     bigint NOT NULL,
    toast_blks_hit      bigint NOT NULL,
    tidx_blks_read      bigint NOT NULL,
    tidx_blks_hit       bigint NOT NULL,
    relsize             bigint NOT NULL,
    relsize_diff        bigint NOT NULL,
    CONSTRAINT pk_snap_stat_tables PRIMARY KEY (node_id, snap_id, datid, relid),
    CONSTRAINT fk_st_tables_dat FOREIGN KEY (node_id, snap_id, datid) REFERENCES snap_stat_database(node_id, snap_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tables_tablespace FOREIGN KEY (node_id, snap_id, tablespaceid) REFERENCES snap_stat_tablespaces(node_id, snap_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tables_tables FOREIGN KEY (node_id, datid, relid) REFERENCES tables_list(node_id, datid, relid) ON DELETE RESTRICT ON UPDATE RESTRICT
);
COMMENT ON TABLE snap_stat_tables IS 'Stats increments for user tables in all databases by snapshots';

CREATE VIEW v_snap_stat_tables AS
    SELECT
        node_id,
        snap_id,
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
        relkind
    FROM snap_stat_tables JOIN tables_list USING (node_id, datid, relid);
COMMENT ON VIEW v_snap_stat_tables IS 'Tables stats view with table names and schemas';

CREATE TABLE last_stat_tables AS SELECT * FROM v_snap_stat_tables WHERE 0=1;
ALTER TABLE last_stat_tables ADD CONSTRAINT pk_last_stat_tables
  PRIMARY KEY (node_id, snap_id, datid, relid);
ALTER TABLE last_stat_tables ADD CONSTRAINT fk_last_stat_tables_dat
  FOREIGN KEY (node_id, snap_id, datid)
  REFERENCES last_stat_database(node_id, snap_id, datid) ON DELETE RESTRICT;
ALTER TABLE last_stat_tables ADD CONSTRAINT fk_last_stat_tablespaces
  FOREIGN KEY (node_id, snap_id, tablespaceid)
  REFERENCES last_stat_tablespaces(node_id, snap_id, tablespaceid) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_tables IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_stat_tables_total (
    node_id             integer,
    snap_id             integer,
    datid               oid,
    tablespaceid        oid,
    relkind             char(1) NOT NULL,
    seq_scan            bigint NOT NULL,
    seq_tup_read        bigint NOT NULL,
    idx_scan            bigint NOT NULL,
    idx_tup_fetch       bigint NOT NULL,
    n_tup_ins           bigint NOT NULL,
    n_tup_upd           bigint NOT NULL,
    n_tup_del           bigint NOT NULL,
    n_tup_hot_upd       bigint NOT NULL,
    vacuum_count        bigint NOT NULL,
    autovacuum_count    bigint NOT NULL,
    analyze_count       bigint NOT NULL,
    autoanalyze_count   bigint NOT NULL,
    heap_blks_read      bigint NOT NULL,
    heap_blks_hit       bigint NOT NULL,
    idx_blks_read       bigint NOT NULL,
    idx_blks_hit        bigint NOT NULL,
    toast_blks_read     bigint NOT NULL,
    toast_blks_hit      bigint NOT NULL,
    tidx_blks_read      bigint NOT NULL,
    tidx_blks_hit       bigint NOT NULL,
    relsize_diff        bigint NOT NULL,
    CONSTRAINT pk_snap_stat_tables_tot PRIMARY KEY (node_id, snap_id, datid, relkind, tablespaceid),
    CONSTRAINT fk_st_tables_tot_dat FOREIGN KEY (node_id, snap_id, datid) REFERENCES snap_stat_database(node_id, snap_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tablespaces_tot_dat FOREIGN KEY (node_id, snap_id, tablespaceid) REFERENCES snap_stat_tablespaces(node_id, snap_id, tablespaceid) ON DELETE CASCADE
);
COMMENT ON TABLE snap_stat_tables_total IS 'Total stats for all tables in all databases by snapshots';

CREATE TABLE indexes_list(
    node_id         integer NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    datid           oid NOT NULL,
    indexrelid      oid NOT NULL,
    relid           oid NOT NULL,
    schemaname      name NOT NULL,
    indexrelname    name NOT NULL,
    CONSTRAINT pk_indexes_list PRIMARY KEY (node_id, datid, indexrelid),
    CONSTRAINT fk_indexes_tables FOREIGN KEY (node_id, datid, relid)
      REFERENCES tables_list(node_id, datid, relid)
);
COMMENT ON TABLE indexes_list IS 'Index names and scheams, captured in snapshots';

CREATE TABLE snap_stat_indexes (
    node_id             integer,
    snap_id             integer,
    datid               oid,
    indexrelid          oid,
    tablespaceid        oid,
    idx_scan            bigint NOT NULL,
    idx_tup_read        bigint NOT NULL,
    idx_tup_fetch       bigint NOT NULL,
    idx_blks_read       bigint NOT NULL,
    idx_blks_hit        bigint NOT NULL,
    relsize             bigint NOT NULL,
    relsize_diff        bigint NOT NULL,
    indisunique         bool NOT NULL,
    CONSTRAINT fk_stat_indexes_indexes FOREIGN KEY (node_id, datid, indexrelid)
      REFERENCES indexes_list(node_id, datid, indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_stat_indexes_dat FOREIGN KEY (node_id, snap_id, datid)
      REFERENCES snap_stat_database(node_id, snap_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_stat_indexes_tablespaces FOREIGN KEY (node_id, snap_id, tablespaceid)
      REFERENCES snap_stat_tablespaces(node_id, snap_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_indexes PRIMARY KEY (node_id, snap_id, datid, indexrelid)
);
COMMENT ON TABLE snap_stat_indexes IS 'Stats increments for user indexes in all databases by snapshots';

CREATE VIEW v_snap_stat_indexes AS
    SELECT
        node_id,
        snap_id,
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
        indisunique
    FROM
        snap_stat_indexes s
        JOIN indexes_list il USING (datid, indexrelid, node_id)
        JOIN tables_list tl USING (datid, relid, node_id);
COMMENT ON VIEW v_snap_stat_indexes IS 'Reconstructed stats view with table and index names and schemas';

CREATE TABLE last_stat_indexes AS SELECT * FROM v_snap_stat_indexes WHERE 0=1;
ALTER TABLE last_stat_indexes ADD CONSTRAINT pk_last_stat_indexes PRIMARY KEY (node_id, snap_id, datid, relid, indexrelid);
ALTER TABLE last_stat_indexes ADD CONSTRAINT fk_last_stat_indexes_dat FOREIGN KEY (node_id, snap_id, datid)
-- Restrict deleting last data snapshot
  REFERENCES last_stat_database(node_id, snap_id, datid) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_indexes IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_stat_indexes_total (
    node_id             integer,
    snap_id             integer,
    datid               oid,
    tablespaceid        oid,
    idx_scan            bigint NOT NULL,
    idx_tup_read        bigint NOT NULL,
    idx_tup_fetch       bigint NOT NULL,
    idx_blks_read       bigint NOT NULL,
    idx_blks_hit        bigint NOT NULL,
    relsize_diff        bigint NOT NULL,
    CONSTRAINT fk_stat_indexes_tot_dat FOREIGN KEY (node_id, snap_id, datid)
      REFERENCES snap_stat_database(node_id, snap_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_stat_tablespaces_tot_dat FOREIGN KEY (node_id, snap_id, tablespaceid)
      REFERENCES snap_stat_tablespaces(node_id, snap_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_indexes_tot PRIMARY KEY (node_id, snap_id, datid, tablespaceid)
);
COMMENT ON TABLE snap_stat_indexes_total IS 'Total stats for indexes in all databases by snapshots';

CREATE TABLE funcs_list(
    node_id integer NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    datid       oid,
    funcid      oid,
    schemaname  name NOT NULL,
    funcname    name NOT NULL,
    funcargs    text NOT NULL,
    CONSTRAINT pk_funcs_list PRIMARY KEY (node_id, datid, funcid)
);
COMMENT ON TABLE funcs_list IS 'Function names and scheams, captured in snapshots';

CREATE TABLE snap_stat_user_functions (
    node_id     integer,
    snap_id     integer,
    datid       oid,
    funcid      oid,
    calls       bigint NOT NULL,
    total_time  double precision NOT NULL,
    self_time   double precision NOT NULL,
    CONSTRAINT fk_user_functions_functions FOREIGN KEY (node_id, datid, funcid)
      REFERENCES funcs_list (node_id, datid, funcid) ON DELETE RESTRICT,
    CONSTRAINT fk_user_functions_dat FOREIGN KEY (node_id, snap_id, datid)
      REFERENCES snap_stat_database (node_id, snap_id, datid) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_user_functions PRIMARY KEY (node_id, snap_id, datid, funcid)
);
COMMENT ON TABLE snap_stat_user_functions IS 'Stats increments for user functions in all databases by snapshots';

CREATE VIEW v_snap_stat_user_functions AS
    SELECT
        node_id,
        snap_id,
        datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        calls,
        total_time,
        self_time
    FROM snap_stat_user_functions JOIN funcs_list USING (node_id, datid, funcid);
COMMENT ON VIEW v_snap_stat_indexes IS 'Reconstructed stats view with function names and schemas';

CREATE TABLE last_stat_user_functions AS SELECT * FROM v_snap_stat_user_functions WHERE 0=1;
ALTER TABLE last_stat_user_functions ADD CONSTRAINT fk_last_stat_user_functions_dat
  FOREIGN KEY (node_id, snap_id, datid)
  -- Restrict deleting last data snapshot
  REFERENCES last_stat_database(node_id, snap_id, datid) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_user_functions IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_stat_user_func_total (
    node_id     integer,
    snap_id     integer,
    datid       oid,
    calls       bigint NOT NULL,
    self_time   double precision NOT NULL,
    CONSTRAINT fk_user_func_tot_dat FOREIGN KEY (node_id, snap_id, datid)
      REFERENCES snap_stat_database (node_id, snap_id, datid) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_user_func_total PRIMARY KEY (node_id, snap_id, datid)
);
COMMENT ON TABLE snap_stat_user_func_total IS 'Total stats for user functions in all databases by snapshots';

CREATE TABLE snap_stat_cluster
(
    node_id                     integer,
    snap_id                     integer,
    checkpoints_timed           bigint NOT NULL,
    checkpoints_req             bigint NOT NULL,
    checkpoint_write_time       double precision NOT NULL,
    checkpoint_sync_time        double precision NOT NULL,
    buffers_checkpoint          bigint NOT NULL,
    buffers_clean               bigint NOT NULL,
    maxwritten_clean            bigint NOT NULL,
    buffers_backend             bigint NOT NULL,
    buffers_backend_fsync       bigint NOT NULL,
    buffers_alloc               bigint NOT NULL,
    stats_reset                 timestamp with time zone,
    wal_size                    bigint NOT NULL,
    CONSTRAINT fk_statcluster_snapshots FOREIGN KEY (node_id, snap_id)
      REFERENCES snapshots (node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_cluster PRIMARY KEY (node_id, snap_id)
);
COMMENT ON TABLE snap_stat_cluster IS 'Snapshot cluster statistics table (fields from pg_stat_bgwriter, etc.)';

CREATE TABLE last_stat_cluster AS SELECT * FROM snap_stat_cluster WHERE 0=1;
ALTER TABLE last_stat_cluster ADD CONSTRAINT fk_last_stat_cluster_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_cluster IS 'Last snapshot data for calculating diffs in next snapshot';
