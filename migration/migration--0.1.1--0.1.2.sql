/* ==== servers table ==== */
ALTER TABLE nodes RENAME TO servers;
ALTER TABLE servers RENAME COLUMN node_id TO server_id;
ALTER TABLE servers RENAME COLUMN node_name TO server_name;
ALTER TABLE servers RENAME COLUMN retention TO max_sample_age;
ALTER TABLE servers RENAME COLUMN last_snap_id TO last_sample_id;
COMMENT ON TABLE servers IS 'Monitored servers (Postgres clusters) list';

/* ==== samples table ==== */
ALTER TABLE snapshots RENAME TO samples;
ALTER TABLE samples RENAME COLUMN node_id TO server_id;
ALTER TABLE samples RENAME COLUMN snap_id TO sample_id;
ALTER TABLE samples RENAME COLUMN snap_time TO sample_time;
ALTER TABLE samples RENAME CONSTRAINT  pk_snapshots TO pk_samples;
ALTER INDEX ix_snap_time RENAME TO ix_sample_time;
COMMENT ON TABLE samples IS 'Sample times list';

/* ==== sample_settings table ==== */
DROP VIEW v_snap_settings;
ALTER TABLE snap_settings RENAME TO sample_settings;
ALTER TABLE sample_settings RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_settings RENAME CONSTRAINT pk_snap_settings TO pk_sample_settings;
ALTER TABLE sample_settings RENAME CONSTRAINT fk_snap_settings_nodes TO fk_sample_settings_servers;
COMMENT ON TABLE sample_settings IS 'pg_settings values changes detected at time of sample';

CREATE OR REPLACE VIEW v_sample_settings AS
  SELECT
    server_id,
    sample_id,
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
  FROM samples s
    JOIN sample_settings ss USING (server_id)
    JOIN LATERAL
      (SELECT server_id, name, max(first_seen) as first_seen
        FROM sample_settings WHERE server_id = s.server_id AND first_seen <= s.sample_time
        GROUP BY server_id, name) lst
      USING (server_id, name, first_seen);

COMMENT ON VIEW v_sample_settings IS 'Provides postgres settings for samples';

/* ==== baselines table ==== */
ALTER TABLE baselines RENAME COLUMN node_id TO server_id;

/* ==== bl_samples table ==== */
ALTER TABLE bl_snaps RENAME TO bl_samples;
ALTER TABLE bl_samples RENAME COLUMN node_id TO server_id;
ALTER TABLE bl_samples RENAME COLUMN snap_id TO sample_id;
ALTER TABLE bl_samples RENAME CONSTRAINT fk_bl_snaps_snapshots TO fk_bl_samples_samples;
ALTER TABLE bl_samples RENAME CONSTRAINT fk_bl_snaps_baselines TO fk_bl_samples_baselines;
ALTER TABLE bl_samples RENAME CONSTRAINT bl_snaps_pk TO pk_bl_samples;
ALTER INDEX ix_bl_snaps_blid RENAME TO ix_bl_samples_blid;
COMMENT ON TABLE bl_samples IS 'Samples in baselines';

/* ==== stmt_list table ==== */
COMMENT ON TABLE stmt_list IS 'Statements, captured in samples';

/* ==== sample_stat_database table ==== */
ALTER TABLE snap_stat_database RENAME TO sample_stat_database;
ALTER TABLE sample_stat_database
  ALTER COLUMN xact_commit DROP NOT NULL,
  ALTER COLUMN xact_rollback DROP NOT NULL,
  ALTER COLUMN blks_read DROP NOT NULL,
  ALTER COLUMN blks_hit DROP NOT NULL,
  ALTER COLUMN tup_returned DROP NOT NULL,
  ALTER COLUMN tup_fetched DROP NOT NULL,
  ALTER COLUMN tup_inserted DROP NOT NULL,
  ALTER COLUMN tup_updated DROP NOT NULL,
  ALTER COLUMN tup_deleted DROP NOT NULL,
  ALTER COLUMN conflicts DROP NOT NULL,
  ALTER COLUMN temp_files DROP NOT NULL,
  ALTER COLUMN temp_bytes DROP NOT NULL,
  ALTER COLUMN deadlocks DROP NOT NULL,
  ALTER COLUMN blk_read_time DROP NOT NULL,
  ALTER COLUMN blk_write_time DROP NOT NULL,
  ALTER COLUMN datsize DROP NOT NULL,
  ALTER COLUMN datsize_delta DROP NOT NULL;
ALTER TABLE sample_stat_database RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_database RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_stat_database RENAME CONSTRAINT fk_statdb_snapshots TO fk_statdb_samples;
ALTER TABLE sample_stat_database RENAME CONSTRAINT pk_snap_stat_database TO pk_sample_stat_database;
COMMENT ON TABLE sample_stat_database IS 'Sample database statistics table (fields from pg_stat_database)';
ALTER TABLE last_stat_database RENAME COLUMN node_id TO server_id;
ALTER TABLE last_stat_database RENAME COLUMN snap_id TO sample_id;
ALTER TABLE last_stat_database RENAME CONSTRAINT fk_last_stat_database_snapshots TO fk_last_stat_database_samples;
COMMENT ON TABLE last_stat_database IS 'Last sample data for calculating diffs in next sample';

/* ==== sample_statements table ==== */
DROP VIEW v_snap_statements;
ALTER TABLE snap_statements RENAME TO sample_statements;
ALTER TABLE sample_statements
  ALTER COLUMN calls DROP NOT NULL,
  ALTER COLUMN total_time DROP NOT NULL,
  ALTER COLUMN min_time DROP NOT NULL,
  ALTER COLUMN max_time DROP NOT NULL,
  ALTER COLUMN mean_time DROP NOT NULL,
  ALTER COLUMN stddev_time DROP NOT NULL,
  ALTER COLUMN rows DROP NOT NULL,
  ALTER COLUMN shared_blks_hit DROP NOT NULL,
  ALTER COLUMN shared_blks_read DROP NOT NULL,
  ALTER COLUMN shared_blks_dirtied DROP NOT NULL,
  ALTER COLUMN shared_blks_written DROP NOT NULL,
  ALTER COLUMN local_blks_hit DROP NOT NULL,
  ALTER COLUMN local_blks_read DROP NOT NULL,
  ALTER COLUMN local_blks_dirtied DROP NOT NULL,
  ALTER COLUMN local_blks_written DROP NOT NULL,
  ALTER COLUMN temp_blks_read DROP NOT NULL,
  ALTER COLUMN temp_blks_written DROP NOT NULL,
  ALTER COLUMN blk_read_time DROP NOT NULL,
  ALTER COLUMN blk_write_time DROP NOT NULL,
  ADD COLUMN plans bigint,
  ADD COLUMN total_plan_time     double precision,
  ADD COLUMN min_plan_time       double precision,
  ADD COLUMN max_plan_time       double precision,
  ADD COLUMN mean_plan_time      double precision,
  ADD COLUMN stddev_plan_time    double precision,
  ADD COLUMN wal_records         bigint,
  ADD COLUMN wal_fpi             bigint,
  ADD COLUMN wal_bytes           numeric;
ALTER TABLE sample_statements RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_statements RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_statements RENAME COLUMN total_time TO total_exec_time;
ALTER TABLE sample_statements RENAME COLUMN min_time TO min_exec_time;
ALTER TABLE sample_statements RENAME COLUMN max_time TO max_exec_time;
ALTER TABLE sample_statements RENAME COLUMN mean_time TO mean_exec_time;
ALTER TABLE sample_statements RENAME COLUMN stddev_time TO stddev_exec_time;
ALTER TABLE sample_statements RENAME CONSTRAINT pk_snap_statements_n TO pk_sample_statements_n;
ALTER INDEX ix_snap_stmts_qid RENAME TO ix_sample_stmts_qid;
COMMENT ON TABLE sample_statements IS 'Sample statement statistics table (fields from pg_stat_statements)';

CREATE VIEW v_sample_statements AS
SELECT
    st.server_id as server_id,
    st.sample_id as sample_id,
    st.userid as userid,
    st.datid as datid,
    st.queryid as queryid,
    queryid_md5 as queryid_md5,
    st.plans as plans,
    st.total_plan_time as total_plan_time,
    st.min_plan_time as min_plan_time,
    st.max_plan_time as max_plan_time,
    st.mean_plan_time as mean_plan_time,
    st.stddev_plan_time as stddev_plan_time,
    st.calls as calls,
    st.total_exec_time as total_exec_time,
    st.min_exec_time as min_exec_time,
    st.max_exec_time as max_exec_time,
    st.mean_exec_time as mean_exec_time,
    st.stddev_exec_time as stddev_exec_time,
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
    st.wal_records as wal_records,
    st.wal_fpi as wal_fpi,
    st.wal_bytes as wal_bytes,
    l.query as query
FROM
    sample_statements st
    JOIN stmt_list l USING (queryid_md5);

/* ==== sample_statements_total table ==== */
ALTER TABLE snap_statements_total RENAME TO sample_statements_total;
ALTER TABLE sample_statements_total
  ALTER COLUMN calls DROP NOT NULL,
  ALTER COLUMN total_time DROP NOT NULL,
  ALTER COLUMN rows DROP NOT NULL,
  ALTER COLUMN shared_blks_hit DROP NOT NULL,
  ALTER COLUMN shared_blks_read DROP NOT NULL,
  ALTER COLUMN shared_blks_dirtied DROP NOT NULL,
  ALTER COLUMN shared_blks_written DROP NOT NULL,
  ALTER COLUMN local_blks_hit DROP NOT NULL,
  ALTER COLUMN local_blks_read DROP NOT NULL,
  ALTER COLUMN local_blks_dirtied DROP NOT NULL,
  ALTER COLUMN local_blks_written DROP NOT NULL,
  ALTER COLUMN temp_blks_read DROP NOT NULL,
  ALTER COLUMN temp_blks_written DROP NOT NULL,
  ALTER COLUMN blk_read_time DROP NOT NULL,
  ALTER COLUMN blk_write_time DROP NOT NULL,
  ALTER COLUMN statements DROP NOT NULL,
  ADD COLUMN plans bigint,
  ADD COLUMN total_plan_time     double precision,
  ADD COLUMN wal_records         bigint,
  ADD COLUMN wal_fpi             bigint,
  ADD COLUMN wal_bytes           numeric;
ALTER TABLE sample_statements_total RENAME COLUMN total_time TO total_exec_time;
ALTER TABLE sample_statements_total RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_statements_total RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_statements_total RENAME CONSTRAINT pk_snap_statements_total TO pk_sample_statements_total;
COMMENT ON TABLE sample_statements_total IS 'Aggregated stats for sample, based on pg_stat_statements';


/* ==== sample_kcache table ==== */
CREATE TABLE sample_kcache (
  server_id           integer,
  sample_id           integer,
  userid              oid,
  datid               oid,
  queryid             bigint,
  queryid_md5         char(10),
  user_time           double precision, --  User CPU time used
  system_time         double precision, --  System CPU time used
  minflts             bigint, -- Number of page reclaims (soft page faults)
  majflts             bigint, -- Number of page faults (hard page faults)
  nswaps              bigint, -- Number of swaps
  reads               bigint, -- Number of bytes read by the filesystem layer
  writes              bigint, -- Number of bytes written by the filesystem layer
  msgsnds             bigint, -- Number of IPC messages sent
  msgrcvs             bigint, -- Number of IPC messages received
  nsignals            bigint, -- Number of signals received
  nvcsws              bigint, -- Number of voluntary context switches
  nivcsws             bigint,
  CONSTRAINT pk_sample_kcache_n PRIMARY KEY (server_id,sample_id,datid,userid,queryid),
  CONSTRAINT fk_kcache_st FOREIGN KEY (server_id, sample_id, datid,userid,queryid)
    REFERENCES sample_statements(server_id, sample_id, datid,userid,queryid) ON DELETE CASCADE
);
CREATE INDEX ix_sample_kcache_qid ON sample_kcache (queryid_md5);
COMMENT ON TABLE sample_kcache IS 'Sample sample_kcache statistics table (fields from pg_stat_kcache)';

CREATE VIEW v_sample_kcache AS
SELECT
    st.server_id as server_id,
    st.sample_id as sample_id,
    st.userid as userid,
    st.datid as datid,
    st.queryid as queryid,
    queryid_md5 as queryid_md5,
    st.user_time as user_time,
    st.system_time as system_time,
    st.minflts as minflts,
    st.majflts as majflts,
    st.nswaps as nswaps,
    st.reads as reads,
    --reads_blks
    st.writes  as writes,
    --writes_blks
    st.msgsnds as msgsnds,
    st.msgrcvs as msgrcvs,
    st.nsignals as nsignals,
    st.nvcsws as nvcsws,
    st.nivcsws as nivcsws,
    l.query as query
FROM
    sample_kcache st
    JOIN stmt_list l USING (queryid_md5);

CREATE TABLE sample_kcache_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    user_time           double precision, --  User CPU time used
    system_time         double precision, --  System CPU time used
    minflts             bigint, -- Number of page reclaims (soft page faults)
    majflts             bigint, -- Number of page faults (hard page faults)
    nswaps              bigint, -- Number of swaps
    reads               bigint, -- Number of bytes read by the filesystem layer
    --reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
    writes              bigint, -- Number of bytes written by the filesystem layer
    --writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
    msgsnds             bigint, -- Number of IPC messages sent
    msgrcvs             bigint, -- Number of IPC messages received
    nsignals            bigint, -- Number of signals received
    nvcsws              bigint, -- Number of voluntary context switches
    nivcsws             bigint,
    statements          bigint NOT NULL,
    CONSTRAINT pk_sample_kcache_total PRIMARY KEY (server_id, sample_id, datid),
    CONSTRAINT fk_kcache_t_st FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_kcache_total IS 'Aggregated stats for kcache, based on pg_stat_kcache';

/* ==== tablespaces_list table ==== */
ALTER TABLE tablespaces_list RENAME COLUMN node_id TO server_id;
COMMENT ON TABLE tablespaces_list IS 'Tablespaces, captured in samples';

/* ==== sample_stat_tablespaces table ==== */
DROP VIEW v_snap_stat_tablespaces;
ALTER TABLE snap_stat_tablespaces RENAME TO sample_stat_tablespaces;
ALTER TABLE sample_stat_tablespaces RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_tablespaces RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_stat_tablespaces RENAME CONSTRAINT fk_stattbs_snapshots TO fk_stattbs_samples;
ALTER TABLE sample_stat_tablespaces RENAME CONSTRAINT pk_snap_stat_tablespaces TO pk_sample_stat_tablespaces;
COMMENT ON TABLE sample_stat_tablespaces IS 'Sample tablespaces statistics (fields from pg_tablespace)';

CREATE VIEW v_sample_stat_tablespaces AS
  SELECT
    server_id,
    sample_id,
    tablespaceid,
    tablespacename,
    tablespacepath,
    size,
    size_delta
  FROM sample_stat_tablespaces JOIN tablespaces_list USING (server_id, tablespaceid);
COMMENT ON VIEW v_sample_stat_tablespaces IS 'Tablespaces stats view with tablespace names';

ALTER TABLE last_stat_tablespaces RENAME COLUMN node_id TO server_id;
ALTER TABLE last_stat_tablespaces RENAME COLUMN snap_id TO sample_id;
ALTER TABLE last_stat_tablespaces RENAME CONSTRAINT fk_last_stat_tablespaces_snapshots TO fk_last_stat_tablespaces_samples;
COMMENT ON TABLE last_stat_tablespaces IS 'Last sample data for calculating diffs in next sample';

/* ==== tables_list table ==== */
ALTER TABLE tables_list RENAME COLUMN node_id TO server_id;
COMMENT ON TABLE tables_list IS 'Table names and scheams, captured in samples';

/* ==== sample_stat_tables table ==== */
ALTER TABLE snap_stat_tables RENAME TO sample_stat_tables;
ALTER TABLE sample_stat_tables
  ALTER COLUMN seq_scan DROP NOT NULL,
  ALTER COLUMN seq_tup_read DROP NOT NULL,
  ALTER COLUMN idx_scan DROP NOT NULL,
  ALTER COLUMN idx_tup_fetch DROP NOT NULL,
  ALTER COLUMN n_tup_ins DROP NOT NULL,
  ALTER COLUMN n_tup_upd DROP NOT NULL,
  ALTER COLUMN n_tup_del DROP NOT NULL,
  ALTER COLUMN n_tup_hot_upd DROP NOT NULL,
  ALTER COLUMN n_live_tup DROP NOT NULL,
  ALTER COLUMN n_dead_tup DROP NOT NULL,
  ALTER COLUMN n_mod_since_analyze DROP NOT NULL,
  ALTER COLUMN vacuum_count DROP NOT NULL,
  ALTER COLUMN autovacuum_count DROP NOT NULL,
  ALTER COLUMN analyze_count DROP NOT NULL,
  ALTER COLUMN autoanalyze_count DROP NOT NULL,
  ALTER COLUMN heap_blks_read DROP NOT NULL,
  ALTER COLUMN heap_blks_hit DROP NOT NULL,
  ALTER COLUMN idx_blks_read DROP NOT NULL,
  ALTER COLUMN idx_blks_hit DROP NOT NULL,
  ALTER COLUMN toast_blks_read DROP NOT NULL,
  ALTER COLUMN toast_blks_hit DROP NOT NULL,
  ALTER COLUMN tidx_blks_read DROP NOT NULL,
  ALTER COLUMN tidx_blks_hit DROP NOT NULL,
  ALTER COLUMN relsize DROP NOT NULL,
  ALTER COLUMN relsize_diff DROP NOT NULL,
  ADD COLUMN n_ins_since_vacuum  bigint;
ALTER TABLE sample_stat_tables RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_tables RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_stat_tables RENAME CONSTRAINT pk_snap_stat_tables TO pk_sample_stat_tables;
COMMENT ON TABLE sample_stat_tables IS 'Stats increments for user tables in all databases by samples';

DROP VIEW v_snap_stat_tables;
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
        relkind
    FROM sample_stat_tables JOIN tables_list USING (server_id, datid, relid);
COMMENT ON VIEW v_sample_stat_tables IS 'Tables stats view with table names and schemas';

ALTER TABLE last_stat_tables ADD COLUMN n_ins_since_vacuum bigint;
ALTER TABLE last_stat_tables RENAME COLUMN node_id TO server_id;
ALTER TABLE last_stat_tables RENAME COLUMN snap_id TO sample_id;

/* ==== sample_stat_tables_total ==== */
ALTER TABLE snap_stat_tables_total RENAME TO sample_stat_tables_total;
ALTER TABLE sample_stat_tables_total
  ALTER COLUMN seq_scan DROP NOT NULL,
  ALTER COLUMN seq_tup_read DROP NOT NULL,
  ALTER COLUMN idx_scan DROP NOT NULL,
  ALTER COLUMN idx_tup_fetch DROP NOT NULL,
  ALTER COLUMN n_tup_ins DROP NOT NULL,
  ALTER COLUMN n_tup_upd DROP NOT NULL,
  ALTER COLUMN n_tup_del DROP NOT NULL,
  ALTER COLUMN n_tup_hot_upd DROP NOT NULL,
  ALTER COLUMN vacuum_count DROP NOT NULL,
  ALTER COLUMN autovacuum_count DROP NOT NULL,
  ALTER COLUMN analyze_count DROP NOT NULL,
  ALTER COLUMN autoanalyze_count DROP NOT NULL,
  ALTER COLUMN heap_blks_read DROP NOT NULL,
  ALTER COLUMN heap_blks_hit DROP NOT NULL,
  ALTER COLUMN idx_blks_read DROP NOT NULL,
  ALTER COLUMN idx_blks_hit DROP NOT NULL,
  ALTER COLUMN toast_blks_read DROP NOT NULL,
  ALTER COLUMN toast_blks_hit DROP NOT NULL,
  ALTER COLUMN tidx_blks_read DROP NOT NULL,
  ALTER COLUMN tidx_blks_hit DROP NOT NULL,
  ALTER COLUMN relsize_diff DROP NOT NULL;
ALTER TABLE sample_stat_tables_total RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_tables_total RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_stat_tables_total RENAME CONSTRAINT pk_snap_stat_tables_tot TO pk_sample_stat_tables_tot;
COMMENT ON TABLE sample_stat_tables_total IS 'Total stats for all tables in all databases by samples';

/* ==== indexes_list ==== */
ALTER TABLE indexes_list RENAME COLUMN node_id TO server_id;
COMMENT ON TABLE indexes_list IS 'Index names and scheams, captured in samples';

/* ==== sample_stat_indexes ==== */
ALTER TABLE snap_stat_indexes RENAME TO sample_stat_indexes;
ALTER TABLE sample_stat_indexes
  ALTER COLUMN idx_scan DROP NOT NULL,
  ALTER COLUMN idx_tup_read DROP NOT NULL,
  ALTER COLUMN idx_tup_fetch DROP NOT NULL,
  ALTER COLUMN idx_blks_read DROP NOT NULL,
  ALTER COLUMN idx_blks_hit DROP NOT NULL,
  ALTER COLUMN relsize DROP NOT NULL,
  ALTER COLUMN relsize_diff DROP NOT NULL,
  ALTER COLUMN indisunique DROP NOT NULL;
ALTER TABLE sample_stat_indexes RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_indexes RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_stat_indexes RENAME CONSTRAINT pk_snap_stat_indexes TO pk_sample_stat_indexes;
COMMENT ON TABLE sample_stat_indexes IS 'Stats increments for user indexes in all databases by samples';

DROP VIEW v_snap_stat_indexes;
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
        indisunique
    FROM
        sample_stat_indexes s
        JOIN indexes_list il USING (datid, indexrelid, server_id)
        JOIN tables_list tl USING (datid, relid, server_id);
COMMENT ON VIEW v_sample_stat_indexes IS 'Reconstructed stats view with table and index names and schemas';
ALTER TABLE last_stat_indexes RENAME COLUMN node_id TO server_id;
ALTER TABLE last_stat_indexes RENAME COLUMN snap_id TO sample_id;
COMMENT ON TABLE last_stat_indexes IS 'Last sample data for calculating diffs in next sample';

/* ==== sample_stat_indexes_total ==== */
ALTER TABLE snap_stat_indexes_total RENAME TO sample_stat_indexes_total;
ALTER TABLE sample_stat_indexes_total
  ALTER COLUMN idx_scan DROP NOT NULL,
  ALTER COLUMN idx_tup_read DROP NOT NULL,
  ALTER COLUMN idx_tup_fetch DROP NOT NULL,
  ALTER COLUMN idx_blks_read DROP NOT NULL,
  ALTER COLUMN idx_blks_hit DROP NOT NULL,
  ALTER COLUMN relsize_diff DROP NOT NULL;
ALTER TABLE sample_stat_indexes_total RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_indexes_total RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_stat_indexes_total RENAME CONSTRAINT pk_snap_stat_indexes_tot TO pk_sample_stat_indexes_tot;
COMMENT ON TABLE sample_stat_indexes_total IS 'Total stats for indexes in all databases by samples';

/* ==== funcs_list ==== */
ALTER TABLE funcs_list RENAME COLUMN node_id TO server_id;
COMMENT ON TABLE funcs_list IS 'Function names and scheams, captured in samples';

/* ==== sample_stat_user_functions ==== */
ALTER TABLE snap_stat_user_functions RENAME TO sample_stat_user_functions;
ALTER TABLE sample_stat_user_functions
  ALTER COLUMN calls DROP NOT NULL,
  ALTER COLUMN total_time DROP NOT NULL,
  ALTER COLUMN self_time DROP NOT NULL,
  ADD COLUMN trg_fn boolean;
ALTER TABLE sample_stat_user_functions RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_user_functions RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_stat_user_functions RENAME CONSTRAINT pk_snap_stat_user_functions TO pk_sample_stat_user_functions;
COMMENT ON TABLE sample_stat_user_functions IS 'Stats increments for user functions in all databases by samples';

UPDATE sample_stat_user_functions SET trg_fn = false;

DROP VIEW v_snap_stat_user_functions;
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
COMMENT ON VIEW v_sample_stat_indexes IS 'Reconstructed stats view with function names and schemas';

ALTER TABLE last_stat_user_functions ADD COLUMN trg_fn boolean;
ALTER TABLE last_stat_user_functions RENAME COLUMN node_id TO server_id;
ALTER TABLE last_stat_user_functions RENAME COLUMN snap_id TO sample_id;
COMMENT ON TABLE last_stat_user_functions IS 'Last sample data for calculating diffs in next sample';

/* ==== sample_stat_user_func_total ==== */
ALTER TABLE snap_stat_user_func_total RENAME TO sample_stat_user_func_total;
ALTER TABLE sample_stat_user_func_total RENAME COLUMN self_time TO total_time;
ALTER TABLE sample_stat_user_func_total RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_user_func_total RENAME COLUMN snap_id TO sample_id;
ALTER TABLE sample_stat_user_func_total ADD COLUMN trg_fn boolean,
  DROP CONSTRAINT pk_snap_stat_user_func_total;
UPDATE sample_stat_user_func_total SET trg_fn = false;
ALTER TABLE sample_stat_user_func_total ADD CONSTRAINT pk_sample_stat_user_func_total PRIMARY KEY (server_id, sample_id, datid, trg_fn);
COMMENT ON TABLE sample_stat_user_func_total IS 'Total stats for user functions in all databases by samples';

/* ==== sample_stat_cluster ==== */
ALTER TABLE snap_stat_cluster RENAME TO sample_stat_cluster;
ALTER TABLE sample_stat_cluster
  ALTER COLUMN checkpoints_timed DROP NOT NULL,
  ALTER COLUMN checkpoints_req DROP NOT NULL,
  ALTER COLUMN checkpoint_write_time DROP NOT NULL,
  ALTER COLUMN checkpoint_sync_time DROP NOT NULL,
  ALTER COLUMN buffers_checkpoint DROP NOT NULL,
  ALTER COLUMN buffers_clean DROP NOT NULL,
  ALTER COLUMN maxwritten_clean DROP NOT NULL,
  ALTER COLUMN buffers_backend DROP NOT NULL,
  ALTER COLUMN buffers_backend_fsync DROP NOT NULL,
  ALTER COLUMN buffers_alloc DROP NOT NULL,
  ALTER COLUMN wal_size DROP NOT NULL;
ALTER TABLE sample_stat_cluster RENAME CONSTRAINT fk_statcluster_snapshots TO fk_statcluster_samples;
ALTER TABLE sample_stat_cluster RENAME CONSTRAINT pk_snap_stat_cluster TO pk_sample_stat_cluster;
ALTER TABLE sample_stat_cluster RENAME COLUMN node_id TO server_id;
ALTER TABLE sample_stat_cluster RENAME COLUMN snap_id TO sample_id;
COMMENT ON TABLE sample_stat_cluster IS 'Sample cluster statistics table (fields from pg_stat_bgwriter, etc.)';

/* ==== last_stat_cluster ==== */
ALTER TABLE last_stat_cluster RENAME COLUMN node_id TO server_id;
ALTER TABLE last_stat_cluster RENAME COLUMN snap_id TO sample_id;
ALTER TABLE last_stat_cluster RENAME CONSTRAINT fk_last_stat_cluster_snapshots TO fk_last_stat_cluster_samples;
COMMENT ON TABLE last_stat_cluster IS 'Last sample data for calculating diffs in next sample';

/* ==== sample_stat_archiver ==== */
CREATE TABLE sample_stat_archiver
(
    server_id                   integer,
    sample_id                   integer,
    archived_count              bigint,
    last_archived_wal           text,
    last_archived_time          timestamp with time zone,
    failed_count                bigint,
    last_failed_wal             text,
    last_failed_time            timestamp with time zone,
    stats_reset                 timestamp with time zone,
    CONSTRAINT fk_sample_stat_archiver_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_archiver PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_archiver IS 'Sample archiver statistics table (fields from pg_stat_archiver)';

CREATE TABLE last_stat_archiver AS SELECT * FROM sample_stat_archiver WHERE 0=1;
ALTER TABLE last_stat_archiver ADD CONSTRAINT fk_last_stat_archiver_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_archiver IS 'Last sample data for calculating diffs in next sample';
