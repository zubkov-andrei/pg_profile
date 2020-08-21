/* === TABLE: snap_stat_database === */
ALTER TABLE snap_stat_database ALTER COLUMN xact_commit DROP NOT NULL,
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

ALTER TABLE snap_statements ALTER COLUMN calls DROP NOT NULL,
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

ALTER TABLE snap_statements RENAME COLUMN total_time TO total_exec_time;
ALTER TABLE snap_statements RENAME COLUMN min_time TO min_exec_time;
ALTER TABLE snap_statements RENAME COLUMN max_time TO max_exec_time;
ALTER TABLE snap_statements RENAME COLUMN mean_time TO mean_exec_time;
ALTER TABLE snap_statements RENAME COLUMN stddev_time TO stddev_exec_time;

DROP VIEW v_snap_statements;
CREATE VIEW v_snap_statements AS
SELECT
    st.node_id as node_id,
    st.snap_id as snap_id,
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
    snap_statements st
    JOIN stmt_list l USING (queryid_md5);

ALTER TABLE snap_statements_total ALTER COLUMN calls DROP NOT NULL,
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

ALTER TABLE snap_statements_total RENAME COLUMN total_time TO total_exec_time;

CREATE TABLE snap_kcache (
    node_id             integer,
    snap_id             integer,
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
    CONSTRAINT pk_snap_kcache_n PRIMARY KEY (node_id,snap_id,datid,userid,queryid),
    CONSTRAINT fk_kcache_st FOREIGN KEY (node_id, snap_id, datid,userid,queryid)
      REFERENCES snap_statements(node_id, snap_id, datid,userid,queryid) ON DELETE CASCADE
);
CREATE INDEX ix_snap_kcache_qid ON snap_kcache (queryid_md5);
COMMENT ON TABLE snap_kcache IS 'Snapshot snap_kcache statistics table (fields from pg_stat_kcache)';

CREATE VIEW v_snap_kcache AS
SELECT
    st.node_id as node_id,
    st.snap_id as snap_id,
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
    snap_kcache st
    JOIN stmt_list l USING (queryid_md5);

CREATE TABLE snap_kcache_total (
    node_id             integer,
    snap_id             integer,
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
    CONSTRAINT pk_snap_kcache_total PRIMARY KEY (node_id, snap_id, datid),
    CONSTRAINT fk_kcache_t_st FOREIGN KEY (node_id, snap_id, datid)
      REFERENCES snap_stat_database(node_id, snap_id, datid) ON DELETE CASCADE
);
COMMENT ON TABLE snap_kcache_total IS 'Aggregated stats for kcache, based on pg_stat_kcache';

ALTER TABLE snap_stat_tables ALTER COLUMN seq_scan DROP NOT NULL,
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

DROP VIEW v_snap_stat_tables;
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
    FROM snap_stat_tables JOIN tables_list USING (node_id, datid, relid);
COMMENT ON VIEW v_snap_stat_tables IS 'Tables stats view with table names and schemas';

ALTER TABLE snap_stat_tables_total ALTER COLUMN seq_scan DROP NOT NULL,
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

ALTER TABLE snap_stat_indexes ALTER COLUMN idx_scan DROP NOT NULL,
ALTER COLUMN idx_tup_read DROP NOT NULL,
ALTER COLUMN idx_tup_fetch DROP NOT NULL,
ALTER COLUMN idx_blks_read DROP NOT NULL,
ALTER COLUMN idx_blks_hit DROP NOT NULL,
ALTER COLUMN relsize DROP NOT NULL,
ALTER COLUMN relsize_diff DROP NOT NULL,
ALTER COLUMN indisunique DROP NOT NULL;

ALTER TABLE snap_stat_indexes_total ALTER COLUMN idx_scan DROP NOT NULL,
ALTER COLUMN idx_tup_read DROP NOT NULL,
ALTER COLUMN idx_tup_fetch DROP NOT NULL,
ALTER COLUMN idx_blks_read DROP NOT NULL,
ALTER COLUMN idx_blks_hit DROP NOT NULL,
ALTER COLUMN relsize_diff DROP NOT NULL;

ALTER TABLE snap_stat_user_functions ALTER COLUMN calls DROP NOT NULL,
ALTER COLUMN total_time DROP NOT NULL,
ALTER COLUMN self_time DROP NOT NULL,
ADD COLUMN trg_fn boolean;

DROP VIEW v_snap_stat_user_functions;
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
        self_time,
        trg_fn
    FROM snap_stat_user_functions JOIN funcs_list USING (node_id, datid, funcid);
COMMENT ON VIEW v_snap_stat_indexes IS 'Reconstructed stats view with function names and schemas';

ALTER TABLE last_stat_user_functions ADD COLUMN trg_fn boolean
DROP CONSTRAINT pk_snap_stat_user_func_total;

ALTER TABLE last_stat_user_functions ADD  CONSTRAINT pk_snap_stat_user_func_total PRIMARY KEY (node_id, snap_id, datid, trg_fn);

SELECT 'drop function '||proc.pronamespace::regnamespace||'.'||proc.proname||'('||pg_get_function_identity_arguments(proc.oid)||');'
FROM pg_depend dep
    JOIN pg_extension ext ON (dep.refobjid = ext.oid)
    JOIN pg_proc proc ON (proc.oid = dep.objid)
WHERE ext.extname='pg_profile' AND dep.deptype='e' AND dep.classid='pg_proc'::regclass;
