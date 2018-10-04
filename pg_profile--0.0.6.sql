\echo Use "CREATE EXTENSION pg_profile" to load this file. \quit

/* ========= Tables ========= */
CREATE TABLE nodes (
    node_id         SERIAL PRIMARY KEY,
    node_name       name UNIQUE NOT NULL,
    enabled         boolean,
    connstr         text,
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

CREATE TABLE snap_params (
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    p_name text,
    setting text,
    CONSTRAINT fk_snap_params_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT pk_snap_params PRIMARY KEY (node_id, snap_id, p_name)
);
COMMENT ON TABLE snap_params IS 'PostgreSQL parameters at time of snapshot';

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
    CONSTRAINT bl_snaps_pk PRIMARY KEY (node_id, snap_id, bl_id)
);
CREATE INDEX ix_bl_snaps_blid ON bl_snaps(bl_id);
COMMENT ON TABLE bl_snaps IS 'Snapshots in baselines';

CREATE TABLE stmt_list(
    queryid_md5    char(10) PRIMARY KEY,
    query          text
);
COMMENT ON TABLE stmt_list IS 'Statements, captured in snapshots';

CREATE TABLE snap_statements (
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    userid oid,
    dbid oid,
    queryid bigint,
    queryid_md5 char(10) REFERENCES stmt_list (queryid_md5) ON DELETE RESTRICT ON UPDATE CASCADE,
    calls bigint,
    total_time double precision,
    min_time double precision,
    max_time double precision,
    mean_time double precision,
    stddev_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    CONSTRAINT pk_snap_statements_n PRIMARY KEY (node_id,snap_id,userid,dbid,queryid),
    CONSTRAINT fk_statments_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE CASCADE
);
CREATE INDEX ix_snap_stmts_qid ON snap_statements (queryid_md5);
COMMENT ON TABLE snap_statements IS 'Snapshot statement statistics table (fields from pg_stat_statements)';

CREATE VIEW v_snap_statements AS
SELECT
    st.node_id as node_id,
    st.snap_id as snap_id,
    st.userid as userid,
    st.dbid as dbid,
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
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    dbid oid,
    calls bigint,
    total_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    statements bigint,
    CONSTRAINT pk_snap_statements_total PRIMARY KEY (node_id, snap_id, dbid),
    CONSTRAINT fk_statments_t_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE CASCADE
);
COMMENT ON TABLE snap_statements_total IS 'Aggregated stats for snapshot, based on pg_stat_statements';

CREATE TABLE tables_list(
    node_id     integer REFERENCES nodes(node_id) ON DELETE CASCADE,
    relid       oid,
    schemaname  name,
    relname     name,
    CONSTRAINT pk_tables_list PRIMARY KEY (node_id, relid)
);
COMMENT ON TABLE tables_list IS 'Table names and scheams, captured in snapshots';

CREATE TABLE snap_stat_user_tables (
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    dbid oid,
    relid oid,
    seq_scan bigint,
    seq_tup_read bigint,
    idx_scan bigint,
    idx_tup_fetch bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    n_live_tup bigint,
    n_dead_tup bigint,
    n_mod_since_analyze bigint,
    last_vacuum timestamp with time zone,
    last_autovacuum timestamp with time zone,
    last_analyze timestamp with time zone,
    last_autoanalyze timestamp with time zone,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint,
    relsize bigint,
    relsize_diff bigint,
    CONSTRAINT pk_snap_stat_user_tables PRIMARY KEY (node_id, snap_id, dbid, relid),
    CONSTRAINT fk_user_tables_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT fk_user_tables_tables FOREIGN KEY (node_id, relid) REFERENCES tables_list(node_id, relid) ON DELETE RESTRICT ON UPDATE RESTRICT
);
COMMENT ON TABLE snap_stat_user_tables IS 'Stats increments for user tables in all databases by snapshots';

CREATE VIEW v_snap_stat_user_tables AS
    SELECT
        node_id,
        snap_id,
        dbid,
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
        relsize,
        relsize_diff
    FROM ONLY snap_stat_user_tables JOIN tables_list USING (node_id, relid);
COMMENT ON VIEW v_snap_stat_user_tables IS 'Reconstructed stats view with table names and schemas';

CREATE TABLE last_stat_user_tables AS SELECT * FROM v_snap_stat_user_tables WHERE 0=1;
COMMENT ON TABLE last_stat_user_tables IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE indexes_list(
    node_id         integer NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    indexrelid      oid,
    schemaname      name,
    indexrelname    name,
    CONSTRAINT pk_indexes_list PRIMARY KEY (node_id, indexrelid)
);
COMMENT ON TABLE indexes_list IS 'Index names and scheams, captured in snapshots';

CREATE TABLE snap_stat_user_indexes (
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    dbid oid,
    relid oid,
    indexrelid oid,
    idx_scan bigint,
    idx_tup_read bigint,
    idx_tup_fetch bigint,
    relsize bigint,
    relsize_diff bigint,
    indisunique bool,
    CONSTRAINT fk_user_indexes_tables FOREIGN KEY (node_id, relid) REFERENCES tables_list(node_id, relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_user_indexes_indexes FOREIGN KEY (node_id, indexrelid) REFERENCES indexes_list(node_id, indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_user_indexes_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots(node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_user_indexes PRIMARY KEY (node_id, snap_id, dbid, relid, indexrelid)
);
COMMENT ON TABLE snap_stat_user_indexes IS 'Stats increments for user indexes in all databases by snapshots';

CREATE VIEW v_snap_stat_user_indexes AS
    SELECT
        s.node_id,
        s.snap_id,
        s.dbid,
        s.relid,
        s.indexrelid,
        il.schemaname,
        tl.relname,
        il.indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        relsize,
        relsize_diff,
        indisunique
    FROM ONLY 
        snap_stat_user_indexes s 
        JOIN indexes_list il ON (il.indexrelid = s.indexrelid AND il.node_id = s.node_id)
        JOIN tables_list tl ON (tl.relid = s.relid AND tl.node_id = s.node_id);
COMMENT ON VIEW v_snap_stat_user_indexes IS 'Reconstructed stats view with table and index names and schemas';

CREATE TABLE last_stat_user_indexes AS SELECT * FROM v_snap_stat_user_indexes WHERE 0=1;
COMMENT ON TABLE last_stat_user_indexes IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE funcs_list(
    node_id integer NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    funcid      oid,
    schemaname  name,
    funcname    name,
    CONSTRAINT pk_funcs_list PRIMARY KEY (node_id, funcid)
);
COMMENT ON TABLE funcs_list IS 'Function names and scheams, captured in snapshots';

CREATE TABLE snap_stat_user_functions (
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    dbid oid,
    funcid oid,
    calls bigint,
    total_time double precision,
    self_time double precision,
    CONSTRAINT fk_user_functions_functions FOREIGN KEY (node_id, funcid) REFERENCES funcs_list (node_id, funcid) ON DELETE RESTRICT,
    CONSTRAINT fk_user_functions_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots (node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_user_functions PRIMARY KEY (node_id, snap_id, dbid, funcid)
);
COMMENT ON TABLE snap_stat_user_functions IS 'Stats increments for user functions in all databases by snapshots';

CREATE VIEW v_snap_stat_user_functions AS
    SELECT
        node_id,
        snap_id,
        dbid,
        funcid,
        schemaname,
        funcname,
        calls,
        total_time,
        self_time
    FROM ONLY snap_stat_user_functions JOIN funcs_list USING (node_id,funcid);
COMMENT ON VIEW v_snap_stat_user_indexes IS 'Reconstructed stats view with function names and schemas';

CREATE TABLE last_stat_user_functions AS SELECT * FROM v_snap_stat_user_functions WHERE 0=1;
COMMENT ON TABLE last_stat_user_functions IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_statio_user_tables (
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    dbid oid NOT NULL,
    relid oid NOT NULL,
    heap_blks_read bigint,
    heap_blks_hit bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    toast_blks_read bigint,
    toast_blks_hit bigint,
    tidx_blks_read bigint,
    tidx_blks_hit bigint,
    relsize bigint,
    relsize_diff bigint,
    CONSTRAINT fk_statio_tables_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots (node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT fk_statio_tables_tables FOREIGN KEY (node_id, relid) REFERENCES tables_list(node_id, relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT pk_snap_statio_user_tables PRIMARY KEY (node_id, snap_id, dbid, relid)
);
COMMENT ON TABLE snap_statio_user_tables IS 'IO Stats increments for user tables in all databases by snapshots';

CREATE VIEW v_snap_statio_user_tables AS
    SELECT
        node_id,
        snap_id,
        dbid,
        relid,
        schemaname,
        relname,
        heap_blks_read,
        heap_blks_hit,
        idx_blks_read,
        idx_blks_hit,
        toast_blks_read,
        toast_blks_hit,
        tidx_blks_read,
        tidx_blks_hit,
        relsize,
        relsize_diff
    FROM ONLY snap_statio_user_tables JOIN tables_list USING (node_id, relid);
COMMENT ON VIEW v_snap_statio_user_tables IS 'Reconstructed stats view with table names and schemas';

CREATE TABLE last_statio_user_tables AS SELECT * FROM v_snap_statio_user_tables WHERE 0=1;
COMMENT ON TABLE last_statio_user_tables IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_statio_user_indexes (
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    dbid oid,
    relid oid,
    indexrelid oid,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    relsize bigint,
    relsize_diff bigint,
    CONSTRAINT fk_statio_indexes_tables FOREIGN KEY (node_id, relid) REFERENCES tables_list(node_id, relid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_statio_indexes_indexes FOREIGN KEY (node_id, indexrelid) REFERENCES indexes_list(node_id, indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_statio_indexes_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots (node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT pk_snap_statio_user_indexes PRIMARY KEY (node_id, snap_id, dbid, relid, indexrelid)
);
COMMENT ON TABLE snap_statio_user_indexes IS 'Stats increments for user indexes in all databases by snapshots';

CREATE VIEW v_snap_statio_user_indexes AS
    SELECT
        s.node_id,
        s.snap_id,
        s.dbid,
        s.relid,
        s.indexrelid,
        il.schemaname,
        tl.relname,
        il.indexrelname,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff
    FROM
        ONLY snap_statio_user_indexes s
        JOIN tables_list tl ON (s.relid = tl.relid and s.node_id=tl.node_id)
        JOIN indexes_list il ON (s.indexrelid = il.indexrelid and s.node_id=il.node_id);
COMMENT ON VIEW v_snap_statio_user_indexes IS 'Reconstructed stats view with table and index names and schemas';

CREATE TABLE last_statio_user_indexes AS SELECT * FROM v_snap_statio_user_indexes WHERE 0=1;
COMMENT ON TABLE last_statio_user_indexes IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_stat_database
(
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    datid oid,
    datname name,
    xact_commit bigint,
    xact_rollback bigint,
    blks_read bigint,
    blks_hit bigint,
    tup_returned bigint,
    tup_fetched bigint,
    tup_inserted bigint,
    tup_updated bigint,
    tup_deleted bigint,
    conflicts bigint,
    temp_files bigint,
    temp_bytes bigint,
    deadlocks bigint,
    blk_read_time double precision,
    blk_write_time double precision,
    stats_reset timestamp with time zone,
    datsize_delta bigint,
    CONSTRAINT fk_statdb_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots (node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_database PRIMARY KEY (node_id,snap_id,datid,datname)
);
COMMENT ON TABLE snap_stat_database IS 'Snapshot database statistics table (fields from pg_stat_database)';
CREATE TABLE last_stat_database AS SELECT * FROM snap_stat_database WHERE 0=1;
COMMENT ON TABLE last_stat_database IS 'Last snapshot data for calculating diffs in next snapshot';

CREATE TABLE snap_stat_cluster
(
    node_id integer NOT NULL,
    snap_id integer NOT NULL,
    checkpoints_timed bigint,
    checkpoints_req bigint,
    checkpoint_write_time double precision,
    checkpoint_sync_time double precision,
    buffers_checkpoint bigint,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    buffers_alloc bigint,
    stats_reset timestamp with time zone,
    wal_size bigint,
    CONSTRAINT fk_statcluster_snapshots FOREIGN KEY (node_id, snap_id) REFERENCES snapshots (node_id, snap_id) ON DELETE CASCADE,
    CONSTRAINT pk_snap_stat_cluster PRIMARY KEY (node_id, snap_id)
);
COMMENT ON TABLE snap_stat_cluster IS 'Snapshot cluster statistics table (fields from pg_stat_bgwriter, etc.)';
CREATE TABLE last_stat_cluster AS SELECT * FROM snap_stat_cluster WHERE 0=1;
COMMENT ON TABLE last_stat_cluster IS 'Last snapshot data for calculating diffs in next snapshot';

/* ========= Internal functions ========= */

CREATE OR REPLACE FUNCTION get_connstr(IN snode_id integer) RETURNS text SET search_path=@extschema@,public SET lock_timeout=300000 AS $$
DECLARE
    node_connstr text = null;
BEGIN
    --Getting node_connstr
    SELECT connstr INTO node_connstr FROM nodes n WHERE n.node_id = snode_id;
    IF (node_connstr IS NULL) THEN
        RAISE 'node_id not found';
    ELSE
        RETURN node_connstr;
    END IF;
END;
$$ LANGUAGE plpgsql;

/* ========= Snapshot functions ========= */

CREATE OR REPLACE FUNCTION snapshot(IN snode_id integer) RETURNS integer SET search_path=@extschema@,public SET lock_timeout=300000 AS $$
DECLARE
    id              integer;
    topn            integer;
    ret             integer;
    lockid          bigint;
    stat_stmt_avail integer;
    pg_version      varchar(10);
    qres            record;
    node_connstr        text;
BEGIN
    -- Only one running snapshot() function allowed!
    -- Explicitly locking nodes table
    BEGIN
        LOCK nodes IN SHARE ROW EXCLUSIVE MODE NOWAIT;
    EXCEPTION
        WHEN OTHERS THEN RAISE 'Can''t get lock on nodes table. Is there another snapshot() running?';
    END;
    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;
    -- Getting retention setting
    BEGIN
        ret := current_setting('pg_profile.retention')::integer;
    EXCEPTION
        WHEN OTHERS THEN ret := 7;
    END;
    
    node_connstr := get_connstr(snode_id);
    
    IF dblink_get_connections() @> ARRAY['node_connection'] THEN
        PERFORM dblink_disconnect('node_connection');
    END IF;
    
    PERFORM dblink_connect('node_connection',node_connstr);
    
    --Getting postgres version
    SELECT setting INTO STRICT pg_version FROM dblink('node_connection','SELECT setting FROM pg_catalog.pg_settings WHERE name = ''server_version_num''') AS t (setting text);
    
    -- Deleting obsolete baselines
    DELETE FROM baselines WHERE keep_until < now();
    -- Deleting obsolote snapshots
    DELETE FROM snapshots WHERE snap_time < now() - (ret || ' days')::interval
        AND (node_id,snap_id) NOT IN (SELECT node_id,snap_id FROM bl_snaps);
    -- Deleting unused statements
    DELETE FROM stmt_list
        WHERE queryid_md5 NOT IN
            (SELECT queryid_md5 FROM snap_statements);


    -- Creating a new snapshot record
    UPDATE nodes SET last_snap_id = last_snap_id + 1 WHERE node_id = snode_id 
    RETURNING last_snap_id INTO id;
    INSERT INTO snapshots(snap_time,node_id,snap_id) 
    VALUES (now(),snode_id,id);

    -- Collecting postgres parameters
    INSERT INTO snap_params
    SELECT snode_id,id,name,setting FROM dblink('node_connection','SELECT name,setting
    FROM pg_catalog.pg_settings
    WHERE name IN (''pg_stat_statements.max'',''pg_stat_statements.track'')') AS dbl (
        name text,
        setting text
        );
    
    INSERT INTO snap_params
    VALUES (snode_id,id,'pg_profile.topn',topn);

    -- collect pg_stat_statements stats if available
    SELECT cnt INTO stat_stmt_avail FROM
      dblink('node_connection','SELECT count(1) FROM pg_catalog.pg_extension WHERE extname=''pg_stat_statements''')
      as t1(cnt integer);
    IF stat_stmt_avail > 0 THEN
        PERFORM collect_statements_stats(snode_id, id, topn);
    END IF;
    
    
    -- pg_stat_database data
    INSERT INTO last_stat_database (
        node_id,
        snap_id,
        datid,
        datname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        conflicts,
        temp_files,
        temp_bytes,
        deadlocks,
        blk_read_time,
        blk_write_time,
        stats_reset,
        datsize_delta)
    SELECT
        snode_id,
        id,
        datid,
        datname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        conflicts,
        temp_files,
        temp_bytes,
        deadlocks,
        blk_read_time,
        blk_write_time,
        stats_reset,
        datsize
    FROM dblink('node_connection','SELECT 
        datid,
        datname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        conflicts,
        temp_files,
        temp_bytes,
        deadlocks,
        blk_read_time,
        blk_write_time,
        stats_reset,
        pg_database_size(datid) as datsize
      FROM pg_catalog.pg_stat_database') AS rs (
        datid oid,
        datname name,
        xact_commit bigint,
        xact_rollback bigint,
        blks_read bigint,
        blks_hit bigint,
        tup_returned bigint,
        tup_fetched bigint,
        tup_inserted bigint,
        tup_updated bigint,
        tup_deleted bigint,
        conflicts bigint,
        temp_files bigint,
        temp_bytes bigint,
        deadlocks bigint,
        blk_read_time double precision,
        blk_write_time double precision,
        stats_reset timestamp with time zone,
        datsize bigint
        );
    
    INSERT INTO snap_stat_database 
    SELECT
        rs.node_id,
        rs.snap_id,
        rs.datid,
        rs.datname,
        rs.xact_commit-ls.xact_commit,
        rs.xact_rollback-ls.xact_rollback,
        rs.blks_read-ls.blks_read,
        rs.blks_hit-ls.blks_hit,
        rs.tup_returned-ls.tup_returned,
        rs.tup_fetched-ls.tup_fetched,
        rs.tup_inserted-ls.tup_inserted,
        rs.tup_updated-ls.tup_updated,
        rs.tup_deleted-ls.tup_deleted,
        rs.conflicts-ls.conflicts,
        rs.temp_files-ls.temp_files,
        rs.temp_bytes-ls.temp_bytes,
        rs.deadlocks-ls.deadlocks,
        rs.blk_read_time-ls.blk_read_time,
        rs.blk_write_time-ls.blk_write_time,
        rs.stats_reset,
        rs.datsize_delta-ls.datsize_delta
    FROM ONLY(last_stat_database) rs
    JOIN ONLY(last_stat_database) ls ON (rs.datid = ls.datid AND rs.datname = ls.datname AND  ls.node_id = rs.node_id
        AND rs.stats_reset = ls.stats_reset AND ls.snap_id = rs.snap_id - 1)
    WHERE rs.snap_id = id AND rs.node_id = snode_id;

    PERFORM snapshot_dbobj_delta(snode_id,id,topn);
    
    DELETE FROM last_stat_database WHERE node_id = snode_id AND snap_id = id - 1;

    -- pg_stat_bgwriter data
    IF pg_version::integer < 100000 THEN
        INSERT INTO last_stat_cluster (
            node_id,
            snap_id,
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            wal_size)
        SELECT
            snode_id,
            id,
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            wal_size
        FROM dblink('node_connection','SELECT
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            CASE WHEN pg_is_in_recovery() THEN 0
                 ELSE pg_xlog_location_diff(pg_current_xlog_location(),''0/00000000'')
            END AS wal_size
          FROM pg_catalog.pg_stat_bgwriter') AS rs (
            checkpoints_timed bigint,
            checkpoints_req bigint,
            checkpoint_write_time double precision,
            checkpoint_sync_time double precision,
            buffers_checkpoint bigint,
            buffers_clean bigint,
            maxwritten_clean bigint,
            buffers_backend bigint,
            buffers_backend_fsync bigint,
            buffers_alloc bigint,
            stats_reset timestamp with time zone,
            wal_size bigint);
    ELSIF pg_version::integer >= 100000 THEN
        INSERT INTO last_stat_cluster (
            node_id,
            snap_id,
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            wal_size)
        SELECT
            snode_id,
            id,
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            wal_size
        FROM dblink('node_connection','SELECT
            checkpoints_timed,
            checkpoints_req,
            checkpoint_write_time,
            checkpoint_sync_time,
            buffers_checkpoint,
            buffers_clean,
            maxwritten_clean,
            buffers_backend,
            buffers_backend_fsync,
            buffers_alloc,
            stats_reset,
            CASE WHEN pg_is_in_recovery() THEN 0
                 ELSE pg_wal_lsn_diff(pg_current_wal_lsn(),''0/00000000'')
            END AS wal_size
          FROM pg_catalog.pg_stat_bgwriter') AS rs (
            checkpoints_timed bigint,
            checkpoints_req bigint,
            checkpoint_write_time double precision,
            checkpoint_sync_time double precision,
            buffers_checkpoint bigint,
            buffers_clean bigint,
            maxwritten_clean bigint,
            buffers_backend bigint,
            buffers_backend_fsync bigint,
            buffers_alloc bigint,
            stats_reset timestamp with time zone,
            wal_size bigint);
    END IF;

    INSERT INTO snap_stat_cluster
    SELECT
        rs.node_id,
        rs.snap_id,
        rs.checkpoints_timed-ls.checkpoints_timed,
        rs.checkpoints_req-ls.checkpoints_req,
        rs.checkpoint_write_time-ls.checkpoint_write_time,
        rs.checkpoint_sync_time-ls.checkpoint_sync_time,
        rs.buffers_checkpoint-ls.buffers_checkpoint,
        rs.buffers_clean-ls.buffers_clean,
        rs.maxwritten_clean-ls.maxwritten_clean,
        rs.buffers_backend-ls.buffers_backend,
        rs.buffers_backend_fsync-ls.buffers_backend_fsync,
        rs.buffers_alloc-ls.buffers_alloc,
        rs.stats_reset,
        rs.wal_size-ls.wal_size
    FROM last_stat_cluster rs 
    JOIN ONLY(last_stat_cluster) ls ON (rs.stats_reset = ls.stats_reset AND rs.node_id=ls.node_id
        AND ls.snap_id = rs.snap_id - 1)
    WHERE rs.snap_id = id AND rs.node_id = snode_id;
    
    DELETE FROM last_stat_cluster WHERE node_id = snode_id AND snap_id = id - 1;
    
    -- Delete unused tables from tables list
    DELETE FROM tables_list WHERE (node_id,relid) NOT IN (
        SELECT node_id, relid FROM snap_stat_user_tables
        UNION ALL
        SELECT node_id, relid FROM snap_statio_user_tables
        UNION ALL
        SELECT node_id, relid FROM snap_stat_user_indexes
        UNION ALL
        SELECT node_id, relid FROM snap_statio_user_indexes
    );
    
    -- Delete unused indexes from indexes list
    DELETE FROM indexes_list WHERE (node_id, indexrelid) NOT IN (
        SELECT node_id, indexrelid FROM snap_stat_user_indexes
        UNION ALL
        SELECT node_id, indexrelid FROM snap_statio_user_indexes
    );
    
    -- Delete unused functions from functions list
    DELETE FROM funcs_list WHERE (node_id, funcid) NOT IN (
        SELECT node_id, funcid FROM snap_stat_user_functions
    );

    PERFORM dblink_disconnect('node_connection');
    RETURN id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION snapshot(IN snode_id integer) IS 'Statistics snapshot creation function (by node_id).';

CREATE OR REPLACE FUNCTION snapshot(IN node name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    snode_id    integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name = node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found';
    ELSE
        RETURN snapshot(snode_id);
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION snapshot(IN node name) IS 'Statistics snapshot creation function (by node name).';

CREATE OR REPLACE FUNCTION snapshot() RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    c_nodes CURSOR FOR
    SELECT node_id FROM nodes WHERE enabled;
    failures    integer ARRAY;
    
    r_result    RECORD;
BEGIN    
    -- Only one running snapshot() function allowed!
    -- Explicitly locking nodes table
    BEGIN
        LOCK nodes IN SHARE ROW EXCLUSIVE MODE NOWAIT;
    EXCEPTION
        WHEN OTHERS THEN RAISE 'Can''t get lock on nodes table. Is there another snapshot() running?';
    END;
    FOR r_result IN c_nodes LOOP
        BEGIN
            PERFORM snapshot(r_result.node_id);
        EXCEPTION
            WHEN OTHERS THEN failures := array_append(failures,r_result.node_id);
        END;
    END LOOP;
    IF array_length(failures,1) > 0 THEN
        RETURN 'FAILED nodes identifiers: '||array_to_string(failures,',');
    ELSE
        RETURN 'OK';
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION snapshot() IS 'Statistics snapshot creation function (for all enabled nodes). Must be explicitly called periodically.';

CREATE OR REPLACE FUNCTION collect_statements_stats(IN snode_id integer, IN s_id integer, IN topn integer) RETURNS void SET search_path=@extschema@,public AS $$
DECLARE
    qres record;
BEGIN
    -- Snapshot data from pg_stat_statements for top whole cluster statements
    FOR qres IN
        SELECT snode_id,s_id as snap_id,dbl.* FROM
        dblink('node_connection','SELECT
            st.userid,
            st.dbid,
            st.queryid,
            left(md5(db.datname || r.rolname || st.query ), 10) AS queryid_md5,
            st.calls,
            st.total_time,
            st.min_time,
            st.max_time,
            st.mean_time,
            st.stddev_time,
            st.rows,
            st.shared_blks_hit,
            st.shared_blks_read,
            st.shared_blks_dirtied,
            st.shared_blks_written,
            st.local_blks_hit,
            st.local_blks_read,
            st.local_blks_dirtied,
            st.local_blks_written,
            st.temp_blks_read,
            st.temp_blks_written,
            st.blk_read_time,
            st.blk_write_time,
            regexp_replace(st.query,''\s+'','' '',''g'') AS query
        FROM pg_stat_statements st 
            JOIN pg_database db ON (db.oid=st.dbid)
            JOIN pg_roles r ON (r.oid=st.userid)
        JOIN
            (SELECT
            userid, dbid, md5(query) as q_md5,
            row_number() over (ORDER BY sum(total_time) DESC) AS time_p, 
            row_number() over (ORDER BY sum(calls) DESC) AS calls_p,
            row_number() over (ORDER BY sum(blk_read_time + blk_write_time) DESC) AS io_time_p,
            row_number() over (ORDER BY sum(shared_blks_hit + shared_blks_read) DESC) AS gets_p,
            row_number() over (ORDER BY sum(temp_blks_written + local_blks_written) DESC) AS temp_p
            FROM pg_stat_statements
            GROUP BY userid, dbid, md5(query)) rank_t
        ON (st.userid=rank_t.userid AND st.dbid=rank_t.dbid AND md5(st.query)=rank_t.q_md5)
        WHERE
            time_p <= '||topn||'
            OR calls_p <= '||topn||'
            OR io_time_p <= '||topn||'
            OR gets_p <= '||topn||'
            OR temp_p <= '||topn)
        AS dbl (
            userid oid,
            dbid oid,
            queryid bigint,
            queryid_md5 char(10),
            calls bigint,
            total_time double precision,
            min_time double precision,
            max_time double precision,
            mean_time double precision,
            stddev_time double precision,
            rows bigint,
            shared_blks_hit bigint,
            shared_blks_read bigint,
            shared_blks_dirtied bigint,
            shared_blks_written bigint,
            local_blks_hit bigint,
            local_blks_read bigint,
            local_blks_dirtied bigint,
            local_blks_written bigint,
            temp_blks_read bigint,
            temp_blks_written bigint,
            blk_read_time double precision,
            blk_write_time double precision,
            query text
        )
    LOOP
        INSERT INTO stmt_list VALUES (qres.queryid_md5,qres.query) ON CONFLICT DO NOTHING;
        INSERT INTO snap_statements VALUES (
            qres.snode_id,
            qres.snap_id,
            qres.userid,
            qres.dbid,
            qres.queryid,
            qres.queryid_md5,
            qres.calls,
            qres.total_time,
            qres.min_time,
            qres.max_time,
            qres.mean_time,
            qres.stddev_time,
            qres.rows,
            qres.shared_blks_hit,
            qres.shared_blks_read,
            qres.shared_blks_dirtied,
            qres.shared_blks_written,
            qres.local_blks_hit,
            qres.local_blks_read,
            qres.local_blks_dirtied,
            qres.local_blks_written,
            qres.temp_blks_read,
            qres.temp_blks_written,
            qres.blk_read_time,
            qres.blk_write_time
        );
    END LOOP;

    -- Aggregeted statistics data
    INSERT INTO snap_statements_total
    SELECT snode_id,s_id,dbl.* FROM
    dblink('node_connection','SELECT dbid,sum(calls),sum(total_time),sum(rows),sum(shared_blks_hit),
        sum(shared_blks_read),sum(shared_blks_dirtied),sum(shared_blks_written),
        sum(local_blks_hit),sum(local_blks_read),sum(local_blks_dirtied),
        sum(local_blks_written),sum(temp_blks_read),sum(temp_blks_written),sum(blk_read_time),
        sum(blk_write_time),count(*)
    FROM pg_stat_statements
    GROUP BY dbid') AS dbl (
        dbid oid,
        calls bigint,
        total_time double precision,
        rows bigint,
        shared_blks_hit bigint,
        shared_blks_read bigint,
        shared_blks_dirtied bigint,
        shared_blks_written bigint,
        local_blks_hit bigint,
        local_blks_read bigint,
        local_blks_dirtied bigint,
        local_blks_written bigint,
        temp_blks_read bigint,
        temp_blks_written bigint,
        blk_read_time double precision,
        blk_write_time double precision,
        stmts integer
    );
    -- Flushing pg_stat_statements    
    SELECT * INTO qres FROM dblink('node_connection','SELECT pg_stat_statements_reset()') AS t(res char(1));
END;
$$ LANGUAGE plpgsql; 

CREATE OR REPLACE FUNCTION collect_obj_stats(IN snode_id integer, IN s_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    --Cursor for db stats
    c_dblist CURSOR FOR
    SELECT datid,datname,port FROM dblink('node_connection',
    'select dbs.datid,dbs.datname,s1.setting as port from pg_catalog.pg_stat_database dbs, pg_catalog.pg_settings s1
    where dbs.datname not like ''template_'' and s1.name=''port''') AS dbl (
        datid oid,
        datname name,
        port text
    );

	r_result    RECORD;
    db_connstr      text;
BEGIN

   -- Load new data from statistic views of all cluster databases
    IF dblink_get_connections() @> ARRAY['node_db_connection'] THEN
        PERFORM dblink_disconnect('node_db_connection');
    END IF;
	FOR r_result IN c_dblist LOOP
      db_connstr := regexp_replace(get_connstr(snode_id),'dbname=\w+','dbname='||r_result.datname,'g');
      PERFORM dblink_connect('node_db_connection',db_connstr);
      INSERT INTO last_stat_user_tables
      SELECT snode_id,s_id,r_result.datid,t.*
      FROM dblink('node_db_connection', 'select *,pg_relation_size(relid) relsize,0 relsize_diff from pg_catalog.pg_stat_user_tables')
      AS t (
         relid oid,
         schemaname name,
         relname name,
         seq_scan bigint,
         seq_tup_read bigint,
         idx_scan bigint,
         idx_tup_fetch bigint,
         n_tup_ins bigint,
         n_tup_upd bigint,
         n_tup_del bigint,
         n_tup_hot_upd bigint,
         n_live_tup bigint,
         n_dead_tup bigint,
         n_mod_since_analyze bigint,
         last_vacuum timestamp with time zone,
         last_autovacuum timestamp with time zone,
         last_analyze timestamp with time zone,
         last_autoanalyze timestamp with time zone,
         vacuum_count bigint,
         autovacuum_count bigint,
         analyze_count bigint,
         autoanalyze_count bigint,
         relsize bigint,
         relsize_diff bigint
      );
      
      INSERT INTO last_stat_user_indexes
      SELECT snode_id,s_id,r_result.datid,t.*
      FROM dblink('node_db_connection', 'select st.*,pg_relation_size(st.indexrelid),0,(ix.indisunique or con.conindid IS NOT NULL) as indisunique
        from pg_catalog.pg_stat_user_indexes st 
        join pg_catalog.pg_index ix on (ix.indexrelid = st.indexrelid) 
        left join pg_catalog.pg_constraint con on(con.conindid = ix.indexrelid and con.contype in (''p'',''u''))')
      AS t (
         relid oid,
         indexrelid oid,
         schemaname name,
         relname name,
         indexrelname name,
         idx_scan bigint,
         idx_tup_read bigint,
         idx_tup_fetch bigint,
         relsize bigint,
         relsize_diff bigint,
         indisunique bool
      );
      
      INSERT INTO last_stat_user_functions
      SELECT snode_id,s_id,r_result.datid,t.*
      FROM dblink('node_db_connection', 'select * from pg_catalog.pg_stat_user_functions')
      AS t (
         funcid oid,
         schemaname name,
         funcname name,
         calls bigint,
         total_time double precision,
         self_time double precision
      );

      INSERT INTO last_statio_user_tables
      SELECT snode_id,s_id,r_result.datid,t.*
      FROM dblink('node_db_connection', 'select *,pg_relation_size(relid),0 from pg_catalog.pg_statio_user_tables')
      AS t (
         relid oid,
         schemaname name,
         relname name,
         heap_blks_read bigint,
         heap_blks_hit bigint,
         idx_blks_read bigint,
         idx_blks_hit bigint,
         toast_blks_read bigint,
         toast_blks_hit bigint,
         tidx_blks_read bigint,
         tidx_blks_hit bigint,
         relsize bigint,
         relsize_diff bigint
      );
      
      INSERT INTO last_statio_user_indexes
      SELECT snode_id,s_id,r_result.datid,t.*
      FROM dblink('node_db_connection', 'select *,pg_relation_size(indexrelid),0 from pg_catalog.pg_statio_user_indexes')
      AS t (
         relid oid,
         indexrelid oid,
         schemaname name,
         relname name,
         indexrelname name,
         idx_blks_read bigint,
         idx_blks_hit bigint,
         relsize bigint,
         relsize_diff bigint
      );
      PERFORM dblink_disconnect('node_db_connection');
	END LOOP;
   RETURN 0;
END;
$$ LANGUAGE plpgsql; 

CREATE OR REPLACE FUNCTION snapshot_dbobj_delta(IN snode_id integer, IN s_id integer, IN topn integer) RETURNS integer AS $$
DECLARE
    qres    record;
BEGIN
    -- Collecting stat info for objects of all databases
    PERFORM collect_obj_stats(snode_id, s_id);

    -- Calculating difference from previous snapshot and storing it in snap_stat_ tables
    -- Stats of user tables
    FOR qres IN
        SELECT
            node_id,
            snap_id,
            dbid,
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
            relsize,
            relsize_diff
        FROM
            (SELECT
                node_id,
                t.snap_id,
                t.dbid,
                t.relid,
                t.schemaname,
                t.relname,
                t.seq_scan-l.seq_scan as seq_scan,
                t.seq_tup_read-l.seq_tup_read as seq_tup_read,
                t.idx_scan-l.idx_scan as idx_scan,
                t.idx_tup_fetch-l.idx_tup_fetch as idx_tup_fetch,
                t.n_tup_ins-l.n_tup_ins as n_tup_ins,
                t.n_tup_upd-l.n_tup_upd as n_tup_upd,
                t.n_tup_del-l.n_tup_del as n_tup_del,
                t.n_tup_hot_upd-l.n_tup_hot_upd as n_tup_hot_upd,
                t.n_live_tup as n_live_tup,
                t.n_dead_tup as n_dead_tup,
                t.n_mod_since_analyze,
                t.last_vacuum,
                t.last_autovacuum,
                t.last_analyze,
                t.last_autoanalyze,
                t.vacuum_count-l.vacuum_count as vacuum_count,
                t.autovacuum_count-l.autovacuum_count as autovacuum_count,
                t.analyze_count-l.analyze_count as analyze_count,
                t.autoanalyze_count-l.autoanalyze_count as autoanalyze_count,
                t.relsize,
                t.relsize-l.relsize as relsize_diff,
                row_number() OVER (ORDER BY t.seq_scan-l.seq_scan desc) scan_rank,
                row_number() OVER (ORDER BY t.n_tup_ins-l.n_tup_ins+t.n_tup_upd-l.n_tup_upd+t.n_tup_del-l.n_tup_del+t.n_tup_hot_upd-l.n_tup_hot_upd desc) dml_rank,
                row_number() OVER (ORDER BY t.n_tup_upd-l.n_tup_upd+t.n_tup_del-l.n_tup_del+t.n_tup_hot_upd-l.n_tup_hot_upd desc) vacuum_rank,
                row_number() OVER (ORDER BY t.relsize-l.relsize desc) growth_rank,
                row_number() OVER (ORDER BY t.n_dead_tup*100/GREATEST(t.n_live_tup+t.n_dead_tup,1) desc) dead_pct_rank,
                row_number() OVER (ORDER BY t.n_mod_since_analyze*100/GREATEST(t.n_live_tup,1) desc) mod_pct_rank
            FROM last_stat_user_tables t JOIN last_stat_user_tables l USING (node_id,dbid,relid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND node_id=snode_id) diff
        WHERE scan_rank <= topn OR dml_rank <= topn OR growth_rank <= topn OR dead_pct_rank <= topn OR mod_pct_rank <= topn OR vacuum_rank <= topn
    LOOP
        INSERT INTO tables_list VALUES (qres.node_id,qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_tables VALUES (
            qres.node_id,
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.seq_scan,
            qres.seq_tup_read,
            qres.idx_scan,
            qres.idx_tup_fetch,
            qres.n_tup_ins,
            qres.n_tup_upd,
            qres.n_tup_del,
            qres.n_tup_hot_upd,
            qres.n_live_tup,
            qres.n_dead_tup,
            qres.n_mod_since_analyze,
            qres.last_vacuum,
            qres.last_autovacuum,
            qres.last_analyze,
            qres.last_autoanalyze,
            qres.vacuum_count,
            qres.autovacuum_count,
            qres.analyze_count,
            qres.autoanalyze_count,
            qres.relsize,
            qres.relsize_diff
        );
    END LOOP;

    -- Stats of user indexes
    FOR qres IN
        SELECT
            node_id,
            snap_id,
            dbid,
            relid,
            indexrelid,
            schemaname,
            relname,
            indexrelname,
            idx_scan,
            idx_tup_read,
            idx_tup_fetch,
            relsize,
            relsize_diff,
            indisunique
        FROM
            (SELECT
                node_id,
                t.snap_id,
                t.dbid,
                t.relid,
                t.indexrelid,
                t.schemaname,
                t.relname,
                t.indexrelname,
                t.idx_scan-l.idx_scan as idx_scan,
                t.idx_tup_read-l.idx_tup_read as idx_tup_read,
                t.idx_tup_fetch-l.idx_tup_fetch as idx_tup_fetch,
                t.relsize,
                t.relsize-l.relsize as relsize_diff,
                t.indisunique,
                row_number() OVER (ORDER BY t.relsize-l.relsize desc) size_rank -- most growing
            FROM last_stat_user_indexes t JOIN last_stat_user_indexes l USING (node_id,dbid,relid,indexrelid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND node_id=snode_id) diff
        WHERE size_rank <= topn
    LOOP
        INSERT INTO indexes_list VALUES (qres.node_id,qres.indexrelid,qres.schemaname,qres.indexrelname) ON CONFLICT DO NOTHING;
        INSERT INTO tables_list VALUES (qres.node_id,qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_indexes VALUES (
            qres.node_id,
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.indexrelid,
            qres.idx_scan,
            qres.idx_tup_read,
            qres.idx_tup_fetch,
            qres.relsize,
            qres.relsize_diff,
            qres.indisunique
        );
    END LOOP;
    
    -- Stats of growing unused user indexes
    FOR qres IN
        SELECT
            node_id,
            snap_id,
            dbid,
            relid,
            indexrelid,
            schemaname,
            relname,
            indexrelname,
            idx_scan,
            idx_tup_read,
            idx_tup_fetch,
            relsize,
            relsize_diff,
            indisunique
        FROM
            (SELECT
                node_id,
                t.snap_id,
                t.dbid,
                t.relid,
                t.indexrelid,
                t.schemaname,
                t.relname,
                t.indexrelname,
                t.idx_scan-l.idx_scan as idx_scan,
                t.idx_tup_read-l.idx_tup_read as idx_tup_read,
                t.idx_tup_fetch-l.idx_tup_fetch as idx_tup_fetch,
                t.relsize,
                t.relsize-l.relsize as relsize_diff,
                t.indisunique,
                row_number() OVER (ORDER BY t.relsize-l.relsize desc) size_rank
            FROM last_stat_user_indexes t JOIN last_stat_user_indexes l USING (node_id,dbid,relid,indexrelid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND node_id=snode_id AND
                NOT t.indisunique
                AND t.idx_scan-l.idx_scan = 0) diff
        WHERE size_rank <= topn
    LOOP
        INSERT INTO indexes_list VALUES (qres.node_id,qres.indexrelid,qres.schemaname,qres.indexrelname) ON CONFLICT DO NOTHING;
        INSERT INTO tables_list VALUES (qres.node_id,qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_indexes VALUES (
            qres.node_id,
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.indexrelid,
            qres.idx_scan,
            qres.idx_tup_read,
            qres.idx_tup_fetch,
            qres.relsize,
            qres.relsize_diff,
            qres.indisunique
        ) ON CONFLICT DO NOTHING;
    END LOOP;

    -- User functions stats
    --INSERT INTO snap_stat_user_functions
    FOR qres IN
        SELECT
            node_id,
            snap_id,
            dbid,
            funcid,
            schemaname,
            funcname,
            calls,
            total_time,
            self_time
        FROM
            (SELECT
                node_id,
                t.snap_id,
                t.dbid,
                t.funcid,
                t.schemaname,
                t.funcname,
                t.calls-l.calls as calls,
                t.total_time-l.total_time as total_time,
                t.self_time-l.self_time as self_time,
                row_number() OVER (ORDER BY t.total_time-l.total_time desc) time_rank,
                row_number() OVER (ORDER BY t.self_time-l.self_time desc) stime_rank,
                row_number() OVER (ORDER BY t.calls-l.calls desc) calls_rank
            FROM last_stat_user_functions t JOIN last_stat_user_functions l USING (node_id,dbid,funcid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND node_id=snode_id
                AND t.calls-l.calls > 0) diff
        WHERE time_rank <= topn OR calls_rank <= topn OR stime_rank <= topn
    LOOP
        INSERT INTO funcs_list VALUES (qres.node_id,qres.funcid,qres.schemaname,qres.funcname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_functions VALUES (
            qres.node_id,
            qres.snap_id,
            qres.dbid,
            qres.funcid,
            qres.calls,
            qres.total_time,
            qres.self_time
        );
    END LOOP;

    --INSERT INTO snap_statio_user_tables
    FOR qres IN
        SELECT
            node_id,
            snap_id,
            dbid,
            relid,
            schemaname,
            relname,
            heap_blks_read,
            heap_blks_hit,
            idx_blks_read,
            idx_blks_hit,
            toast_blks_read,
            toast_blks_hit,
            tidx_blks_read,
            tidx_blks_hit,
            relsize,
            relsize_diff
        FROM
            (SELECT
                node_id,
                t.snap_id,
                t.dbid,
                t.relid,
                t.schemaname,
                t.relname,
                t.heap_blks_read-l.heap_blks_read as heap_blks_read,
                t.heap_blks_hit-l.heap_blks_hit as heap_blks_hit,
                t.idx_blks_read-l.idx_blks_read as idx_blks_read,
                t.idx_blks_hit-l.idx_blks_hit as idx_blks_hit,
                t.toast_blks_read-l.toast_blks_read as toast_blks_read,
                t.toast_blks_hit-l.toast_blks_hit as toast_blks_hit,
                t.tidx_blks_read-l.tidx_blks_read as tidx_blks_read,
                t.tidx_blks_hit-l.tidx_blks_hit as tidx_blks_hit,
                t.relsize as relsize,
                t.relsize-l.relsize as relsize_diff,
                row_number() OVER (ORDER BY t.heap_blks_read-l.heap_blks_read+
                t.idx_blks_read-l.idx_blks_read+t.toast_blks_read-l.toast_blks_read+
                t.tidx_blks_read-l.tidx_blks_read desc) read_rank
            FROM last_statio_user_tables t JOIN last_statio_user_tables l USING (node_id,dbid,relid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND node_id=snode_id AND
                t.heap_blks_read-l.heap_blks_read+
                t.idx_blks_read-l.idx_blks_read+t.toast_blks_read-l.toast_blks_read+
                t.tidx_blks_read-l.tidx_blks_read > 0) diff
        WHERE read_rank <= topn
    LOOP
        INSERT INTO tables_list VALUES (qres.node_id,qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_statio_user_tables VALUES (
            qres.node_id,
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.heap_blks_read,
            qres.heap_blks_hit,
            qres.idx_blks_read,
            qres.idx_blks_hit,
            qres.toast_blks_read,
            qres.toast_blks_hit,
            qres.tidx_blks_read,
            qres.tidx_blks_hit,
            qres.relsize,
            qres.relsize_diff
        );
    END LOOP;

    --INSERT INTO snap_statio_user_indexes
    FOR qres IN
        SELECT
            node_id,
            snap_id,
            dbid,
            relid,
            indexrelid,
            schemaname,
            relname,
            indexrelname,
            idx_blks_read,
            idx_blks_hit,
            relsize,
            relsize_diff
        FROM
            (SELECT
                node_id,
                t.snap_id,
                t.dbid,
                t.relid,
                t.indexrelid,
                t.schemaname,
                t.relname,
                t.indexrelname,
                t.idx_blks_read-l.idx_blks_read as idx_blks_read,
                t.idx_blks_hit-l.idx_blks_hit as idx_blks_hit,
                t.relsize,
                t.relsize-l.relsize as relsize_diff,
                row_number() OVER (ORDER BY t.idx_blks_read-l.idx_blks_read desc) read_rank
            FROM last_statio_user_indexes t JOIN last_statio_user_indexes l USING (node_id,dbid,relid,indexrelid)
            WHERE l.snap_id=t.snap_id-1 AND t.snap_id=s_id AND node_id=snode_id AND
                t.idx_blks_read-l.idx_blks_read > 0) diff
        WHERE read_rank <= topn
    LOOP
        INSERT INTO indexes_list VALUES (qres.node_id,qres.indexrelid,qres.schemaname,qres.indexrelname) ON CONFLICT DO NOTHING;
        INSERT INTO tables_list VALUES (qres.node_id,qres.relid,qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_statio_user_indexes VALUES (
            qres.node_id,
            qres.snap_id,
            qres.dbid,
            qres.relid,
            qres.indexrelid,
            qres.idx_blks_read,
            qres.idx_blks_hit,
            qres.relsize,
            qres.relsize_diff
        );
    END LOOP;

    -- Clear data in last_ tables, holding data only for next diff snapshot
    DELETE FROM last_stat_user_tables WHERE node_id=snode_id AND snap_id = s_id - 1;

    DELETE FROM last_stat_user_indexes WHERE node_id=snode_id AND snap_id = s_id - 1;

    DELETE FROM last_stat_user_functions WHERE node_id=snode_id AND snap_id = s_id - 1;

    DELETE FROM last_statio_user_tables WHERE node_id=snode_id AND snap_id = s_id - 1;

    DELETE FROM last_statio_user_indexes WHERE node_id=snode_id AND snap_id = s_id - 1;
    
    RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION snapshot_show(IN node name,IN days integer = NULL) RETURNS TABLE(snapshot integer, date_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
    SELECT snap_id, snap_time
    FROM snapshots s JOIN nodes n USING (node_id)
    WHERE (days IS NULL OR snap_time > now() - (days || ' days')::interval)
        AND node_name = node
    ORDER BY snap_id;
$$ LANGUAGE SQL;
COMMENT ON FUNCTION snapshot_show(IN node name,IN days integer) IS 'Display available node snapshots';

CREATE OR REPLACE FUNCTION snapshot_show(IN days integer = NULL) RETURNS TABLE(snapshot integer, date_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
    SELECT snap_id, snap_time
    FROM snapshots s JOIN nodes n USING (node_id)
    WHERE (days IS NULL OR snap_time > now() - (days || ' days')::interval)
        AND node_name = 'local'
    ORDER BY snap_id;
$$ LANGUAGE SQL;
COMMENT ON FUNCTION snapshot_show(IN days integer) IS 'Display available snapshots for local node';

/* ========= Baseline management functions ========= */

CREATE OR REPLACE FUNCTION baseline_new(IN node name, IN name varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    baseline_id integer;
    snode_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found';
    END IF;
    
    INSERT INTO baselines(node_id,bl_name,keep_until)
    VALUES (snode_id,name,now() + (days || ' days')::interval)
    RETURNING bl_id INTO baseline_id;

    INSERT INTO bl_snaps (node_id,snap_id,bl_id)
    SELECT node_id,snap_id,baseline_id
    FROM snapshots s JOIN nodes n USING (node_id)
    WHERE node_id=snode_id AND snap_id BETWEEN start_id AND end_id;

    RETURN baseline_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_drop(IN node name, IN name varchar(25)) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    del_rows integer;
BEGIN
    DELETE FROM baselines WHERE bl_name = name AND node_id IN (SELECT node_id FROM nodes WHERE node_name = node);
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_keep(IN node name, IN name varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE baselines SET keep_until = now() + (days || ' days')::interval WHERE (name IS NULL OR bl_name = name) AND node_id IN (SELECT node_id FROM nodes WHERE node_name = node);
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_show(IN node name) RETURNS TABLE(baseline varchar(25), min_snap integer, max_snap integer, keep_until_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
    SELECT bl_name as baseline,min_snap_id,max_snap_id, keep_until 
    FROM baselines b JOIN 
        (SELECT node_id,bl_id,min(snap_id) min_snap_id,max(snap_id) max_snap_id FROM bl_snaps GROUP BY node_id,bl_id) b_agg
    USING (node_id,bl_id)
    WHERE node_id IN (SELECT node_id FROM nodes WHERE node_name = node)
    ORDER BY min_snap_id;
$$ LANGUAGE SQL;

/* ========= Node functions ========= */

CREATE OR REPLACE FUNCTION node_new(IN node name, IN node_connstr text, IN node_enabled boolean = TRUE) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    snode_id     integer;
BEGIN
    
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NOT NULL THEN
        RAISE 'Node already exists.';
    END IF;
    
    INSERT INTO nodes(node_name,connstr,enabled)
    VALUES (node,node_connstr,node_enabled)
    RETURNING node_id INTO snode_id;

    RETURN snode_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_new(IN node name, IN node_connstr text, IN node_enabled boolean) IS 'Create new node';

CREATE OR REPLACE FUNCTION node_drop(IN node name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    del_rows integer;
BEGIN
    DELETE FROM nodes WHERE node_name = node;
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_drop(IN node name) IS 'Drop a node';

CREATE OR REPLACE FUNCTION node_rename(IN node name, IN node_new_name name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET node_name = node_new_name WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_rename(IN node name, IN node_new_name name) IS 'Rename existing node';

CREATE OR REPLACE FUNCTION node_connstr(IN node name, IN node_connstr text) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET connstr = node_connstr WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_connstr(IN node name, IN node_connstr text) IS 'Update node connection string';

CREATE OR REPLACE FUNCTION node_enable(IN node name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET enabled = TRUE WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_enable(IN node name) IS 'Enable existing node (will be included in snapshot() call)';

CREATE OR REPLACE FUNCTION node_disable(IN node name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET enabled = FALSE WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_disable(IN node name) IS 'Disable existing node (will be excluded from snapshot() call)';

CREATE OR REPLACE FUNCTION node_show() RETURNS TABLE(node_name name, connstr text, enabled boolean) SET search_path=@extschema@,public AS $$
    SELECT node_name,connstr,enabled FROM nodes;
$$ LANGUAGE SQL;

COMMENT ON FUNCTION node_show() IS 'Displays all nodes';


/* ========= Reporting functions ========= */

/* ===== Cluster report functions ===== */
CREATE OR REPLACE FUNCTION dbstats_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Commits</th><th>Rollbacks</th><th>BlkHit%(read/hit)</th><th>Tup Ret/Fet</th><th>Tup Ins</th><th>Tup Del</th><th>Temp Size(Files)</th><th>Growth</th><th>Deadlocks</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s%%(%s/%s)</td><td>%s/%s</td><td>%s</td><td>%s</td><td>%s(%s)</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT 
        datname as dbname,
        sum(xact_commit) as xact_commit,
        sum(xact_rollback) as xact_rollback,
        sum(blks_read) as blks_read,
        sum(blks_hit) as blks_hit,
        sum(tup_returned) as tup_returned,
        sum(tup_fetched) as tup_fetched,
        sum(tup_inserted) as tup_inserted,
        sum(tup_updated) as tup_updated,
        sum(tup_deleted) as tup_deleted,
        sum(temp_files) as temp_files,
        pg_size_pretty(sum(temp_bytes)) as temp_bytes,
        pg_size_pretty(sum(datsize_delta)) as datsize_delta,
        sum(deadlocks) as deadlocks, 
        sum(blks_hit)*100/GREATEST(sum(blks_hit)+sum(blks_read),1) as blks_hit_pct
    FROM snap_stat_database
    WHERE node_id = snode_id AND datname not like 'template_' and snap_id between s_id + 1 and e_id
    GROUP BY datid,datname
    HAVING max(stats_reset)=min(stats_reset);

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.xact_commit,
            r_result.xact_rollback,
            round(CAST(r_result.blks_hit_pct AS numeric),2),
            r_result.blks_read,
            r_result.blks_hit,
            r_result.tup_returned,
            r_result.tup_fetched,
            r_result.tup_inserted,
            r_result.tup_deleted,
            r_result.temp_bytes,
            r_result.temp_files,
            r_result.datsize_delta,
            r_result.deadlocks
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION statements_stats_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Calls</th><th>Total time(s)</th><th>Shared gets</th><th>Local gets</th><th>Shared dirtied</th><th>Local dirtied</th><th>Work_r (blk)</th><th>Work_w (blk)</th><th>Local_r (blk)</th><th>Local_w (blk)</th><th>Statements</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT 
        db_s.datname AS dbname,
        sum(st.calls) AS calls,
        sum(st.total_time)/1000 AS total_time,
        sum(st.shared_blks_hit + st.shared_blks_read) AS shared_gets,
        sum(st.local_blks_hit + st.local_blks_read) AS local_gets,
        sum(st.shared_blks_dirtied) AS shared_blks_dirtied,
        sum(st.local_blks_dirtied) AS local_blks_dirtied,
        sum(st.temp_blks_read) AS temp_blks_read,
        sum(st.temp_blks_written) AS temp_blks_written,
        sum(st.local_blks_read) AS local_blks_read,
        sum(st.local_blks_written) AS local_blks_written,
        sum(st.statements) AS statements
    FROM snap_statements_total st 
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid AND db_s.node_id=st.node_id AND db_s.snap_id=s_id)
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid AND db_e.node_id=st.node_id AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY ROLLUP(db_s.datname)
    ORDER BY db_s.datname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            r_result.shared_gets,
            r_result.local_gets,
            r_result.shared_blks_dirtied,
            r_result.local_blks_dirtied,
            r_result.temp_blks_read,
            r_result.temp_blks_written,
            r_result.local_blks_read,
            r_result.local_blks_written,
            r_result.statements
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_stats_htbl(IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Metric</th><th>Value</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR (s_id integer, e_id integer) FOR
    SELECT
        sum(checkpoints_timed) as checkpoints_timed,
        sum(checkpoints_req) as checkpoints_req,
        sum(checkpoint_write_time) as checkpoint_write_time,
        sum(checkpoint_sync_time) as checkpoint_sync_time,
        sum(buffers_checkpoint) as buffers_checkpoint,
        sum(buffers_clean) as buffers_clean,
        sum(buffers_backend) as buffers_backend,
        sum(buffers_backend_fsync) as buffers_backend_fsync,
        sum(maxwritten_clean) as maxwritten_clean,
        sum(buffers_alloc) as buffers_alloc,
        pg_size_pretty(sum(wal_size)) as wal_size
    FROM snap_stat_cluster
    WHERE node_id = snode_ID AND snap_id between s_id + 1 and e_id
    HAVING max(stats_reset)=min(stats_reset);

    r_result RECORD;
BEGIN
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats(start_id, end_id) LOOP
        report := report||format(row_tpl,'Scheduled checkpoints',r_result.checkpoints_timed);
        report := report||format(row_tpl,'Requested checkpoints',r_result.checkpoints_req);
        report := report||format(row_tpl,'Checkpoint write time (s)',round(cast(r_result.checkpoint_write_time/1000 as numeric),2));
        report := report||format(row_tpl,'Checkpoint sync time (s)',round(cast(r_result.checkpoint_sync_time/1000 as numeric),2));
        report := report||format(row_tpl,'Checkpoints pages written',r_result.buffers_checkpoint);
        report := report||format(row_tpl,'Background pages written',r_result.buffers_clean);
        report := report||format(row_tpl,'Backend pages written',r_result.buffers_backend);
        report := report||format(row_tpl,'Backend fsync count',r_result.buffers_backend_fsync);
        report := report||format(row_tpl,'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean);
        report := report||format(row_tpl,'Number of buffers allocated',r_result.buffers_alloc);
        report := report||format(row_tpl,'WAL generated',r_result.wal_size);
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

/* ===== Objects report functions ===== */
CREATE OR REPLACE FUNCTION top_scan_tables_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>SeqScan</th><th>SeqFet</th><th>IxScan</th><th>IxFet</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname AS dbname,
        schemaname,
        relname,
        sum(seq_scan) AS seq_scan,
        sum(seq_tup_read) AS seq_tup_read,
        sum(idx_scan) AS idx_scan,
        sum(idx_tup_fetch) AS idx_tup_fetch,
        sum(n_tup_ins) AS n_tup_ins,
        sum(n_tup_upd)-sum(n_tup_hot_upd) AS n_tup_upd,
        sum(n_tup_del) AS n_tup_del,
        sum(n_tup_hot_upd) AS n_tup_hot_upd
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,schemaname,relname
    HAVING sum(seq_scan) > 0
    ORDER BY sum(seq_scan) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.seq_scan,
            r_result.seq_tup_read,
            r_result.idx_scan,
            r_result.idx_tup_fetch,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_dml_tables_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th><th>SeqScan</th><th>SeqFet</th><th>IxScan</th><th>IxFet</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname AS dbname,
        schemaname,
        relname,
        sum(seq_scan) AS seq_scan,
        sum(seq_tup_read) AS seq_tup_read,
        sum(idx_scan) AS idx_scan,
        sum(idx_tup_fetch) AS idx_tup_fetch,
        sum(n_tup_ins) AS n_tup_ins,
        sum(n_tup_upd)-sum(n_tup_hot_upd) AS n_tup_upd,
        sum(n_tup_del) AS n_tup_del,
        sum(n_tup_hot_upd) AS n_tup_hot_upd
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,schemaname,relname
    HAVING sum(n_tup_ins)+sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) > 0
    ORDER BY sum(n_tup_ins)+sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd,
            r_result.seq_scan,
            r_result.seq_tup_read,
            r_result.idx_scan,
            r_result.idx_tup_fetch
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_upd_vac_tables_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Upd</th><th>Upd(HOT)</th><th>Del</th><th>Vacuum</th><th>AutoVacuum</th><th>Analyze</th><th>AutoAnalyze</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname AS dbname,
        schemaname,
        relname,
        sum(n_tup_upd)-sum(n_tup_hot_upd) AS n_tup_upd,
        sum(n_tup_del) AS n_tup_del,
        sum(n_tup_hot_upd) AS n_tup_hot_upd,
        sum(vacuum_count) AS vacuum_count,
        sum(autovacuum_count) AS autovacuum_count,
        sum(analyze_count) AS analyze_count,
        sum(autoanalyze_count) AS autoanalyze_count
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,schemaname,relname
    HAVING sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) > 0
    ORDER BY sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_upd,
            r_result.n_tup_hot_upd,
            r_result.n_tup_del,
            r_result.vacuum_count,
            r_result.autovacuum_count,
            r_result.analyze_count,
            r_result.autoanalyze_count
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_growth_tables_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Size</th><th>Growth</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname AS dbname,
        st.schemaname,
        st.relname,
        sum(st.seq_scan) AS seq_scan,
        sum(st.seq_tup_read) AS seq_tup_read,
        sum(st.idx_scan) AS idx_scan,
        sum(st.idx_tup_fetch) AS idx_tup_fetch,
        sum(st.n_tup_ins) AS n_tup_ins,
        sum(st.n_tup_upd)-sum(st.n_tup_hot_upd) AS n_tup_upd,
        sum(st.n_tup_del) AS n_tup_del,
        sum(st.n_tup_hot_upd) AS n_tup_hot_upd,
        pg_size_pretty(sum(st.relsize_diff)) AS growth,
        pg_size_pretty(max(st_last.relsize)) AS relsize
    FROM v_snap_stat_user_tables st
        JOIN v_snap_stat_user_tables st_last USING (node_id,dbid,relid)
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id=snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
        AND st_last.snap_id=db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,st.schemaname,st.relname
    HAVING sum(st.relsize_diff) > 0
    ORDER BY sum(st.relsize_diff) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.relsize,
            r_result.growth,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_growth_indexes_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>Size</th><th>Growth</th><th>Scans</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname as dbname,
        st.schemaname,
        st.relname,
        st.indexrelname,
        sum(st.idx_scan) as idx_scan,
        pg_size_pretty(sum(st.relsize_diff)) as growth,
        pg_size_pretty(max(st_last.relsize)) as relsize
    FROM v_snap_stat_user_indexes st
        JOIN v_snap_stat_user_indexes st_last using (node_id,dbid,relid,indexrelid)
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id=snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
        AND st_last.snap_id=db_e.snap_id
    GROUP BY db_s.datid,relid,indexrelid,db_s.datname,st.schemaname,st.relname,st.indexrelname
    HAVING sum(st.relsize_diff) > 0
    ORDER BY sum(st.relsize_diff) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize,
            r_result.growth,
            r_result.idx_scan
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_dead_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Top dead tuples table
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Live</th><th>Dead</th><th>%Dead</th><th>Last AV</th><th>Size</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (e_id integer, cnt integer) FOR
    SELECT 
        db_e.datname AS dbname,
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        n_dead_tup*100/(n_live_tup + n_dead_tup) AS dead_pct,
        last_autovacuum,
        pg_size_pretty(relsize) AS relsize
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id)
    WHERE st.node_id=snode_id AND db_e.datname not like 'template_' AND st.snap_id = db_e.snap_id
        -- Min 5 MB in size
        AND st.relsize > 5 * 1024^2
        AND st.n_dead_tup > 0
    ORDER BY n_dead_tup*100/(n_live_tup + n_dead_tup) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_live_tup,
            r_result.n_dead_tup,
            r_result.dead_pct,
            r_result.last_autovacuum,
            r_result.relsize
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_mods_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Top modified tuples table
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Live</th><th>Dead</th><th>Mods</th><th>%Mod</th><th>Last AA</th><th>Size</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (e_id integer, cnt integer) FOR
    SELECT 
        db_e.datname AS dbname,
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze AS mods,
        n_mod_since_analyze*100/(n_live_tup + n_dead_tup) AS mods_pct,
        last_autoanalyze,
        pg_size_pretty(relsize) AS relsize
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id)
    WHERE st.node_id = snode_id AND db_e.datname not like 'template_' AND st.snap_id = db_e.snap_id
        -- Min 5 MB in size
        AND relsize > 5 * 1024^2
        AND n_mod_since_analyze > 0
    ORDER BY n_mod_since_analyze*100/(n_live_tup + n_dead_tup) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_live_tup,
            r_result.n_dead_tup,
            r_result.mods,
            r_result.mods_pct,
            r_result.last_autoanalyze,
            r_result.relsize
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_unused_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>ixSize</th><th>Table DML ops (w/o HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_ix_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_e.datname AS dbname,
        schemaname,
        relname,
        indexrelname,
        pg_size_pretty(max(ix_last.relsize)) AS relsize,
        sum(tab.n_tup_ins+tab.n_tup_upd+tab.n_tup_del) AS dml_ops
    FROM v_snap_stat_user_indexes ix
        JOIN v_snap_stat_user_tables tab USING (node_id,snap_id,dbid,relid,schemaname,relname)
        JOIN v_snap_stat_user_indexes ix_last USING (node_id,dbid,relid,indexrelid,schemaname,relname,indexrelname)
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=ix.node_id AND db_s.datid=ix.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=ix.node_id AND db_e.datid=ix.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
    WHERE ix.node_id = snode_id AND ix_last.snap_id = db_e.snap_id 
        AND ix.snap_id BETWEEN db_s.snap_id + 1 and db_e.snap_id
        AND NOT ix.indisunique
        AND ix.idx_scan = 0
    GROUP BY dbid,relid,indexrelid,dbname,schemaname,relname,indexrelname
    ORDER BY sum(tab.n_tup_ins+tab.n_tup_upd+tab.n_tup_del) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_ix_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize,
            r_result.dml_ops
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_io_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Heap</th><th>Ix</th><th>TOAST</th><th>TOAST-Ix</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname AS dbname,
        st.schemaname,
        st.relname,
        sum(st.heap_blks_read) AS heap_blks_read,
        sum(st.idx_blks_read) AS idx_blks_read,
        sum(st.toast_blks_read) AS toast_blks_read,
        sum(st.tidx_blks_read) AS tidx_blks_read
    FROM v_snap_statio_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,db_s.datname,st.schemaname,st.relname
    ORDER BY sum(st.heap_blks_read + st.idx_blks_read + st.toast_blks_read + st.tidx_blks_read) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_read,
            r_result.idx_blks_read,
            r_result.toast_blks_read,
            r_result.tidx_blks_read
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_top_io_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>Blk Reads</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname AS dbname,
        st.schemaname,
        st.relname,
        st.indexrelname,
        sum(st.idx_blks_read) AS idx_blks_read
    FROM v_snap_statio_user_indexes st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname NOT LIKE 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,relid,indexrelid,db_s.datname,st.schemaname,st.relname,st.indexrelname
    ORDER BY sum(st.idx_blks_read) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
    report := report||format(
        row_tpl,
        r_result.dbname,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_blks_read
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

/* ===== Functions report ===== */

CREATE OR REPLACE FUNCTION func_top_time_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname AS dbname,
        st.schemaname,
        st.funcname,
        sum(st.calls) AS calls,
        sum(st.total_time) AS total_time,
        sum(st.self_time) AS self_time,
        sum(st.total_time)/sum(st.calls) AS m_time,
        sum(st.self_time)/sum(st.calls) AS m_stime
    FROM v_snap_stat_user_functions st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,funcid,db_s.datname,st.schemaname,st.funcname
    ORDER BY sum(st.total_time) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_top_calls_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        db_s.datname AS dbname,
        st.schemaname,
        st.funcname,
        sum(st.calls) AS calls,
        sum(st.total_time) AS total_time,
        sum(st.self_time) AS self_time,
        sum(st.total_time)/sum(st.calls) AS m_time,
        sum(st.self_time)/sum(st.calls) AS m_stime
    FROM v_snap_stat_user_functions st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=e_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datid,funcid,db_s.datname,st.schemaname,st.funcname
    ORDER BY sum(st.calls) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

   IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
   ELSE
        RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

/* ===== Statements report ===== */
CREATE OR REPLACE FUNCTION check_stmt_cnt(IN snode_id integer, IN start_id integer = 0, IN end_id integer = 0) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tab_tpl CONSTANT text := '<table><tr><th>Snapshot ID</th><th>Snapshot Time</th><th>Stmts Captured</th><th>pg_stat_statements.max</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    report text := '';

    c_stmt_all_stats CURSOR FOR
    SELECT snap_id,snap_time,stmt_cnt,prm.setting AS max_cnt 
    FROM snap_params prm 
        JOIN (
            SELECT snap_id,sum(statements) stmt_cnt
            FROM snap_statements_total
            WHERE node_id = snode_id
            GROUP BY snap_id
        ) snap_stmt_cnt USING(snap_id)
        JOIN snapshots USING (node_id,snap_id)
    WHERE node_id = snode_id AND prm.p_name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer);

    c_stmt_stats CURSOR (s_id integer, e_id integer) FOR
    SELECT snap_id,snap_time,stmt_cnt,prm.setting AS max_cnt
    FROM snap_params prm 
        JOIN (
            SELECT snap_id,sum(statements) stmt_cnt
            FROM snap_statements_total
            WHERE node_id = snode_id AND snap_id BETWEEN s_id + 1 AND e_id
            GROUP BY snap_id
        ) snap_stmt_cnt USING(snap_id)
        JOIN snapshots USING (node_id,snap_id)
    WHERE node_id = snode_id AND prm.p_name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer);

    r_result RECORD;
BEGIN
    IF start_id = 0 THEN
        FOR r_result IN c_stmt_all_stats LOOP
            report := report||format(
                row_tpl,
                r_result.snap_id,
                r_result.snap_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    ELSE
        FOR r_result IN c_stmt_stats(start_id,end_id) LOOP
            report := report||format(
                row_tpl,
                r_result.snap_id,
                r_result.snap_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    END IF; 

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION check_stmt_all_setting(IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snap_cnt    integer;    
BEGIN
    SELECT count(1) INTO snap_cnt 
    FROM snap_params 
    WHERE node_id = snode_id AND p_name = 'pg_stat_statements.track' 
        AND setting = 'all' AND snap_id BETWEEN start_id + 1 AND end_id;
        
    IF snap_cnt > 0 THEN
        RETURN '<p><b>Warning!</b> Report includes '||snap_cnt||' snapshot(s) with setting <i>pg_stat_statements.track = all</i>.'||
        'Value of %Total columns may be incorrect.</p>';
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION top_elapsed_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Elapsed time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) queries ordered by epapsed time 
    c_elapsed_time CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT GREATEST(sum(total_time),1) AS total_time
        FROM snap_statements_total
        WHERE node_id = snode_id AND snap_id BETWEEN s_id + 1 AND e_id)
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.total_time*100/tot.total_time) as total_pct,
        min(st.min_time) as min_time,max(st.max_time) as max_time,
        sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
        sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) as shared_blks_hit,
        sum(st.shared_blks_read) as shared_blks_read,
        sum(st.shared_blks_dirtied) as shared_blks_dirtied,
        sum(st.shared_blks_written) as shared_blks_written,
        sum(st.local_blks_hit) as local_blks_hit,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_dirtied) as local_blks_dirtied,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.blk_read_time) as blk_read_time,
        sum(st.blk_write_time) as blk_write_time
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    ORDER BY total_time DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.total_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_time AS numeric),3),
            round(CAST(r_result.min_time AS numeric),3),
            round(CAST(r_result.max_time AS numeric),3),
            round(CAST(r_result.stddev_time AS numeric),3),
            r_result.calls
        );
        PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_exec_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Executions sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Executions</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Total(s)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by executions 
    c_calls CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT GREATEST(sum(calls),1) AS calls
        FROM snap_statements_total
        WHERE node_id = snode_id AND snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.calls/tot.calls)*100 as total_pct,
        sum(st.total_time)/1000 as total_time,
        min(st.min_time) as min_time,
        max(st.max_time) as max_time,
        sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
        sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
        sum(st.rows) as rows
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    ORDER BY calls DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.calls,
            round(CAST(r_result.total_pct AS numeric),2),
            r_result.rows,
            round(CAST(r_result.mean_time AS numeric),3),
            round(CAST(r_result.min_time AS numeric),3),
            round(CAST(r_result.max_time AS numeric),3),
            round(CAST(r_result.stddev_time AS numeric),3),
            round(CAST(r_result.total_time AS numeric),1)
        );
        PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_iowait_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- IOWait time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>IO wait(s)</th><th>%Total</th><th>Reads</th><th>Writes</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by I/O Wait time 
    c_iowait_time CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT 
            CASE WHEN sum(blk_read_time) = 0 THEN 1 ELSE sum(blk_read_time) END AS blk_read_time,
            CASE WHEN sum(blk_write_time) = 0 THEN 1 ELSE sum(blk_write_time) END AS blk_write_time
        FROM snap_statements_total
        WHERE node_id = snode_id AND snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 AS queryid,
        st.query,db_s.datname AS dbname,
        sum(st.calls) AS calls,
        sum(st.total_time)/1000 AS total_time,
        sum(st.rows) AS rows,
        sum(st.shared_blks_hit) AS shared_blks_hit,
        sum(st.shared_blks_read) AS shared_blks_read,
        sum(st.shared_blks_dirtied) AS shared_blks_dirtied,
        sum(st.shared_blks_written) AS shared_blks_written,
        sum(st.local_blks_hit) AS local_blks_hit,
        sum(st.local_blks_read) AS local_blks_read,
        sum(st.local_blks_dirtied) AS local_blks_dirtied,
        sum(st.local_blks_written) AS local_blks_written,
        sum(st.temp_blks_read) AS temp_blks_read,
        sum(st.temp_blks_written) AS temp_blks_written,
        sum(st.blk_read_time) AS blk_read_time,
        sum(st.blk_write_time) AS blk_write_time,
        (sum(st.blk_read_time + st.blk_write_time))/1000 AS io_time,
        (sum(st.blk_read_time + st.blk_write_time)*100/min(tot.blk_read_time+tot.blk_write_time)) AS total_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.blk_read_time) + sum(st.blk_write_time) > 0
    ORDER BY io_time DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.io_time AS numeric),3),
            round(CAST(r_result.total_pct AS numeric),2),
            round(CAST(r_result.shared_blks_read AS numeric)),
            round(CAST(r_result.shared_blks_written AS numeric)),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_gets_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Gets sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by gets
    c_gets CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT GREATEST(sum(shared_blks_hit),1) AS shared_blks_hit,
            GREATEST(sum(shared_blks_read),1) AS shared_blks_read
        FROM snap_statements_total
        WHERE node_id = snode_id AND snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) + sum(st.shared_blks_read) as gets,
        (sum(st.shared_blks_hit + st.shared_blks_read)*100/min(tot.shared_blks_read + tot.shared_blks_hit)) as total_pct,
        sum(st.shared_blks_hit) * 100 / CASE WHEN (sum(st.shared_blks_hit)+sum(st.shared_blks_read)) = 0 THEN 1
            ELSE (sum(st.shared_blks_hit)+sum(st.shared_blks_read)) END as hit_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.shared_blks_hit) + sum(st.shared_blks_read) > 0
    ORDER BY gets DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by gets
    FOR r_result IN c_gets(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.total_pct AS numeric),2),
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.calls
        );
      PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_temp_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Temp usage sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>Hits(%)</th><th>Work_w(blk)</th><th>%Total</th><th>Work_r(blk)</th><th>%Total</th><th>Local_w(blk)</th><th>%Total</th><th>Local_r(blk)</th><th>%Total</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by temp usage 
    c_temp CURSOR (s_id integer, e_id integer, cnt integer) FOR 
    WITH tot AS (
        SELECT
            GREATEST(sum(temp_blks_read),1) AS temp_blks_read,
            GREATEST(sum(temp_blks_written),1) AS temp_blks_written,
            GREATEST(sum(local_blks_read),1) AS local_blks_read,
            GREATEST(sum(local_blks_written),1) AS local_blks_written
        FROM snap_statements_total
        WHERE node_id = snode_id AND snap_id BETWEEN s_id + 1 AND e_id
        )
    SELECT st.queryid_md5 as queryid,
        st.query,db_s.datname as dbname,
        sum(st.calls) as calls,
        sum(st.total_time)/1000 as total_time,
        sum(st.rows) as rows,
        sum(st.shared_blks_hit) + sum(st.shared_blks_read) as gets,
        sum(st.shared_blks_hit) * 100 / GREATEST(sum(st.shared_blks_hit)+sum(st.shared_blks_read),1) as hit_pct,
        sum(st.temp_blks_read) as temp_blks_read,
        sum(st.temp_blks_written) as temp_blks_written,
        sum(st.local_blks_read) as local_blks_read,
        sum(st.local_blks_written) as local_blks_written,
        sum(st.temp_blks_read*100/tot.temp_blks_read) as temp_read_total_pct,
        sum(st.temp_blks_written*100/tot.temp_blks_written) as temp_write_total_pct,
        sum(st.local_blks_read*100/tot.local_blks_read) as local_read_total_pct,
        sum(st.local_blks_written*100/tot.local_blks_written) as local_write_total_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid and db_s.snap_id=s_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.queryid_md5,st.query,db_s.datname
    HAVING sum(st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written) > 0
    ORDER BY sum(st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp(start_id, end_id,topn) LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.hit_pct AS numeric),2),
            r_result.temp_blks_written,
            round(CAST(r_result.temp_write_total_pct AS numeric),2),
            r_result.temp_blks_read,
            round(CAST(r_result.temp_read_total_pct AS numeric),2),
            r_result.local_blks_written,
            round(CAST(r_result.local_write_total_pct AS numeric),2),
            r_result.local_blks_read,
            round(CAST(r_result.local_read_total_pct AS numeric),2),
            r_result.calls
        );
        PERFORM collect_queries(r_result.queryid,r_result.query);
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION collect_queries(IN query_id char(10), IN query_text text) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    INSERT INTO queries_list
    VALUES (query_id,regexp_replace(query_text,'\s+',' ','g'))
    ON CONFLICT DO NOTHING;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION report_queries() RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    c_queries CURSOR FOR SELECT queryid, querytext FROM queries_list;
    qr_result RECORD;
    report text := '';
    query_text text := '';
    tab_tpl CONSTANT text := '<table><tr><th>QueryID</th><th>Query Text</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a NAME=%s>%s</a></td><td>%s</td></tr>';
BEGIN
    FOR qr_result IN c_queries LOOP
        query_text := replace(qr_result.querytext,'<','&lt;');
        query_text := replace(query_text,'>','&gt;');
        report := report||format(
            row_tpl,
            qr_result.queryid,
            qr_result.queryid,
            query_text
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nodata_wrapper(IN section_text text) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    IF section_text IS NULL OR section_text = '' THEN
        RETURN '<p>No data in this section</p>';
    ELSE
        RETURN section_text;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION report(IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    topn        integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile report {snaps}</title></head><body><H1>Postgres profile report {snaps}</H1><p>Report interval: {report_start} - {report_end}</p>{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} table tr:nth-child(even) {background-color: #eee;} table tr:nth-child(odd) {background-color: #fff;} table tr:hover{background-color:#d9ffcc} table th {color: black; background-color: #ffcc99;}';
    --Cursor and variable for checking existance of snapshots
    c_snap CURSOR (snapshot_id integer) FOR SELECT * FROM snapshots WHERE node_id = snode_id AND snap_id = snapshot_id;
    snap_rec snapshots%rowtype;
BEGIN
    -- Creating temporary table for reported queries
    CREATE TEMPORARY TABLE IF NOT EXISTS queries_list (queryid char(10) PRIMARY KEY, querytext text) ON COMMIT DELETE ROWS;

    -- CSS
    report := replace(report_tpl,'{css}',report_css);

    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Checking snapshot existance, header generation
    OPEN c_snap(start_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'Start snapshot % does not exists', start_id;
        END IF;
        report := replace(report,'{report_start}',cast(snap_rec.snap_time as text));
        tmp_text := '(StartID: ' || snap_rec.snap_id ||', ';
    CLOSE c_snap;

    OPEN c_snap(end_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'End snapshot % does not exists', end_id;
        END IF;
        report := replace(report,'{report_end}',cast(snap_rec.snap_time as text));
        tmp_text := tmp_text || 'EndID: ' || snap_rec.snap_id ||')';
    CLOSE c_snap;
    report := replace(report,'{snaps}',tmp_text);
    tmp_text := '';

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(snode_id, start_id, end_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>This interval contains snapshot(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;
    
    -- pg_stat_statements.tarck warning
    tmp_text := tmp_text || check_stmt_all_setting(snode_id, start_id, end_id);

    -- Table of Contents
    tmp_text := tmp_text ||'<H2>Report sections</H2><ul>';
    tmp_text := tmp_text || '<li><a HREF=#cl_stat>Cluster statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#db_stat>Databases stats</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#st_stat>Statements stats by database</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#clu_stat>Cluster stats</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '<li><a HREF=#sql_stat>SQL Query stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#top_ela>Top SQL by elapsed time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_calls>Top SQL by executions</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_iowait>Top SQL by I/O wait time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_gets>Top SQL by gets</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_temp>Top SQL by temp usage</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#sql_list>Complete List of SQL Text</a></li>';
    tmp_text := tmp_text || '</ul>';

    tmp_text := tmp_text || '<li><a HREF=#schema_stat>Schema objects stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#scanned_tbl>Most scanned tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#dml_tbl>Top DML tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#vac_tbl>Top Delete/Update tables with vacuum run count</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_tbl>Top growing tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_idx>Top growing indexes</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#ix_unused>Unused indexes</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '<li><a HREF=#io_stat>I/O Schema objects stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#tbl_io_stat>Top tables by I/O</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#ix_io_stat>Top indexes by I/O</a></li>';
    tmp_text := tmp_text || '</ul>';

    tmp_text := tmp_text || '<li><a HREF=#func_stat>User function stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#funs_time_stat>Top functions by total time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#funs_calls_stat>Top functions by executions</a></li>';
    tmp_text := tmp_text || '</ul>';


    tmp_text := tmp_text || '<li><a HREF=#vacuum_stats>Vacuum related stats</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#dead_tbl>Tables ordered by dead tuples ratio</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#mod_tbl>Tables ordered by modified tuples ratio</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '</ul>';


    --Reporting cluster stats
    tmp_text := tmp_text || '<H2><a NAME=cl_stat>Cluster statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=db_stat>Databases stats</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(dbstats_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=st_stat>Statements stats by database</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(statements_stats_htbl(snode_id, start_id, end_id, topn));
    
    tmp_text := tmp_text || '<H3><a NAME=clu_stat>Cluster stats</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_htbl(snode_id, start_id, end_id));

    --Reporting on top queries by elapsed time
    tmp_text := tmp_text||'<H2><a NAME=sql_stat>SQL Query stats</a></H2>';
    tmp_text := tmp_text||'<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_elapsed_htbl(snode_id, start_id, end_id, topn));

    -- Reporting on top queries by executions
    tmp_text := tmp_text||'<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_exec_htbl(snode_id, start_id, end_id, topn));

    -- Reporting on top queries by I/O wait time
    tmp_text := tmp_text||'<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_iowait_htbl(snode_id, start_id, end_id, topn));

    -- Reporting on top queries by gets
    tmp_text := tmp_text||'<H3><a NAME=top_gets>Top SQL by gets</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_gets_htbl(snode_id, start_id, end_id, topn));

    -- Reporting on top queries by temp usage
    tmp_text := tmp_text||'<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_temp_htbl(snode_id, start_id, end_id, topn));

    -- Listing queries
    tmp_text := tmp_text||'<H3><a NAME=sql_list>Complete List of SQL Text</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(report_queries());

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text||'<H2><a NAME=schema_stat>Schema objects stats</a></H2>';
    tmp_text := tmp_text||'<H3><a NAME=scanned_tbl>Most seq. scanned tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_htbl(snode_id, start_id, end_id, topn));
    
    tmp_text := tmp_text||'<H3><a NAME=vac_tbl>Top Delete/Update tables with vacuum run count</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_upd_vac_tables_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=growth_tbl>Top growing tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_tables_htbl(snode_id, start_id, end_id, topn));
    tmp_text := tmp_text||'<H3><a NAME=growth_idx>Top growing indexes</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_indexes_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=ix_unused>Unused growing indexes</a></H3>';
    tmp_text := tmp_text||'<p>This table contains not-scanned indexes (during report period), ordered by number of DML operations on underlying tables. Constraint indexes are excluded.</p>';
    tmp_text := tmp_text || nodata_wrapper(ix_unused_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=io_stat>I/O Schema objects stats</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=tbl_io_stat>Top tables by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=ix_io_stat>Top indexes by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_io_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=func_stat>User function stats</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=funs_time_stat>Top functions by total time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_time_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=funs_calls_stat>Top functions by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_calls_htbl(snode_id, start_id, end_id, topn));

    -- Reporting vacuum related stats
    tmp_text := tmp_text||'<H2><a NAME=vacuum_stats>Vacuum related stats</a></H2>';
    tmp_text := tmp_text||'<p>Data in this section is not incremental. This data is valid for ending snapshot only.</p>';
    tmp_text := tmp_text||'<H3><a NAME=dead_tbl>Tables ordered by dead tuples ratio</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_dead_htbl(snode_id, start_id, end_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=mod_tbl>Tables ordered by modified tuples ratio</a></H3>';
    tmp_text := tmp_text||'<p>Table shows modified tuples stats since last analyze.</p>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_mods_htbl(snode_id, start_id, end_id, topn));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(snode_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>Snapshot repository contains snapshots with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    RETURN replace(report,'{report}',tmp_text);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION report(IN snode_id integer, IN start_id integer, IN end_id integer) IS 'Statistics report generation function. Takes node_id and IDs of start and end snapshot (inclusive)';

CREATE OR REPLACE FUNCTION report(IN node name, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snode_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found.';
    END IF;
    
    RETURN report(snode_id,start_id,end_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report(IN node name, IN start_id integer, IN end_id integer) IS 'Statistics report generation function. Takes node name and IDs of start and end snapshot (inclusive)';

CREATE OR REPLACE FUNCTION report(IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snode_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name='local';
    IF snode_id IS NULL THEN
        RAISE 'Node "local" not found.';
    END IF;
    
    RETURN report(snode_id,start_id,end_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report(IN start_id integer, IN end_id integer) IS 'Statistics report generation function for local node. Takes IDs of start and end snapshot (inclusive)';
