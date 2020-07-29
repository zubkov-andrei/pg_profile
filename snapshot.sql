
/* ========= Snapshot functions ========= */

CREATE OR REPLACE FUNCTION snapshot(IN snode_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    s_id            integer;
    topn            integer;
    ret             integer;
    lockid          bigint;
    pg_version      varchar(10);
    qres            record;
    node_connstr    text;
    settings_refresh    boolean = true;
BEGIN
    -- Get node connstr
    node_connstr := get_connstr(snode_id);

    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    IF dblink_get_connections() @> ARRAY['node_connection'] THEN
        PERFORM dblink_disconnect('node_connection');
    END IF;

    -- Creating a new snapshot record
    UPDATE nodes SET last_snap_id = last_snap_id + 1 WHERE node_id = snode_id
      RETURNING last_snap_id INTO s_id;
    INSERT INTO snapshots(snap_time,node_id,snap_id)
      VALUES (now(),snode_id,s_id);

    -- Only one running snapshot() function allowed per node!
    -- Explicitly lock node in nodes table
    BEGIN
        SELECT * INTO qres FROM nodes WHERE node_id = snode_id FOR UPDATE NOWAIT;
    EXCEPTION
        WHEN OTHERS THEN RAISE 'Can''t get lock on node. Is there another snapshot() function running on this node?';
    END;
    -- Getting retention setting
    BEGIN
        ret := COALESCE(current_setting('pg_profile.retention')::integer);
    EXCEPTION
        WHEN OTHERS THEN ret := 7;
    END;

    PERFORM dblink_connect('node_connection',node_connstr);
    -- Setting lock_timout prevents hanging of snapshot() call due to DDL in long transaction
    PERFORM dblink('node_connection','SET lock_timeout=3000');

    --Getting postgres version
    SELECT setting INTO STRICT pg_version FROM dblink('node_connection','SELECT setting FROM pg_catalog.pg_settings WHERE name = ''server_version_num''') AS t (setting text);

    -- Collecting postgres parameters
    -- We will refresh all parameters if version() was changed
    SELECT ss.setting != dblver.version INTO settings_refresh
    FROM v_snap_settings ss, dblink('node_connection','SELECT version() as version') AS dblver (version text)
    WHERE ss.node_id = snode_id AND ss.snap_id = s_id AND ss.name='version' AND ss.setting_scope = 2;
    settings_refresh := COALESCE(settings_refresh,true);

    INSERT INTO snap_settings
    SELECT
      s.node_id as node_id,
      s.snap_time as first_seen,
      cur.setting_scope,
      cur.name,
      cur.setting,
      cur.reset_val,
      cur.boot_val,
      cur.unit,
      cur.sourcefile,
      cur.sourceline,
      cur.pending_restart
    FROM
      snap_settings lst JOIN
      -- Getting last versions of settings
        (SELECT node_id, name, max(first_seen) as first_seen
        FROM snap_settings
        WHERE node_id = snode_id AND NOT settings_refresh
        GROUP BY node_id, name
        -- HAVING first_seen >= (select max(first_seen) from snap_settings where node_id = snode_id and name='cluster_version/edition')
        ) lst_times
      USING (node_id, name, first_seen)
      -- Getting current settings values
      RIGHT OUTER JOIN dblink('node_connection','SELECT 1 as setting_scope,name,setting,reset_val,boot_val,unit,sourcefile,sourceline,pending_restart '||
          'FROM pg_catalog.pg_settings '||
          'UNION ALL SELECT 2 as setting_scope,''version'',version(),version(),NULL,NULL,NULL,NULL,False '||
          'UNION ALL SELECT 2 as setting_scope,''pg_postmaster_start_time'',pg_postmaster_start_time()::text,pg_postmaster_start_time()::text,NULL,NULL,NULL,NULL,False '||
          'UNION ALL SELECT 2 as setting_scope,''pg_conf_load_time'',pg_conf_load_time()::text,pg_conf_load_time()::text,NULL,NULL,NULL,NULL,False '||
          'UNION ALL SELECT 2 as setting_scope,''system_identifier'',system_identifier::text,system_identifier::text,system_identifier::text,NULL,NULL,NULL,False FROM pg_control_system() '
          ) AS cur (
            setting_scope smallint,
            name text,
            setting text,
            reset_val text,
            boot_val text,
            unit text,
            sourcefile text,
            sourceline integer,
            pending_restart boolean
          )
        USING (name)
      JOIN snapshots s ON (s.node_id = snode_id AND s.snap_id = s_id)
    WHERE
      cur.reset_val IS NOT NULL AND (
        lst.name IS NULL
        OR cur.reset_val != lst.reset_val
        OR cur.pending_restart != lst.pending_restart
        OR lst.sourcefile != cur.sourcefile
        OR lst.sourceline != cur.sourceline
        OR lst.unit != cur.unit
      );

    -- Check system identifier change
    SELECT min(reset_val::bigint) != max(reset_val::bigint) AS sysid_changed INTO STRICT qres
    FROM snap_settings
    WHERE node_id = snode_id AND name = 'system_identifier';
    IF qres.sysid_changed THEN
      RAISE 'Node system_identifier has changed! Ensure node connection string is correct. Consider creating a new node for this cluster.';
    END IF;

    INSERT INTO snap_settings
    SELECT
      s.node_id,
      s.snap_time,
      1 as setting_scope,
      'pg_profile.topn',
      topn,
      topn,
      topn,
      null,
      null,
      null,
      false
    FROM snapshots s LEFT OUTER JOIN  v_snap_settings prm ON
      (s.node_id = prm.node_id AND s.snap_id = prm.snap_id AND prm.name = 'pg_profile.topn' AND prm.setting_scope = 1 AND NOT settings_refresh)
    WHERE s.node_id = snode_id AND s.snap_id = s_id AND (prm.setting IS NULL OR prm.setting::integer != topn);

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
        datsize,
        datsize_delta)
    SELECT
        snode_id,
        s_id,
        datid,
        datname,
        COALESCE(xact_commit,0) AS xact_commit,
        COALESCE(xact_rollback,0) AS xact_rollback,
        COALESCE(blks_read,0) AS blks_read,
        COALESCE(blks_hit,0) AS blks_hit,
        COALESCE(tup_returned,0) AS tup_returned,
        COALESCE(tup_fetched,0) AS tup_fetched,
        COALESCE(tup_inserted,0) AS tup_inserted,
        COALESCE(tup_updated,0) AS tup_updated,
        COALESCE(tup_deleted,0) AS tup_deleted,
        COALESCE(conflicts,0) AS conflicts,
        COALESCE(temp_files,0) AS temp_files,
        COALESCE(temp_bytes,0) AS temp_bytes,
        COALESCE(deadlocks,0) AS deadlocks,
        COALESCE(blk_read_time,0) AS blk_read_time,
        COALESCE(blk_write_time,0) AS blk_write_time,
        stats_reset,
        COALESCE(datsize,0) AS datsize,
        COALESCE(datsize_delta,0) AS datsize_delta
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
        pg_database_size(datid) as datsize,
        0 as datsize_delta
      FROM pg_catalog.pg_stat_database WHERE datname IS NOT NULL') AS rs (
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
        datsize bigint,
        datsize_delta bigint
        );

    -- Calc stat_database diff
    INSERT INTO snap_stat_database
    SELECT
        cur.node_id,
        cur.snap_id,
        cur.datid,
        cur.datname,
        cur.xact_commit - COALESCE(lst.xact_commit,0),
        cur.xact_rollback - COALESCE(lst.xact_rollback,0),
        cur.blks_read - COALESCE(lst.blks_read,0),
        cur.blks_hit - COALESCE(lst.blks_hit,0),
        cur.tup_returned - COALESCE(lst.tup_returned,0),
        cur.tup_fetched - COALESCE(lst.tup_fetched,0),
        cur.tup_inserted - COALESCE(lst.tup_inserted,0),
        cur.tup_updated - COALESCE(lst.tup_updated,0),
        cur.tup_deleted - COALESCE(lst.tup_deleted,0),
        cur.conflicts - COALESCE(lst.conflicts,0),
        cur.temp_files - COALESCE(lst.temp_files,0),
        cur.temp_bytes - COALESCE(lst.temp_bytes,0),
        cur.deadlocks - COALESCE(lst.deadlocks,0),
        cur.blk_read_time - COALESCE(lst.blk_read_time,0),
        cur.blk_write_time - COALESCE(lst.blk_write_time,0),
        cur.stats_reset,
        cur.datsize as datsize,
        cur.datsize - COALESCE(lst.datsize,0) as datsize_delta
    FROM last_stat_database cur
      LEFT OUTER JOIN last_stat_database lst ON
        (lst.node_id = cur.node_id AND lst.snap_id = cur.snap_id - 1 AND lst.datid = cur.datid AND lst.datname = cur.datname AND lst.stats_reset = cur.stats_reset)
    WHERE cur.snap_id = s_id AND cur.node_id = snode_id;

    -- Get tablespace stats
    INSERT INTO last_stat_tablespaces
      SELECT
        snode_id,
        s_id,
        dbl.tablespaceid,
        dbl.tablespacename,
        dbl.tablespacepath,
        COALESCE(dbl.size,0) AS size,
        COALESCE(dbl.size_delta,0) AS size_delta
      FROM dblink('node_connection', 'SELECT
        oid as tablespaceid,
        spcname as tablespacename,
        pg_tablespace_location(oid) as tablespacepath,
        pg_tablespace_size(oid) as size,
        0 as size_delta
        FROM pg_tablespace ')
      AS dbl (
         tablespaceid            oid,
         tablespacename          name,
         tablespacepath          text,
         size                    bigint,
         size_delta              bigint
      );

    -- collect pg_stat_statements stats if available
    PERFORM collect_statements_stats(snode_id, s_id, topn);

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
            s_id,
            COALESCE(checkpoints_timed,0) AS checkpoints_timed,
            COALESCE(checkpoints_req,0) AS checkpoints_req,
            COALESCE(checkpoint_write_time,0) AS checkpoint_write_time,
            COALESCE(checkpoint_sync_time,0) AS checkpoint_sync_time,
            COALESCE(buffers_checkpoint,0) AS buffers_checkpoint,
            COALESCE(buffers_clean,0) AS buffers_clean,
            COALESCE(maxwritten_clean,0) AS maxwritten_clean,
            COALESCE(buffers_backend,0) AS buffers_backend,
            COALESCE(buffers_backend_fsync,0) AS buffers_backend_fsync,
            COALESCE(buffers_alloc,0) AS buffers_alloc,
            stats_reset,
            COALESCE(wal_size,0) AS wal_size
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
            s_id,
            COALESCE(checkpoints_timed,0) AS checkpoints_timed,
            COALESCE(checkpoints_req,0) AS checkpoints_req,
            COALESCE(checkpoint_write_time,0) AS checkpoint_write_time,
            COALESCE(checkpoint_sync_time,0) AS checkpoint_sync_time,
            COALESCE(buffers_checkpoint,0) AS buffers_checkpoint,
            COALESCE(buffers_clean,0) AS buffers_clean,
            COALESCE(maxwritten_clean,0) AS maxwritten_clean,
            COALESCE(buffers_backend,0) AS buffers_backend,
            COALESCE(buffers_backend_fsync,0) AS buffers_backend_fsync,
            COALESCE(buffers_alloc,0) AS buffers_alloc,
            stats_reset,
            COALESCE(wal_size,0) AS wal_size
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


    -- Collecting stat info for objects of all databases
    PERFORM collect_obj_stats(snode_id, s_id);
    PERFORM dblink_disconnect('node_connection');

    -- analyze last_* tables will help with more accurate plans
    ANALYZE last_stat_indexes;
    ANALYZE last_stat_tables;
    ANALYZE last_stat_tablespaces;
    ANALYZE last_stat_user_functions;

    -- Updating dictionary table in case of object renaming:
    -- Databases
    UPDATE snap_stat_database AS db
    SET datname = lst.datname
    FROM last_stat_database AS lst
    WHERE db.node_id = lst.node_id AND db.datid = lst.datid
      AND db.datname != lst.datname
      AND lst.snap_id = s_id;
    -- Tables
    UPDATE tables_list AS tl
    SET schemaname = lst.schemaname, relname = lst.relname
    FROM last_stat_tables AS lst
    WHERE tl.node_id = lst.node_id AND tl.datid = lst.datid AND tl.relid = lst.relid AND tl.relkind = lst.relkind
      AND (tl.schemaname != lst.schemaname OR tl.relname != lst.relname)
      AND lst.snap_id = s_id;
    -- Indexes
    UPDATE indexes_list AS il
    SET schemaname = lst.schemaname, indexrelname = lst.indexrelname
    FROM last_stat_indexes AS lst
    WHERE il.node_id = lst.node_id AND il.datid = lst.datid AND il.indexrelid = lst.indexrelid
      AND il.relid = lst.relid
      AND (il.schemaname != lst.schemaname OR il.indexrelname != lst.indexrelname)
      AND lst.snap_id = s_id;
    -- Functions
    UPDATE funcs_list AS fl
    SET schemaname = lst.schemaname, funcname = lst.funcname, funcargs = lst.funcargs
    FROM last_stat_user_functions AS lst
    WHERE fl.node_id = lst.node_id AND fl.datid = lst.datid AND fl.funcid = lst.funcid
      AND (fl.schemaname != lst.schemaname OR fl.funcname != lst.funcname OR fl.funcargs != lst.funcargs)
      AND lst.snap_id = s_id;
    -- Tablespaces
    UPDATE tablespaces_list AS tl
    SET tablespacename = lst.tablespacename, tablespacepath = lst.tablespacepath
    FROM last_stat_tablespaces AS lst
    WHERE tl.node_id = lst.node_id AND tl.tablespaceid = lst.tablespaceid
      AND (tl.tablespacename != lst.tablespacename OR tl.tablespacepath != lst.tablespacepath)
      AND lst.snap_id = s_id;

    -- Calculate diffs for tablespaces
    FOR qres IN
        SELECT
            node_id,
            snap_id,
            tablespaceid,
            tablespacename,
            tablespacepath,
            size,
            size_delta
        FROM
            (SELECT
                cur.node_id,
                cur.snap_id,
                cur.tablespaceid as tablespaceid,
                cur.tablespacename AS tablespacename,
                cur.tablespacepath AS tablespacepath,
                cur.size as size,
                cur.size - COALESCE(lst.size,0) AS size_delta
            FROM last_stat_tablespaces cur LEFT OUTER JOIN last_stat_tablespaces lst ON
              (cur.node_id = lst.node_id AND lst.snap_id=cur.snap_id-1 AND cur.tablespaceid = lst.tablespaceid)
            WHERE cur.snap_id=s_id AND cur.node_id=snode_id ) diff
    LOOP
      -- insert tablespaces to tablespaces_list
      INSERT INTO tablespaces_list VALUES (qres.node_id,qres.tablespaceid,qres.tablespacename,qres.tablespacepath) ON CONFLICT DO NOTHING;
      INSERT INTO snap_stat_tablespaces VALUES (
          qres.node_id,
          qres.snap_id,
          qres.tablespaceid,
          qres.size,
          qres.size_delta
      );
    END LOOP;

    -- collect databases objects stats
    PERFORM snapshot_dbobj_delta(snode_id,s_id,topn);

    DELETE FROM last_stat_tablespaces WHERE node_id = snode_id AND snap_id != s_id;

    DELETE FROM last_stat_database WHERE node_id = snode_id AND snap_id != s_id;

    -- Calc stat cluster diff
    INSERT INTO snap_stat_cluster
    SELECT
        cur.node_id,
        cur.snap_id,
        cur.checkpoints_timed - COALESCE(lst.checkpoints_timed,0),
        cur.checkpoints_req - COALESCE(lst.checkpoints_req,0),
        cur.checkpoint_write_time - COALESCE(lst.checkpoint_write_time,0),
        cur.checkpoint_sync_time - COALESCE(lst.checkpoint_sync_time,0),
        cur.buffers_checkpoint - COALESCE(lst.buffers_checkpoint,0),
        cur.buffers_clean - COALESCE(lst.buffers_clean,0),
        cur.maxwritten_clean - COALESCE(lst.maxwritten_clean,0),
        cur.buffers_backend - COALESCE(lst.buffers_backend,0),
        cur.buffers_backend_fsync - COALESCE(lst.buffers_backend_fsync,0),
        cur.buffers_alloc - COALESCE(lst.buffers_alloc,0),
        cur.stats_reset,
        cur.wal_size - COALESCE(lst.wal_size,0)
    FROM last_stat_cluster cur
    LEFT OUTER JOIN last_stat_cluster lst ON
      (cur.stats_reset = lst.stats_reset AND cur.node_id = lst.node_id AND lst.snap_id = cur.snap_id - 1)
    WHERE cur.snap_id = s_id AND cur.node_id = snode_id;

    DELETE FROM last_stat_cluster WHERE node_id = snode_id AND snap_id != s_id;

    -- Deleting obsolete baselines
    DELETE FROM baselines
    WHERE keep_until < now()
      AND node_id = snode_id;
    -- Deleting obsolote snapshots
    DELETE FROM snapshots s
      USING nodes n
    WHERE n.node_id = s.node_id AND s.node_id = snode_id
        AND s.snap_time < now() - (COALESCE(n.retention,ret) || ' days')::interval
        AND (s.node_id,s.snap_id) NOT IN (SELECT node_id,snap_id FROM bl_snaps WHERE node_id = snode_id);
    -- Deleting unused statements
    DELETE FROM stmt_list
        WHERE queryid_md5 NOT IN
            (SELECT queryid_md5 FROM snap_statements);

    -- Delete unused tablespaces list
    DELETE FROM tablespaces_list
    WHERE node_id = snode_id
      AND (node_id, tablespaceid) NOT IN (
        SELECT node_id, tablespaceid FROM snap_stat_tablespaces
        WHERE node_id = snode_id
    );

    -- Delete unused indexes from indexes list
    DELETE FROM indexes_list
    WHERE node_id = snode_id
      AND(node_id, datid, indexrelid) NOT IN (
        SELECT node_id, datid, indexrelid FROM snap_stat_indexes
    );

    -- Delete unused tables from tables list
    WITH used_tables AS (
        SELECT node_id, datid, relid FROM snap_stat_tables WHERE node_id = snode_id
        UNION ALL
        SELECT node_id, datid, relid FROM indexes_list WHERE node_id = snode_id)
    DELETE FROM tables_list
    WHERE node_id = snode_id
      AND (node_id, datid, relid) NOT IN (SELECT node_id, datid, relid FROM used_tables)
      AND (node_id, datid, reltoastrelid) NOT IN (SELECT node_id, datid, relid FROM used_tables);

    -- Delete unused functions from functions list
    DELETE FROM funcs_list
    WHERE node_id = snode_id
      AND (node_id, funcid) NOT IN (
        SELECT node_id, funcid FROM snap_stat_user_functions WHERE node_id = snode_id
    );

    -- Delete obsolete values of postgres parameters
    DELETE FROM snap_settings ss
    USING (
      SELECT node_id, max(first_seen) AS first_seen, setting_scope, name
      FROM snap_settings
      WHERE node_id = snode_id AND first_seen <= (SELECT min(snap_time) FROM snapshots WHERE node_id = snode_id)
      GROUP BY node_id, setting_scope, name) AS ss_ref
    WHERE ss.node_id = ss_ref.node_id AND ss.setting_scope = ss_ref.setting_scope AND ss.name = ss_ref.name
      AND ss.first_seen < ss_ref.first_seen;
    -- Delete obsolete values of postgres parameters from previous versions of postgres on node
    DELETE FROM snap_settings
    WHERE node_id = snode_id AND first_seen <
      (SELECT min(first_seen) FROM snap_settings WHERE node_id = snode_id AND name = 'version' AND setting_scope = 2);

    RETURN 0;
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

CREATE OR REPLACE FUNCTION snapshot() RETURNS TABLE (
    node        name,
    result      text
)
SET search_path=@extschema@,public AS $$
DECLARE
    c_nodes CURSOR FOR
    SELECT node_id,node_name FROM nodes WHERE enabled;
    node_snapres        integer;
    etext               text := '';
    edetail             text := '';
    econtext            text := '';

    qres    RECORD;
BEGIN
    -- Only one running snapshot() function allowed!
    -- Explicitly locking nodes table
    BEGIN
        LOCK nodes IN SHARE ROW EXCLUSIVE MODE NOWAIT;
    EXCEPTION
        WHEN OTHERS THEN RAISE 'Can''t get lock on nodes table. Is there another snapshot() running?';
    END;
    FOR qres IN c_nodes LOOP
        BEGIN
            node := qres.node_name;
            node_snapres := snapshot(qres.node_id);
            CASE node_snapres
              WHEN 0 THEN
                result := 'OK';
              ELSE
                result := 'FAIL';
            END CASE;
            RETURN NEXT;
        EXCEPTION
            WHEN OTHERS THEN
                BEGIN
                    GET STACKED DIAGNOSTICS etext = MESSAGE_TEXT,
                        edetail = PG_EXCEPTION_DETAIL,
                        econtext = PG_EXCEPTION_CONTEXT;
                    result := format (E'%s\n%s\n%s', etext, econtext, edetail);
                    RETURN NEXT;
                END;
        END;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION snapshot() IS 'Statistics snapshot creation function (for all enabled nodes). Must be explicitly called periodically.';

CREATE OR REPLACE FUNCTION collect_statements_stats(IN snode_id integer, IN s_id integer, IN topn integer) RETURNS void SET search_path=@extschema@,public AS $$
DECLARE
    qres record;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Adding pg_stat_statements extension schema to search_path of node_connection if it does not already there
    SELECT * INTO qres FROM dblink('node_connection',
        'SELECT extnamespace::regnamespace::name AS stat_statements_schema, current_setting(''search_path'') AS current_search_path
         FROM pg_catalog.pg_extension WHERE extname = ''pg_stat_statements''')
      AS dbl(stat_statements_schema name, current_search_path text);
    IF qres.stat_statements_schema IS NULL THEN
      RETURN;
    ELSIF NOT string_to_array(qres.current_search_path,',') @> ARRAY[qres.stat_statements_schema::text] THEN
      PERFORM dblink('node_connection','SET search_path TO ' || qres.current_search_path || ',' || qres.stat_statements_schema::text);
    END IF;

    -- Snapshot data from pg_stat_statements for top whole cluster statements
    FOR qres IN
        SELECT
          snode_id,
          s_id AS snap_id,
          dbl.userid AS userid,
          dbl.datid AS datid,
          dbl.queryid AS queryid,
          dbl.queryid_md5 AS queryid_md5,
          COALESCE(dbl.calls,0) AS calls,
          COALESCE(dbl.total_time,0) AS total_time,
          COALESCE(dbl.min_time,0) AS min_time,
          COALESCE(dbl.max_time,0) AS max_time,
          COALESCE(dbl.mean_time,0) AS mean_time,
          COALESCE(dbl.stddev_time,0) AS stddev_time,
          COALESCE(dbl.rows,0) AS rows,
          COALESCE(dbl.shared_blks_hit,0) AS shared_blks_hit,
          COALESCE(dbl.shared_blks_read,0) AS shared_blks_read,
          COALESCE(dbl.shared_blks_dirtied,0) AS shared_blks_dirtied,
          COALESCE(dbl.shared_blks_written,0) AS shared_blks_written,
          COALESCE(dbl.local_blks_hit,0) AS local_blks_hit,
          COALESCE(dbl.local_blks_read,0) AS local_blks_read,
          COALESCE(dbl.local_blks_dirtied,0) AS local_blks_dirtied,
          COALESCE(dbl.local_blks_written,0) AS local_blks_written,
          COALESCE(dbl.temp_blks_read,0) AS temp_blks_read,
          COALESCE(dbl.temp_blks_written,0) AS temp_blks_written,
          COALESCE(dbl.blk_read_time,0) AS blk_read_time,
          COALESCE(dbl.blk_write_time,0) AS blk_write_time,
          dbl.query AS query
        FROM dblink('node_connection',format('SELECT
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
            row_number() over (ORDER BY sum(total_time) DESC) AS time_rank,
            row_number() over (ORDER BY sum(calls) DESC) AS calls_rank,
            row_number() over (ORDER BY sum(blk_read_time + blk_write_time) DESC) AS io_time_rank,
            row_number() over (ORDER BY sum(shared_blks_hit + shared_blks_read) DESC) AS gets_rank,
            row_number() over (ORDER BY sum(shared_blks_read) DESC) AS read_rank,
            row_number() over (ORDER BY sum(shared_blks_dirtied) DESC) AS dirtied_rank,
            row_number() over (ORDER BY sum(shared_blks_written) DESC) AS written_rank,
            row_number() over (ORDER BY sum(temp_blks_written + local_blks_written) DESC) AS tempw_rank,
            row_number() over (ORDER BY sum(temp_blks_read + local_blks_read) DESC) AS tempr_rank
            FROM pg_stat_statements
            GROUP BY userid, dbid, md5(query)) rank_t
        ON (st.userid=rank_t.userid AND st.dbid=rank_t.dbid AND md5(st.query)=rank_t.q_md5)
        WHERE
            st.queryid IS NOT NULL AND
            least (time_rank, calls_rank, io_time_rank, gets_rank, read_rank, tempw_rank, tempr_rank, dirtied_rank, written_rank) <= %1$s',topn)
        ) AS dbl (
            userid oid,
            datid oid,
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
        ) JOIN snap_stat_database sd ON (dbl.datid = sd.datid AND sd.snap_id = s_id AND sd.node_id = snode_id)
    LOOP
        INSERT INTO stmt_list VALUES (qres.queryid_md5,qres.query) ON CONFLICT DO NOTHING;
        INSERT INTO snap_statements VALUES (
            qres.snode_id,
            qres.snap_id,
            qres.userid,
            qres.datid,
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
    SELECT sd.node_id,sd.snap_id,dbl.*
    FROM
    dblink('node_connection','SELECT dbid as datid,sum(calls),sum(total_time),sum(rows),sum(shared_blks_hit),
        sum(shared_blks_read),sum(shared_blks_dirtied),sum(shared_blks_written),
        sum(local_blks_hit),sum(local_blks_read),sum(local_blks_dirtied),
        sum(local_blks_written),sum(temp_blks_read),sum(temp_blks_written),sum(blk_read_time),
        sum(blk_write_time),count(*)
    FROM pg_stat_statements
    GROUP BY dbid') AS dbl (
        datid oid,
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
    ) JOIN snap_stat_database sd USING (datid)
    WHERE sd.snap_id = s_id AND sd.node_id = snode_id;

    -- Flushing pg_stat_statements
    SELECT * INTO qres FROM dblink('node_connection','SELECT pg_stat_statements_reset()') AS t(res char(1));
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION collect_obj_stats(IN snode_id integer, IN s_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    --Cursor for db stats
    c_dblist CURSOR FOR
    SELECT datid,datname,tablespaceid FROM dblink('node_connection',
    'select dbs.oid,dbs.datname,dbs.dattablespace from pg_catalog.pg_database dbs
    where dbs.datname not like ''template_'' and dbs.datallowconn') AS dbl (
        datid oid,
        datname name,
        tablespaceid oid
    ) JOIN nodes n ON (n.node_id = snode_id AND array_position(n.db_exclude,dbl.datname) IS NULL);

    qres        record;
    db_connstr  text;
    t_query     text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;


    -- Disconnecting existing connection
    IF dblink_get_connections() @> ARRAY['node_db_connection'] THEN
        PERFORM dblink_disconnect('node_db_connection');
    END IF;

    -- Load new data from statistic views of all cluster databases
    FOR qres IN c_dblist LOOP
      db_connstr := regexp_replace(get_connstr(snode_id),'dbname=\w+','dbname='||qres.datname,'g');
      PERFORM dblink_connect('node_db_connection',db_connstr);
      -- Setting lock_timout prevents hanging of snapshot() call due to DDL in long transaction
      PERFORM dblink('node_db_connection','SET lock_timeout=3000');

      -- Generate Table stats query
      t_query := 'SELECT st.*,
        stio.heap_blks_read,
        stio.heap_blks_hit,
        stio.idx_blks_read,
        stio.idx_blks_hit,
        stio.toast_blks_read,
        stio.toast_blks_hit,
        stio.tidx_blks_read,
        stio.tidx_blks_hit,
        pg_relation_size(relid) relsize,0 relsize_diff,class.reltablespace AS tablespaceid,class.reltoastrelid,class.relkind
      FROM pg_catalog.pg_stat_all_tables st
      JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname)
      JOIN pg_catalog.pg_class class ON (st.relid = class.oid)';

      INSERT INTO last_stat_tables
      SELECT
        snode_id,
        s_id,
        qres.datid,
        relid,
        schemaname,
        relname,
        COALESCE(dbl.seq_scan,0) AS seq_scan,
        COALESCE(dbl.seq_tup_read,0) AS seq_tup_read,
        COALESCE(dbl.idx_scan,0) AS idx_scan,
        COALESCE(dbl.idx_tup_fetch,0) AS idx_tup_fetch,
        COALESCE(dbl.n_tup_ins,0) AS n_tup_ins,
        COALESCE(dbl.n_tup_upd,0) AS n_tup_upd,
        COALESCE(dbl.n_tup_del,0) AS n_tup_del,
        COALESCE(dbl.n_tup_hot_upd,0) AS n_tup_hot_upd,
        COALESCE(dbl.n_live_tup,0) AS n_live_tup,
        COALESCE(dbl.n_dead_tup,0) AS n_dead_tup,
        COALESCE(dbl.n_mod_since_analyze,0) AS n_mod_since_analyze,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        COALESCE(dbl.vacuum_count,0) AS vacuum_count,
        COALESCE(dbl.autovacuum_count,0) AS autovacuum_count,
        COALESCE(dbl.analyze_count,0) AS analyze_count,
        COALESCE(dbl.autoanalyze_count,0) AS autoanalyze_count,
        COALESCE(dbl.heap_blks_read,0) AS heap_blks_read,
        COALESCE(dbl.heap_blks_hit,0) AS heap_blks_hit,
        COALESCE(dbl.idx_blks_read,0) AS idx_blks_read,
        COALESCE(dbl.idx_blks_hit,0) AS idx_blks_hit,
        COALESCE(dbl.toast_blks_read,0) AS toast_blks_read,
        COALESCE(dbl.toast_blks_hit,0) AS toast_blks_hit,
        COALESCE(dbl.tidx_blks_read,0) AS tidx_blks_read,
        COALESCE(dbl.tidx_blks_hit,0) AS tidx_blks_hit,
        COALESCE(dbl.relsize,0) AS relsize,
        COALESCE(dbl.relsize_diff,0) AS relsize_diff,
        CASE WHEN tablespaceid=0 THEN qres.tablespaceid ELSE tablespaceid END tablespaceid,
        reltoastrelid,
        relkind
      FROM dblink('node_db_connection', t_query)
      AS dbl (
          relid                 oid,
          schemaname            name,
          relname               name,
          seq_scan              bigint,
          seq_tup_read          bigint,
          idx_scan              bigint,
          idx_tup_fetch         bigint,
          n_tup_ins             bigint,
          n_tup_upd             bigint,
          n_tup_del             bigint,
          n_tup_hot_upd         bigint,
          n_live_tup            bigint,
          n_dead_tup            bigint,
          n_mod_since_analyze   bigint,
          last_vacuum           timestamp with time zone,
          last_autovacuum       timestamp with time zone,
          last_analyze          timestamp with time zone,
          last_autoanalyze      timestamp with time zone,
          vacuum_count          bigint,
          autovacuum_count      bigint,
          analyze_count         bigint,
          autoanalyze_count     bigint,
          heap_blks_read        bigint,
          heap_blks_hit         bigint,
          idx_blks_read         bigint,
          idx_blks_hit          bigint,
          toast_blks_read       bigint,
          toast_blks_hit        bigint,
          tidx_blks_read        bigint,
          tidx_blks_hit         bigint,
          relsize               bigint,
          relsize_diff          bigint,
          tablespaceid          oid,
          reltoastrelid         oid,
          relkind               char
      );

      -- Generate index stats query
      t_query := 'SELECT st.*,stio.idx_blks_read,stio.idx_blks_hit,pg_relation_size(st.indexrelid),0, pg_class.reltablespace as tablespaceid, (ix.indisunique OR con.conindid IS NOT NULL) AS indisunique
      FROM pg_catalog.pg_stat_all_indexes st
      JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname)
      JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid)
      JOIN pg_class ON (pg_class.oid = st.indexrelid)
      LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.conindid = ix.indexrelid AND con.contype in (''p'',''u''))';

      INSERT INTO last_stat_indexes
      SELECT
        snode_id,
        s_id,
        qres.datid,
        relid,
        indexrelid,
        schemaname,
        relname,
        indexrelname,
        COALESCE(dbl.idx_scan,0) AS idx_scan,
        COALESCE(dbl.idx_tup_read,0) AS idx_tup_read,
        COALESCE(dbl.idx_tup_fetch,0) AS idx_tup_fetch,
        COALESCE(dbl.idx_blks_read,0) AS idx_blks_read,
        COALESCE(dbl.idx_blks_hit,0) AS idx_blks_hit,
        COALESCE(dbl.relsize,0) AS relsize,
        COALESCE(dbl.relsize_diff,0) AS relsize_diff,
        CASE WHEN tablespaceid=0 THEN qres.tablespaceid ELSE tablespaceid END tablespaceid,
        indisunique
      FROM dblink('node_db_connection', t_query)
      AS dbl (
         relid          oid,
         indexrelid     oid,
         schemaname     name,
         relname        name,
         indexrelname   name,
         idx_scan       bigint,
         idx_tup_read   bigint,
         idx_tup_fetch  bigint,
         idx_blks_read  bigint,
         idx_blks_hit   bigint,
         relsize        bigint,
         relsize_diff   bigint,
         tablespaceid   oid,
         indisunique    bool
      );

      -- Generate Function stats query
      t_query := 'SELECT funcid,schemaname,funcname,pg_get_function_arguments(funcid) AS funcargs,
      calls,total_time,self_time
      FROM pg_catalog.pg_stat_user_functions';

      INSERT INTO last_stat_user_functions
      SELECT
        snode_id,
        s_id,
        qres.datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        COALESCE(dbl.calls,0) AS calls,
        COALESCE(dbl.total_time,0) AS total_time,
        COALESCE(dbl.self_time,0) AS self_time
      FROM dblink('node_db_connection', t_query)
      AS dbl (
         funcid oid,
         schemaname name,
         funcname name,
         funcargs text,
         calls bigint,
         total_time double precision,
         self_time double precision
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

    -- Calculating difference from previous snapshot and storing it in snap_stat_ tables
    -- Stats of user tables
    FOR qres IN
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
            relkind,
            toastrelid,
            toastschemaname,
            toastrelname,
            toastseq_scan,
            toastseq_tup_read,
            toastidx_scan,
            toastidx_tup_fetch,
            toastn_tup_ins,
            toastn_tup_upd,
            toastn_tup_del,
            toastn_tup_hot_upd,
            toastn_live_tup,
            toastn_dead_tup,
            toastn_mod_since_analyze,
            toastlast_vacuum,
            toastlast_autovacuum,
            toastlast_analyze,
            toastlast_autoanalyze,
            toastvacuum_count,
            toastautovacuum_count,
            toastanalyze_count,
            toastautoanalyze_count,
            toastheap_blks_read,
            toastheap_blks_hit,
            toastidx_blks_read,
            toastidx_blks_hit,
            toastrelsize,
            toastrelsize_diff,
            toastrelkind
        FROM
            (SELECT
                cur.node_id AS node_id,
                cur.snap_id AS snap_id,
                cur.datid AS datid,
                cur.relid AS relid,
                cur.schemaname AS schemaname,
                cur.relname AS relname,
                cur.seq_scan - COALESCE(lst.seq_scan,0) AS seq_scan,
                cur.seq_tup_read - COALESCE(lst.seq_tup_read,0) AS seq_tup_read,
                cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
                cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
                cur.n_tup_ins - COALESCE(lst.n_tup_ins,0) AS n_tup_ins,
                cur.n_tup_upd - COALESCE(lst.n_tup_upd,0) AS n_tup_upd,
                cur.n_tup_del - COALESCE(lst.n_tup_del,0) AS n_tup_del,
                cur.n_tup_hot_upd - COALESCE(lst.n_tup_hot_upd,0) AS n_tup_hot_upd,
                cur.n_live_tup AS n_live_tup,
                cur.n_dead_tup AS n_dead_tup,
                cur.n_mod_since_analyze AS n_mod_since_analyze,
                cur.last_vacuum AS last_vacuum,
                cur.last_autovacuum AS last_autovacuum,
                cur.last_analyze AS last_analyze,
                cur.last_autoanalyze AS last_autoanalyze,
                cur.vacuum_count - COALESCE(lst.vacuum_count,0) AS vacuum_count,
                cur.autovacuum_count - COALESCE(lst.autovacuum_count,0) AS autovacuum_count,
                cur.analyze_count - COALESCE(lst.analyze_count,0) AS analyze_count,
                cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) AS autoanalyze_count,
                cur.heap_blks_read - COALESCE(lst.heap_blks_read,0) AS heap_blks_read,
                cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0) AS heap_blks_hit,
                cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
                cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
                cur.toast_blks_read - COALESCE(lst.toast_blks_read,0) AS toast_blks_read,
                cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0) AS toast_blks_hit,
                cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0) AS tidx_blks_read,
                cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0) AS tidx_blks_hit,
                cur.relsize AS relsize,
                cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
                cur.tablespaceid AS tablespaceid,
                cur.relkind AS relkind,
                tcur.relid AS toastrelid,
                tcur.schemaname AS toastschemaname,
                tcur.relname AS toastrelname,
                tcur.seq_scan - COALESCE(tlst.seq_scan,0) AS toastseq_scan,
                tcur.seq_tup_read - COALESCE(tlst.seq_tup_read,0) AS toastseq_tup_read,
                tcur.idx_scan - COALESCE(tlst.idx_scan,0) AS toastidx_scan,
                tcur.idx_tup_fetch - COALESCE(tlst.idx_tup_fetch,0) AS toastidx_tup_fetch,
                tcur.n_tup_ins - COALESCE(tlst.n_tup_ins,0) AS toastn_tup_ins,
                tcur.n_tup_upd - COALESCE(tlst.n_tup_upd,0) AS toastn_tup_upd,
                tcur.n_tup_del - COALESCE(tlst.n_tup_del,0) AS toastn_tup_del,
                tcur.n_tup_hot_upd - COALESCE(tlst.n_tup_hot_upd,0) AS toastn_tup_hot_upd,
                tcur.n_live_tup AS toastn_live_tup,
                tcur.n_dead_tup AS toastn_dead_tup,
                tcur.n_mod_since_analyze AS toastn_mod_since_analyze,
                tcur.last_vacuum AS toastlast_vacuum,
                tcur.last_autovacuum AS toastlast_autovacuum,
                tcur.last_analyze AS toastlast_analyze,
                tcur.last_autoanalyze AS toastlast_autoanalyze,
                tcur.vacuum_count - COALESCE(tlst.vacuum_count,0) AS toastvacuum_count,
                tcur.autovacuum_count - COALESCE(tlst.autovacuum_count,0) AS toastautovacuum_count,
                tcur.analyze_count - COALESCE(tlst.analyze_count,0) AS toastanalyze_count,
                tcur.autoanalyze_count - COALESCE(tlst.autoanalyze_count,0) AS toastautoanalyze_count,
                tcur.heap_blks_read - COALESCE(tlst.heap_blks_read,0) AS toastheap_blks_read,
                tcur.heap_blks_hit - COALESCE(tlst.heap_blks_hit,0) AS toastheap_blks_hit,
                tcur.idx_blks_read - COALESCE(tlst.idx_blks_read,0) AS toastidx_blks_read,
                tcur.idx_blks_hit - COALESCE(tlst.idx_blks_hit,0) AS toastidx_blks_hit,
                tcur.relsize AS toastrelsize,
                tcur.relsize - COALESCE(tlst.relsize,0) AS toastrelsize_diff,
                tcur.relkind AS toastrelkind,
                row_number() OVER (ORDER BY (cur.seq_scan + COALESCE(tcur.seq_scan,0) -
                  COALESCE(lst.seq_scan,0) - COALESCE(tlst.seq_scan,0))*(cur.relsize + COALESCE(tcur.relsize,0)) DESC) scan_rank, --weighted scans
                row_number() OVER (ORDER BY cur.n_tup_ins + cur.n_tup_upd + cur.n_tup_del -
                  COALESCE(lst.n_tup_ins + lst.n_tup_upd + lst.n_tup_del, 0) +
                  COALESCE(tcur.n_tup_ins + tcur.n_tup_upd + tcur.n_tup_del, 0) -
                  COALESCE(tlst.n_tup_ins + tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) dml_rank,
                row_number() OVER (ORDER BY cur.n_tup_upd+cur.n_tup_del -
                  COALESCE(lst.n_tup_upd + lst.n_tup_del, 0) +
                  COALESCE(tcur.n_tup_upd + tcur.n_tup_del, 0) -
                  COALESCE(tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) vacuum_rank,
                row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize,0) +
                  COALESCE(tcur.relsize,0) - COALESCE(tlst.relsize,0) DESC) growth_rank,
                row_number() OVER (ORDER BY cur.n_dead_tup * 100 / GREATEST(cur.n_live_tup+cur.n_dead_tup,1) DESC) dead_pct_rank,
                row_number() OVER (ORDER BY cur.n_mod_since_analyze * 100 / GREATEST(cur.n_live_tup,1) DESC) mod_pct_rank,
                -- Read rank
                row_number() OVER (ORDER BY
                  cur.heap_blks_read - COALESCE(lst.heap_blks_read,0) +
                  cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) +
                  cur.toast_blks_read - COALESCE(lst.toast_blks_read,0) +
                  cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0) DESC) read_rank,
                -- Page processing rank
                row_number() OVER (ORDER BY cur.heap_blks_read+cur.heap_blks_hit+cur.idx_blks_read+cur.idx_blks_hit+
                  cur.toast_blks_read+cur.toast_blks_hit+cur.tidx_blks_read+cur.tidx_blks_hit-
                  COALESCE(lst.heap_blks_read+lst.heap_blks_hit+lst.idx_blks_read+lst.idx_blks_hit+
                  lst.toast_blks_read+lst.toast_blks_hit+lst.tidx_blks_read+lst.tidx_blks_hit, 0) DESC) gets_rank
            FROM
              -- main relations diff
              last_stat_tables cur JOIN snap_stat_database dbcur USING (node_id, snap_id, datid)
              LEFT OUTER JOIN snap_stat_database dblst ON
                (dbcur.node_id = dblst.node_id AND dbcur.datid = dblst.datid AND dblst.snap_id = dbcur.snap_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
              LEFT OUTER JOIN last_stat_tables lst ON
                (dblst.node_id=lst.node_id AND lst.snap_id = dblst.snap_id AND lst.datid=dblst.datid AND cur.relid=lst.relid)
              -- toast relations diff
              LEFT OUTER JOIN last_stat_tables tcur ON
                (tcur.node_id=dbcur.node_id AND tcur.snap_id = dbcur.snap_id  AND tcur.datid=dbcur.datid AND cur.reltoastrelid=tcur.relid)
              LEFT OUTER JOIN last_stat_tables tlst ON
                (tlst.node_id=dblst.node_id AND tlst.snap_id = dblst.snap_id AND tlst.datid=dblst.datid AND lst.reltoastrelid=tlst.relid)
            WHERE cur.snap_id=s_id AND cur.node_id=snode_id
              AND cur.relkind IN ('r','m')) diff
        WHERE scan_rank <= topn OR dml_rank <= topn OR growth_rank <= topn OR dead_pct_rank <= topn
          OR mod_pct_rank <= topn OR vacuum_rank <= topn OR read_rank <= topn OR gets_rank <= topn
    LOOP
        IF qres.toastrelid IS NOT NULL THEN
          INSERT INTO tables_list VALUES (qres.node_id,qres.datid,qres.toastrelid,qres.toastrelkind,NULL,qres.toastschemaname,qres.toastrelname) ON CONFLICT DO NOTHING;
          INSERT INTO snap_stat_tables VALUES (
              qres.node_id,
              qres.snap_id,
              qres.datid,
              qres.toastrelid,
              qres.tablespaceid,
              qres.toastseq_scan,
              qres.toastseq_tup_read,
              qres.toastidx_scan,
              qres.toastidx_tup_fetch,
              qres.toastn_tup_ins,
              qres.toastn_tup_upd,
              qres.toastn_tup_del,
              qres.toastn_tup_hot_upd,
              qres.toastn_live_tup,
              qres.toastn_dead_tup,
              qres.toastn_mod_since_analyze,
              qres.toastlast_vacuum,
              qres.toastlast_autovacuum,
              qres.toastlast_analyze,
              qres.toastlast_autoanalyze,
              qres.toastvacuum_count,
              qres.toastautovacuum_count,
              qres.toastanalyze_count,
              qres.toastautoanalyze_count,
              qres.toastheap_blks_read,
              qres.toastheap_blks_hit,
              qres.toastidx_blks_read,
              qres.toastidx_blks_hit,
              0,
              0,
              0,
              0,
              qres.toastrelsize,
              qres.toastrelsize_diff
          );
        END IF;

        INSERT INTO tables_list VALUES (qres.node_id,qres.datid,qres.relid,qres.relkind,NULLIF(qres.toastrelid,0),qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_tables VALUES (
            qres.node_id,
            qres.snap_id,
            qres.datid,
            qres.relid,
            qres.tablespaceid,
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

    -- Total table stats
    INSERT INTO snap_stat_tables_total
    SELECT
      cur.node_id,
      cur.snap_id,
      cur.datid,
      cur.tablespaceid,
      cur.relkind,
      sum(cur.seq_scan - COALESCE(lst.seq_scan,0)),
      sum(cur.seq_tup_read - COALESCE(lst.seq_tup_read,0)),
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.n_tup_ins - COALESCE(lst.n_tup_ins,0)),
      sum(cur.n_tup_upd - COALESCE(lst.n_tup_upd,0)),
      sum(cur.n_tup_del - COALESCE(lst.n_tup_del,0)),
      sum(cur.n_tup_hot_upd - COALESCE(lst.n_tup_hot_upd,0)),
      sum(cur.vacuum_count - COALESCE(lst.vacuum_count,0)),
      sum(cur.autovacuum_count - COALESCE(lst.autovacuum_count,0)),
      sum(cur.analyze_count - COALESCE(lst.analyze_count,0)),
      sum(cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0)),
      sum(cur.heap_blks_read - COALESCE(lst.heap_blks_read,0)),
      sum(cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      sum(cur.toast_blks_read - COALESCE(lst.toast_blks_read,0)),
      sum(cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0)),
      sum(cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0)),
      sum(cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0)),
      sum(cur.relsize - COALESCE(lst.relsize,0))
    FROM last_stat_tables cur JOIN snap_stat_database dbcur USING (node_id, snap_id, datid)
      LEFT OUTER JOIN snap_stat_database dblst ON
        (dbcur.node_id = dblst.node_id AND dbcur.datid = dblst.datid AND dblst.snap_id = dbcur.snap_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
      LEFT OUTER JOIN last_stat_tables lst ON
        (dblst.node_id=lst.node_id AND lst.snap_id = dblst.snap_id AND lst.datid=dblst.datid AND cur.relid=lst.relid AND cur.tablespaceid=lst.tablespaceid)
    WHERE cur.snap_id = s_id AND cur.node_id = snode_id
    GROUP BY cur.node_id, cur.snap_id, cur.datid, cur.relkind, cur.tablespaceid;

    -- Stats of user indexes
    FOR qres IN
        SELECT
            node_id,
            snap_id,
            datid,
            relid,
            indexrelid,
            tablespaceid,
            schemaname,
            relname,
            indexrelname,
            idx_scan,
            idx_tup_read,
            idx_tup_fetch,
            idx_blks_read,
            idx_blks_hit,
            relsize,
            relsize_diff,
            indisunique,
            relkind,
            reltoastrelid,
            reltablespaceid,
            mrelid,
            mrelkind,
            mreltoastrelid,
            mschemaname,
            mrelname,
            trelid,
            trelkind,
            treltoastrelid,
            tschemaname,
            trelname,
            tbl_seq_scan,
            tbl_seq_tup_read,
            tbl_idx_scan,
            tbl_idx_tup_fetch,
            tbl_n_tup_ins,
            tbl_n_tup_upd,
            tbl_n_tup_del,
            tbl_n_tup_hot_upd,
            tbl_n_live_tup,
            tbl_n_dead_tup,
            tbl_n_mod_since_analyze,
            tbl_last_vacuum,
            tbl_last_autovacuum,
            tbl_last_analyze,
            tbl_last_autoanalyze,
            tbl_vacuum_count,
            tbl_autovacuum_count,
            tbl_analyze_count,
            tbl_autoanalyze_count,
            tbl_heap_blks_read,
            tbl_heap_blks_hit,
            tbl_idx_blks_read,
            tbl_idx_blks_hit,
            tbl_toast_blks_read,
            tbl_toast_blks_hit,
            tbl_tidx_blks_read,
            tbl_tidx_blks_hit,
            tbl_relsize,
            tbl_relsize_diff
        FROM
            (SELECT
                cur.node_id,
                cur.snap_id,
                cur.datid,
                cur.relid,
                cur.indexrelid,
                cur.tablespaceid,
                cur.schemaname,
                cur.relname,
                cur.indexrelname,
                cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
                cur.idx_tup_read - COALESCE(lst.idx_tup_read,0) AS idx_tup_read,
                cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
                cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
                cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
                cur.relsize,
                cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
                cur.indisunique,
                tblcur.relkind AS relkind,
                tblcur.reltoastrelid AS reltoastrelid,
                tblcur.tablespaceid AS reltablespaceid,
                mtbl.relid AS mrelid,
                mtbl.relkind AS mrelkind,
                mtbl.reltoastrelid AS mreltoastrelid,
                mtbl.schemaname AS mschemaname,
                mtbl.relname AS mrelname,
                ttbl.relid AS trelid,
                ttbl.relkind AS trelkind,
                ttbl.reltoastrelid AS treltoastrelid,
                ttbl.schemaname AS tschemaname,
                ttbl.relname AS trelname,
                -- Underlying table stats
                tblcur.seq_scan - COALESCE(tbllst.seq_scan,0) AS tbl_seq_scan,
                tblcur.seq_tup_read - COALESCE(tbllst.seq_tup_read,0) AS tbl_seq_tup_read,
                tblcur.idx_scan - COALESCE(tbllst.idx_scan,0) AS tbl_idx_scan,
                tblcur.idx_tup_fetch - COALESCE(tbllst.idx_tup_fetch,0) AS tbl_idx_tup_fetch,
                tblcur.n_tup_ins - COALESCE(tbllst.n_tup_ins,0) AS tbl_n_tup_ins,
                tblcur.n_tup_upd - COALESCE(tbllst.n_tup_upd,0) AS tbl_n_tup_upd,
                tblcur.n_tup_del - COALESCE(tbllst.n_tup_del,0) AS tbl_n_tup_del,
                tblcur.n_tup_hot_upd - COALESCE(tbllst.n_tup_hot_upd,0) AS tbl_n_tup_hot_upd,
                tblcur.n_live_tup AS tbl_n_live_tup,
                tblcur.n_dead_tup AS tbl_n_dead_tup,
                tblcur.n_mod_since_analyze AS tbl_n_mod_since_analyze,
                tblcur.last_vacuum AS tbl_last_vacuum,
                tblcur.last_autovacuum AS tbl_last_autovacuum,
                tblcur.last_analyze AS tbl_last_analyze,
                tblcur.last_autoanalyze AS tbl_last_autoanalyze,
                tblcur.vacuum_count - COALESCE(tbllst.vacuum_count,0) AS tbl_vacuum_count,
                tblcur.autovacuum_count - COALESCE(tbllst.autovacuum_count,0) AS tbl_autovacuum_count,
                tblcur.analyze_count - COALESCE(tbllst.analyze_count,0) AS tbl_analyze_count,
                tblcur.autoanalyze_count - COALESCE(tbllst.autoanalyze_count,0) AS tbl_autoanalyze_count,
                tblcur.heap_blks_read - COALESCE(tbllst.heap_blks_read,0) AS tbl_heap_blks_read,
                tblcur.heap_blks_hit - COALESCE(tbllst.heap_blks_hit,0) AS tbl_heap_blks_hit,
                tblcur.idx_blks_read - COALESCE(tbllst.idx_blks_read,0) AS tbl_idx_blks_read,
                tblcur.idx_blks_hit - COALESCE(tbllst.idx_blks_hit,0) AS tbl_idx_blks_hit,
                tblcur.toast_blks_read - COALESCE(tbllst.toast_blks_read,0) AS tbl_toast_blks_read,
                tblcur.toast_blks_hit - COALESCE(tbllst.toast_blks_hit,0) AS tbl_toast_blks_hit,
                tblcur.tidx_blks_read - COALESCE(tbllst.tidx_blks_read,0) AS tbl_tidx_blks_read,
                tblcur.tidx_blks_hit - COALESCE(tbllst.tidx_blks_hit,0) AS tbl_tidx_blks_hit,
                tblcur.relsize AS tbl_relsize,
                tblcur.relsize - COALESCE(tbllst.relsize,0) AS tbl_relsize_diff,
                -- Index ranks
                row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize,0) DESC) grow_rank,
                row_number() OVER (ORDER BY cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) DESC) read_rank,
                row_number() OVER (ORDER BY cur.idx_blks_read+cur.idx_blks_hit-
                  COALESCE(lst.idx_blks_read+lst.idx_blks_hit,0) DESC) gets_rank,
                row_number() OVER (PARTITION BY cur.idx_scan - COALESCE(lst.idx_scan,0) = 0 ORDER BY cur.relsize - COALESCE(lst.relsize,0) DESC) grow_unused_rank
            FROM last_stat_indexes cur JOIN last_stat_tables tblcur USING (node_id, snap_id, datid, relid)
              JOIN snap_stat_database dbcur USING (node_id, snap_id, datid)
              LEFT OUTER JOIN snap_stat_database dblst ON
                (dbcur.node_id = dblst.node_id AND dbcur.datid = dblst.datid AND dblst.snap_id = dbcur.snap_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
              LEFT OUTER JOIN last_stat_indexes lst ON
                (dblst.node_id = lst.node_id AND lst.snap_id=dblst.snap_id AND dblst.datid = lst.datid AND cur.relid = lst.relid AND cur.indexrelid = lst.indexrelid)
              LEFT OUTER JOIN last_stat_tables tbllst ON
                (tbllst.node_id = dblst.node_id AND tbllst.snap_id = dblst.snap_id AND tbllst.datid = dblst.datid AND tbllst.relid = lst.relid)
              -- Join main table if index is toast index
              LEFT OUTER JOIN last_stat_tables mtbl ON (tblcur.relkind = 't' AND mtbl.node_id = dbcur.node_id AND mtbl.snap_id = dbcur.snap_id
                AND mtbl.datid = dbcur.datid AND mtbl.reltoastrelid = tblcur.relid)
              -- Join toast table if exists
              LEFT OUTER JOIN last_stat_tables ttbl ON (ttbl.relkind = 't' AND ttbl.node_id = dbcur.node_id AND ttbl.snap_id = dbcur.snap_id
                AND ttbl.datid = dbcur.datid AND tblcur.reltoastrelid = ttbl.relid)
            WHERE cur.snap_id = s_id AND cur.node_id = snode_id) diff
        WHERE grow_rank <= topn OR read_rank <= topn OR gets_rank <= topn
          OR (grow_unused_rank <= topn AND idx_scan = 0)
    LOOP
        -- Insert TOAST table (if exists) in tables list before parent table
        IF qres.trelid IS NOT NULL THEN
          INSERT INTO tables_list VALUES (qres.node_id,qres.datid,qres.trelid,qres.trelkind,NULLIF(qres.treltoastrelid,0),qres.tschemaname,qres.trelname) ON CONFLICT DO NOTHING;
        END IF;
        -- Insert index parent table in tables list
        INSERT INTO tables_list VALUES (qres.node_id,qres.datid,qres.relid,qres.relkind,NULLIF(qres.reltoastrelid,0),qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        -- Insert main table (if index is on toast table)
        IF qres.mrelid IS NOT NULL THEN
          INSERT INTO tables_list VALUES (qres.node_id,qres.datid,qres.mrelid,qres.mrelkind,NULLIF(qres.mreltoastrelid,0),qres.mschemaname,qres.mrelname) ON CONFLICT DO NOTHING;
        END IF;
        -- insert index to index list
        INSERT INTO indexes_list VALUES (qres.node_id,qres.datid,qres.indexrelid,qres.relid,qres.schemaname,qres.indexrelname) ON CONFLICT DO NOTHING;
        -- insert index stats
        INSERT INTO snap_stat_indexes VALUES (
            qres.node_id,
            qres.snap_id,
            qres.datid,
            qres.indexrelid,
            qres.tablespaceid,
            qres.idx_scan,
            qres.idx_tup_read,
            qres.idx_tup_fetch,
            qres.idx_blks_read,
            qres.idx_blks_hit,
            qres.relsize,
            qres.relsize_diff,
            qres.indisunique
        );
        -- insert underlying table stats
        INSERT INTO snap_stat_tables VALUES (
            qres.node_id,
            qres.snap_id,
            qres.datid,
            qres.relid,
            qres.reltablespaceid,
            qres.tbl_seq_scan,
            qres.tbl_seq_tup_read,
            qres.tbl_idx_scan,
            qres.tbl_idx_tup_fetch,
            qres.tbl_n_tup_ins,
            qres.tbl_n_tup_upd,
            qres.tbl_n_tup_del,
            qres.tbl_n_tup_hot_upd,
            qres.tbl_n_live_tup,
            qres.tbl_n_dead_tup,
            qres.tbl_n_mod_since_analyze,
            qres.tbl_last_vacuum,
            qres.tbl_last_autovacuum,
            qres.tbl_last_analyze,
            qres.tbl_last_autoanalyze,
            qres.tbl_vacuum_count,
            qres.tbl_autovacuum_count,
            qres.tbl_analyze_count,
            qres.tbl_autoanalyze_count,
            qres.tbl_heap_blks_read,
            qres.tbl_heap_blks_hit,
            qres.tbl_idx_blks_read,
            qres.tbl_idx_blks_hit,
            qres.tbl_toast_blks_read,
            qres.tbl_toast_blks_hit,
            qres.tbl_tidx_blks_read,
            qres.tbl_tidx_blks_hit,
            qres.tbl_relsize,
            qres.tbl_relsize_diff
        ) ON CONFLICT DO NOTHING;
    END LOOP;

    -- Total indexes stats
    INSERT INTO snap_stat_indexes_total
    SELECT
      cur.node_id,
      cur.snap_id,
      cur.datid,
      cur.tablespaceid,
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_read - COALESCE(lst.idx_tup_read,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      sum(cur.relsize_diff - COALESCE(lst.relsize_diff,0))
    FROM last_stat_indexes cur JOIN snap_stat_database dbcur USING (node_id, snap_id, datid)
      LEFT OUTER JOIN snap_stat_database dblst ON
        (dbcur.node_id = dblst.node_id AND dbcur.datid = dblst.datid AND dblst.snap_id = dbcur.snap_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
      LEFT OUTER JOIN last_stat_indexes lst
        ON (lst.node_id = dblst.node_id AND lst.snap_id = dblst.snap_id AND lst.datid = dblst.datid AND lst.relid = cur.relid AND lst.indexrelid = cur.indexrelid AND cur.tablespaceid=lst.tablespaceid)
    WHERE cur.snap_id = s_id
    GROUP BY cur.node_id, cur.snap_id, cur.datid,cur.tablespaceid;

    -- User functions stats
    FOR qres IN
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
        FROM
            (SELECT
                cur.node_id,
                cur.snap_id,
                cur.datid,
                cur.funcid,
                cur.schemaname,
                cur.funcname,
                cur.funcargs,
                cur.calls - COALESCE(lst.calls,0) AS calls,
                cur.total_time - COALESCE(lst.total_time,0) AS total_time,
                cur.self_time - COALESCE(lst.self_time,0) AS self_time,
                row_number() OVER (ORDER BY cur.total_time - COALESCE(lst.total_time,0) DESC) time_rank,
                row_number() OVER (ORDER BY cur.self_time - COALESCE(lst.self_time,0) DESC) stime_rank,
                row_number() OVER (ORDER BY cur.calls - COALESCE(lst.calls,0) DESC) calls_rank
            FROM last_stat_user_functions cur JOIN snap_stat_database dbcur USING (node_id, snap_id, datid)
              LEFT OUTER JOIN snap_stat_database dblst ON
                (dbcur.node_id = dblst.node_id AND dbcur.datid = dblst.datid AND dblst.snap_id = dbcur.snap_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
              LEFT OUTER JOIN last_stat_user_functions lst ON
                (lst.node_id = dblst.node_id AND lst.snap_id = dblst.snap_id AND lst.datid = dblst.datid AND cur.funcid=lst.funcid)
            WHERE cur.snap_id = s_id AND cur.node_id = snode_id
                AND cur.calls - COALESCE(lst.calls,0) > 0) diff
        WHERE time_rank <= topn OR calls_rank <= topn OR stime_rank <= topn
    LOOP
        INSERT INTO funcs_list VALUES (qres.node_id,qres.datid,qres.funcid,qres.schemaname,qres.funcname,qres.funcargs) ON CONFLICT DO NOTHING;
        INSERT INTO snap_stat_user_functions VALUES (
            qres.node_id,
            qres.snap_id,
            qres.datid,
            qres.funcid,
            qres.calls,
            qres.total_time,
            qres.self_time
        );
    END LOOP;

    -- Total functions stats
    INSERT INTO snap_stat_user_func_total
    SELECT
      cur.node_id,
      cur.snap_id,
      cur.datid,
      sum(cur.calls - COALESCE(lst.calls,0)),
      sum(cur.self_time - COALESCE(lst.self_time,0))
    FROM last_stat_user_functions cur JOIN snap_stat_database dbcur USING (node_id, snap_id, datid)
      LEFT OUTER JOIN snap_stat_database dblst ON
        (dbcur.node_id = dblst.node_id AND dbcur.datid = dblst.datid AND dblst.snap_id = dbcur.snap_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.node_id = dblst.node_id AND lst.snap_id = dblst.snap_id AND lst.datid = dblst.datid AND cur.funcid=lst.funcid)
    WHERE cur.snap_id = s_id
    GROUP BY cur.node_id, cur.snap_id, cur.datid;

    -- Clear data in last_ tables, holding data only for next diff snapshot
    DELETE FROM last_stat_tables WHERE node_id=snode_id AND snap_id != s_id;

    DELETE FROM last_stat_indexes WHERE node_id=snode_id AND snap_id != s_id;

    DELETE FROM last_stat_user_functions WHERE node_id=snode_id AND snap_id != s_id;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION snapshot_show(IN node name,IN days integer = NULL)
RETURNS TABLE(
    snapshot integer,
    snapshot_time timestamp (0) with time zone,
    dbstats_reset timestamp (0) with time zone,
    clustats_reset timestamp (0) with time zone)
SET search_path=@extschema@,public AS $$
  SELECT
    s.snap_id,
    s.snap_time,
    max(nullif(db1.stats_reset,coalesce(db2.stats_reset,db1.stats_reset))) AS dbstats_reset,
    max(nullif(clu1.stats_reset,coalesce(clu2.stats_reset,clu1.stats_reset))) AS clustats_reset
  FROM snapshots s JOIN nodes n USING (node_id)
    JOIN snap_stat_database db1 USING (node_id,snap_id)
    JOIN snap_stat_cluster clu1 USING (node_id,snap_id)
    LEFT OUTER JOIN snap_stat_database db2 ON (db1.node_id = db2.node_id AND db1.datid = db2.datid AND db2.snap_id = db1.snap_id - 1)
    LEFT OUTER JOIN snap_stat_cluster clu2 ON (clu1.node_id = clu2.node_id AND clu2.snap_id = clu1.snap_id - 1)
  WHERE (days IS NULL OR s.snap_time > now() - (days || ' days')::interval)
    AND node_name = node
  GROUP BY s.snap_id, s.snap_time
  ORDER BY s.snap_id ASC
$$ LANGUAGE sql;
COMMENT ON FUNCTION snapshot_show(IN node name,IN days integer) IS 'Display available node snapshots';

CREATE OR REPLACE FUNCTION snapshot_show(IN days integer = NULL)
RETURNS TABLE(
    snapshot integer,
    snapshot_time timestamp (0) with time zone,
    dbstats_reset timestamp (0) with time zone,
    clustats_reset timestamp (0) with time zone)
SET search_path=@extschema@,public AS $$
    SELECT * FROM snapshot_show('local',days);
$$ LANGUAGE sql;
COMMENT ON FUNCTION snapshot_show(IN days integer) IS 'Display available snapshots for local node';
