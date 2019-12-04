\echo Use "ALTER EXTENSION pg_profile UPDATE" to load this file. \quit

ALTER TABLE bl_snaps DROP CONSTRAINT bl_snaps_pk;

ALTER TABLE bl_snaps ADD CONSTRAINT bl_snaps_pk PRIMARY KEY (node_id, bl_id, snap_id);

/* Drop all previous functions and create new functions */
DO LANGUAGE plpgsql 
$$DECLARE
    func_drop_sql   record;
BEGIN
FOR func_drop_sql IN (SELECT 'drop function '||proc.pronamespace::regnamespace||'.'||proc.proname||'('||pg_get_function_identity_arguments(proc.oid)||');' AS query
    FROM pg_depend dep 
        JOIN pg_extension ext ON (dep.refobjid = ext.oid)
        JOIN pg_proc proc ON (proc.oid = dep.objid)
    WHERE ext.extname='pg_profile' AND dep.deptype='e' AND dep.classid='pg_proc'::regclass)
LOOP
    EXECUTE func_drop_sql.query;
END LOOP;
END$$;

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

CREATE OR REPLACE FUNCTION nodata_wrapper(IN section_text text) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    IF section_text IS NULL OR section_text = '' THEN
        RETURN '<p>No data in this section</p>';
    ELSE
        RETURN section_text;
    END IF;
END;
$$ LANGUAGE plpgsql;

/* ========= Snapshot functions ========= */

CREATE OR REPLACE FUNCTION snapshot(IN snode_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
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
        WHEN OTHERS THEN RAISE 'Can''t get lock on nodes table. Is there another snapshot() function running?';
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
    -- Setting lock_timout prevents hanging of snapshot() call due to DDL in long transaction
    PERFORM dblink('node_connection','SET lock_timeout=3000');
    
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

    -- collect databases objects stats
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
    faillog     text := '';
    etext       text := '';
    edetail     text := '';
    econtext     text := '';
    
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
            WHEN OTHERS THEN 
                BEGIN
                    GET STACKED DIAGNOSTICS etext = MESSAGE_TEXT,
                        edetail = PG_EXCEPTION_DETAIL,
                        econtext = PG_EXCEPTION_CONTEXT;
                    faillog := faillog || format (E'Node: %s\n%s\n%s\n%s\n', r_result.node_id, etext, econtext, edetail);
                END;
        END;
    END LOOP;
    IF faillog != '' THEN
        RETURN faillog;
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
    SELECT datid,datname FROM dblink('node_connection',
    'select dbs.oid,dbs.datname from pg_catalog.pg_database dbs
    where dbs.datname not like ''template_'' and dbs.datallowconn') AS dbl (
        datid oid,
        datname name
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
      -- Setting lock_timout prevents hanging of snapshot() call due to DDL in long transaction
      PERFORM dblink('node_db_connection','SET lock_timeout=3000');
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
                row_number() OVER (ORDER BY (t.seq_scan-l.seq_scan)*t.relsize desc) scan_rank, --weighted scans
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

CREATE OR REPLACE FUNCTION baseline_new(IN name varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    RETURN baseline_new('local',name,start_id,end_id,days);
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

CREATE OR REPLACE FUNCTION baseline_drop(IN name varchar(25)) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    RETURN baseline_drop('local',name);
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

CREATE OR REPLACE FUNCTION baseline_keep(IN name varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    RETURN baseline_keep('local',name,days);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_show(IN node name = 'local') RETURNS TABLE(baseline varchar(25), min_snap integer, max_snap integer, keep_until_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
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

/* ========= Cluster report functions ========= */

CREATE OR REPLACE FUNCTION dbstats(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) 
RETURNS TABLE(
    datid oid,
    dbname name,
    xact_commit     bigint,
    xact_rollback   bigint,
    blks_read       bigint,
    blks_hit        bigint,
    tup_returned    bigint,
    tup_fetched     bigint,
    tup_inserted    bigint,
    tup_updated     bigint,
    tup_deleted     bigint,
    temp_files      bigint,
    temp_bytes      bigint,
    datsize_delta   bigint,
    deadlocks       bigint,
    blks_hit_pct    double precision)
SET search_path=@extschema@,public AS $$
    SELECT
        datid,
        datname as dbname,
        sum(xact_commit)::bigint as xact_commit,
        sum(xact_rollback)::bigint as xact_rollback,
        sum(blks_read)::bigint as blks_read,
        sum(blks_hit)::bigint as blks_hit,
        sum(tup_returned)::bigint as tup_returned,
        sum(tup_fetched)::bigint as tup_fetched,
        sum(tup_inserted)::bigint as tup_inserted,
        sum(tup_updated)::bigint as tup_updated,
        sum(tup_deleted)::bigint as tup_deleted,
        sum(temp_files)::bigint as temp_files,
        sum(temp_bytes)::bigint as temp_bytes,
        sum(datsize_delta)::bigint as datsize_delta,
        sum(deadlocks)::bigint as deadlocks, 
        sum(blks_hit)*100/GREATEST(sum(blks_hit)+sum(blks_read),1)::double precision as blks_hit_pct
    FROM snap_stat_database
    WHERE node_id = snode_id AND datname not like 'template_' and snap_id between start_id + 1 and end_id
    GROUP BY datid,datname
    HAVING max(stats_reset)=min(stats_reset);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION dbstats_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Commits</th><th>Rollbacks</th><th>BlkHit%(read/hit)</th><th>Tup Ret/Fet</th><th>Tup Ins</th><th>Tup Del</th><th>Temp Size(Files)</th><th>Growth</th><th>Deadlocks</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s%%(%s/%s)</td><td>%s/%s</td><td>%s</td><td>%s</td><td>%s(%s)</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT 
        dbname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        temp_files,
        pg_size_pretty(temp_bytes) as temp_bytes,
        pg_size_pretty(datsize_delta) as datsize_delta,
        deadlocks, 
        blks_hit_pct
    FROM dbstats(snode_id,start_id,end_id,topn);

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
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


CREATE OR REPLACE FUNCTION dbstats_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
   IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>I</th><th>Commits</th><th>Rollbacks</th><th>BlkHit%(read/hit)</th><th>Tup Ret/Fet</th><th>Tup Ins</th><th>Tup Del</th><th>Temp Size(Files)</th><th>Growth</th><th>Deadlocks</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s%%(%s/%s)</td><td>%s/%s</td><td>%s</td><td>%s</td><td>%s(%s)</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s%%(%s/%s)</td><td>%s/%s</td><td>%s</td><td>%s</td><td>%s(%s)</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT 
        COALESCE(dbs1.dbname,dbs2.dbname) as dbname,
        dbs1.xact_commit as xact_commit1,
        dbs1.xact_rollback as xact_rollback1,
        dbs1.blks_read as blks_read1,
        dbs1.blks_hit as blks_hit1,
        dbs1.tup_returned as tup_returned1,
        dbs1.tup_fetched as tup_fetched1,
        dbs1.tup_inserted as tup_inserted1,
        dbs1.tup_updated as tup_updated1,
        dbs1.tup_deleted as tup_deleted1,
        dbs1.temp_files as temp_files1,
        pg_size_pretty(dbs1.temp_bytes) as temp_bytes1,
        pg_size_pretty(dbs1.datsize_delta) as datsize_delta1,
        dbs1.deadlocks as deadlocks1,
        dbs1.blks_hit_pct as blks_hit_pct1,
        dbs2.xact_commit as xact_commit2,
        dbs2.xact_rollback as xact_rollback2,
        dbs2.blks_read as blks_read2,
        dbs2.blks_hit as blks_hit2,
        dbs2.tup_returned as tup_returned2,
        dbs2.tup_fetched as tup_fetched2,
        dbs2.tup_inserted as tup_inserted2,
        dbs2.tup_updated as tup_updated2,
        dbs2.tup_deleted as tup_deleted2,
        dbs2.temp_files as temp_files2,
        pg_size_pretty(dbs2.temp_bytes) as temp_bytes2,
        pg_size_pretty(dbs2.datsize_delta) as datsize_delta2,
        dbs2.deadlocks as deadlocks2,
        dbs2.blks_hit_pct as blks_hit_pct2
    FROM dbstats(snode_id,start1_id,end1_id,topn) dbs1 full outer join dbstats(snode_id,start2_id,end2_id,topn) dbs2
        USING (datid);

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.xact_commit1,
            r_result.xact_rollback1,
            round(CAST(r_result.blks_hit_pct1 AS numeric),2),
            r_result.blks_read1,
            r_result.blks_hit1,
            r_result.tup_returned1,
            r_result.tup_fetched1,
            r_result.tup_inserted1,
            r_result.tup_deleted1,
            r_result.temp_bytes1,
            r_result.temp_files1,
            r_result.datsize_delta1,
            r_result.deadlocks1,
            r_result.xact_commit2,
            r_result.xact_rollback2,
            round(CAST(r_result.blks_hit_pct2 AS numeric),2),
            r_result.blks_read2,
            r_result.blks_hit2,
            r_result.tup_returned2,
            r_result.tup_fetched2,
            r_result.tup_inserted2,
            r_result.tup_deleted2,
            r_result.temp_bytes2,
            r_result.temp_files2,
            r_result.datsize_delta2,
            r_result.deadlocks2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
/* ========= Statement stats functions ========= */

CREATE OR REPLACE FUNCTION statements_stats(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) 
RETURNS TABLE(
        dbname name,
        datid oid,
        calls bigint,
        total_time double precision,
        shared_gets bigint,
        local_gets bigint,
        shared_blks_dirtied bigint,
        local_blks_dirtied bigint,
        temp_blks_read bigint,
        temp_blks_written bigint,
        local_blks_read bigint,
        local_blks_written bigint,
        statements bigint
)
SET search_path=@extschema@,public AS $$
    SELECT 
        db_s.datname AS dbname,
        db_s.datid AS datid,
        sum(st.calls)::bigint AS calls,
        sum(st.total_time)/1000::double precision AS total_time,
        sum(st.shared_blks_hit + st.shared_blks_read)::bigint AS shared_gets,
        sum(st.local_blks_hit + st.local_blks_read)::bigint AS local_gets,
        sum(st.shared_blks_dirtied)::bigint AS shared_blks_dirtied,
        sum(st.local_blks_dirtied)::bigint AS local_blks_dirtied,
        sum(st.temp_blks_read)::bigint AS temp_blks_read,
        sum(st.temp_blks_written)::bigint AS temp_blks_written,
        sum(st.local_blks_read)::bigint AS local_blks_read,
        sum(st.local_blks_written)::bigint AS local_blks_written,
        sum(st.statements)::bigint AS statements
    FROM snap_statements_total st 
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.datid=st.dbid AND db_s.node_id=st.node_id AND db_s.snap_id=start_id)
        JOIN snap_stat_database db_e ON (db_e.datid=st.dbid AND db_e.node_id=st.node_id AND db_e.snap_id=end_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.datname, db_s.datid;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION statements_stats_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Calls</th><th>Total time(s)</th><th>Shared gets</th><th>Local gets</th><th>Shared dirtied</th><th>Local dirtied</th><th>Work_r (blk)</th><th>Work_w (blk)</th><th>Local_r (blk)</th><th>Local_w (blk)</th><th>Statements</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT 
        COALESCE(dbname,'Total') as dbname_t,
        sum(calls) as calls,
        sum(total_time) as total_time,
        sum(shared_gets) as shared_gets,
        sum(local_gets) as local_gets,
        sum(shared_blks_dirtied) as shared_blks_dirtied,
        sum(local_blks_dirtied) as local_blks_dirtied,
        sum(temp_blks_read) as temp_blks_read,
        sum(temp_blks_written) as temp_blks_written,
        sum(local_blks_read) as local_blks_read,
        sum(local_blks_written) as local_blks_written,
        sum(statements) as statements
    FROM statements_stats(snode_id,start_id,end_id,topn)
    GROUP BY ROLLUP(dbname)
    ORDER BY dbname NULLS LAST;

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname_t,
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

CREATE OR REPLACE FUNCTION statements_stats_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>I</th><th>Calls</th><th>Total time(s)</th><th>Shared gets</th><th>Local gets</th><th>Shared dirtied</th><th>Local dirtied</th><th>Work_r (blk)</th><th>Work_w (blk)</th><th>Local_r (blk)</th><th>Local_w (blk)</th><th>Statements</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT 
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.calls as calls1,
        st1.total_time as total_time1,
        st1.shared_gets as shared_gets1,
        st1.local_gets as local_gets1,
        st1.shared_blks_dirtied as shared_blks_dirtied1,
        st1.local_blks_dirtied as local_blks_dirtied1,
        st1.temp_blks_read as temp_blks_read1,
        st1.temp_blks_written as temp_blks_written1,
        st1.local_blks_read as local_blks_read1,
        st1.local_blks_written as local_blks_written1,
        st1.statements as statements1,
        st2.calls as calls2,
        st2.total_time as total_time2,
        st2.shared_gets as shared_gets2,
        st2.local_gets as local_gets2,
        st2.shared_blks_dirtied as shared_blks_dirtied2,
        st2.local_blks_dirtied as local_blks_dirtied2,
        st2.temp_blks_read as temp_blks_read2,
        st2.temp_blks_written as temp_blks_written2,
        st2.local_blks_read as local_blks_read2,
        st2.local_blks_written as local_blks_written2,
        st2.statements as statements2
    FROM statements_stats(snode_id,start1_id,end1_id,topn) st1 
        FULL OUTER JOIN statements_stats(snode_id,start2_id,end2_id,topn) st2 USING (datid)
    ORDER BY COALESCE(st1.dbname,st2.dbname);

    c_dbstats_total CURSOR FOR
    SELECT 
        'Total' as dbname,
        sum(st1.calls) as calls1,
        sum(st1.total_time) as total_time1,
        sum(st1.shared_gets) as shared_gets1,
        sum(st1.local_gets) as local_gets1,
        sum(st1.shared_blks_dirtied) as shared_blks_dirtied1,
        sum(st1.local_blks_dirtied) as local_blks_dirtied1,
        sum(st1.temp_blks_read) as temp_blks_read1,
        sum(st1.temp_blks_written) as temp_blks_written1,
        sum(st1.local_blks_read) as local_blks_read1,
        sum(st1.local_blks_written) as local_blks_written1,
        sum(st1.statements) as statements1,
        sum(st2.calls) as calls2,
        sum(st2.total_time) as total_time2,
        sum(st2.shared_gets) as shared_gets2,
        sum(st2.local_gets) as local_gets2,
        sum(st2.shared_blks_dirtied) as shared_blks_dirtied2,
        sum(st2.local_blks_dirtied) as local_blks_dirtied2,
        sum(st2.temp_blks_read) as temp_blks_read2,
        sum(st2.temp_blks_written) as temp_blks_written2,
        sum(st2.local_blks_read) as local_blks_read2,
        sum(st2.local_blks_written) as local_blks_written2,
        sum(st2.statements) as statements2
    FROM statements_stats(snode_id,start1_id,end1_id,topn) st1 
        FULL OUTER JOIN statements_stats(snode_id,start2_id,end2_id,topn) st2 USING (datid);

    r_result RECORD;
BEGIN
    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            r_result.shared_gets1,
            r_result.local_gets1,
            r_result.shared_blks_dirtied1,
            r_result.local_blks_dirtied1,
            r_result.temp_blks_read1,
            r_result.temp_blks_written1,
            r_result.local_blks_read1,
            r_result.local_blks_written1,
            r_result.statements1,
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            r_result.shared_gets2,
            r_result.local_gets2,
            r_result.shared_blks_dirtied2,
            r_result.local_blks_dirtied2,
            r_result.temp_blks_read2,
            r_result.temp_blks_written2,
            r_result.local_blks_read2,
            r_result.local_blks_written2,
            r_result.statements2
        );
    END LOOP;
    FOR r_result IN c_dbstats_total LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            r_result.shared_gets1,
            r_result.local_gets1,
            r_result.shared_blks_dirtied1,
            r_result.local_blks_dirtied1,
            r_result.temp_blks_read1,
            r_result.temp_blks_written1,
            r_result.local_blks_read1,
            r_result.local_blks_written1,
            r_result.statements1,
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            r_result.shared_gets2,
            r_result.local_gets2,
            r_result.shared_blks_dirtied2,
            r_result.local_blks_dirtied2,
            r_result.temp_blks_read2,
            r_result.temp_blks_written2,
            r_result.local_blks_read2,
            r_result.local_blks_written2,
            r_result.statements2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;/* ===== Cluster stats functions ===== */

CREATE OR REPLACE FUNCTION cluster_stats(IN snode_id integer, IN start_id integer, IN end_id integer) 
RETURNS TABLE(
        node_id integer,
        checkpoints_timed bigint,
        checkpoints_req bigint,
        checkpoint_write_time double precision,
        checkpoint_sync_time double precision,
        buffers_checkpoint bigint,
        buffers_clean bigint,
        buffers_backend bigint,
        buffers_backend_fsync bigint,
        maxwritten_clean bigint,
        buffers_alloc bigint,
        wal_size bigint
)
SET search_path=@extschema@,public AS $$
    SELECT
        node_id,
        sum(checkpoints_timed)::bigint as checkpoints_timed,
        sum(checkpoints_req)::bigint as checkpoints_req,
        sum(checkpoint_write_time)::double precision as checkpoint_write_time,
        sum(checkpoint_sync_time)::double precision as checkpoint_sync_time,
        sum(buffers_checkpoint)::bigint as buffers_checkpoint,
        sum(buffers_clean)::bigint as buffers_clean,
        sum(buffers_backend)::bigint as buffers_backend,
        sum(buffers_backend_fsync)::bigint as buffers_backend_fsync,
        sum(maxwritten_clean)::bigint as maxwritten_clean,
        sum(buffers_alloc)::bigint as buffers_alloc,
        sum(wal_size)::bigint as wal_size
    FROM snap_stat_cluster
    WHERE node_id = snode_id AND snap_id between start_id + 1 and end_id
    GROUP BY node_id
    HAVING max(stats_reset)=min(stats_reset);
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION cluster_stats_htbl(IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Metric</th><th>Value</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        buffers_backend,
        buffers_backend_fsync,
        maxwritten_clean,
        buffers_alloc,
        pg_size_pretty(wal_size) as wal_size
    FROM cluster_stats(snode_id,start_id,end_id);

    r_result RECORD;
BEGIN
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats LOOP
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

CREATE OR REPLACE FUNCTION cluster_stats_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Database stats TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Metric</th><th title="{i1_title}">Value (1)</th><th title="{i2_title}">Value (2)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        stat1.checkpoints_timed as checkpoints_timed1,
        stat1.checkpoints_req as checkpoints_req1,
        stat1.checkpoint_write_time as checkpoint_write_time1,
        stat1.checkpoint_sync_time as checkpoint_sync_time1,
        stat1.buffers_checkpoint as buffers_checkpoint1,
        stat1.buffers_clean as buffers_clean1,
        stat1.buffers_backend as buffers_backend1,
        stat1.buffers_backend_fsync as buffers_backend_fsync1,
        stat1.maxwritten_clean as maxwritten_clean1,
        stat1.buffers_alloc as buffers_alloc1,
        pg_size_pretty(stat1.wal_size) as wal_size1,
        stat2.checkpoints_timed as checkpoints_timed2,
        stat2.checkpoints_req as checkpoints_req2,
        stat2.checkpoint_write_time as checkpoint_write_time2,
        stat2.checkpoint_sync_time as checkpoint_sync_time2,
        stat2.buffers_checkpoint as buffers_checkpoint2,
        stat2.buffers_clean as buffers_clean2,
        stat2.buffers_backend as buffers_backend2,
        stat2.buffers_backend_fsync as buffers_backend_fsync2,
        stat2.maxwritten_clean as maxwritten_clean2,
        stat2.buffers_alloc as buffers_alloc2,
        pg_size_pretty(stat2.wal_size) as wal_size2
    FROM cluster_stats(snode_id,start1_id,end1_id) stat1
        FULL OUTER JOIN cluster_stats(snode_id,start2_id,end2_id) stat2 USING (node_id);

    r_result RECORD;
BEGIN
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(row_tpl,'Scheduled checkpoints',r_result.checkpoints_timed1,r_result.checkpoints_timed2);
        report := report||format(row_tpl,'Requested checkpoints',r_result.checkpoints_req1,r_result.checkpoints_req2);
        report := report||format(row_tpl,'Checkpoint write time (s)',
            round(cast(r_result.checkpoint_write_time1/1000 as numeric),2),
            round(cast(r_result.checkpoint_write_time2/1000 as numeric),2));
        report := report||format(row_tpl,'Checkpoint sync time (s)',
            round(cast(r_result.checkpoint_sync_time1/1000 as numeric),2),
            round(cast(r_result.checkpoint_sync_time2/1000 as numeric),2));
        report := report||format(row_tpl,'Checkpoints pages written',r_result.buffers_checkpoint1,r_result.buffers_checkpoint2);
        report := report||format(row_tpl,'Background pages written',r_result.buffers_clean1,r_result.buffers_clean2);
        report := report||format(row_tpl,'Backend pages written',r_result.buffers_backend1,r_result.buffers_backend2);
        report := report||format(row_tpl,'Backend fsync count',r_result.buffers_backend_fsync1,r_result.buffers_backend_fsync2);
        report := report||format(row_tpl,'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean1,r_result.maxwritten_clean2);
        report := report||format(row_tpl,'Number of buffers allocated',r_result.buffers_alloc1,r_result.buffers_alloc2);
        report := report||format(row_tpl,'WAL generated',r_result.wal_size1,r_result.wal_size2);
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
/* ===== Tables stats functions ===== */

CREATE OR REPLACE FUNCTION top_tables(IN snode_id integer, IN start_id integer, IN end_id integer) 
RETURNS TABLE(
    node_id integer,
    dbid oid,
    relid oid,
    dbname name,
    schemaname name,
    relname name,
    seq_scan bigint,
    seq_tup_read bigint,
    seq_scan_page_cnt bigint,
    idx_scan bigint,
    idx_tup_fetch bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint,
    growth bigint
) SET search_path=@extschema@,public AS $$
    SELECT
        st.node_id,
        db_s.datid,
        relid,
        db_s.datname AS dbname,
        schemaname,
        relname,
        sum(seq_scan)::bigint AS seq_scan,
        sum(seq_tup_read)::bigint AS seq_tup_read,
        sum(seq_scan * (relsize / current_setting('block_size')::double precision))::bigint seq_scan_page_cnt,
        sum(idx_scan)::bigint AS idx_scan,
        sum(idx_tup_fetch)::bigint AS idx_tup_fetch,
        sum(n_tup_ins)::bigint AS n_tup_ins,
        (sum(n_tup_upd)-sum(n_tup_hot_upd))::bigint AS n_tup_upd,
        sum(n_tup_del)::bigint AS n_tup_del,
        sum(n_tup_hot_upd)::bigint AS n_tup_hot_upd,
        sum(vacuum_count)::bigint AS vacuum_count,
        sum(autovacuum_count)::bigint AS autovacuum_count,
        sum(analyze_count)::bigint AS analyze_count,
        sum(autoanalyze_count)::bigint AS autoanalyze_count,
        sum(st.relsize_diff)::bigint AS growth
    FROM v_snap_stat_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=start_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=end_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.node_id,db_s.datid,relid,db_s.datname,schemaname,relname
$$ LANGUAGE SQL;

/* ===== Objects report functions ===== */
CREATE OR REPLACE FUNCTION top_scan_tables_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>SeqScan</th><th>SeqPages</th><th>IxScan</th><th>IxFet</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR FOR
    SELECT 
        dbname,
        schemaname,
        relname,
        seq_scan,
        seq_scan_page_cnt,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd
    FROM top_tables(snode_id, start_id, end_id)
    WHERE seq_scan > 0
    ORDER BY seq_scan_page_cnt DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.seq_scan,
            r_result.seq_scan_page_cnt,
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

CREATE OR REPLACE FUNCTION top_scan_tables_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer, 
IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>I</th><th>SeqScan</th><th>SeqPages</th><th>IxScan</th><th>IxFet</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        tbl1.seq_scan as seq_scan1,
        tbl1.seq_scan_page_cnt as seq_scan_page_cnt1,
        tbl1.idx_scan as idx_scan1,
        tbl1.idx_tup_fetch as idx_tup_fetch1,
        tbl1.n_tup_ins as n_tup_ins1,
        tbl1.n_tup_upd as n_tup_upd1,
        tbl1.n_tup_del as n_tup_del1,
        tbl1.n_tup_hot_upd as n_tup_hot_upd1,
        tbl2.seq_scan as seq_scan2,
        tbl2.seq_scan_page_cnt as seq_scan_page_cnt2,
        tbl2.idx_scan as idx_scan2,
        tbl2.idx_tup_fetch as idx_tup_fetch2,
        tbl2.n_tup_ins as n_tup_ins2,
        tbl2.n_tup_upd as n_tup_upd2,
        tbl2.n_tup_del as n_tup_del2,
        tbl2.n_tup_hot_upd as n_tup_hot_upd2,
        row_number() over (ORDER BY COALESCE(tbl1.seq_scan_page_cnt,0) DESC) as rn_seqpg1,
        row_number() over (ORDER BY COALESCE(tbl2.seq_scan_page_cnt,0) DESC) as rn_seqpg2
    FROM top_tables(snode_id, start1_id, end1_id) tbl1
        FULL OUTER JOIN top_tables(snode_id, start2_id, end2_id) tbl2 USING (node_id, dbid, relid)
    WHERE COALESCE(tbl1.seq_scan,tbl2.seq_scan) > 0
    ORDER BY COALESCE(tbl1.seq_scan_page_cnt,0) + COALESCE(tbl2.seq_scan_page_cnt,0) DESC) t1
    WHERE rn_seqpg1 <= topn OR rn_seqpg2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.seq_scan1,
            r_result.seq_scan_page_cnt1,
            r_result.idx_scan1,
            r_result.idx_tup_fetch1,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.seq_scan2,
            r_result.seq_scan_page_cnt2,
            r_result.idx_scan2,
            r_result.idx_tup_fetch2,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2
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
    c_tbl_stats CURSOR FOR
    SELECT 
        dbname,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd
    FROM top_tables(snode_id, start_id, end_id)
    WHERE n_tup_ins+n_tup_upd+n_tup_del+n_tup_hot_upd > 0
    ORDER BY n_tup_ins+n_tup_upd+n_tup_del+n_tup_hot_upd DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
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

CREATE OR REPLACE FUNCTION top_dml_tables_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>I</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th><th>SeqScan</th><th>SeqFet</th><th>IxScan</th><th>IxFet</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        tbl1.seq_scan as seq_scan1,
        tbl1.seq_tup_read as seq_tup_read1,
        tbl1.idx_scan as idx_scan1,
        tbl1.idx_tup_fetch as idx_tup_fetch1,
        tbl1.n_tup_ins as n_tup_ins1,
        tbl1.n_tup_upd as n_tup_upd1,
        tbl1.n_tup_del as n_tup_del1,
        tbl1.n_tup_hot_upd as n_tup_hot_upd1,
        tbl2.seq_scan as seq_scan2,
        tbl2.seq_tup_read as seq_tup_read2,
        tbl2.idx_scan as idx_scan2,
        tbl2.idx_tup_fetch as idx_tup_fetch2,
        tbl2.n_tup_ins as n_tup_ins2,
        tbl2.n_tup_upd as n_tup_upd2,
        tbl2.n_tup_del as n_tup_del2,
        tbl2.n_tup_hot_upd as n_tup_hot_upd2,
        row_number() OVER (ORDER BY COALESCE(tbl1.n_tup_ins + tbl1.n_tup_upd + tbl1.n_tup_del + tbl1.n_tup_hot_upd,0) DESC) rn_dml1,
        row_number() OVER (ORDER BY COALESCE(tbl2.n_tup_ins + tbl2.n_tup_upd + tbl2.n_tup_del + tbl2.n_tup_hot_upd,0) DESC) rn_dml2
    FROM top_tables(snode_id, start1_id, end1_id) tbl1
        FULL OUTER JOIN top_tables(snode_id, start2_id, end2_id) tbl2 USING (node_id, dbid, relid)
    WHERE COALESCE(tbl1.n_tup_ins + tbl1.n_tup_upd + tbl1.n_tup_del + tbl1.n_tup_hot_upd,
        tbl2.n_tup_ins + tbl2.n_tup_upd + tbl2.n_tup_del + tbl2.n_tup_hot_upd) > 0
    ORDER BY COALESCE(tbl1.n_tup_ins + tbl1.n_tup_upd + tbl1.n_tup_del + tbl1.n_tup_hot_upd,0) +
          COALESCE(tbl2.n_tup_ins + tbl2.n_tup_upd + tbl2.n_tup_del + tbl2.n_tup_hot_upd,0) DESC) t1
    WHERE rn_dml1 <= topn OR rn_dml2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.seq_scan1,
            r_result.seq_tup_read1,
            r_result.idx_scan1,
            r_result.idx_tup_fetch1,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2,
            r_result.seq_scan2,
            r_result.seq_tup_read2,
            r_result.idx_scan2,
            r_result.idx_tup_fetch2
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
    c_tbl_stats CURSOR FOR
    SELECT 
        dbname,
        schemaname,
        relname,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count
    FROM top_tables(snode_id, start_id, end_id)
    WHERE n_tup_upd+n_tup_del+n_tup_hot_upd > 0
    ORDER BY n_tup_upd+n_tup_del+n_tup_hot_upd DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
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

CREATE OR REPLACE FUNCTION top_upd_vac_tables_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer, 
IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>I</th><th>Upd</th><th>Upd(HOT)</th><th>Del</th><th>Vacuum</th><th>AutoVacuum</th><th>Analyze</th><th>AutoAnalyze</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT 
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        tbl1.n_tup_upd as n_tup_upd1,
        tbl1.n_tup_del as n_tup_del1,
        tbl1.n_tup_hot_upd as n_tup_hot_upd1,
        tbl1.vacuum_count as vacuum_count1,
        tbl1.autovacuum_count as autovacuum_count1,
        tbl1.analyze_count as analyze_count1,
        tbl1.autoanalyze_count as autoanalyze_count1,
        tbl2.n_tup_upd as n_tup_upd2,
        tbl2.n_tup_del as n_tup_del2,
        tbl2.n_tup_hot_upd as n_tup_hot_upd2,
        tbl2.vacuum_count as vacuum_count2,
        tbl2.autovacuum_count as autovacuum_count2,
        tbl2.analyze_count as analyze_count2,
        tbl2.autoanalyze_count as autoanalyze_count2,
        row_number() OVER (ORDER BY COALESCE(tbl1.n_tup_upd + tbl1.n_tup_del + tbl1.n_tup_hot_upd,0) DESC) as rn_vactpl1,
        row_number() OVER (ORDER BY COALESCE(tbl2.n_tup_upd + tbl2.n_tup_del + tbl2.n_tup_hot_upd,0) DESC) as rn_vactpl2
    FROM top_tables(snode_id, start1_id, end1_id) tbl1
        FULL OUTER JOIN top_tables(snode_id, start2_id, end2_id) tbl2 USING (node_id, dbid, relid)
    WHERE COALESCE(tbl1.n_tup_upd + tbl1.n_tup_del + tbl1.n_tup_hot_upd,
            tbl2.n_tup_upd + tbl2.n_tup_del + tbl2.n_tup_hot_upd) > 0
    ORDER BY COALESCE(tbl1.n_tup_upd + tbl1.n_tup_del + tbl1.n_tup_hot_upd,0) +
          COALESCE(tbl2.n_tup_upd + tbl2.n_tup_del + tbl2.n_tup_hot_upd,0) DESC) t1
    WHERE rn_vactpl1 <= topn OR rn_vactpl2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_upd1,
            r_result.n_tup_hot_upd1,
            r_result.n_tup_del1,
            r_result.vacuum_count1,
            r_result.autovacuum_count1,
            r_result.analyze_count1,
            r_result.autoanalyze_count1,
            r_result.n_tup_upd2,
            r_result.n_tup_hot_upd2,
            r_result.n_tup_del2,
            r_result.vacuum_count2,
            r_result.autovacuum_count2,
            r_result.analyze_count2,
            r_result.autoanalyze_count2
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
    c_tbl_stats CURSOR FOR
    SELECT 
        dbname,
        top.schemaname,
        top.relname,
        top.seq_scan,
        top.seq_tup_read,
        top.idx_scan,
        top.idx_tup_fetch,
        top.n_tup_ins,
        top.n_tup_upd,
        top.n_tup_del,
        top.n_tup_hot_upd,
        pg_size_pretty(top.growth) AS growth,
        pg_size_pretty(st_last.relsize) AS relsize
    FROM top_tables(snode_id, start_id, end_id) top
        JOIN v_snap_stat_user_tables st_last USING (node_id,dbid,relid)
    WHERE st_last.snap_id=end_id AND top.growth > 0
    ORDER BY top.growth DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
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

CREATE OR REPLACE FUNCTION top_growth_tables_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer, 
    IN start2_id integer, IN end2_id integer, IN topn integer)
RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>I</th><th>Size</th><th>Growth</th><th>Ins</th><th>Upd</th><th>Del</th><th>Upd(HOT)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT 
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        tbl1.seq_scan as seq_scan1,
        tbl1.seq_tup_read as seq_tup_read1,
        tbl1.idx_scan as idx_scan1,
        tbl1.idx_tup_fetch as idx_tup_fetch1,
        tbl1.n_tup_ins as n_tup_ins1,
        tbl1.n_tup_upd as n_tup_upd1,
        tbl1.n_tup_del as n_tup_del1,
        tbl1.n_tup_hot_upd as n_tup_hot_upd1,
        pg_size_pretty(tbl1.growth) AS growth1,
        pg_size_pretty(st_last1.relsize) AS relsize1,
        tbl2.seq_scan as seq_scan2,
        tbl2.seq_tup_read as seq_tup_read2,
        tbl2.idx_scan as idx_scan2,
        tbl2.idx_tup_fetch as idx_tup_fetch2,
        tbl2.n_tup_ins as n_tup_ins2,
        tbl2.n_tup_upd as n_tup_upd2,
        tbl2.n_tup_del as n_tup_del2,
        tbl2.n_tup_hot_upd as n_tup_hot_upd2,
        pg_size_pretty(tbl2.growth) AS growth2,
        pg_size_pretty(st_last2.relsize) AS relsize2,
        row_number() OVER (ORDER BY COALESCE(tbl1.growth,0) DESC) as rn_growth1,
        row_number() OVER (ORDER BY COALESCE(tbl2.growth,0) DESC) as rn_growth2
    FROM top_tables(snode_id, start1_id, end1_id) tbl1
        FULL OUTER JOIN top_tables(snode_id, start2_id, end2_id) tbl2 USING (node_id,dbid,relid)
        LEFT OUTER JOIN v_snap_stat_user_tables st_last1 ON (tbl1.node_id = st_last1.node_id AND tbl1.dbid = st_last1.dbid AND tbl1.relid = st_last1.relid AND st_last1.snap_id=end1_id)
        LEFT OUTER JOIN v_snap_stat_user_tables st_last2 ON (tbl2.node_id = st_last2.node_id AND tbl2.dbid = st_last2.dbid AND tbl2.relid = st_last2.relid AND st_last2.snap_id=end2_id)
    WHERE COALESCE(tbl1.growth, tbl2.growth) > 0
    ORDER BY COALESCE(tbl1.growth,0) + COALESCE(tbl2.growth,0) DESC) t1
    WHERE rn_growth1 <= topn OR rn_growth2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.relsize1,
            r_result.growth1,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.relsize2,
            r_result.growth2,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
/* ===== Indexes stats functions ===== */

CREATE OR REPLACE FUNCTION top_indexes(IN snode_id integer, IN start_id integer, IN end_id integer) 
RETURNS TABLE(
    node_id integer,
    dbid oid,
    relid oid,
    indexrelid oid,
    dbname name,
    schemaname name,
    relname name,
    indexrelname name,
    idx_scan bigint,
    growth bigint
)
SET search_path=@extschema@,public AS $$
    SELECT
        db_s.node_id,
        db_s.datid,
        relid,
        indexrelid,
        db_s.datname,
        st.schemaname,
        st.relname,
        st.indexrelname,
        sum(st.idx_scan)::bigint as idx_scan,
        sum(st.relsize_diff)::bigint as growth
    FROM v_snap_stat_user_indexes st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=start_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=end_id AND db_s.datname=db_e.datname)
    WHERE st.node_id=snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY db_s.node_id,db_s.datid,relid,indexrelid,db_s.datname,st.schemaname,st.relname,st.indexrelname
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION top_growth_indexes_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>Size</th><th>Growth</th><th>Scans</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
    SELECT 
        st.dbname,
        st.schemaname,
        st.relname,
        st.indexrelname,
        st.idx_scan,
        st.growth,
        pg_size_pretty(st_last.relsize) as relsize
    FROM top_indexes(snode_id, start_id, end_id) st
        JOIN v_snap_stat_user_indexes st_last using (node_id,dbid,relid,indexrelid)
    WHERE st_last.snap_id=end_id AND st.growth > 0
    ORDER BY st.growth DESC
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

CREATE OR REPLACE FUNCTION top_growth_indexes_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer, 
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Tables stats template
    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>I</th><th>Size</th><th>Growth</th><th>Scans</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for tables stats
    c_ix_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(ix1.dbname,ix2.dbname) as dbname,
        COALESCE(ix1.schemaname,ix2.schemaname) as schemaname,
        COALESCE(ix1.relname,ix2.relname) as relname,
        COALESCE(ix1.indexrelname,ix2.indexrelname) as indexrelname,
        ix1.idx_scan as idx_scan1,
        ix1.growth as growth1,
        pg_size_pretty(ix_last1.relsize) as relsize1,
        ix2.idx_scan as idx_scan2,
        ix2.growth as growth2,
        pg_size_pretty(ix_last2.relsize) as relsize2,
        row_number() over (ORDER BY COALESCE(ix1.growth,0) DESC) as rn_growth1,
        row_number() over (ORDER BY COALESCE(ix2.growth,0) DESC) as rn_growth2
    FROM top_indexes(snode_id, start1_id, end1_id) ix1
        FULL OUTER JOIN top_indexes(snode_id, start2_id, end2_id) ix2 USING (node_id, dbid, indexrelid)
        LEFT OUTER JOIN v_snap_stat_user_indexes ix_last1 
            ON (ix_last1.snap_id = end1_id AND ix_last1.node_id=ix1.node_id AND ix_last1.dbid = ix1.dbid AND ix_last1.indexrelid = ix1.indexrelid AND ix_last1.relid = ix1.relid)
        LEFT OUTER JOIN v_snap_stat_user_indexes ix_last2
            ON (ix_last2.snap_id = end2_id AND ix_last2.node_id=ix2.node_id AND ix_last2.dbid = ix2.dbid AND ix_last2.indexrelid = ix2.indexrelid AND ix_last2.relid = ix2.relid)
    WHERE COALESCE(ix1.growth,ix2.growth) > 0
    ORDER BY COALESCE(ix1.growth,0) + COALESCE(ix2.growth,0) DESC) t1
    WHERE rn_growth1 <= topn OR rn_growth2 <= topn;
    
    r_result RECORD;
BEGIN
    -- Reporting table stats
    FOR r_result IN c_ix_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            r_result.relsize1,
            r_result.growth1,
            r_result.idx_scan1,
            r_result.relsize2,
            r_result.growth2,
            r_result.idx_scan2
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
/* ===== Top IO objects ===== */

CREATE OR REPLACE FUNCTION top_io_tables(IN snode_id integer, IN start_id integer, IN end_id integer) 
RETURNS TABLE(
    node_id integer,
    dbid oid,
    relid oid,
    dbname name,
    schemaname name,
    relname name,
    heap_blks_read bigint,
    idx_blks_read bigint,
    toast_blks_read bigint,
    tidx_blks_read bigint
) SET search_path=@extschema@,public AS $$
    SELECT
        st.node_id,
        st.dbid,
        st.relid,
        db_s.datname AS dbname,
        st.schemaname,
        st.relname,
        sum(st.heap_blks_read)::bigint AS heap_blks_read,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.toast_blks_read)::bigint AS toast_blks_read,
        sum(st.tidx_blks_read)::bigint AS tidx_blks_read
    FROM v_snap_statio_user_tables st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=start_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=end_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.node_id,st.dbid,st.relid,db_s.datname,st.schemaname,st.relname;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION top_io_indexes(IN snode_id integer, IN start_id integer, IN end_id integer) 
RETURNS TABLE(
    node_id integer,
    dbid oid,
    relid oid,
    dbname name,
    schemaname name,
    relname name,
    indexrelid oid,
    indexrelname name,
    idx_blks_read bigint
) SET search_path=@extschema@,public AS $$

    SELECT 
        st.node_id,
        st.dbid,
        st.relid,
        db_s.datname AS dbname,
        st.schemaname,
        st.relname,
        st.indexrelid,
        st.indexrelname,
        sum(st.idx_blks_read)::bigint AS idx_blks_read
    FROM v_snap_statio_user_indexes st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=start_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=end_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname NOT LIKE 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.node_id,st.dbid,st.relid,db_s.datname,st.schemaname,st.relname,st.indexrelid,st.indexrelname;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION tbl_top_io_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Heap</th><th>Ix</th><th>TOAST</th><th>TOAST-Ix</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        relname,
        heap_blks_read,
        idx_blks_read,
        toast_blks_read,
        tidx_blks_read
    FROM top_io_tables(snode_id,start_id,end_id)
    WHERE heap_blks_read + idx_blks_read + toast_blks_read + tidx_blks_read > 0
    ORDER BY heap_blks_read + idx_blks_read + toast_blks_read + tidx_blks_read DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats LOOP
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

CREATE OR REPLACE FUNCTION tbl_top_io_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer, 
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>I</th><th>Heap</th><th>Ix</th><th>TOAST</th><th>TOAST-Ix</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.schemaname,st2.schemaname) as schemaname,
        COALESCE(st1.relname,st2.relname) as relname,
        st1.heap_blks_read as heap_blks_read1,
        st1.idx_blks_read as idx_blks_read1,
        st1.toast_blks_read as toast_blks_read1,
        st1.tidx_blks_read as tidx_blks_read1,
        st2.heap_blks_read as heap_blks_read2,
        st2.idx_blks_read as idx_blks_read2,
        st2.toast_blks_read as toast_blks_read2,
        st2.tidx_blks_read as tidx_blks_read2,
        row_number() OVER (ORDER BY st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read + st1.tidx_blks_read DESC) rn_read1,
        row_number() OVER (ORDER BY st2.heap_blks_read + st2.idx_blks_read + st2.toast_blks_read + st2.tidx_blks_read DESC) rn_read2
    FROM top_io_tables(snode_id,start1_id,end1_id) st1
        FULL OUTER JOIN top_io_tables(snode_id,start2_id,end2_id) st2 USING (node_id, dbid, relid)
    WHERE COALESCE(st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read + st1.tidx_blks_read,
        st2.heap_blks_read + st2.idx_blks_read + st2.toast_blks_read + st2.tidx_blks_read) > 0 
    ORDER BY COALESCE(st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read + st1.tidx_blks_read,0) +
        COALESCE(st2.heap_blks_read + st2.idx_blks_read + st2.toast_blks_read + st2.tidx_blks_read,0) ) t1
    WHERE rn_read1 <= topn OR rn_read2 <= topn;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.heap_blks_read1,
            r_result.idx_blks_read1,
            r_result.toast_blks_read1,
            r_result.tidx_blks_read1,
            r_result.heap_blks_read2,
            r_result.idx_blks_read2,
            r_result.toast_blks_read2,
            r_result.tidx_blks_read2
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

    c_tbl_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        relname,
        indexrelname,
        idx_blks_read
    FROM top_io_indexes(snode_id,start_id,end_id)
    WHERE idx_blks_read > 0
    ORDER BY idx_blks_read DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats LOOP
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

CREATE OR REPLACE FUNCTION ix_top_io_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer, 
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>I</th><th>Blk Reads</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td></tr>';

    c_tbl_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.dbname,st2.dbname) as dbname,
        COALESCE(st1.schemaname,st2.schemaname) as schemaname,
        COALESCE(st1.relname,st2.relname) as relname,
        COALESCE(st1.indexrelname,st2.indexrelname) as indexrelname,
        st1.idx_blks_read as idx_blks_read1,
        st2.idx_blks_read as idx_blks_read2,
        row_number() OVER (ORDER BY st1.idx_blks_read DESC) as rn_read1,
        row_number() OVER (ORDER BY st2.idx_blks_read DESC) as rn_read2
    FROM
        top_io_indexes(snode_id,start1_id,end1_id) st1
        FULL OUTER JOIN top_io_indexes(snode_id,start2_id,end2_id) st2 USING (node_id, dbid, relid, indexrelid)
    WHERE COALESCE(st1.idx_blks_read, st2.idx_blks_read) > 0
    ORDER BY COALESCE(st1.idx_blks_read,0) + COALESCE(st2.idx_blks_read,0) DESC ) t1
    WHERE rn_read1 <= topn OR rn_read2 <= topn;

    r_result RECORD;
BEGIN
    FOR r_result IN c_tbl_stats LOOP
    report := report||format(
        row_tpl,
        r_result.dbname,
        r_result.schemaname,
        r_result.relname,
        r_result.indexrelname,
        r_result.idx_blks_read1,
        r_result.idx_blks_read2
    );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
/* ===== Function stats functions ===== */

CREATE OR REPLACE FUNCTION top_functions(IN snode_id integer, IN start_id integer, IN end_id integer) 
RETURNS TABLE(
    node_id integer,
    dbid oid,
    funcid oid,
    dbname name,
    schemaname name,
    funcname name,
    calls bigint,
    total_time double precision,
    self_time double precision,
    m_time double precision,
    m_stime double precision
)
SET search_path=@extschema@,public AS $$
    SELECT
        st.node_id,
        st.dbid,
        st.funcid,
        db_s.datname AS dbname,
        st.schemaname,
        st.funcname,
        sum(st.calls)::bigint AS calls,
        sum(st.total_time) AS total_time,
        sum(st.self_time) AS self_time,
        sum(st.total_time)/sum(st.calls) AS m_time,
        sum(st.self_time)/sum(st.calls) AS m_stime
    FROM v_snap_stat_user_functions st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid AND db_s.snap_id=start_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid AND db_e.snap_id=end_id AND db_s.datname=db_e.datname)
    WHERE st.node_id = snode_id AND db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.node_id,st.dbid,st.funcid,db_s.datname,st.schemaname,st.funcname
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION func_top_time_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        calls,
        total_time,
        self_time,
        m_time,
        m_stime
    FROM top_functions(snode_id, start_id, end_id)
    ORDER BY total_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    FOR r_result IN c_fun_stats LOOP
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

CREATE OR REPLACE FUNCTION func_top_time_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>I</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_fun_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        f1.calls as calls1,
        f1.total_time as total_time1,
        f1.self_time as self_time1,
        f1.m_time as m_time1,
        f1.m_stime as m_stime1,
        f2.calls as calls2,
        f2.total_time as total_time2,
        f2.self_time as self_time2,
        f2.m_time as m_time2,
        f2.m_stime as m_stime2,
        row_number() OVER (ORDER BY COALESCE(f1.total_time,0) DESC) as rn_time1,
        row_number() OVER (ORDER BY COALESCE(f2.total_time,0) DESC) as rn_time2
    FROM top_functions(snode_id, start1_id, end1_id) f1
        FULL OUTER JOIN top_functions(snode_id, start2_id, end2_id) f2 USING (node_id, dbid, funcid)
    ORDER BY COALESCE(f1.total_time,0) + COALESCE(f2.total_time,0) DESC) t1
    WHERE rn_time1 <= topn OR rn_time2 <= topn;

    r_result RECORD;
BEGIN
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.funcname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            round(CAST(r_result.self_time1 AS numeric),2),
            round(CAST(r_result.m_time1 AS numeric),3),
            round(CAST(r_result.m_stime1 AS numeric),3),
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            round(CAST(r_result.self_time2 AS numeric),2),
            round(CAST(r_result.m_time2 AS numeric),3),
            round(CAST(r_result.m_stime2 AS numeric),3)
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

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        calls,
        total_time,
        self_time,
        m_time,
        m_stime
    FROM top_functions(snode_id, start_id, end_id)
    ORDER BY calls DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    FOR r_result IN c_fun_stats LOOP
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

CREATE OR REPLACE FUNCTION func_top_calls_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>I</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    c_fun_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        f1.calls as calls1,
        f1.total_time as total_time1,
        f1.self_time as self_time1,
        f1.m_time as m_time1,
        f1.m_stime as m_stime1,
        f2.calls as calls2,
        f2.total_time as total_time2,
        f2.self_time as self_time2,
        f2.m_time as m_time2,
        f2.m_stime as m_stime2,
        row_number() OVER (ORDER BY COALESCE(f1.calls,0) DESC) as rn_calls1,
        row_number() OVER (ORDER BY COALESCE(f2.calls,0) DESC) as rn_calls2
    FROM top_functions(snode_id, start1_id, end1_id) f1
        FULL OUTER JOIN top_functions(snode_id, start2_id, end2_id) f2 USING (node_id, dbid, funcid)
    ORDER BY COALESCE(f1.calls,0) + COALESCE(f2.calls,0) DESC) t1
    WHERE rn_calls1 <= topn OR rn_calls2 <= topn;

    r_result RECORD;
BEGIN
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            row_tpl,
            r_result.dbname,
            r_result.schemaname,
            r_result.funcname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            round(CAST(r_result.self_time1 AS numeric),2),
            round(CAST(r_result.m_time1 AS numeric),3),
            round(CAST(r_result.m_stime1 AS numeric),3),
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            round(CAST(r_result.self_time2 AS numeric),2),
            round(CAST(r_result.m_time2 AS numeric),3),
            round(CAST(r_result.m_stime2 AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(tab_tpl,'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
/* ===== pg_stat_statements checks ===== */

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

CREATE OR REPLACE FUNCTION check_stmt_all_setting(IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
    SELECT count(1)::integer
    FROM snap_params 
    WHERE node_id = snode_id AND p_name = 'pg_stat_statements.track' 
        AND setting = 'all' AND snap_id BETWEEN start_id + 1 AND end_id;
/*    IF snap_cnt > 0 THEN
    RETURN '<p><b>Warning!</b> Report includes '||snap_cnt||' snapshot(s) with setting <i>pg_stat_statements.track = all</i>.'||
    'Value of %Total columns may be incorrect.</p>';
ELSE
    RETURN '';
END IF;*/
$$ LANGUAGE sql;
/* ===== Statements stats functions ===== */

CREATE OR REPLACE FUNCTION top_statements(IN snode_id integer, IN start_id integer, IN end_id integer) 
RETURNS TABLE(
    node_id integer,
    dbid oid,
    dbname name,
    userid oid,
    queryid bigint,
    queryid_md5 char(10),
    query text,
    calls bigint,
    calls_pct float,
    total_time double precision,
    total_time_pct float,
    min_time double precision,
    max_time double precision,
    mean_time double precision,
    stddev_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    gets bigint,
    gets_pct float,
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
    io_time double precision,
    io_time_pct float,
    temp_read_total_pct float,
    temp_write_total_pct float,
    local_read_total_pct float,
    local_write_total_pct float
) SET search_path=@extschema@,public AS $$    
    WITH tot AS (
        SELECT
            GREATEST(sum(total_time),1) AS total_time,
            CASE WHEN sum(blk_read_time) = 0 THEN 1 ELSE sum(blk_read_time) END AS blk_read_time,
            CASE WHEN sum(blk_write_time) = 0 THEN 1 ELSE sum(blk_write_time) END AS blk_write_time,
            GREATEST(sum(shared_blks_hit),1) AS shared_blks_hit,
            GREATEST(sum(shared_blks_read),1) AS shared_blks_read,
            GREATEST(sum(temp_blks_read),1) AS temp_blks_read,
            GREATEST(sum(temp_blks_written),1) AS temp_blks_written,
            GREATEST(sum(local_blks_read),1) AS local_blks_read,
            GREATEST(sum(local_blks_written),1) AS local_blks_written,
            GREATEST(sum(calls),1) AS calls
        FROM snap_statements_total
        WHERE node_id = snode_id AND snap_id BETWEEN start_id + 1 AND end_id)
    SELECT
        st.node_id as node_id,
        db_s.datid as dbid,
        db_s.datname as dbname,
        st.userid as userid,
        st.queryid as queryid,
        st.queryid_md5 as queryid_md5,
        st.query as query,
        sum(st.calls)::bigint as calls,
        sum(st.calls*100/tot.calls)::float as calls_pct,
        sum(st.total_time)/1000 as total_time,
        sum(st.total_time*100/tot.total_time) as total_time_pct,
        min(st.min_time) as min_time,
        max(st.max_time) as max_time,
        sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
        sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
        sum(st.rows)::bigint as rows,
        sum(st.shared_blks_hit)::bigint as shared_blks_hit,
        sum(st.shared_blks_read)::bigint as shared_blks_read,
        (sum(st.shared_blks_hit) + sum(st.shared_blks_read))::bigint as gets,
        (sum(st.shared_blks_hit + st.shared_blks_read)*100/min(tot.shared_blks_read + tot.shared_blks_hit))::float as gets_pct,
        sum(st.shared_blks_dirtied)::bigint as shared_blks_dirtied,
        sum(st.shared_blks_written)::bigint as shared_blks_written,
        sum(st.local_blks_hit)::bigint as local_blks_hit,
        sum(st.local_blks_read)::bigint as local_blks_read,
        sum(st.local_blks_dirtied)::bigint as local_blks_dirtied,
        sum(st.local_blks_written)::bigint as local_blks_written,
        sum(st.temp_blks_read)::bigint as temp_blks_read,
        sum(st.temp_blks_written)::bigint as temp_blks_written,
        sum(st.blk_read_time) as blk_read_time,
        sum(st.blk_write_time) as blk_write_time,
        (sum(st.blk_read_time + st.blk_write_time))/1000 as io_time,
        (sum(st.blk_read_time + st.blk_write_time)*100/min(tot.blk_read_time+tot.blk_write_time)) as io_time_pct,
        sum(st.temp_blks_read*100/tot.temp_blks_read)::float as temp_read_total_pct,
        sum(st.temp_blks_written*100/tot.temp_blks_written)::float as temp_write_total_pct,
        sum(st.local_blks_read*100/tot.local_blks_read)::float as local_read_total_pct,
        sum(st.local_blks_written*100/tot.local_blks_written)::float as local_write_total_pct
    FROM v_snap_statements st
        -- Database name and existance condition
        JOIN snap_stat_database db_s ON (db_s.node_id=st.node_id AND db_s.datid=st.dbid and db_s.snap_id=start_id) 
        JOIN snap_stat_database db_e ON (db_e.node_id=st.node_id AND db_e.datid=st.dbid and db_e.snap_id=end_id and db_s.datname=db_e.datname)
        -- Total stats
        CROSS JOIN tot
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
    GROUP BY st.node_id,db_s.datid,db_s.datname,st.userid,st.queryid,st.queryid_md5,st.query
$$ LANGUAGE SQL;
    

CREATE OR REPLACE FUNCTION top_elapsed_htbl(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Elapsed time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) queries ordered by epapsed time 
    c_elapsed_time CURSOR FOR
    SELECT
        st.queryid_md5 as queryid,
        st.query,
        st.dbname,
        st.calls,
        st.total_time,
        st.total_time_pct,
        st.min_time,
        st.max_time,
        st.mean_time,
        st.stddev_time,
        st.rows
    FROM top_statements(snode_id, start_id, end_id) st
    ORDER BY st.total_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.total_time_pct AS numeric),2),
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

CREATE OR REPLACE FUNCTION top_elapsed_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Elapsed time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>I</th><th>Elapsed(s)</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell"><a HREF="#%s">%s</a></td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) queries ordered by epapsed time 
    c_elapsed_time CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.query,st2.query) as query,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.calls as calls1,
        st1.total_time as total_time1,
        st1.total_time_pct as total_time_pct1,
        st1.min_time as min_time1,
        st1.max_time as max_time1,
        st1.mean_time as mean_time1,
        st1.stddev_time as stddev_time1,
        st1.rows as rows1,
        st2.calls as calls2,
        st2.total_time as total_time2,
        st2.total_time_pct as total_time_pct2,
        st2.min_time as min_time2,
        st2.max_time as max_time2,
        st2.mean_time as mean_time2,
        st2.stddev_time as stddev_time2,
        st2.rows as rows2,
        row_number() over (ORDER BY st1.total_time DESC) as rn_time1,
        row_number() over (ORDER BY st2.total_time DESC) as rn_time2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, dbid, userid, queryid_md5)
    ORDER BY COALESCE(st1.total_time,0) + COALESCE(st2.total_time,0) DESC ) t1
    WHERE rn_time1 <= topn OR rn_time2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            round(CAST(r_result.total_time_pct1 AS numeric),2),
            r_result.rows1,
            round(CAST(r_result.mean_time1 AS numeric),3),
            round(CAST(r_result.min_time1 AS numeric),3),
            round(CAST(r_result.max_time1 AS numeric),3),
            round(CAST(r_result.stddev_time1 AS numeric),3),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            round(CAST(r_result.total_time_pct2 AS numeric),2),
            r_result.rows2,
            round(CAST(r_result.mean_time2 AS numeric),3),
            round(CAST(r_result.min_time2 AS numeric),3),
            round(CAST(r_result.max_time2 AS numeric),3),
            round(CAST(r_result.stddev_time2 AS numeric),3),
            r_result.calls2
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

    -- Cursor for topn querues ordered by executions 
    c_calls CURSOR FOR 
    SELECT
        st.queryid_md5 as queryid,
        st.query,
        st.dbname,
        st.calls,
        st.calls_pct,
        st.total_time,
        st.min_time,
        st.max_time,
        st.mean_time,
        st.stddev_time,
        st.rows
    FROM top_statements(snode_id, start_id, end_id) st
    ORDER BY st.calls DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.calls,
            round(CAST(r_result.calls_pct AS numeric),2),
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

CREATE OR REPLACE FUNCTION top_exec_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer, 
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Executions sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>I</th><th>Executions</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Total(s)</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell"><a HREF="#%s">%s</a></td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    -- Cursor for topn querues ordered by executions 
    c_calls CURSOR FOR 
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.query,st2.query) as query,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.calls as calls1,
        st1.calls_pct as calls_pct1,
        st1.total_time as total_time1,
        st1.min_time as min_time1,
        st1.max_time as max_time1,
        st1.mean_time as mean_time1,
        st1.stddev_time as stddev_time1,
        st1.rows as rows1,
        st2.calls as calls2,
        st2.calls_pct as calls_pct2,
        st2.total_time as total_time2,
        st2.min_time as min_time2,
        st2.max_time as max_time2,
        st2.mean_time as mean_time2,
        st2.stddev_time as stddev_time2,
        st2.rows as rows2,
        row_number() over (ORDER BY st1.calls DESC) as rn_calls1,
        row_number() over (ORDER BY st2.calls DESC) as rn_calls2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, dbid, userid, queryid_md5)
    ORDER BY COALESCE(st1.calls,0) + COALESCE(st2.calls,0) DESC ) t1
    WHERE rn_calls1 <= topn OR rn_calls2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by executions
    FOR r_result IN c_calls LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            r_result.calls1,
            round(CAST(r_result.calls_pct1 AS numeric),2),
            r_result.rows1,
            round(CAST(r_result.mean_time1 AS numeric),3),
            round(CAST(r_result.min_time1 AS numeric),3),
            round(CAST(r_result.max_time1 AS numeric),3),
            round(CAST(r_result.stddev_time1 AS numeric),3),
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.calls2,
            round(CAST(r_result.calls_pct2 AS numeric),2),
            r_result.rows2,
            round(CAST(r_result.mean_time2 AS numeric),3),
            round(CAST(r_result.min_time2 AS numeric),3),
            round(CAST(r_result.max_time2 AS numeric),3),
            round(CAST(r_result.stddev_time2 AS numeric),3),
            round(CAST(r_result.total_time2 AS numeric),1)
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
    c_iowait_time CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.query,
        st.dbname,
        st.total_time,
        st.io_time,
        st.io_time_pct,
        st.shared_blks_read,
        st.shared_blks_written,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE st.io_time > 0
    ORDER BY st.io_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            round(CAST(r_result.io_time AS numeric),3),
            round(CAST(r_result.io_time_pct AS numeric),2),
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

CREATE OR REPLACE FUNCTION top_iowait_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- IOWait time sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>I</th><th>Total(s)</th><th>IO wait(s)</th><th>%Total</th><th>Reads</th><th>Writes</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell"><a HREF="#%s">%s</a></td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by I/O Wait time 
    c_iowait_time CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.query,st2.query) as query,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.calls as calls1,
        st1.total_time as total_time1,
        st1.io_time as io_time1,
        st1.io_time_pct as io_time_pct1,
        st1.shared_blks_read as shared_blks_read1,
        st1.shared_blks_written as shared_blks_written1,
        st2.calls as calls2,
        st2.total_time as total_time2,
        st2.io_time as io_time2,
        st2.io_time_pct as io_time_pct2,
        st2.shared_blks_read as shared_blks_read2,
        st2.shared_blks_written as shared_blks_written2,
        row_number() over (ORDER BY st1.io_time DESC) as rn_iotime1,
        row_number() over (ORDER BY st2.io_time DESC) as rn_iotime2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, dbid, userid, queryid_md5)
    WHERE COALESCE(st1.io_time,st2.io_time) > 0
    ORDER BY COALESCE(st1.io_time,0) + COALESCE(st2.io_time,0) DESC ) t1
    WHERE rn_iotime1 <= topn OR rn_iotime2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting on top 10 queries by I/O wait time
    FOR r_result IN c_iowait_time LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            round(CAST(r_result.io_time1 AS numeric),3),
            round(CAST(r_result.io_time_pct1 AS numeric),2),
            round(CAST(r_result.shared_blks_read1 AS numeric)),
            round(CAST(r_result.shared_blks_written1 AS numeric)),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            round(CAST(r_result.io_time2 AS numeric),3),
            round(CAST(r_result.io_time_pct2 AS numeric),2),
            round(CAST(r_result.shared_blks_read2 AS numeric)),
            round(CAST(r_result.shared_blks_written2 AS numeric)),
            r_result.calls2
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

    --Cursor for top(cnt) queries ordered by gets
    c_gets CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.query,
        st.dbname,
        st.total_time,
        st.rows,
        st.gets,
        st.gets_pct,
        st.shared_blks_hit * 100 / GREATEST(gets,1) as hit_pct,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE gets > 0
    ORDER BY st.gets DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by gets
    FOR r_result IN c_gets LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time AS numeric),1),
            r_result.rows,
            r_result.gets,
            round(CAST(r_result.gets_pct AS numeric),2),
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

CREATE OR REPLACE FUNCTION top_gets_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Gets sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>I</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell"><a HREF="#%s">%s</a></td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) queries ordered by gets
    c_gets CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.query,st2.query) as query,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.gets as gets1,
        st1.gets_pct as gets_pct1,
        st1.shared_blks_hit * 100 / GREATEST(st1.gets,1) as hit_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.gets as gets2,
        st2.gets_pct as gets_pct2,
        st2.shared_blks_hit * 100 / GREATEST(st2.gets,1) as hit_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.gets DESC) as rn_gets1,
        row_number() over (ORDER BY st2.gets DESC) as rn_gets2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, dbid, userid, queryid_md5)
    WHERE COALESCE(st1.gets,st2.gets) > 0
    ORDER BY COALESCE(st1.gets,0) + COALESCE(st2.gets,0) DESC ) t1
    WHERE rn_gets1 <= topn OR rn_gets2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by gets
    FOR r_result IN c_gets LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.gets1,
            round(CAST(r_result.gets_pct1 AS numeric),2),
            round(CAST(r_result.hit_pct1 AS numeric),2),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
            r_result.gets2,
            round(CAST(r_result.gets_pct2 AS numeric),2),
            round(CAST(r_result.hit_pct2 AS numeric),2),
            r_result.calls2
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
    c_temp CURSOR FOR
    SELECT
        st.queryid_md5 AS queryid,
        st.query,
        st.dbname,
        st.total_time,
        st.rows,
        st.gets,
        st.shared_blks_hit * 100 / GREATEST(gets,1) as hit_pct,
        st.temp_blks_written,
        st.temp_write_total_pct,
        st.temp_blks_read,
        st.temp_read_total_pct,
        st.local_blks_written,
        st.local_write_total_pct,
        st.local_blks_read,
        st.local_read_total_pct,
        st.calls
    FROM top_statements(snode_id, start_id, end_id) st
    WHERE st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written > 0
    ORDER BY st.temp_blks_read + st.temp_blks_written + st.local_blks_read + st.local_blks_written DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp LOOP
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

CREATE OR REPLACE FUNCTION top_temp_diff_htbl(IN snode_id integer, IN start1_id integer, IN end1_id integer, 
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';

    -- Temp usage sorted list TPLs
    tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>I</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>Hits(%)</th><th>Work_w(blk)</th><th>%Total</th><th>Work_r(blk)</th><th>%Total</th><th>Local_w(blk)</th><th>%Total</th><th>Local_r(blk)</th><th>%Total</th><th>Executions</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td rowspan=2 class="spancell"><a HREF="#%s">%s</a></td><td rowspan=2 class="spancell">%s</td><td class="interval1" title="{i1_title}">1</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr><tr><td class="interval2" title="{i2_title}">2</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    --Cursor for top(cnt) querues ordered by temp usage
    c_temp CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(st1.queryid_md5,st2.queryid_md5) as queryid,
        COALESCE(st1.query,st2.query) as query,
        COALESCE(st1.dbname,st2.dbname) as dbname,
        st1.total_time as total_time1,
        st1.rows as rows1,
        st1.gets as gets1,
        st1.shared_blks_hit * 100 / GREATEST(st1.gets,1) as hit_pct1,
        st1.temp_blks_written as temp_blks_written1,
        st1.temp_write_total_pct as temp_write_total_pct1,
        st1.temp_blks_read as temp_blks_read1,
        st1.temp_read_total_pct as temp_read_total_pct1,
        st1.local_blks_written as local_blks_written1,
        st1.local_write_total_pct as local_write_total_pct1,
        st1.local_blks_read as local_blks_read1,
        st1.local_read_total_pct as local_read_total_pct1,
        st1.calls as calls1,
        st2.total_time as total_time2,
        st2.rows as rows2,
        st2.gets as gets2,
        st2.shared_blks_hit * 100 / GREATEST(st2.gets,1) as hit_pct2,
        st2.temp_blks_written as temp_blks_written2,
        st2.temp_write_total_pct as temp_write_total_pct2,
        st2.temp_blks_read as temp_blks_read2,
        st2.temp_read_total_pct as temp_read_total_pct2,
        st2.local_blks_written as local_blks_written2,
        st2.local_write_total_pct as local_write_total_pct2,
        st2.local_blks_read as local_blks_read2,
        st2.local_read_total_pct as local_read_total_pct2,
        st2.calls as calls2,
        row_number() over (ORDER BY st1.temp_blks_read + st1.temp_blks_written + st1.local_blks_read + st1.local_blks_written DESC) as rn_temp1,
        row_number() over (ORDER BY st2.temp_blks_read + st2.temp_blks_written + st2.local_blks_read + st2.local_blks_written DESC) as rn_temp2
    FROM top_statements(snode_id, start1_id, end1_id) st1
        FULL OUTER JOIN top_statements(snode_id, start2_id, end2_id) st2 USING (node_id, dbid, userid, queryid_md5)
    WHERE COALESCE(st1.temp_blks_read + st1.temp_blks_written + st1.local_blks_read + st1.local_blks_written,
        st2.temp_blks_read + st2.temp_blks_written + st2.local_blks_read + st2.local_blks_written) > 0
    ORDER BY COALESCE(st1.temp_blks_read + st1.temp_blks_written + st1.local_blks_read + st1.local_blks_written,0) + 
        COALESCE(st2.temp_blks_read + st2.temp_blks_written + st2.local_blks_read + st2.local_blks_written,0) DESC ) t1
    WHERE rn_temp1 <= topn OR rn_temp2 <= topn;

    r_result RECORD;
BEGIN
    -- Reporting on top queries by temp usage
    FOR r_result IN c_temp LOOP
        report := report||format(
            row_tpl,
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.total_time1 AS numeric),1),
            r_result.rows1,
            r_result.gets1,
            round(CAST(r_result.hit_pct1 AS numeric),2),
            r_result.temp_blks_written1,
            round(CAST(r_result.temp_write_total_pct1 AS numeric),2),
            r_result.temp_blks_read1,
            round(CAST(r_result.temp_read_total_pct1 AS numeric),2),
            r_result.local_blks_written1,
            round(CAST(r_result.local_write_total_pct1 AS numeric),2),
            r_result.local_blks_read1,
            round(CAST(r_result.local_read_total_pct1 AS numeric),2),
            r_result.calls1,
            round(CAST(r_result.total_time2 AS numeric),1),
            r_result.rows2,
            r_result.gets2,
            round(CAST(r_result.hit_pct2 AS numeric),2),
            r_result.temp_blks_written2,
            round(CAST(r_result.temp_write_total_pct2 AS numeric),2),
            r_result.temp_blks_read2,
            round(CAST(r_result.temp_read_total_pct2 AS numeric),2),
            r_result.local_blks_written2,
            round(CAST(r_result.local_write_total_pct2 AS numeric),2),
            r_result.local_blks_read2,
            round(CAST(r_result.local_read_total_pct2 AS numeric),2),
            r_result.calls2
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
/* ===== Main report function ===== */

CREATE OR REPLACE FUNCTION report(IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    topn        integer;
    stmt_all_cnt    integer;
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
        report := replace(report,'{report_start}',snap_rec.snap_time::timestamp(0) without time zone::text);
        tmp_text := '(StartID: ' || snap_rec.snap_id ||', ';
    CLOSE c_snap;

    OPEN c_snap(end_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'End snapshot % does not exists', end_id;
        END IF;
        report := replace(report,'{report_end}',snap_rec.snap_time::timestamp(0) without time zone::text);
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
    stmt_all_cnt := check_stmt_all_setting(snode_id, start_id, end_id);
    tmp_report := '';
    IF stmt_all_cnt > 0 THEN
        tmp_report := 'Report includes '||stmt_all_cnt||' snapshot(s) with setting <i>pg_stat_statements.track = all</i>.'||
        'Value of %Total columns may be incorrect.';
    END IF;
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b>'||tmp_report||'</p>';
    END IF;

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
    tmp_text := tmp_text||'<p>Data in this section is not differential. This data is valid for ending snapshot only.</p>';
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

COMMENT ON FUNCTION report(IN snode_id integer, IN start_id integer, IN end_id integer) IS 'Statistics report generation function. Takes node_id and IDs of start and end snapshot (inclusive).';

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
COMMENT ON FUNCTION report(IN node name, IN start_id integer, IN end_id integer) IS 'Statistics report generation function. Takes node name and IDs of start and end snapshot (inclusive).';

CREATE OR REPLACE FUNCTION report(IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snode_id     integer;
BEGIN
    RETURN report('local',start_id,end_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report(IN start_id integer, IN end_id integer) IS 'Statistics report generation function for local node. Takes IDs of start and end snapshot (inclusive).';

CREATE OR REPLACE FUNCTION report(IN node name, IN baseline varchar(25)) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snode_id    integer;
    bstart_id    integer;
    bend_id      integer;
BEGIN
    SELECT node_id,min(snap_id),max(snap_id) INTO snode_id,bstart_id,bend_id
    FROM nodes JOIN baselines USING(node_id) JOIN bl_snaps USING (bl_id,node_id)
    WHERE node_name = node AND bl_name = baseline
    GROUP BY node_id;
    IF snode_id IS NULL THEN
        RAISE 'Node baseline not found.';
    END IF;
    
    RETURN report(snode_id,bstart_id,bend_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report(IN node name, IN baseline varchar(25)) IS 'Statistics report generation function for node baseline. Takes node name and baseline name.';

CREATE OR REPLACE FUNCTION report(IN baseline varchar(25)) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    RETURN report('local',baseline);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report(IN baseline varchar(25)) IS 'Statistics report generation function for local node baseline. Takes baseline name.';
/* ===== Differential report functions ===== */

CREATE OR REPLACE FUNCTION report_diff(IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    i1_title    text;
    i2_title    text;
    topn        integer;
    stmt_all_cnt    integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile differential report {snaps}</title></head><body><H1>Postgres profile differential report {snaps}</H1><p>First interval (1): {i1_title}</p><p>Second interval (2): {i2_title}</p>{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} table tr:nth-child(even) {background-color: #eee;} table tr:nth-child(odd) {background-color: #fff;} table tr:hover{background-color:#d9ffcc} table th {color: black; background-color: #ffcc99;} .spancell {background-color: #fff;} .interval1, .interval2 {color: grey;}';
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
    OPEN c_snap(start1_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'Start snapshot % does not exists', start_id;
        END IF;
        i1_title := snap_rec.snap_time::timestamp(0) without time zone::text|| ' - ';
        tmp_text := '(1): [' || snap_rec.snap_id ||' - ';
    CLOSE c_snap;

    OPEN c_snap(end1_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'End snapshot % does not exists', end_id;
        END IF;
        i1_title := i1_title||snap_rec.snap_time::timestamp(0) without time zone::text;
        tmp_text := tmp_text || snap_rec.snap_id ||'] with ';
    CLOSE c_snap;
        
    OPEN c_snap(start2_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'Start snapshot % does not exists', start_id;
        END IF;
        i2_title := snap_rec.snap_time::timestamp(0) without time zone::text|| ' - ';
        tmp_text := tmp_text|| '(2): [' || snap_rec.snap_id ||' - ';
    CLOSE c_snap;

    OPEN c_snap(end2_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'End snapshot % does not exists', end_id;
        END IF;
        i2_title := i2_title||snap_rec.snap_time::timestamp(0) without time zone::text;
        tmp_text := tmp_text || snap_rec.snap_id ||']';
    CLOSE c_snap;
    report := replace(report,'{snaps}',tmp_text);
    tmp_text := '';

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(snode_id, start1_id, end1_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>Interval (1) contains snapshot(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;
    tmp_report := check_stmt_cnt(snode_id, start2_id, end2_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p>Interval (2) contains snapshot(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;
    
    -- pg_stat_statements.track warning
    tmp_report := '';
    stmt_all_cnt := check_stmt_all_setting(snode_id, start1_id, end1_id);
    IF stmt_all_cnt > 0 THEN
        tmp_report := '<p>Interval (1) includes '||stmt_all_cnt||' snapshot(s) with setting <i>pg_stat_statements.track = all</i>.'||
        'Value of %Total columns may be incorrect.</p>';
    END IF;
    stmt_all_cnt := check_stmt_all_setting(snode_id, start2_id, end2_id);
    IF stmt_all_cnt > 0 THEN
        tmp_report := 'Interval (2) includes '||stmt_all_cnt||' snapshot(s) with setting <i>pg_stat_statements.track = all</i>.'||
        'Value of %Total columns may be incorrect.';
    END IF;
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b></p>'||tmp_report;
    END IF;

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

    tmp_text := tmp_text || '</ul>';


    --Reporting cluster stats
    tmp_text := tmp_text || '<H2><a NAME=cl_stat>Cluster statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=db_stat>Databases stats</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(dbstats_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=st_stat>Statements stats by database</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(statements_stats_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));
    
    tmp_text := tmp_text || '<H3><a NAME=clu_stat>Cluster stats</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id));
    --Reporting on top queries by elapsed time
    tmp_text := tmp_text||'<H2><a NAME=sql_stat>SQL Query stats</a></H2>';
    tmp_text := tmp_text||'<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_elapsed_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    -- Reporting on top queries by executions
    tmp_text := tmp_text||'<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_exec_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    -- Reporting on top queries by I/O wait time
    tmp_text := tmp_text||'<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_iowait_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    -- Reporting on top queries by gets
    tmp_text := tmp_text||'<H3><a NAME=top_gets>Top SQL by gets</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_gets_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    -- Reporting on top queries by temp usage
    tmp_text := tmp_text||'<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_temp_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));
    -- Listing queries
    tmp_text := tmp_text||'<H3><a NAME=sql_list>Complete List of SQL Text</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(report_queries());

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text||'<H2><a NAME=schema_stat>Schema objects stats</a></H2>';
    tmp_text := tmp_text||'<H3><a NAME=scanned_tbl>Most seq. scanned tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));
    
    tmp_text := tmp_text||'<H3><a NAME=vac_tbl>Top Delete/Update tables with vacuum run count</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_upd_vac_tables_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=growth_tbl>Top growing tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_tables_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text||'<H3><a NAME=growth_idx>Top growing indexes</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_indexes_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=io_stat>I/O Schema objects stats</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=tbl_io_stat>Top tables by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=ix_io_stat>Top indexes by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_io_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));
    tmp_text := tmp_text || '<H2><a NAME=func_stat>User function stats</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=funs_time_stat>Top functions by total time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_time_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=funs_calls_stat>Top functions by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_calls_diff_htbl(snode_id, start1_id, end1_id, start2_id, end2_id, topn));
    report := replace(report,'{report}',tmp_text);
    -- Substitute interval hints in report
    report := replace(report,'{i1_title}',i1_title);
    report := replace(report,'{i2_title}',i2_title);
    RETURN report;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION report_diff(IN snode_id integer, IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer) IS 'Statistics differential report generation function. Takes node_id and IDs of start and end snapshot (inclusive) for first and second intervals';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snode_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found.';
    END IF;
    
    RETURN report_diff(snode_id,start1_id,end1_id,start2_id,end2_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report_diff(IN node name, IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer) IS 'Statistics differential report generation function. Takes node name and IDs of start and end snapshot (inclusive) for first and second intervals';

CREATE OR REPLACE FUNCTION report_diff(IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    RETURN report_diff('local',start1_id,end1_id,start2_id,end2_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report_diff(IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer) IS 'Statistics differential report generation function for local node. Takes IDs of start and end snapshot (inclusive) for first and second intervals';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN baseline1 varchar(25), IN baseline2 varchar(25)) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snode_id    integer;
    start1_id   integer;
    start2_id   integer;
    end1_id     integer;
    end2_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found.';
    END IF;
    
    SELECT min(snap_id), max(snap_id) INTO start1_id, end1_id
    FROM baselines bl JOIN bl_snaps bls USING (bl_id, node_id)
    WHERE bl_name = baseline1 AND node_id = snode_id;
    IF start1_id IS NULL THEN
        RAISE 'baseline1 not found.';
    END IF;
    
    SELECT min(snap_id), max(snap_id) INTO start2_id, end2_id
    FROM baselines bl JOIN bl_snaps bls USING (bl_id, node_id)
    WHERE bl_name = baseline2 AND node_id = snode_id;
    IF start2_id IS NULL THEN
        RAISE 'baseline2 not found.';
    END IF;
    
    RETURN report_diff(snode_id,start1_id,end1_id,start2_id,end2_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report_diff(IN node name, IN baseline1 varchar(25), IN baseline2 varchar(25)) IS 'Statistics differential report generation function. Takes node name and two baselines to compare.';

CREATE OR REPLACE FUNCTION report_diff(IN baseline1 varchar(25), IN baseline2 varchar(25)) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    RETURN report_diff('local',baseline1,baseline2);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report_diff(IN baseline1 varchar(25), IN baseline2 varchar(25)) IS 'Statistics differential report generation function for local node. Takes two baselines to compare.';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN baseline varchar(25), IN start2_id integer,
IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snode_id    integer;
    start1_id   integer;
    end1_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found.';
    END IF;
    
    SELECT min(snap_id), max(snap_id) INTO start1_id, end1_id
    FROM baselines bl JOIN bl_snaps bls USING (bl_id, node_id)
    WHERE bl_name = baseline AND node_id = snode_id;
    IF start1_id IS NULL THEN
        RAISE 'baseline not found.';
    END IF;
    
    RETURN report_diff(snode_id,start1_id,end1_id,start2_id,end2_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report_diff(IN node name, IN baseline varchar(25), IN start2_id integer,
IN end2_id integer) IS 'Statistics differential report generation function. Takes node name, reference baseline name as first interval, start and end snapshot_ids of second interval.';

CREATE OR REPLACE FUNCTION report_diff(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    RETURN report_diff('local',baseline,start2_id,end2_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report_diff(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer) IS 'Statistics differential report generation function for local node. Takes reference baseline name as first interval, start and end snapshot_ids of second interval.';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN start1_id integer, IN end1_id integer,
IN baseline varchar(25)) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    snode_id    integer;
    start2_id   integer;
    end2_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found.';
    END IF;
    
    SELECT min(snap_id), max(snap_id) INTO start2_id, end2_id
    FROM baselines bl JOIN bl_snaps bls USING (bl_id, node_id)
    WHERE bl_name = baseline AND node_id = snode_id;
    IF start2_id IS NULL THEN
        RAISE 'baseline not found.';
    END IF;
    
    RETURN report_diff(snode_id,start1_id,end1_id,start2_id,end2_id);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report_diff(IN node name, IN start1_id integer, IN end1_id integer,
IN baseline varchar(25)) IS 'Statistics differential report generation function. Takes node name, start and end snapshot_ids of first interval and reference baseline name as second interval.';

CREATE OR REPLACE FUNCTION report_diff(IN start1_id integer, IN end1_id integer,
IN baseline varchar(25)) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    RETURN report_diff('local',start1_id,end1_id,baseline);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report_diff(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer) IS 'Statistics differential report generation function for local node. Takes start and end snapshot_ids of first interval and reference baseline name as second interval.';
SELECT 'drop function '||proc.pronamespace::regnamespace||'.'||proc.proname||'('||pg_get_function_identity_arguments(proc.oid)||');' 
FROM pg_depend dep 
    JOIN pg_extension ext ON (dep.refobjid = ext.oid)
    JOIN pg_proc proc ON (proc.oid = dep.objid)
WHERE ext.extname='pg_profile' AND dep.deptype='e' AND dep.classid='pg_proc'::regclass;