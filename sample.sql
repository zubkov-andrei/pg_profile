
/* ========= Sample functions ========= */

CREATE OR REPLACE FUNCTION take_sample(IN sserver_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    s_id            integer;
    topn            integer;
    ret             integer;
    server_properties jsonb = '{"extensions":[],"settings":[]}'; -- version, extensions, etc.
    qres            record;
    server_connstr    text;
    settings_refresh    boolean = true;

    server_query      text;
BEGIN
    -- Get server connstr
    server_connstr := get_connstr(sserver_id);

    -- Getting TopN setting
    BEGIN
        topn := current_setting('{pg_profile}.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    IF dblink_get_connections() @> ARRAY['server_connection'] THEN
        PERFORM dblink_disconnect('server_connection');
    END IF;

    -- Creating a new sample record
    UPDATE servers SET last_sample_id = last_sample_id + 1 WHERE server_id = sserver_id
      RETURNING last_sample_id INTO s_id;
    INSERT INTO samples(sample_time,server_id,sample_id)
      VALUES (now(),sserver_id,s_id);

    -- Only one running take_sample() function allowed per server!
    -- Explicitly lock server in servers table
    BEGIN
        SELECT * INTO qres FROM servers WHERE server_id = sserver_id FOR UPDATE NOWAIT;
    EXCEPTION
        WHEN OTHERS THEN RAISE 'Can''t get lock on server. Is there another take_sample() function running on this server?';
    END;
    -- Getting max_sample_age setting
    BEGIN
        ret := COALESCE(current_setting('{pg_profile}.max_sample_age')::integer);
    EXCEPTION
        WHEN OTHERS THEN ret := 7;
    END;

    PERFORM dblink_connect('server_connection',server_connstr);
    -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
    PERFORM dblink('server_connection','SET lock_timeout=3000');

    -- Get settings values for the server
    FOR qres IN
      SELECT * FROM dblink('server_connection',
          'SELECT name, '
          'reset_val, '
          'unit, '
          'pending_restart '
          'FROM pg_catalog.pg_settings '
          'WHERE name IN ('
            '''server_version_num'''
          ')')
        AS dbl(name text, reset_val text, unit text, pending_restart boolean)
    LOOP
      server_properties := jsonb_insert(server_properties,'{"settings",0}',to_jsonb(qres));
    END LOOP;

    -- Get extensions, that we need to perform statements stats collection
    FOR qres IN
      SELECT * FROM dblink('server_connection',
          'SELECT extname, '
          'extnamespace::regnamespace::name AS extnamespace, '
          'extversion '
          'FROM pg_catalog.pg_extension '
          'WHERE extname IN ('
            '''pg_stat_statements'','
            '''pg_stat_kcache'''
          ')')
        AS dbl(extname name, extnamespace name, extversion text)
    LOOP
      server_properties := jsonb_insert(server_properties,'{"extensions",0}',to_jsonb(qres));
    END LOOP;

    -- Collecting postgres parameters
    -- We will refresh all parameters if version() was changed
    SELECT ss.setting != dblver.version INTO settings_refresh
    FROM v_sample_settings ss, dblink('server_connection','SELECT version() as version') AS dblver (version text)
    WHERE ss.server_id = sserver_id AND ss.sample_id = s_id AND ss.name='version' AND ss.setting_scope = 2;
    settings_refresh := COALESCE(settings_refresh,true);

    -- Constructing server sql query for settings
    server_query := 'SELECT 1 as setting_scope,name,setting,reset_val,boot_val,unit,sourcefile,sourceline,pending_restart '
      'FROM pg_catalog.pg_settings '
      'UNION ALL SELECT 2 as setting_scope,''version'',version(),version(),NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''pg_postmaster_start_time'',pg_postmaster_start_time()::text,pg_postmaster_start_time()::text,NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''pg_conf_load_time'',pg_conf_load_time()::text,pg_conf_load_time()::text,NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''system_identifier'',system_identifier::text,system_identifier::text,system_identifier::text,NULL,NULL,NULL,False FROM pg_control_system()';

    INSERT INTO sample_settings(
      server_id,
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
    )
    SELECT
      s.server_id as server_id,
      s.sample_time as first_seen,
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
      sample_settings lst JOIN
      -- Getting last versions of settings
        (SELECT server_id, name, max(first_seen) as first_seen
        FROM sample_settings
        WHERE server_id = sserver_id AND NOT settings_refresh
        GROUP BY server_id, name
        -- HAVING first_seen >= (select max(first_seen) from sample_settings where server_id = sserver_id and name='cluster_version/edition')
        ) lst_times
      USING (server_id, name, first_seen)
      -- Getting current settings values
      RIGHT OUTER JOIN dblink('server_connection',server_query
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
        USING (setting_scope, name)
      JOIN samples s ON (s.server_id = sserver_id AND s.sample_id = s_id)
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
    FROM sample_settings
    WHERE server_id = sserver_id AND name = 'system_identifier';
    IF qres.sysid_changed THEN
      RAISE 'Server system_identifier has changed! Ensure server connection string is correct. Consider creating a new server for this cluster.';
    END IF;

    INSERT INTO sample_settings(
      server_id,
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
    )
    SELECT
      s.server_id,
      s.sample_time,
      1 as setting_scope,
      '{pg_profile}.topn',
      topn,
      topn,
      topn,
      null,
      null,
      null,
      false
    FROM samples s LEFT OUTER JOIN  v_sample_settings prm ON
      (s.server_id = prm.server_id AND s.sample_id = prm.sample_id AND prm.name = '{pg_profile}.topn' AND prm.setting_scope = 1 AND NOT settings_refresh)
    WHERE s.server_id = sserver_id AND s.sample_id = s_id AND (prm.setting IS NULL OR prm.setting::integer != topn);

    -- Construct pg_stat_database query
    server_query := 'SELECT
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
      FROM pg_catalog.pg_stat_database WHERE datname IS NOT NULL';

    -- pg_stat_database data
    INSERT INTO last_stat_database (
        server_id,
        sample_id,
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
        sserver_id,
        s_id,
        datid,
        datname,
        xact_commit AS xact_commit,
        xact_rollback AS xact_rollback,
        blks_read AS blks_read,
        blks_hit AS blks_hit,
        tup_returned AS tup_returned,
        tup_fetched AS tup_fetched,
        tup_inserted AS tup_inserted,
        tup_updated AS tup_updated,
        tup_deleted AS tup_deleted,
        conflicts AS conflicts,
        temp_files AS temp_files,
        temp_bytes AS temp_bytes,
        deadlocks AS deadlocks,
        blk_read_time AS blk_read_time,
        blk_write_time AS blk_write_time,
        stats_reset,
        datsize AS datsize,
        datsize_delta AS datsize_delta
    FROM dblink('server_connection',server_query) AS rs (
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
    INSERT INTO sample_stat_database(
      server_id,
      sample_id,
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
      datsize_delta
    )
    SELECT
        cur.server_id,
        cur.sample_id,
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
        (lst.server_id = cur.server_id AND lst.sample_id = cur.sample_id - 1 AND lst.datid = cur.datid AND lst.datname = cur.datname AND lst.stats_reset = cur.stats_reset)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    -- Construct tablespace stats query
    server_query := 'SELECT
        oid as tablespaceid,
        spcname as tablespacename,
        pg_tablespace_location(oid) as tablespacepath,
        pg_tablespace_size(oid) as size,
        0 as size_delta
        FROM pg_tablespace ';

    -- Get tablespace stats
    INSERT INTO last_stat_tablespaces(
      server_id,
      sample_id,
      tablespaceid,
      tablespacename,
      tablespacepath,
      size,
      size_delta
    )
    SELECT
      sserver_id,
      s_id,
      dbl.tablespaceid,
      dbl.tablespacename,
      dbl.tablespacepath,
      dbl.size AS size,
      dbl.size_delta AS size_delta
    FROM dblink('server_connection', server_query)
    AS dbl (
        tablespaceid            oid,
        tablespacename          name,
        tablespacepath          text,
        size                    bigint,
        size_delta              bigint
    );

    -- collect pg_stat_statements stats if available
    PERFORM collect_statements_stats(server_properties, sserver_id, s_id, topn);

    -- pg_stat_bgwriter data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer < 100000
      )
      THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_is_in_recovery() THEN 0 '
            'ELSE pg_xlog_location_diff(pg_current_xlog_location(),''0/00000000'') '
          'END AS wal_size '
          'FROM pg_catalog.pg_stat_bgwriter';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 100000
      )
      THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_is_in_recovery() THEN 0 '
              'ELSE pg_wal_lsn_diff(pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size '
        'FROM pg_catalog.pg_stat_bgwriter';
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_cluster (
        server_id,
        sample_id,
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
        sserver_id,
        s_id,
        checkpoints_timed AS checkpoints_timed,
        checkpoints_req AS checkpoints_req,
        checkpoint_write_time AS checkpoint_write_time,
        checkpoint_sync_time AS checkpoint_sync_time,
        buffers_checkpoint AS buffers_checkpoint,
        buffers_clean AS buffers_clean,
        maxwritten_clean AS maxwritten_clean,
        buffers_backend AS buffers_backend,
        buffers_backend_fsync AS buffers_backend_fsync,
        buffers_alloc AS buffers_alloc,
        stats_reset,
        wal_size AS wal_size
      FROM dblink('server_connection',server_query) AS rs (
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

    -- pg_stat_archiver data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer > 90500
      )
      THEN
        server_query := 'SELECT '
          'archived_count,'
          'last_archived_wal,'
          'last_archived_time,'
          'failed_count,'
          'last_failed_wal,'
          'last_failed_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_archiver';
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_archiver (
        server_id,
        sample_id,
        archived_count,
        last_archived_wal,
        last_archived_time,
        failed_count,
        last_failed_wal,
        last_failed_time,
        stats_reset)
      SELECT
        sserver_id,
        s_id,
        archived_count as archived_count,
        last_archived_wal as last_archived_wal,
        last_archived_time as last_archived_time,
        failed_count as failed_count,
        last_failed_wal as last_failed_wal,
        last_failed_time as last_failed_time,
        stats_reset as stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        archived_count              bigint,
        last_archived_wal           text,
        last_archived_time          timestamp with time zone,
        failed_count                bigint,
        last_failed_wal             text,
        last_failed_time            timestamp with time zone,
        stats_reset                 timestamp with time zone
      );
    END IF;

    -- Collecting stat info for objects of all databases
    PERFORM collect_obj_stats(server_properties, sserver_id, s_id);
    PERFORM dblink_disconnect('server_connection');

    -- analyze last_* tables will help with more accurate plans
    ANALYZE last_stat_indexes;
    ANALYZE last_stat_tables;
    ANALYZE last_stat_tablespaces;
    ANALYZE last_stat_user_functions;

    -- Updating dictionary table in case of object renaming:
    -- Databases
    UPDATE sample_stat_database AS db
    SET datname = lst.datname
    FROM last_stat_database AS lst
    WHERE db.server_id = lst.server_id AND db.datid = lst.datid
      AND db.datname != lst.datname
      AND lst.sample_id = s_id;
    -- Tables
    UPDATE tables_list AS tl
    SET schemaname = lst.schemaname, relname = lst.relname
    FROM last_stat_tables AS lst
    WHERE tl.server_id = lst.server_id AND tl.datid = lst.datid AND tl.relid = lst.relid AND tl.relkind = lst.relkind
      AND (tl.schemaname != lst.schemaname OR tl.relname != lst.relname)
      AND lst.sample_id = s_id;
    -- Indexes
    UPDATE indexes_list AS il
    SET schemaname = lst.schemaname, indexrelname = lst.indexrelname
    FROM last_stat_indexes AS lst
    WHERE il.server_id = lst.server_id AND il.datid = lst.datid AND il.indexrelid = lst.indexrelid
      AND il.relid = lst.relid
      AND (il.schemaname != lst.schemaname OR il.indexrelname != lst.indexrelname)
      AND lst.sample_id = s_id;
    -- Functions
    UPDATE funcs_list AS fl
    SET schemaname = lst.schemaname, funcname = lst.funcname, funcargs = lst.funcargs
    FROM last_stat_user_functions AS lst
    WHERE fl.server_id = lst.server_id AND fl.datid = lst.datid AND fl.funcid = lst.funcid
      AND (fl.schemaname != lst.schemaname OR fl.funcname != lst.funcname OR fl.funcargs != lst.funcargs)
      AND lst.sample_id = s_id;
    -- Tablespaces
    UPDATE tablespaces_list AS tl
    SET tablespacename = lst.tablespacename, tablespacepath = lst.tablespacepath
    FROM last_stat_tablespaces AS lst
    WHERE tl.server_id = lst.server_id AND tl.tablespaceid = lst.tablespaceid
      AND (tl.tablespacename != lst.tablespacename OR tl.tablespacepath != lst.tablespacepath)
      AND lst.sample_id = s_id;

    -- Calculate diffs for tablespaces
    FOR qres IN
        SELECT
            server_id,
            sample_id,
            tablespaceid,
            tablespacename,
            tablespacepath,
            size,
            size_delta
        FROM
            (SELECT
                cur.server_id,
                cur.sample_id,
                cur.tablespaceid as tablespaceid,
                cur.tablespacename AS tablespacename,
                cur.tablespacepath AS tablespacepath,
                cur.size as size,
                cur.size - COALESCE(lst.size,0) AS size_delta
            FROM last_stat_tablespaces cur LEFT OUTER JOIN last_stat_tablespaces lst ON
              (cur.server_id = lst.server_id AND lst.sample_id=cur.sample_id-1 AND cur.tablespaceid = lst.tablespaceid)
            WHERE cur.sample_id=s_id AND cur.server_id=sserver_id ) diff
    LOOP
      -- insert tablespaces to tablespaces_list
      INSERT INTO tablespaces_list(
        server_id,
        tablespaceid,
        tablespacename,
        tablespacepath
      )
      VALUES (qres.server_id,qres.tablespaceid,qres.tablespacename,qres.tablespacepath) ON CONFLICT DO NOTHING;
      INSERT INTO sample_stat_tablespaces(
        server_id,
        sample_id,
        tablespaceid,
        size,
        size_delta
      )
      VALUES (
          qres.server_id,
          qres.sample_id,
          qres.tablespaceid,
          qres.size,
          qres.size_delta
      );
    END LOOP;

    -- collect databases objects stats
    PERFORM sample_dbobj_delta(sserver_id,s_id,topn);

    DELETE FROM last_stat_tablespaces WHERE server_id = sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_database WHERE server_id = sserver_id AND sample_id != s_id;

    -- Calc stat cluster diff
    INSERT INTO sample_stat_cluster(
      server_id,
      sample_id,
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
    )
    SELECT
        cur.server_id,
        cur.sample_id,
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
      (cur.stats_reset = lst.stats_reset AND cur.server_id = lst.server_id AND lst.sample_id = cur.sample_id - 1)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    DELETE FROM last_stat_cluster WHERE server_id = sserver_id AND sample_id != s_id;

    -- Calc stat archiver diff
    INSERT INTO sample_stat_archiver(
      server_id,
      sample_id,
      archived_count,
      last_archived_wal,
      last_archived_time,
      failed_count,
      last_failed_wal,
      last_failed_time,
      stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.archived_count - COALESCE(lst.archived_count,0),
        cur.last_archived_wal,
        cur.last_archived_time,
        cur.failed_count - COALESCE(lst.failed_count,0),
        cur.last_failed_wal,
        cur.last_failed_time,
        cur.stats_reset
    FROM last_stat_archiver cur
    LEFT OUTER JOIN last_stat_archiver lst ON
      (cur.stats_reset = lst.stats_reset AND cur.server_id = lst.server_id AND lst.sample_id = cur.sample_id - 1)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    DELETE FROM last_stat_archiver WHERE server_id = sserver_id AND sample_id != s_id;

    -- Deleting obsolete baselines
    DELETE FROM baselines
    WHERE keep_until < now()
      AND server_id = sserver_id;
    -- Deleting obsolote samples
    DELETE FROM samples s
      USING servers n
    WHERE n.server_id = s.server_id AND s.server_id = sserver_id
        AND s.sample_time < now() - (COALESCE(n.max_sample_age,ret) || ' days')::interval
        AND (s.server_id,s.sample_id) NOT IN (SELECT server_id,sample_id FROM bl_samples WHERE server_id = sserver_id);
    -- Deleting unused statements
    DELETE FROM stmt_list
        WHERE queryid_md5 NOT IN
            (SELECT queryid_md5 FROM sample_statements
                UNION
             SELECT queryid_md5 FROM sample_kcache);

    -- Delete unused tablespaces from list
    DELETE FROM tablespaces_list
    WHERE server_id = sserver_id
      AND (server_id, tablespaceid) NOT IN (
        SELECT server_id, tablespaceid FROM sample_stat_tablespaces
        WHERE server_id = sserver_id
    );

    -- Delete unused indexes from indexes list
    DELETE FROM indexes_list
    WHERE server_id = sserver_id
      AND(server_id, datid, indexrelid) NOT IN (
        SELECT server_id, datid, indexrelid FROM sample_stat_indexes
    );

    -- Delete unused tables from tables list
    WITH used_tables AS (
        SELECT server_id, datid, relid FROM sample_stat_tables WHERE server_id = sserver_id
        UNION ALL
        SELECT server_id, datid, relid FROM indexes_list WHERE server_id = sserver_id)
    DELETE FROM tables_list
    WHERE server_id = sserver_id
      AND (server_id, datid, relid) NOT IN (SELECT server_id, datid, relid FROM used_tables)
      AND (server_id, datid, reltoastrelid) NOT IN (SELECT server_id, datid, relid FROM used_tables);

    -- Delete unused functions from functions list
    DELETE FROM funcs_list
    WHERE server_id = sserver_id
      AND (server_id, funcid) NOT IN (
        SELECT server_id, funcid FROM sample_stat_user_functions WHERE server_id = sserver_id
    );

    -- Delete obsolete values of postgres parameters
    DELETE FROM sample_settings ss
    USING (
      SELECT server_id, max(first_seen) AS first_seen, setting_scope, name
      FROM sample_settings
      WHERE server_id = sserver_id AND first_seen <= (SELECT min(sample_time) FROM samples WHERE server_id = sserver_id)
      GROUP BY server_id, setting_scope, name) AS ss_ref
    WHERE ss.server_id = ss_ref.server_id AND ss.setting_scope = ss_ref.setting_scope AND ss.name = ss_ref.name
      AND ss.first_seen < ss_ref.first_seen;
    -- Delete obsolete values of postgres parameters from previous versions of postgres on server
    DELETE FROM sample_settings
    WHERE server_id = sserver_id AND first_seen <
      (SELECT min(first_seen) FROM sample_settings WHERE server_id = sserver_id AND name = 'version' AND setting_scope = 2);

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample(IN sserver_id integer) IS 'Statistics sample creation function (by server_id).';

CREATE OR REPLACE FUNCTION take_sample(IN server name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    sserver_id    integer;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name = server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found';
    ELSE
        RETURN take_sample(sserver_id);
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample(IN server name) IS 'Statistics sample creation function (by server name).';

CREATE OR REPLACE FUNCTION take_subset_sample(IN sets_cnt integer = 1, IN current_set integer = 0) RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@,public AS $$
DECLARE
    c_servers CURSOR FOR
      SELECT server_id,server_name FROM (
        SELECT server_id,server_name, row_number() OVER () AS srv_rn
        FROM servers WHERE enabled
        ) AS t1
      WHERE srv_rn % sets_cnt = current_set;
    server_sampleres        integer;
    etext               text := '';
    edetail             text := '';
    econtext            text := '';

    qres          RECORD;
    start_clock   timestamp (2) with time zone;
BEGIN
    IF sets_cnt IS NULL OR sets_cnt < 1 THEN
      RAISE 'sets_cnt value is invalid. Must be positive';
    END IF;
    IF current_set IS NULL OR current_set < 0 OR current_set > sets_cnt - 1 THEN
      RAISE 'current_cnt value is invalid. Must be between 0 and sets_cnt - 1';
    END IF;
    FOR qres IN c_servers LOOP
        BEGIN
            start_clock := clock_timestamp()::timestamp (2) with time zone;
            server := qres.server_name;
            server_sampleres := take_sample(qres.server_id);
            elapsed := clock_timestamp()::timestamp (2) with time zone - start_clock;
            CASE server_sampleres
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
                    elapsed := clock_timestamp()::timestamp (2) with time zone - start_clock;
                    RETURN NEXT;
                END;
        END;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_subset_sample(IN sets_cnt integer, IN current_set integer) IS 'Statistics sample creation function (for subset of enabled servers). Used for simplification of parallel sample collection.';

CREATE OR REPLACE FUNCTION take_sample() RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@,public AS $$
  SELECT * FROM take_subset_sample(1,0);
$$ LANGUAGE sql;

COMMENT ON FUNCTION take_sample() IS 'Statistics sample creation function (for all enabled servers). Must be explicitly called periodically.';

CREATE OR REPLACE FUNCTION collect_statements_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer) RETURNS void SET search_path=@extschema@,public AS $$
DECLARE
  qres              record;
  st_query          text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Check if mandatory extensions exists
    IF NOT
      (
        SELECT count(*) > 0
        FROM jsonb_to_recordset(properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_statements'
      )
    THEN
      RETURN;
    END IF;

    -- Dynamic statements query
    st_query := format(
      'SELECT '
        'st.userid,'
        'st.dbid,'
        'st.queryid,'
        'left(md5(db.datname || r.rolname || st.query ), 10) AS queryid_md5,'
        '{statements_fields}'
        '{kcache_fields}'
      ' FROM '
      '{statements_view} st '
      'JOIN pg_catalog.pg_database db ON (db.oid=st.dbid) '
      'JOIN pg_catalog.pg_roles r ON (r.oid=st.userid) '
      '{statements_join}'
      '{kcache_join}'
      ' WHERE '
        'st.queryid IS NOT NULL '
        'AND '
        'least('
          '{statements_rank}'
          '{kcache_rank}'
          ') <= %1$s',
      topn);

    -- pg_stat_kcache placeholders processing if extension is available
    CASE
      (
        SELECT extversion FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extversion text)
        WHERE extname = 'pg_stat_kcache'
      )
      WHEN '2.1.0','2.1.1','2.1.2','2.1.3' THEN
        st_query := replace(st_query, '{kcache_fields}',
          ',true as kcache_avail,'
          'kc.user_time as user_time,'
          'kc.system_time as system_time,'
          'kc.minflts as minflts,'
          'kc.majflts as majflts,'
          'kc.nswaps as nswaps,'
          'kc.reads as reads,'
          'kc.writes  as writes,'
          'kc.msgsnds as msgsnds,'
          'kc.msgrcvs as msgrcvs,'
          'kc.nsignals as nsignals,'
          'kc.nvcsws as nvcsws,'
          'kc.nivcsws as nivcsws'
        );
        st_query := replace(st_query, '{kcache_join}',format(
          'LEFT OUTER JOIN %1$I.pg_stat_kcache() kc ON (st.queryid = kc.queryid AND st.userid = kc.userid AND st.dbid = kc.dbid) '
          'LEFT OUTER JOIN '
            '(SELECT '
              'k.userid, k.dbid, md5(s.query) as q_md5,'
              'row_number() over (ORDER BY sum(user_time+system_time) DESC) AS cpu_time_rank,'
              'row_number() over (ORDER BY sum(reads+writes) DESC) AS io_rank '
            'FROM %1$I.pg_stat_kcache() k '
            'JOIN {statements_view} s ON (s.queryid=k.queryid) '
            'GROUP BY k.userid, k.dbid, md5(s.query)) rank_kc '
          'ON (kc.userid=rank_kc.userid AND kc.dbid=rank_kc.dbid AND md5(st.query)=rank_kc.q_md5)',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          )
        );
        st_query := replace(st_query, '{kcache_rank}',
          ',cpu_time_rank,'
          'io_rank'
        );
      ELSE
        st_query := replace(st_query, '{kcache_join}','');
        st_query := replace(st_query, '{kcache_rank}','');
        st_query := replace(st_query, '{kcache_fields}',
          ',false as kcache_avail,'
          'NULL as user_time,'
          'NULL as system_time,'
          'NULL as minflts,'
          'NULL as majflts,'
          'NULL as nswaps,'
          'NULL as reads,'
          'NULL  as writes,'
          'NULL as msgsnds,'
          'NULL as msgrcvs,'
          'NULL as nsignals,'
          'NULL as nvcsws,'
          'NULL as nivcsws');
    END CASE;

    -- statements placeholders processing
    CASE
      -- pg_stat_statements v 1.3-1.7
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
          AND extversion IN ('1.3','1.4','1.5','1.6','1.7')
      )
      THEN
        st_query := replace(st_query, '{statements_join}',
          'JOIN '
          '(SELECT '
            'userid,'
            'dbid,'
            'md5(query) as q_md5,'
            'row_number() over (ORDER BY sum(total_time) DESC) AS exec_time_rank,'
            'row_number() over (ORDER BY sum(calls) DESC) AS calls_rank,'
            'row_number() over (ORDER BY sum(blk_read_time + blk_write_time) DESC) AS io_time_rank,'
            'row_number() over (ORDER BY sum(shared_blks_hit + shared_blks_read) DESC) AS gets_rank,'
            'row_number() over (ORDER BY sum(shared_blks_read) DESC) AS read_rank,'
            'row_number() over (ORDER BY sum(shared_blks_dirtied) DESC) AS dirtied_rank,'
            'row_number() over (ORDER BY sum(shared_blks_written) DESC) AS written_rank,'
            'row_number() over (ORDER BY sum(temp_blks_written + local_blks_written) DESC) AS tempw_rank,'
            'row_number() over (ORDER BY sum(temp_blks_read + local_blks_read) DESC) AS tempr_rank '
          'FROM {statements_view} '
          'GROUP BY userid, dbid, md5(query)) rank_st '
          'ON (st.userid=rank_st.userid AND st.dbid=rank_st.dbid AND md5(st.query) = rank_st.q_md5) '
        );
        st_query := replace(st_query, '{statements_rank}',
          'exec_time_rank,'
          'calls_rank,'
          'io_time_rank,'
          'gets_rank,'
          'read_rank,'
          'tempw_rank,'
          'tempr_rank,'
          'dirtied_rank,'
          'written_rank'
        );
        st_query := replace(st_query, '{statements_fields}',
          'NULL as plans,'
          'NULL as total_plan_time,'
          'NULL as min_plan_time,'
          'NULL as max_plan_time,'
          'NULL as mean_plan_time,'
          'NULL as stddev_plan_time,'
          'st.calls,'
          'st.total_time as total_exec_time,'
          'st.min_time as min_exec_time,'
          'st.max_time as max_exec_time,'
          'st.mean_time as mean_exec_time,'
          'st.stddev_time as stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'NULL as wal_records,'
          'NULL as wal_fpi,'
          'NULL as wal_bytes,'
          'regexp_replace(st.query,''\s+'','' '',''g'') AS query'
        );
        st_query := replace(st_query, '{statements_view}',
          format('%1$I.pg_stat_statements',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        );
      -- pg_stat_statements v 1.8
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
          AND extversion IN ('1.8')
      )
      THEN
        st_query := replace(st_query, '{statements_join}',
          'JOIN '
          '(SELECT '
            'userid,'
            'dbid,'
            'md5(query) as q_md5,'
            'row_number() over (ORDER BY sum(total_plan_time + total_exec_time) DESC) AS time_rank,'
            'row_number() over (ORDER BY sum(total_plan_time) DESC) AS plan_time_rank,'
            'row_number() over (ORDER BY sum(total_exec_time) DESC) AS exec_time_rank,'
            'row_number() over (ORDER BY sum(calls) DESC) AS calls_rank,'
            'row_number() over (ORDER BY sum(blk_read_time + blk_write_time) DESC) AS io_time_rank,'
            'row_number() over (ORDER BY sum(shared_blks_hit + shared_blks_read) DESC) AS gets_rank,'
            'row_number() over (ORDER BY sum(shared_blks_read) DESC) AS read_rank,'
            'row_number() over (ORDER BY sum(shared_blks_dirtied) DESC) AS dirtied_rank,'
            'row_number() over (ORDER BY sum(shared_blks_written) DESC) AS written_rank,'
            'row_number() over (ORDER BY sum(temp_blks_written + local_blks_written) DESC) AS tempw_rank,'
            'row_number() over (ORDER BY sum(temp_blks_read + local_blks_read) DESC) AS tempr_rank,'
            'row_number() over (ORDER BY sum(wal_bytes) DESC) AS wal_rank '
          'FROM {statements_view} '
          'GROUP BY userid, dbid, md5(query)) rank_st '
          'ON (st.userid=rank_st.userid AND st.dbid=rank_st.dbid AND md5(st.query) = rank_st.q_md5) '
        );
        st_query := replace(st_query, '{statements_rank}',
          'time_rank,'
          'plan_time_rank,'
          'exec_time_rank,'
          'calls_rank,'
          'io_time_rank,'
          'gets_rank,'
          'read_rank,'
          'dirtied_rank,'
          'written_rank,'
          'tempw_rank,'
          'tempr_rank,'
          'wal_rank'
        );
        st_query := replace(st_query, '{statements_fields}',
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes,'
          'regexp_replace(st.query,''\s+'','' '',''g'') AS query'
        );
        st_query := replace(st_query, '{statements_view}',
          format('%1$I.pg_stat_statements',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        );
      ELSE
        RAISE 'Unsupported pg_stat_statements version. Supported versions are 1.3 - 1.8';
    END CASE;

    -- Sample data from pg_stat_statements and pg_stat_kcache top whole cluster statements
    FOR qres IN
        SELECT
          -- pg_stat_statements fields
          sserver_id,
          s_id AS sample_id,
          dbl.userid AS userid,
          dbl.datid AS datid,
          dbl.queryid AS queryid,
          dbl.queryid_md5 AS queryid_md5,
          dbl.plans AS plans,
          dbl.total_plan_time AS total_plan_time,
          dbl.min_plan_time AS min_plan_time,
          dbl.max_plan_time AS max_plan_time,
          dbl.mean_plan_time AS mean_plan_time,
          dbl.stddev_plan_time AS stddev_plan_time,
          dbl.calls  AS calls,
          dbl.total_exec_time  AS total_exec_time,
          dbl.min_exec_time  AS min_exec_time,
          dbl.max_exec_time  AS max_exec_time,
          dbl.mean_exec_time  AS mean_exec_time,
          dbl.stddev_exec_time  AS stddev_exec_time,
          dbl.rows  AS rows,
          dbl.shared_blks_hit  AS shared_blks_hit,
          dbl.shared_blks_read  AS shared_blks_read,
          dbl.shared_blks_dirtied  AS shared_blks_dirtied,
          dbl.shared_blks_written  AS shared_blks_written,
          dbl.local_blks_hit  AS local_blks_hit,
          dbl.local_blks_read  AS local_blks_read,
          dbl.local_blks_dirtied  AS local_blks_dirtied,
          dbl.local_blks_written  AS local_blks_written,
          dbl.temp_blks_read  AS temp_blks_read,
          dbl.temp_blks_written  AS temp_blks_written,
          dbl.blk_read_time  AS blk_read_time,
          dbl.blk_write_time  AS blk_write_time,
          dbl.wal_records AS wal_records,
          dbl.wal_fpi AS wal_fpi,
          dbl.wal_bytes AS wal_bytes,
          dbl.query AS query,
          -- pg_stat_kcache fields
          dbl.kcache_avail AS kcache_avail,
          dbl.user_time  AS user_time,
          dbl.system_time  AS system_time,
          dbl.minflts  AS minflts,
          dbl.majflts  AS majflts,
          dbl.nswaps  AS nswaps,
          dbl.reads  AS reads,
          dbl.writes  AS writes,
          dbl.msgsnds  AS msgsnds,
          dbl.msgrcvs  AS msgrcvs,
          dbl.nsignals  AS nsignals,
          dbl.nvcsws  AS nvcsws,
          dbl.nivcsws  AS nivcsws
        FROM dblink('server_connection',st_query)
        AS dbl (
          -- pg_stat_statements fields
            userid              oid,
            datid               oid,
            queryid             bigint,
            queryid_md5         char(10),
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
            query               text,
          -- pg_stat_kcache fields
            kcache_avail        boolean,
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
            nivcsws             bigint
        ) JOIN sample_stat_database sd ON (dbl.datid = sd.datid AND sd.sample_id = s_id AND sd.server_id = sserver_id)
    LOOP
        INSERT INTO stmt_list(
          queryid_md5,
          query
        )
        VALUES (qres.queryid_md5,qres.query) ON CONFLICT DO NOTHING;

        INSERT INTO sample_statements(
          server_id,
          sample_id,
          userid,
          datid,
          queryid,
          queryid_md5,
          plans,
          total_plan_time,
          min_plan_time,
          max_plan_time,
          mean_plan_time,
          stddev_plan_time,
          calls,
          total_exec_time,
          min_exec_time,
          max_exec_time,
          mean_exec_time,
          stddev_exec_time,
          rows,
          shared_blks_hit,
          shared_blks_read,
          shared_blks_dirtied,
          shared_blks_written,
          local_blks_hit,
          local_blks_read,
          local_blks_dirtied,
          local_blks_written,
          temp_blks_read,
          temp_blks_written,
          blk_read_time,
          blk_write_time,
          wal_records,
          wal_fpi,
          wal_bytes
        )
        VALUES (
            qres.sserver_id,
            qres.sample_id,
            qres.userid,
            qres.datid,
            qres.queryid,
            qres.queryid_md5,
            qres.plans,
            qres.total_plan_time,
            qres.min_plan_time,
            qres.max_plan_time,
            qres.mean_plan_time,
            qres.stddev_plan_time,
            qres.calls,
            qres.total_exec_time,
            qres.min_exec_time,
            qres.max_exec_time,
            qres.mean_exec_time,
            qres.stddev_exec_time,
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
            qres.blk_write_time,
            qres.wal_records,
            qres.wal_fpi,
            qres.wal_bytes
        );
        IF qres.kcache_avail THEN
          INSERT INTO sample_kcache(
            server_id,
            sample_id,
            userid,
            datid,
            queryid,
            queryid_md5,
            user_time,
            system_time,
            minflts,
            majflts,
            nswaps,
            reads,
            writes,
            msgsnds,
            msgrcvs,
            nsignals,
            nvcsws,
            nivcsws
          )
          VALUES (
            qres.sserver_id,
            qres.sample_id,
            qres.userid,
            qres.datid,
            qres.queryid,
            qres.queryid_md5,
            qres.user_time,
            qres.system_time,
            qres.minflts,
            qres.majflts,
            qres.nswaps,
            qres.reads,
            qres.writes,
            qres.msgsnds,
            qres.msgrcvs,
            qres.nsignals,
            qres.nvcsws,
            qres.nivcsws
          );
        END IF;
    END LOOP;

    -- Aggregeted pg_stat_kcache data
    CASE (
        SELECT extversion FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extversion text)
        WHERE extname = 'pg_stat_kcache'
    )
      WHEN '2.1.0','2.1.1','2.1.2','2.1.3' THEN
        INSERT INTO sample_kcache_total(
          server_id,
          sample_id,
          datid,
          user_time,
          system_time,
          minflts,
          majflts,
          nswaps,
          reads,
          writes,
          msgsnds,
          msgrcvs,
          nsignals,
          nvcsws,
          nivcsws,
          statements
        )
        SELECT sd.server_id,sd.sample_id,dbl.*
        FROM
        dblink('server_connection',
          format('SELECT
              dbid as datid,
              sum(user_time),
              sum(system_time),
              sum(minflts),
              sum(majflts),
              sum(nswaps),
              sum(reads),
              sum(writes),
              sum(msgsnds),
              sum(msgrcvs),
              sum(nsignals),
              sum(nvcsws),
              sum(nivcsws ),
              count(*)
            FROM %1$I.pg_stat_kcache()
            GROUP BY dbid',
              (
                SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                  AS x(extname text, extnamespace text)
                WHERE extname = 'pg_stat_kcache'
              )
            )
          ) AS dbl (
            datid               oid,
            user_time           double precision,
            system_time         double precision,
            minflts             bigint,
            majflts             bigint, -- Number of page faults (hard page faults)
            nswaps              bigint, -- Number of swaps
            reads               bigint, -- Number of bytes read by the filesystem layer
            writes              bigint, -- Number of bytes written by the filesystem layer
            msgsnds             bigint, -- Number of IPC messages sent
            msgrcvs             bigint, -- Number of IPC messages received
            nsignals            bigint, -- Number of signals received
            nvcsws              bigint, -- Number of voluntary context switches
            nivcsws             bigint,
            stmts               integer
        ) JOIN sample_stat_database sd USING (datid)
        WHERE sd.sample_id = s_id AND sd.server_id = sserver_id;

        -- Flushing pg_stat_kcache
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_kcache_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          )
        ) AS t(res char(1));
      ELSE
        NULL;
    END CASE;

    -- Aggregeted statements data
    CASE
      -- pg_stat_statements v 1.3-1.7
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
          AND extversion IN ('1.3','1.4','1.5','1.6','1.7')
      )
      THEN
        st_query := format('SELECT '
            'dbid as datid,'
            'NULL,' -- plans
            'NULL,' -- total_plan_time
            'sum(calls),'
            'sum(total_time),'
            'sum(rows),'
            'sum(shared_blks_hit),'
            'sum(shared_blks_read),'
            'sum(shared_blks_dirtied),'
            'sum(shared_blks_written),'
            'sum(local_blks_hit),'
            'sum(local_blks_read),'
            'sum(local_blks_dirtied),'
            'sum(local_blks_written),'
            'sum(temp_blks_read),'
            'sum(temp_blks_written),'
            'sum(blk_read_time),'
            'sum(blk_write_time),'
            'NULL,' -- wal_records
            'NULL,' -- wal_fpi
            'NULL,' -- wal_bytes
            'count(*) '
        'FROM %1$I.pg_stat_statements '
        'GROUP BY dbid',
          (
            SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
              AS x(extname text, extnamespace text)
            WHERE extname = 'pg_stat_statements'
          )
        );
      -- pg_stat_statements v 1.8
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
          AND extversion IN ('1.8')
      )
      THEN
        st_query := format('SELECT '
            'dbid as datid,'
            'sum(plans),'
            'sum(total_plan_time),'
            'sum(calls),'
            'sum(total_exec_time),'
            'sum(rows),'
            'sum(shared_blks_hit),'
            'sum(shared_blks_read),'
            'sum(shared_blks_dirtied),'
            'sum(shared_blks_written),'
            'sum(local_blks_hit),'
            'sum(local_blks_read),'
            'sum(local_blks_dirtied),'
            'sum(local_blks_written),'
            'sum(temp_blks_read),'
            'sum(temp_blks_written),'
            'sum(blk_read_time),'
            'sum(blk_write_time),'
            'sum(wal_records),'
            'sum(wal_fpi),'
            'sum(wal_bytes),'
            'count(*) '
        'FROM %1$I.pg_stat_statements '
        'GROUP BY dbid',
          (
            SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
              AS x(extname text, extnamespace text)
            WHERE extname = 'pg_stat_statements'
          )
        );
      ELSE
        RAISE 'Unsupported pg_stat_statements version. Supported versions are 1.3, 1.4, 1.5, 1.6, 1.7, 1.8';
    END CASE;

    INSERT INTO sample_statements_total(
      server_id,
      sample_id,
      datid,
      plans,
      total_plan_time,
      calls,
      total_exec_time,
      rows,
      shared_blks_hit,
      shared_blks_read,
      shared_blks_dirtied,
      shared_blks_written,
      local_blks_hit,
      local_blks_read,
      local_blks_dirtied,
      local_blks_written,
      temp_blks_read,
      temp_blks_written,
      blk_read_time,
      blk_write_time,
      wal_records,
      wal_fpi,
      wal_bytes,
      statements
    )
    SELECT sd.server_id,sd.sample_id,dbl.*
    FROM
    dblink('server_connection',st_query
    ) AS dbl (
        datid               oid,
        plans               bigint,
        total_plan_time     double precision,
        calls               bigint,
        total_exec_time     double precision,
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
        stmts               integer
    ) JOIN sample_stat_database sd USING (datid)
    WHERE sd.sample_id = s_id AND sd.server_id = sserver_id;

    -- Flushing statements
    CASE
      -- pg_stat_statements v 1.3-1.8
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
          AND extversion IN ('1.3','1.4','1.5','1.6','1.7','1.8')
      )
      THEN
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_statements_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        ) AS t(res char(1));
      ELSE
        RAISE 'Unsupported pg_stat_statements version. Supported versions are 1.3 - 1.8';
    END CASE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION collect_obj_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    --Cursor for db stats
    c_dblist CURSOR FOR
    SELECT datid,datname,tablespaceid FROM dblink('server_connection',
    'select dbs.oid,dbs.datname,dbs.dattablespace from pg_catalog.pg_database dbs
    where dbs.datname not like ''template_'' and dbs.datallowconn') AS dbl (
        datid oid,
        datname name,
        tablespaceid oid
    ) JOIN servers n ON (n.server_id = sserver_id AND array_position(n.db_exclude,dbl.datname) IS NULL);

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
    IF dblink_get_connections() @> ARRAY['server_db_connection'] THEN
        PERFORM dblink_disconnect('server_db_connection');
    END IF;

    -- Load new data from statistic views of all cluster databases
    FOR qres IN c_dblist LOOP
      db_connstr := concat_ws(' ',get_connstr(sserver_id),format('dbname=%L',qres.datname));
      PERFORM dblink_connect('server_db_connection',db_connstr);
      -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
      PERFORM dblink('server_db_connection','SET lock_timeout=3000');

      -- Generate Table stats query
      CASE
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 130000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'NULL as n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            'CASE locked.objid WHEN st.relid THEN NULL ELSE pg_table_size(st.relid) - coalesce(pg_relation_size(class.reltoastrelid),0) END relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          'LEFT OUTER JOIN '
            '(WITH RECURSIVE deps (objid) AS ('
              'SELECT relation FROM pg_locks WHERE granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'' '
              'UNION '
              'SELECT refobjid FROM pg_depend d JOIN deps dd ON (d.objid = dd.objid)'
            ') '
            'SELECT objid FROM deps) AS locked ON (st.relid = locked.objid)';

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer >= 130000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'st.n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            'CASE locked.objid WHEN st.relid THEN NULL ELSE pg_table_size(st.relid) - coalesce(pg_relation_size(class.reltoastrelid),0) END relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          'LEFT OUTER JOIN '
            '(WITH RECURSIVE deps (objid) AS ('
              'SELECT relation FROM pg_locks WHERE granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'' '
              'UNION '
              'SELECT refobjid FROM pg_depend d JOIN deps dd ON (d.objid = dd.objid)'
            ') '
            'SELECT objid FROM deps) AS locked ON (st.relid = locked.objid)';
        ELSE
          RAISE 'Unsupported server version.';
      END CASE;

      INSERT INTO last_stat_tables(
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
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        dbl.relid,
        dbl.schemaname,
        dbl.relname,
        dbl.seq_scan AS seq_scan,
        dbl.seq_tup_read AS seq_tup_read,
        dbl.idx_scan AS idx_scan,
        dbl.idx_tup_fetch AS idx_tup_fetch,
        dbl.n_tup_ins AS n_tup_ins,
        dbl.n_tup_upd AS n_tup_upd,
        dbl.n_tup_del AS n_tup_del,
        dbl.n_tup_hot_upd AS n_tup_hot_upd,
        dbl.n_live_tup AS n_live_tup,
        dbl.n_dead_tup AS n_dead_tup,
        dbl.n_mod_since_analyze AS n_mod_since_analyze,
        dbl.n_ins_since_vacuum AS n_ins_since_vacuum,
        dbl.last_vacuum,
        dbl.last_autovacuum,
        dbl.last_analyze,
        dbl.last_autoanalyze,
        dbl.vacuum_count AS vacuum_count,
        dbl.autovacuum_count AS autovacuum_count,
        dbl.analyze_count AS analyze_count,
        dbl.autoanalyze_count AS autoanalyze_count,
        dbl.heap_blks_read AS heap_blks_read,
        dbl.heap_blks_hit AS heap_blks_hit,
        dbl.idx_blks_read AS idx_blks_read,
        dbl.idx_blks_hit AS idx_blks_hit,
        dbl.toast_blks_read AS toast_blks_read,
        dbl.toast_blks_hit AS toast_blks_hit,
        dbl.tidx_blks_read AS tidx_blks_read,
        dbl.tidx_blks_hit AS tidx_blks_hit,
        dbl.relsize AS relsize,
        dbl.relsize_diff AS relsize_diff,
        CASE WHEN dbl.tablespaceid=0 THEN qres.tablespaceid ELSE dbl.tablespaceid END AS tablespaceid,
        dbl.reltoastrelid,
        dbl.relkind
      FROM dblink('server_db_connection', t_query)
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
          n_ins_since_vacuum    bigint,
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
      t_query := 'SELECT st.*,'
        'stio.idx_blks_read,'
        'stio.idx_blks_hit,'
        'CASE l.relation WHEN st.indexrelid THEN NULL ELSE pg_relation_size(st.indexrelid) END relsize,'
        '0,'
        'pg_class.reltablespace as tablespaceid,'
        '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique '
      'FROM pg_catalog.pg_stat_all_indexes st '
        'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
        'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
        'JOIN pg_class ON (pg_class.oid = st.indexrelid) '
        'LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.conindid = ix.indexrelid AND con.contype in (''p'',''u'')) '
        'LEFT OUTER JOIN pg_catalog.pg_locks l ON (l.relation = st.indexrelid AND l.granted AND l.locktype = ''relation'' AND l.mode=''AccessExclusiveLock'')';

      INSERT INTO last_stat_indexes(
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
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
        tablespaceid,
        indisunique
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        relid,
        indexrelid,
        schemaname,
        relname,
        indexrelname,
        dbl.idx_scan AS idx_scan,
        dbl.idx_tup_read AS idx_tup_read,
        dbl.idx_tup_fetch AS idx_tup_fetch,
        dbl.idx_blks_read AS idx_blks_read,
        dbl.idx_blks_hit AS idx_blks_hit,
        dbl.relsize AS relsize,
        dbl.relsize_diff AS relsize_diff,
        CASE WHEN tablespaceid=0 THEN qres.tablespaceid ELSE tablespaceid END tablespaceid,
        indisunique
      FROM dblink('server_db_connection', t_query)
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
      t_query := 'SELECT f.funcid,'
        'f.schemaname,'
        'f.funcname,'
        'pg_get_function_arguments(f.funcid) AS funcargs,'
        'f.calls,'
        'f.total_time,'
        'f.self_time,'
        'p.prorettype::regtype::text =''trigger'' AS trg_fn '
      'FROM pg_catalog.pg_stat_user_functions f '
        'JOIN pg_catalog.pg_proc p ON (f.funcid = p.oid)';

      INSERT INTO last_stat_user_functions(
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
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        dbl.calls AS calls,
        dbl.total_time AS total_time,
        dbl.self_time AS self_time,
        dbl.trg_fn
      FROM dblink('server_db_connection', t_query)
      AS dbl (
         funcid       oid,
         schemaname   name,
         funcname     name,
         funcargs     text,
         calls        bigint,
         total_time   double precision,
         self_time    double precision,
         trg_fn       boolean
      );

      PERFORM dblink_disconnect('server_db_connection');
    END LOOP;
   RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sample_dbobj_delta(IN sserver_id integer, IN s_id integer, IN topn integer) RETURNS integer AS $$
DECLARE
    qres    record;
BEGIN

    -- Calculating difference from previous sample and storing it in sample_stat_ tables
    -- Stats of user tables
    FOR qres IN
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
            toastn_ins_since_vacuum,
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
                cur.server_id AS server_id,
                cur.sample_id AS sample_id,
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
                cur.n_ins_since_vacuum AS n_ins_since_vacuum,
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
                tcur.n_ins_since_vacuum AS toastn_ins_since_vacuum,
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
                  COALESCE(tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) vacuum_dml_rank,
                row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize, 0) +
                  COALESCE(tcur.relsize,0) - COALESCE(tlst.relsize, 0) DESC) growth_rank,
                row_number() OVER (ORDER BY
                  cur.n_dead_tup / NULLIF(cur.n_live_tup+cur.n_dead_tup, 0)
                  DESC NULLS LAST) dead_pct_rank,
                row_number() OVER (ORDER BY
                  cur.n_mod_since_analyze / NULLIF(cur.n_live_tup, 0)
                  DESC NULLS LAST) mod_pct_rank,
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
                  lst.toast_blks_read+lst.toast_blks_hit+lst.tidx_blks_read+lst.tidx_blks_hit, 0) DESC) gets_rank,
                -- Vacuum rank
                row_number() OVER (ORDER BY cur.vacuum_count - COALESCE(lst.vacuum_count, 0) +
                  cur.autovacuum_count - COALESCE(lst.autovacuum_count, 0) DESC) vacuum_rank,
                row_number() OVER (ORDER BY cur.analyze_count - COALESCE(lst.analyze_count,0) +
                  cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) DESC) analyze_rank
            FROM
              -- main relations diff
              last_stat_tables cur JOIN sample_stat_database dbcur USING (server_id, sample_id, datid)
              LEFT OUTER JOIN sample_stat_database dblst ON
                (dbcur.server_id = dblst.server_id AND dbcur.datid = dblst.datid AND dblst.sample_id = dbcur.sample_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
              LEFT OUTER JOIN last_stat_tables lst ON
                (dblst.server_id=lst.server_id AND lst.sample_id = dblst.sample_id AND lst.datid=dblst.datid AND cur.relid=lst.relid)
              -- toast relations diff
              LEFT OUTER JOIN last_stat_tables tcur ON
                (tcur.server_id=dbcur.server_id AND tcur.sample_id = dbcur.sample_id  AND tcur.datid=dbcur.datid AND cur.reltoastrelid=tcur.relid)
              LEFT OUTER JOIN last_stat_tables tlst ON
                (tlst.server_id=dblst.server_id AND tlst.sample_id = dblst.sample_id AND tlst.datid=dblst.datid AND lst.reltoastrelid=tlst.relid)
            WHERE cur.sample_id=s_id AND cur.server_id=sserver_id
              AND cur.relkind IN ('r','m')) diff
        WHERE
          least(
            scan_rank,
            dml_rank,
            growth_rank,
            dead_pct_rank,
            mod_pct_rank,
            vacuum_dml_rank,
            read_rank,
            gets_rank,
            vacuum_rank,
            analyze_rank
          ) <= topn
    LOOP
        IF qres.toastrelid IS NOT NULL THEN
          INSERT INTO tables_list(
            server_id,
            datid,
            relid,
            relkind,
            reltoastrelid,
            schemaname,
            relname
          )
          VALUES (qres.server_id,qres.datid,qres.toastrelid,qres.toastrelkind,NULL,qres.toastschemaname,qres.toastrelname) ON CONFLICT DO NOTHING;
          INSERT INTO sample_stat_tables(
            server_id,
            sample_id,
            datid,
            relid,
            tablespaceid,
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
            relsize_diff
          )
          VALUES (
              qres.server_id,
              qres.sample_id,
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
              qres.toastn_ins_since_vacuum,
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

        INSERT INTO tables_list(
            server_id,
            datid,
            relid,
            relkind,
            reltoastrelid,
            schemaname,
            relname
          )
        VALUES (qres.server_id,qres.datid,qres.relid,qres.relkind,NULLIF(qres.toastrelid,0),qres.schemaname,qres.relname)
          ON CONFLICT DO NOTHING;
        INSERT INTO sample_stat_tables(
            server_id,
            sample_id,
            datid,
            relid,
            tablespaceid,
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
            relsize_diff
          )
          VALUES (
            qres.server_id,
            qres.sample_id,
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
            qres.n_ins_since_vacuum,
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
    INSERT INTO sample_stat_tables_total(
      server_id,
      sample_id,
      datid,
      tablespaceid,
      relkind,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_tup_ins,
      n_tup_upd,
      n_tup_del,
      n_tup_hot_upd,
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
      relsize_diff
    )
    SELECT
      cur.server_id,
      cur.sample_id,
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
    FROM last_stat_tables cur JOIN sample_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN sample_stat_database dblst ON
        (dbcur.server_id = dblst.server_id AND dbcur.datid = dblst.datid AND dblst.sample_id = dbcur.sample_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
      LEFT OUTER JOIN last_stat_tables lst ON
        (dblst.server_id=lst.server_id AND lst.sample_id = dblst.sample_id AND lst.datid=dblst.datid AND cur.relid=lst.relid AND cur.tablespaceid=lst.tablespaceid)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid;

    -- Stats of user indexes
    FOR qres IN
        SELECT
            server_id,
            sample_id,
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
            tbl_n_ins_since_vacuum,
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
                cur.server_id,
                cur.sample_id,
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
                tblcur.n_ins_since_vacuum as tbl_n_ins_since_vacuum,
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
                row_number() OVER (PARTITION BY cur.idx_scan - COALESCE(lst.idx_scan,0) = 0 ORDER BY cur.relsize - COALESCE(lst.relsize,0) DESC) grow_unused_rank,
                row_number() OVER (ORDER BY (tblcur.vacuum_count - COALESCE(tbllst.vacuum_count,0) +
                  tblcur.autovacuum_count - COALESCE(tbllst.autovacuum_count,0)) * cur.relsize DESC) vacuum_bytes_rank
            FROM last_stat_indexes cur JOIN last_stat_tables tblcur USING (server_id, sample_id, datid, relid)
              JOIN sample_stat_database dbcur USING (server_id, sample_id, datid)
              LEFT OUTER JOIN sample_stat_database dblst ON
                (dbcur.server_id = dblst.server_id AND dbcur.datid = dblst.datid AND dblst.sample_id = dbcur.sample_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
              LEFT OUTER JOIN last_stat_indexes lst ON
                (dblst.server_id = lst.server_id AND lst.sample_id=dblst.sample_id AND dblst.datid = lst.datid AND cur.relid = lst.relid AND cur.indexrelid = lst.indexrelid)
              LEFT OUTER JOIN last_stat_tables tbllst ON
                (tbllst.server_id = dblst.server_id AND tbllst.sample_id = dblst.sample_id AND tbllst.datid = dblst.datid AND tbllst.relid = lst.relid)
              -- Join main table if index is toast index
              LEFT OUTER JOIN last_stat_tables mtbl ON (tblcur.relkind = 't' AND mtbl.server_id = dbcur.server_id AND mtbl.sample_id = dbcur.sample_id
                AND mtbl.datid = dbcur.datid AND mtbl.reltoastrelid = tblcur.relid)
              -- Join toast table if exists
              LEFT OUTER JOIN last_stat_tables ttbl ON (ttbl.relkind = 't' AND ttbl.server_id = dbcur.server_id AND ttbl.sample_id = dbcur.sample_id
                AND ttbl.datid = dbcur.datid AND tblcur.reltoastrelid = ttbl.relid)
            WHERE cur.sample_id = s_id AND cur.server_id = sserver_id) diff
        WHERE least(
            grow_rank,
            read_rank,
            gets_rank,
            vacuum_bytes_rank
          ) <= topn
          OR (grow_unused_rank <= topn AND idx_scan = 0)
    LOOP
        -- Insert TOAST table (if exists) in tables list before parent table
        IF qres.trelid IS NOT NULL THEN
          INSERT INTO tables_list(
            server_id,
            datid,
            relid,
            relkind,
            reltoastrelid,
            schemaname,
            relname
          )
          VALUES (qres.server_id,qres.datid,qres.trelid,qres.trelkind,NULLIF(qres.treltoastrelid,0),qres.tschemaname,qres.trelname) ON CONFLICT DO NOTHING;
        END IF;
        -- Insert index parent table in tables list
        INSERT INTO tables_list(
            server_id,
            datid,
            relid,
            relkind,
            reltoastrelid,
            schemaname,
            relname
          )
          VALUES (qres.server_id,qres.datid,qres.relid,qres.relkind,NULLIF(qres.reltoastrelid,0),qres.schemaname,qres.relname) ON CONFLICT DO NOTHING;
        -- Insert main table (if index is on toast table)
        IF qres.mrelid IS NOT NULL THEN
          INSERT INTO tables_list(
            server_id,
            datid,
            relid,
            relkind,
            reltoastrelid,
            schemaname,
            relname
          )
          VALUES (qres.server_id,qres.datid,qres.mrelid,qres.mrelkind,NULLIF(qres.mreltoastrelid,0),qres.mschemaname,qres.mrelname) ON CONFLICT DO NOTHING;
        END IF;
        -- insert index to index list
        INSERT INTO indexes_list(
          server_id,
          datid,
          indexrelid,
          relid,
          schemaname,
          indexrelname
        )
        VALUES (qres.server_id,qres.datid,qres.indexrelid,qres.relid,qres.schemaname,qres.indexrelname) ON CONFLICT DO NOTHING;
        -- insert index stats
        INSERT INTO sample_stat_indexes(
          server_id,
          sample_id,
          datid,
          indexrelid,
          tablespaceid,
          idx_scan,
          idx_tup_read,
          idx_tup_fetch,
          idx_blks_read,
          idx_blks_hit,
          relsize,
          relsize_diff,
          indisunique
        )
        VALUES (
            qres.server_id,
            qres.sample_id,
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
        INSERT INTO sample_stat_tables(
          server_id,
          sample_id,
          datid,
          relid,
          tablespaceid,
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
          relsize_diff
        )
        VALUES (
            qres.server_id,
            qres.sample_id,
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
            qres.tbl_n_ins_since_vacuum,
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
    INSERT INTO sample_stat_indexes_total(
      server_id,
      sample_id,
      datid,
      tablespaceid,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch,
      idx_blks_read,
      idx_blks_hit,
      relsize_diff
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      cur.tablespaceid,
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_read - COALESCE(lst.idx_tup_read,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      sum(cur.relsize_diff - COALESCE(lst.relsize_diff,0))
    FROM last_stat_indexes cur JOIN sample_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN sample_stat_database dblst ON
        (dbcur.server_id = dblst.server_id AND dbcur.datid = dblst.datid AND dblst.sample_id = dbcur.sample_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
      LEFT OUTER JOIN last_stat_indexes lst
        ON (lst.server_id = dblst.server_id AND lst.sample_id = dblst.sample_id AND lst.datid = dblst.datid AND lst.relid = cur.relid AND lst.indexrelid = cur.indexrelid AND cur.tablespaceid=lst.tablespaceid)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id
    GROUP BY cur.server_id, cur.sample_id, cur.datid,cur.tablespaceid;

    -- User functions stats
    FOR qres IN
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
        FROM
            (SELECT
                cur.server_id,
                cur.sample_id,
                cur.datid,
                cur.funcid,
                cur.schemaname,
                cur.funcname,
                cur.funcargs,
                cur.calls - COALESCE(lst.calls,0) AS calls,
                cur.total_time - COALESCE(lst.total_time,0) AS total_time,
                cur.self_time - COALESCE(lst.self_time,0) AS self_time,
                cur.trg_fn,
                row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.total_time - COALESCE(lst.total_time,0) DESC) time_rank,
                row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.self_time - COALESCE(lst.self_time,0) DESC) stime_rank,
                row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.calls - COALESCE(lst.calls,0) DESC) calls_rank
            FROM last_stat_user_functions cur JOIN sample_stat_database dbcur USING (server_id, sample_id, datid)
              LEFT OUTER JOIN sample_stat_database dblst ON
                (dbcur.server_id = dblst.server_id AND dbcur.datid = dblst.datid AND dblst.sample_id = dbcur.sample_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
              LEFT OUTER JOIN last_stat_user_functions lst ON
                (lst.server_id = dblst.server_id AND lst.sample_id = dblst.sample_id AND lst.datid = dblst.datid AND cur.funcid=lst.funcid)
            WHERE cur.sample_id = s_id AND cur.server_id = sserver_id
                AND cur.calls - COALESCE(lst.calls,0) > 0) diff
        WHERE time_rank <= topn OR calls_rank <= topn OR stime_rank <= topn
    LOOP
        INSERT INTO funcs_list(
          server_id,
          datid,
          funcid,
          schemaname,
          funcname,
          funcargs
        )
        VALUES (qres.server_id,qres.datid,qres.funcid,qres.schemaname,qres.funcname,qres.funcargs) ON CONFLICT DO NOTHING;
        INSERT INTO sample_stat_user_functions(
          server_id,
          sample_id,
          datid,
          funcid,
          calls,
          total_time,
          self_time,
          trg_fn
        )
        VALUES (
            qres.server_id,
            qres.sample_id,
            qres.datid,
            qres.funcid,
            qres.calls,
            qres.total_time,
            qres.self_time,
            qres.trg_fn
        );
    END LOOP;

    -- Total functions stats
    INSERT INTO sample_stat_user_func_total(
      server_id,
      sample_id,
      datid,
      calls,
      total_time,
      trg_fn
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      sum(cur.calls - COALESCE(lst.calls,0)),
      sum(cur.total_time - COALESCE(lst.total_time,0)),
      cur.trg_fn
    FROM last_stat_user_functions cur JOIN sample_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN sample_stat_database dblst ON
        (dbcur.server_id = dblst.server_id AND dbcur.datid = dblst.datid AND dblst.sample_id = dbcur.sample_id - 1 AND dbcur.stats_reset = dblst.stats_reset)
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id = dblst.server_id AND lst.sample_id = dblst.sample_id AND lst.datid = dblst.datid AND cur.funcid=lst.funcid)
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.trg_fn;

    -- Clear data in last_ tables, holding data only for next diff sample
    DELETE FROM last_stat_tables WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_indexes WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_user_functions WHERE server_id=sserver_id AND sample_id != s_id;

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION show_samples(IN server name,IN days integer = NULL)
RETURNS TABLE(
    sample integer,
    sample_time timestamp (0) with time zone,
    dbstats_reset timestamp (0) with time zone,
    bgwrstats_reset timestamp (0) with time zone,
    archstats_reset timestamp (0) with time zone)
SET search_path=@extschema@,public AS $$
  SELECT
    s.sample_id,
    s.sample_time,
    max(nullif(db1.stats_reset,coalesce(db2.stats_reset,db1.stats_reset))) AS dbstats_reset,
    max(nullif(bgwr1.stats_reset,coalesce(bgwr2.stats_reset,bgwr1.stats_reset))) AS bgwrstats_reset,
    max(nullif(arch1.stats_reset,coalesce(arch2.stats_reset,arch1.stats_reset))) AS archstats_reset
  FROM samples s JOIN servers n USING (server_id)
    JOIN sample_stat_database db1 USING (server_id,sample_id)
    JOIN sample_stat_cluster bgwr1 USING (server_id,sample_id)
    LEFT OUTER JOIN sample_stat_archiver arch1 USING (server_id,sample_id)
    LEFT OUTER JOIN sample_stat_database db2 ON (db1.server_id = db2.server_id AND db1.datid = db2.datid AND db2.sample_id = db1.sample_id - 1)
    LEFT OUTER JOIN sample_stat_cluster bgwr2 ON (bgwr1.server_id = bgwr2.server_id AND bgwr2.sample_id = bgwr1.sample_id - 1)
    LEFT OUTER JOIN sample_stat_archiver arch2 ON (arch1.server_id = arch2.server_id AND arch2.sample_id = arch1.sample_id - 1)
  WHERE (days IS NULL OR s.sample_time > now() - (days || ' days')::interval)
    AND server_name = server
  GROUP BY s.sample_id, s.sample_time
  ORDER BY s.sample_id ASC
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_samples(IN server name,IN days integer) IS 'Display available server samples';

CREATE OR REPLACE FUNCTION show_samples(IN days integer = NULL)
RETURNS TABLE(
    sample integer,
    sample_time timestamp (0) with time zone,
    dbstats_reset timestamp (0) with time zone,
    clustats_reset timestamp (0) with time zone,
    archstats_reset timestamp (0) with time zone)
SET search_path=@extschema@,public AS $$
    SELECT * FROM show_samples('local',days);
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_samples(IN days integer) IS 'Display available samples for local server';
