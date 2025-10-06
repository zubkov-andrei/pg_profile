CREATE FUNCTION take_sample(IN sserver_id integer, IN skip_sizes boolean
) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    s_id              integer;
    topn              integer;
    ret               integer;
    server_properties jsonb;
    qres              record;
    qres_settings     record;
    settings_refresh  boolean = true;
    collect_timings   boolean = false;

    server_query      text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;

    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Only one running take_sample() function allowed per server!
    -- Explicitly lock server in servers table
    BEGIN
        SELECT * INTO qres FROM servers WHERE server_id = sserver_id FOR UPDATE NOWAIT;
    EXCEPTION
        WHEN OTHERS THEN RAISE 'Can''t get lock on server. Is there another take_sample() function running on this server?';
    END;

    -- Initialize sample
    server_properties := init_sample(sserver_id);
    ASSERT server_properties IS NOT NULL, 'lost properties';

    -- Merge srv_settings into server_properties structure
    FOR qres_settings IN (
      SELECT key, value
      FROM jsonb_each(qres.srv_settings)
    ) LOOP
      server_properties := jsonb_set(
        server_properties,
        ARRAY[qres_settings.key],
        qres_settings.value
      );
    END LOOP; -- over srv_settings enties
    ASSERT server_properties IS NOT NULL, 'lost properties on srv_settings merge';

    /* Set the in_sample flag notifying sampling functions that the current
      processing caused by take_sample(), not by take_subsample()
    */
    server_properties := jsonb_set(server_properties, '{properties,in_sample}', to_jsonb(true));

    topn := (server_properties #>> '{properties,topn}')::integer;

    -- Creating a new sample record
    UPDATE servers SET last_sample_id = last_sample_id + 1 WHERE server_id = sserver_id
      RETURNING last_sample_id INTO s_id;
    INSERT INTO samples(sample_time,server_id,sample_id)
      VALUES (now(),sserver_id,s_id);

    -- Once the new sample is created it becomes last one
    server_properties := jsonb_set(
      server_properties,
      '{properties,last_sample_id}',
      to_jsonb(s_id)
    );

    -- Getting max_sample_age setting
    BEGIN
        ret := COALESCE(current_setting('{pg_profile}.max_sample_age')::integer);
    EXCEPTION
        WHEN OTHERS THEN ret := 7;
    END;

    -- Applying skip sizes policy
    IF skip_sizes IS NULL THEN
      CASE COALESCE(
        qres.srv_settings #>> '{relsizes,collect_mode}',
        nullif(current_setting('{pg_profile}.relsize_collect_mode', true)::text,''),
        'off'
      )
        WHEN 'on' THEN
          skip_sizes := false;
        WHEN 'off' THEN
          skip_sizes := true;
        WHEN 'schedule' THEN
          /*
          Skip sizes collection if there was a sample with sizes recently
          or if we are not in size collection time window
          */
          SELECT
            count(*) > 0 OR
            NOT
            CASE WHEN timezone('UTC',current_time) > timezone('UTC',(qres.srv_settings #>> '{relsizes,window_start}')::timetz) THEN
              timezone('UTC',now()) <=
              timezone('UTC',(timezone('UTC',now())::pg_catalog.date +
              timezone('UTC',(qres.srv_settings #>> '{relsizes,window_start}')::timetz) +
              (qres.srv_settings #>> '{relsizes,window_duration}')::interval))
            ELSE
              timezone('UTC',now()) <=
              timezone('UTC',(timezone('UTC',now() - interval '1 day')::pg_catalog.date +
              timezone('UTC',(qres.srv_settings #>> '{relsizes,window_start}')::timetz) +
              (qres.srv_settings #>> '{relsizes,window_duration}')::interval))
            END
              INTO STRICT skip_sizes
          FROM
            sample_stat_tables_total st
            JOIN samples s USING (server_id, sample_id)
          WHERE
            server_id = sserver_id
            AND st.relsize_diff IS NOT NULL
            AND sample_time > now() - (qres.srv_settings #>> '{relsizes,sample_interval}')::interval;
        ELSE
          skip_sizes := true;
      END CASE;
    END IF;

    -- Collecting postgres parameters
    /* We might refresh all parameters if version() was changed
    * This is needed for deleting obsolete parameters, not appearing in new
    * Postgres version.
    */
    SELECT ss.setting != dblver.version INTO settings_refresh
    FROM v_sample_settings ss, dblink('server_connection','SELECT version() as version') AS dblver (version text)
    WHERE ss.server_id = sserver_id AND ss.sample_id = s_id AND ss.name='version' AND ss.setting_scope = 2;
    settings_refresh := COALESCE(settings_refresh,true);

    -- Constructing server sql query for settings
    server_query := 'SELECT 1 as setting_scope,name,setting,reset_val,boot_val,unit,sourcefile,sourceline,pending_restart '
      'FROM pg_catalog.pg_settings '
      'UNION ALL SELECT 2 as setting_scope,''version'',version(),version(),NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''pg_postmaster_start_time'','
      'pg_catalog.pg_postmaster_start_time()::text,'
      'pg_catalog.pg_postmaster_start_time()::text,NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''pg_conf_load_time'','
      'pg_catalog.pg_conf_load_time()::text,pg_catalog.pg_conf_load_time()::text,NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''system_identifier'','
      'system_identifier::text,system_identifier::text,system_identifier::text,'
      'NULL,NULL,NULL,False FROM pg_catalog.pg_control_system()';

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
      sample_settings lst JOIN (
        -- Getting last versions of settings
        SELECT server_id, name, max(first_seen) as first_seen
        FROM sample_settings
        WHERE server_id = sserver_id AND (
          NOT settings_refresh
          -- system identifier shouldn't have a duplicate in case of version change
          -- this breaks export/import procedures, as those are related to this ID
          OR name = 'system_identifier'
        )
        GROUP BY server_id, name
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
      (s.server_id = prm.server_id AND s.sample_id = prm.sample_id AND prm.name = '{pg_profile}.topn' AND prm.setting_scope = 1)
    WHERE s.server_id = sserver_id AND s.sample_id = s_id AND (prm.setting IS NULL OR prm.setting::integer != topn);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,get server environment,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect database stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Construct pg_stat_database query
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 180000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'dbs.checksum_failures, '
            'dbs.checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'dbs.session_time, '
            'dbs.active_time, '
            'dbs.idle_in_transaction_time, '
            'dbs.sessions, '
            'dbs.sessions_abandoned, '
            'dbs.sessions_fatal, '
            'dbs.sessions_killed, '
            'dbs.parallel_workers_to_launch, '
            'dbs.parallel_workers_launched, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate, '
            'db.dattablespace, '
            'db.datallowconn '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 140000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'dbs.checksum_failures, '
            'dbs.checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'dbs.session_time, '
            'dbs.active_time, '
            'dbs.idle_in_transaction_time, '
            'dbs.sessions, '
            'dbs.sessions_abandoned, '
            'dbs.sessions_fatal, '
            'dbs.sessions_killed, '
            'NULL as parallel_workers_to_launch, '
            'NULL as parallel_workers_launched, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate, '
            'db.dattablespace, '
            'db.datallowconn '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 120000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'dbs.checksum_failures, '
            'dbs.checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'NULL as session_time, '
            'NULL as active_time, '
            'NULL as idle_in_transaction_time, '
            'NULL as sessions, '
            'NULL as sessions_abandoned, '
            'NULL as sessions_fatal, '
            'NULL as sessions_killed, '
            'NULL as parallel_workers_to_launch, '
            'NULL as parallel_workers_launched, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate, '
            'db.dattablespace, '
            'db.datallowconn '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer < 120000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'NULL as checksum_failures, '
            'NULL as checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'NULL as session_time, '
            'NULL as active_time, '
            'NULL as idle_in_transaction_time, '
            'NULL as sessions, '
            'NULL as sessions_abandoned, '
            'NULL as sessions_fatal, '
            'NULL as sessions_killed, '
            'NULL as parallel_workers_to_launch, '
            'NULL as parallel_workers_launched, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate, '
            'db.dattablespace, '
            'db.datallowconn '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
    END CASE;

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
        checksum_failures,
        checksum_last_failure,
        blk_read_time,
        blk_write_time,
        session_time,
        active_time,
        idle_in_transaction_time,
        sessions,
        sessions_abandoned,
        sessions_fatal,
        sessions_killed,
        parallel_workers_to_launch,
        parallel_workers_launched,
        stats_reset,
        datsize,
        datsize_delta,
        datistemplate,
        dattablespace,
        datallowconn)
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
        checksum_failures as checksum_failures,
        checksum_last_failure as checksum_failures,
        blk_read_time AS blk_read_time,
        blk_write_time AS blk_write_time,
        session_time AS session_time,
        active_time AS active_time,
        idle_in_transaction_time AS idle_in_transaction_time,
        sessions AS sessions,
        sessions_abandoned AS sessions_abandoned,
        sessions_fatal AS sessions_fatal,
        sessions_killed AS sessions_killed,
        parallel_workers_to_launch as parallel_workers_to_launch,
        parallel_workers_launched as parallel_workers_launched,
        stats_reset,
        datsize AS datsize,
        datsize_delta AS datsize_delta,
        datistemplate AS datistemplate,
        dattablespace AS dattablespace,
        datallowconn AS datallowconn
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
        checksum_failures bigint,
        checksum_last_failure timestamp with time zone,
        blk_read_time double precision,
        blk_write_time double precision,
        session_time double precision,
        active_time double precision,
        idle_in_transaction_time double precision,
        sessions bigint,
        sessions_abandoned bigint,
        sessions_fatal bigint,
        sessions_killed bigint,
        parallel_workers_to_launch bigint,
        parallel_workers_launched bigint,
        stats_reset timestamp with time zone,
        datsize bigint,
        datsize_delta bigint,
        datistemplate boolean,
        dattablespace oid,
        datallowconn boolean
        );

    EXECUTE format('ANALYZE last_stat_database_srv%1$s',
      sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect database stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate database stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;
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
      checksum_failures,
      checksum_last_failure,
      blk_read_time,
      blk_write_time,
      session_time,
      active_time,
      idle_in_transaction_time,
      sessions,
      sessions_abandoned,
      sessions_fatal,
      sessions_killed,
      parallel_workers_to_launch,
      parallel_workers_launched,
      stats_reset,
      datsize,
      datsize_delta,
      datistemplate
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
        cur.checksum_failures - COALESCE(lst.checksum_failures,0),
        cur.checksum_last_failure,
        cur.blk_read_time - COALESCE(lst.blk_read_time,0),
        cur.blk_write_time - COALESCE(lst.blk_write_time,0),
        cur.session_time - COALESCE(lst.session_time,0),
        cur.active_time - COALESCE(lst.active_time,0),
        cur.idle_in_transaction_time - COALESCE(lst.idle_in_transaction_time,0),
        cur.sessions - COALESCE(lst.sessions,0),
        cur.sessions_abandoned - COALESCE(lst.sessions_abandoned,0),
        cur.sessions_fatal - COALESCE(lst.sessions_fatal,0),
        cur.sessions_killed - COALESCE(lst.sessions_killed,0),
        cur.parallel_workers_to_launch - COALESCE(lst.parallel_workers_to_launch,0),
        cur.parallel_workers_launched - COALESCE(lst.parallel_workers_launched,0),
        cur.stats_reset,
        cur.datsize as datsize,
        cur.datsize - COALESCE(lst.datsize,0) as datsize_delta,
        cur.datistemplate
    FROM last_stat_database cur
      LEFT OUTER JOIN last_stat_database lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.datname) =
        (sserver_id, s_id - 1, cur.datid, cur.datname)
        AND lst.stats_reset IS NOT DISTINCT FROM cur.stats_reset
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id);

    /*
    * In case of statistics reset full database size, and checksum checksum_failures
    * is incorrectly considered as increment by previous query.
    * So, we need to update it with correct value
    */
    UPDATE sample_stat_database sdb
    SET
      datsize_delta = cur.datsize - lst.datsize,
      checksum_failures = cur.checksum_failures - lst.checksum_failures,
      checksum_last_failure = cur.checksum_last_failure
    FROM
      last_stat_database cur
      JOIN last_stat_database lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.datname) =
        (sserver_id, s_id - 1, cur.datid, cur.datname)
    WHERE cur.stats_reset IS DISTINCT FROM lst.stats_reset AND
      (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
      (sdb.server_id, sdb.sample_id, sdb.datid, sdb.datname) =
      (cur.server_id, cur.sample_id, cur.datid, cur.datname);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate database stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect tablespace stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Construct tablespace stats query
    server_query := 'SELECT '
        'oid as tablespaceid,'
        'spcname as tablespacename,'
        'pg_catalog.pg_tablespace_location(oid) as tablespacepath,'
        'pg_catalog.pg_tablespace_size(oid) as size,'
        '0 as size_delta '
        'FROM pg_catalog.pg_tablespace ';

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

    EXECUTE format('ANALYZE last_stat_tablespaces_srv%1$s',
      sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect tablespace stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect statement stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Search for statements statistics extension
    CASE
      -- pg_stat_statements statistics collection
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(server_properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_statements'
      ) AND COALESCE((server_properties #> '{collect,pg_stat_statements}')::boolean, true) THEN
        PERFORM collect_pg_stat_statements_stats(server_properties, sserver_id, s_id, topn);
      ELSE
        NULL;
    END CASE;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect statement stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect wait sampling stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Search for wait sampling extension
    CASE
      -- pg_wait_sampling statistics collection
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(server_properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_wait_sampling'
      ) AND COALESCE((server_properties #> '{collect,pg_wait_sampling}')::boolean, true)THEN
        PERFORM collect_pg_wait_sampling_stats(server_properties, sserver_id, s_id, topn);
      ELSE
        NULL;
    END CASE;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect wait sampling stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_bgwriter}',jsonb_build_object('start',clock_timestamp()));
    END IF;

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
          'NULL as checkpoints_done,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'NULL as slru_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
          'THEN pg_catalog.pg_xlog_location_diff(pg_catalog.pg_last_xlog_replay_location(),''0/00000000'') '
          'ELSE pg_catalog.pg_xlog_location_diff(pg_catalog.pg_current_xlog_location(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
          'THEN pg_catalog.pg_last_xlog_replay_location() '
          'ELSE pg_catalog.pg_current_xlog_location() '
          'END AS wal_lsn,'
          'pg_is_in_recovery() AS in_recovery,'
          'NULL AS restartpoints_timed,'
          'NULL AS restartpoints_req,'
          'NULL AS restartpoints_done,'
          'stats_reset as checkpoint_stats_reset '
          'FROM pg_catalog.pg_stat_bgwriter';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer < 170000
      )
      THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'NULL as checkpoints_done,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'NULL as slru_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(),''0/00000000'') '
            'ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_last_wal_replay_lsn() '
            'ELSE pg_catalog.pg_current_wal_lsn() '
          'END AS wal_lsn,'
          'pg_catalog.pg_is_in_recovery() as in_recovery, '
          'NULL AS restartpoints_timed,'
          'NULL AS restartpoints_req,'
          'NULL AS restartpoints_done,'
          'stats_reset as checkpoint_stats_reset '
        'FROM pg_catalog.pg_stat_bgwriter';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer < 180000
      )
      THEN
        server_query := 'SELECT '
          'c.num_timed as checkpoints_timed,'
          'c.num_requested as checkpoints_req,'
          'NULL as checkpoints_done,'
          'c.write_time as checkpoint_write_time,'
          'c.sync_time as checkpoint_sync_time,'
          'c.buffers_written as buffers_checkpoint,'
          'NULL as slru_checkpoint,'
          'b.buffers_clean as buffers_clean,'
          'b.maxwritten_clean as maxwritten_clean,'
          'NULL as buffers_backend,'
          'NULL as buffers_backend_fsync,'
          'b.buffers_alloc,'
          'b.stats_reset as stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(),''0/00000000'') '
            'ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery()'
            'THEN pg_catalog.pg_last_wal_replay_lsn()'
            'ELSE pg_catalog.pg_current_wal_lsn()'
          'END AS wal_lsn,'
          'pg_catalog.pg_is_in_recovery() as in_recovery,'
          'c.restartpoints_timed,'
          'c.restartpoints_req,'
          'c.restartpoints_done,'
          'c.stats_reset as checkpoint_stats_reset '
        'FROM '
          'pg_catalog.pg_stat_checkpointer c CROSS JOIN '
          'pg_catalog.pg_stat_bgwriter b';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 180000
      )
      THEN
        server_query := 'SELECT '
          'c.num_timed as checkpoints_timed,'
          'c.num_requested as checkpoints_req,'
          'c.num_done as checkpoints_done,'
          'c.write_time as checkpoint_write_time,'
          'c.sync_time as checkpoint_sync_time,'
          'c.buffers_written as buffers_checkpoint,'
          'c.slru_written as slru_checkpoint,'
          'b.buffers_clean as buffers_clean,'
          'b.maxwritten_clean as maxwritten_clean,'
          'NULL as buffers_backend,'
          'NULL as buffers_backend_fsync,'
          'b.buffers_alloc,'
          'b.stats_reset as stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(),''0/00000000'') '
            'ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery()'
            'THEN pg_catalog.pg_last_wal_replay_lsn()'
            'ELSE pg_catalog.pg_current_wal_lsn()'
          'END AS wal_lsn,'
          'pg_catalog.pg_is_in_recovery() as in_recovery,'
          'c.restartpoints_timed,'
          'c.restartpoints_req,'
          'c.restartpoints_done,'
          'c.stats_reset as checkpoint_stats_reset '
        'FROM '
          'pg_catalog.pg_stat_checkpointer c CROSS JOIN '
          'pg_catalog.pg_stat_bgwriter b';
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_cluster (
        server_id,
        sample_id,
        checkpoints_timed,
        checkpoints_req,
        checkpoints_done,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        slru_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset,
        wal_size,
        wal_lsn,
        in_recovery,
        restartpoints_timed,
        restartpoints_req,
        restartpoints_done,
        checkpoint_stats_reset)
      SELECT
        sserver_id,
        s_id,
        checkpoints_timed,
        checkpoints_req,
        checkpoints_done,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        slru_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset,
        wal_size,
        wal_lsn,
        in_recovery,
        restartpoints_timed,
        restartpoints_req,
        restartpoints_done,
        checkpoint_stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        checkpoints_timed bigint,
        checkpoints_req bigint,
        checkpoints_done bigint,
        checkpoint_write_time double precision,
        checkpoint_sync_time double precision,
        buffers_checkpoint bigint,
        slru_checkpoint bigint,
        buffers_clean bigint,
        maxwritten_clean bigint,
        buffers_backend bigint,
        buffers_backend_fsync bigint,
        buffers_alloc bigint,
        stats_reset timestamp with time zone,
        wal_size bigint,
        wal_lsn pg_lsn,
        in_recovery boolean,
        restartpoints_timed bigint,
        restartpoints_req bigint,
        restartpoints_done bigint,
        checkpoint_stats_reset timestamp with time zone);
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_bgwriter,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_wal}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_wal data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 180000
      )
      THEN
        server_query := 'SELECT '
          'wal.wal_records,'
          'wal.wal_fpi,'
          'wal.wal_bytes,'
          'wal.wal_buffers_full,'
          'NULL as wal_write,'
          'NULL as wal_sync,'
          'NULL as wal_write_time,'
          'NULL as wal_sync_time,'
          'wal.stats_reset '
          'FROM pg_catalog.pg_stat_wal wal';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 140000
      )
      THEN
        server_query := 'SELECT '
          'wal_records,'
          'wal_fpi,'
          'wal_bytes,'
          'wal_buffers_full,'
          'wal_write,'
          'wal_sync,'
          'wal_write_time,'
          'wal_sync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_wal';
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_wal (
        server_id,
        sample_id,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time,
        stats_reset
      )
      SELECT
        sserver_id,
        s_id,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        wal_buffers_full    bigint,
        wal_write           bigint,
        wal_sync            bigint,
        wal_write_time      double precision,
        wal_sync_time       double precision,
        stats_reset         timestamp with time zone);
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_wal,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_io}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_io data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 180000
      )
      THEN
        server_query := 'SELECT '
          'backend_type,'
          'object,'
          'pg_stat_io.context,'
          'reads,'
          'read_bytes,'
          'read_time,'
          'writes,'
          'write_bytes,'
          'write_time,'
          'writebacks,'
          'writeback_time,'
          'extends,'
          'extend_bytes,'
          'extend_time,'
          'ps.setting::integer AS op_bytes,'
          'hits,'
          'evictions,'
          'reuses,'
          'fsyncs,'
          'fsync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_io '
          'JOIN pg_catalog.pg_settings ps ON name = ''block_size'' '
          'WHERE greatest('
              'reads,'
              'writes,'
              'writebacks,'
              'extends,'
              'hits,'
              'evictions,'
              'reuses,'
              'fsyncs'
            ') > 0'
          ;
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 160000
      )
      THEN
        server_query := 'SELECT '
          'backend_type,'
          'object,'
          'context,'
          'reads,'
          'NULL as read_bytes,'
          'read_time,'
          'writes,'
          'NULL as write_bytes,'
          'write_time,'
          'writebacks,'
          'writeback_time,'
          'extends,'
          'NULL as extend_bytes,'
          'extend_time,'
          'op_bytes,'
          'hits,'
          'evictions,'
          'reuses,'
          'fsyncs,'
          'fsync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_io '
          'WHERE greatest('
              'reads,'
              'writes,'
              'writebacks,'
              'extends,'
              'hits,'
              'evictions,'
              'reuses,'
              'fsyncs'
            ') > 0'
          ;
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_io (
        server_id,
        sample_id,
        backend_type,
        object,
        context,
        reads,
        read_bytes,
        read_time,
        writes,
        write_bytes,
        write_time,
        writebacks,
        writeback_time,
        extends,
        extend_bytes,
        extend_time,
        op_bytes,
        hits,
        evictions,
        reuses,
        fsyncs,
        fsync_time,
        stats_reset
      )
      SELECT
        sserver_id,
        s_id,
        backend_type,
        object,
        context,
        reads,
        read_bytes,
        read_time,
        writes,
        write_bytes,
        write_time,
        writebacks,
        writeback_time,
        extends,
        extend_bytes,
        extend_time,
        op_bytes,
        hits,
        evictions,
        reuses,
        fsyncs,
        fsync_time,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        backend_type      text,
        object            text,
        context           text,
        reads             bigint,
        read_bytes        numeric,
        read_time         double precision,
        writes            bigint,
        write_bytes       numeric,
        write_time        double precision,
        writebacks        bigint,
        writeback_time    double precision,
        extends           bigint,
        extend_bytes      numeric,
        extend_time       double precision,
        op_bytes          bigint,
        hits              bigint,
        evictions         bigint,
        reuses            bigint,
        fsyncs            bigint,
        fsync_time        double precision,
        stats_reset       timestamp with time zone
      );
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_io,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_slru}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_slru data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 130000
      )
      THEN
        server_query := 'SELECT '
          'name,'
          'blks_zeroed,'
          'blks_hit,'
          'blks_read,'
          'blks_written,'
          'blks_exists,'
          'flushes,'
          'truncates,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_slru '
          'WHERE greatest('
              'blks_zeroed,'
              'blks_hit,'
              'blks_read,'
              'blks_written,'
              'blks_exists,'
              'flushes,'
              'truncates'
            ') > 0'
          ;
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_slru (
        server_id,
        sample_id,
        name,
        blks_zeroed,
        blks_hit,
        blks_read,
        blks_written,
        blks_exists,
        flushes,
        truncates,
        stats_reset
      )
      SELECT
        sserver_id,
        s_id,
        name,
        blks_zeroed,
        blks_hit,
        blks_read,
        blks_written,
        blks_exists,
        flushes,
        truncates,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        name          text,
        blks_zeroed   bigint,
        blks_hit      bigint,
        blks_read     bigint,
        blks_written  bigint,
        blks_exists   bigint,
        flushes       bigint,
        truncates     bigint,
        stats_reset   timestamp with time zone
      );
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_slru,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_archiver}',jsonb_build_object('start',clock_timestamp()));
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

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_archiver,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect object stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Collecting stat info for objects of all databases
    IF COALESCE((server_properties #> '{collect,objects}')::boolean, true) THEN
      server_properties := collect_obj_stats(server_properties, sserver_id, s_id, skip_sizes);
      ASSERT server_properties IS NOT NULL, 'lost properties';
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect object stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,processing subsamples}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Process subsamples if enabled
    IF (server_properties #>> '{properties,subsample_enabled}')::boolean THEN
      /*
       We must get a lock on subsample before taking a subsample to avoid
       sample failure due to lock held in concurrent take_subsample() call.
       take_subsample() function acquires a lock in NOWAIT mode to avoid long
       waits in a subsample. But we should wait here in sample because sample
       must be taken anyway and we need to avoid subsample interfere.
      */
      PERFORM
      FROM server_subsample
      WHERE server_id = sserver_id
      FOR UPDATE;

      server_properties := take_subsample(sserver_id, server_properties);
      server_properties := collect_subsamples(sserver_id, s_id, server_properties);
      ASSERT server_properties IS NOT NULL, 'lost properties';
    END IF;

    IF (SELECT count(*) > 0 FROM last_stat_activity_count WHERE server_id = sserver_id) OR
       (SELECT count(*) > 0 FROM last_stat_activity WHERE server_id = sserver_id)
    THEN
      EXECUTE format('DELETE FROM last_stat_activity_srv%1$s',
        sserver_id);
      EXECUTE format('DELETE FROM last_stat_activity_count_srv%1$s',
        sserver_id);
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,processing subsamples,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,disconnect}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    PERFORM dblink('server_connection', 'COMMIT');
    PERFORM dblink_disconnect('server_connection');

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,disconnect,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,maintain repository}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Updating dictionary table in case of object renaming:
    -- Databases
    UPDATE sample_stat_database AS db
    SET datname = lst.datname
    FROM last_stat_database AS lst
    WHERE
      (db.server_id, lst.server_id, lst.sample_id, db.datid) =
      (sserver_id, sserver_id, s_id, lst.datid)
      AND db.datname != lst.datname;
    -- Tables
    UPDATE tables_list AS tl
    SET (schemaname, relname) = (lst.schemaname, lst.relname)
    FROM last_stat_tables AS lst
    WHERE (tl.server_id, lst.server_id, lst.sample_id, tl.datid, tl.relid, tl.relkind) =
        (sserver_id, sserver_id, s_id, lst.datid, lst.relid, lst.relkind)
      AND (tl.schemaname, tl.relname) != (lst.schemaname, lst.relname);
    -- Functions
    UPDATE funcs_list AS fl
    SET (schemaname, funcname, funcargs) =
      (lst.schemaname, lst.funcname, lst.funcargs)
    FROM last_stat_user_functions AS lst
    WHERE (fl.server_id, lst.server_id, lst.sample_id, fl.datid, fl.funcid) =
        (sserver_id, sserver_id, s_id, lst.datid, lst.funcid)
      AND (fl.schemaname, fl.funcname, fl.funcargs) !=
        (lst.schemaname, lst.funcname, lst.funcargs);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,maintain repository,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate tablespace stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    INSERT INTO tablespaces_list AS itl (
        server_id,
        last_sample_id,
        tablespaceid,
        tablespacename,
        tablespacepath
      )
    SELECT
      cur.server_id,
      NULL,
      cur.tablespaceid,
      cur.tablespacename,
      cur.tablespacepath
    FROM
      last_stat_tablespaces cur
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    ON CONFLICT ON CONSTRAINT pk_tablespace_list DO
    UPDATE SET
        (last_sample_id, tablespacename, tablespacepath) =
        (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath)
      WHERE
        (itl.last_sample_id, itl.tablespacename, itl.tablespacepath) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath);

    -- Calculate diffs for tablespaces
    INSERT INTO sample_stat_tablespaces(
      server_id,
      sample_id,
      tablespaceid,
      size,
      size_delta
    )
    SELECT
      cur.server_id as server_id,
      cur.sample_id as sample_id,
      cur.tablespaceid as tablespaceid,
      cur.size as size,
      cur.size - COALESCE(lst.size, 0) AS size_delta
    FROM last_stat_tablespaces cur
      LEFT OUTER JOIN last_stat_tablespaces lst ON
        (lst.server_id, lst.sample_id, cur.tablespaceid) =
        (sserver_id, s_id - 1, lst.tablespaceid)
    WHERE (cur.server_id, cur.sample_id) = ( sserver_id, s_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate tablespace stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate object stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- collect databases objects stats
    IF COALESCE((server_properties #> '{collect,objects}')::boolean, true) THEN
      server_properties := sample_dbobj_delta(server_properties,sserver_id,s_id,topn,skip_sizes);
      ASSERT server_properties IS NOT NULL, 'lost properties';
    END IF;

    DELETE FROM last_stat_tablespaces WHERE server_id = sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_database WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate object stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate cluster stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc stat cluster diff
    INSERT INTO sample_stat_cluster(
      server_id,
      sample_id,
      checkpoints_timed,
      checkpoints_req,
      checkpoints_done,
      checkpoint_write_time,
      checkpoint_sync_time,
      buffers_checkpoint,
      slru_checkpoint,
      buffers_clean,
      maxwritten_clean,
      buffers_backend,
      buffers_backend_fsync,
      buffers_alloc,
      stats_reset,
      wal_size,
      wal_lsn,
      in_recovery,
      restartpoints_timed,
      restartpoints_req,
      restartpoints_done,
      checkpoint_stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.checkpoints_timed - COALESCE(lstc.checkpoints_timed,0),
        cur.checkpoints_req - COALESCE(lstc.checkpoints_req,0),
        cur.checkpoints_done - COALESCE(lstc.checkpoints_done,0),
        cur.checkpoint_write_time - COALESCE(lstc.checkpoint_write_time,0),
        cur.checkpoint_sync_time - COALESCE(lstc.checkpoint_sync_time,0),
        cur.buffers_checkpoint - COALESCE(lstc.buffers_checkpoint,0),
        cur.slru_checkpoint - COALESCE(lstc.slru_checkpoint,0),
        cur.buffers_clean - COALESCE(lstb.buffers_clean,0),
        cur.maxwritten_clean - COALESCE(lstb.maxwritten_clean,0),
        cur.buffers_backend - COALESCE(lstb.buffers_backend,0),
        cur.buffers_backend_fsync - COALESCE(lstb.buffers_backend_fsync,0),
        cur.buffers_alloc - COALESCE(lstb.buffers_alloc,0),
        cur.stats_reset,
        cur.wal_size - COALESCE(lstb.wal_size,0),
        /* We will overwrite this value in case of stats reset
         * (see below)
         */
        cur.wal_lsn,
        cur.in_recovery,
        cur.restartpoints_timed - COALESCE(lstc.restartpoints_timed,0),
        cur.restartpoints_timed - COALESCE(lstc.restartpoints_timed,0),
        cur.restartpoints_timed - COALESCE(lstc.restartpoints_timed,0),
        cur.checkpoint_stats_reset
    FROM last_stat_cluster cur
      LEFT OUTER JOIN last_stat_cluster lstb ON
        (lstb.server_id, lstb.sample_id) =
        (sserver_id, s_id - 1)
        AND cur.stats_reset IS NOT DISTINCT FROM lstb.stats_reset
      LEFT OUTER JOIN last_stat_cluster lstc ON
        (lstc.server_id, lstc.sample_id) =
        (sserver_id, s_id - 1)
        AND cur.checkpoint_stats_reset IS NOT DISTINCT FROM lstc.checkpoint_stats_reset
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id);

    /* wal_size is calculated since 0 to current value when stats reset happened
     * so, we need to update it
     */
    UPDATE sample_stat_cluster ssc
    SET wal_size = cur.wal_size - lst.wal_size
    FROM last_stat_cluster cur
      JOIN last_stat_cluster lst ON
        (lst.server_id, lst.sample_id) =
        (sserver_id, s_id - 1)
    WHERE
      (ssc.server_id, ssc.sample_id) = (sserver_id, s_id) AND
      (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
      cur.stats_reset IS DISTINCT FROM lst.stats_reset;

    DELETE FROM last_stat_cluster WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate cluster stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate IO stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc I/O stat diff
    INSERT INTO sample_stat_io(
        server_id,
        sample_id,
        backend_type,
        object,
        context,
        reads,
        read_bytes,
        read_time,
        writes,
        write_bytes,
        write_time,
        writebacks,
        writeback_time,
        extends,
        extend_bytes,
        extend_time,
        op_bytes,
        hits,
        evictions,
        reuses,
        fsyncs,
        fsync_time,
        stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.backend_type,
        cur.object,
        cur.context,
        cur.reads - COALESCE(lst.reads, 0),
        cur.read_bytes - COALESCE(lst.read_bytes, 0),
        cur.read_time - COALESCE(lst.read_time, 0),
        cur.writes - COALESCE(lst.writes, 0),
        cur.write_bytes - COALESCE(lst.write_bytes, 0),
        cur.write_time - COALESCE(lst.write_time, 0),
        cur.writebacks - COALESCE(lst.writebacks, 0),
        cur.writeback_time - COALESCE(lst.writeback_time, 0),
        cur.extends - COALESCE(lst.extends, 0),
        cur.extend_bytes - COALESCE(lst.extend_bytes, 0),
        cur.extend_time - COALESCE(lst.extend_time, 0),
        cur.op_bytes,
        cur.hits - COALESCE(lst.hits, 0),
        cur.evictions - COALESCE(lst.evictions, 0),
        cur.reuses - COALESCE(lst.reuses, 0),
        cur.fsyncs - COALESCE(lst.fsyncs, 0),
        cur.fsync_time - COALESCE(lst.fsync_time, 0),
        cur.stats_reset
    FROM last_stat_io cur
    LEFT OUTER JOIN last_stat_io lst ON
      (lst.server_id, lst.sample_id, lst.backend_type, lst.object, lst.context) =
      (sserver_id, s_id - 1, cur.backend_type, cur.object, cur.context)
      AND (cur.op_bytes,cur.stats_reset) IS NOT DISTINCT FROM (lst.op_bytes,lst.stats_reset)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
      GREATEST(
        cur.reads - COALESCE(lst.reads, 0),
        cur.writes - COALESCE(lst.writes, 0),
        cur.writebacks - COALESCE(lst.writebacks, 0),
        cur.extends - COALESCE(lst.extends, 0),
        cur.hits - COALESCE(lst.hits, 0),
        cur.evictions - COALESCE(lst.evictions, 0),
        cur.reuses - COALESCE(lst.reuses, 0),
        cur.fsyncs - COALESCE(lst.fsyncs, 0)
      ) > 0;

    DELETE FROM last_stat_io WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate IO stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate SLRU stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc SLRU stat diff
    INSERT INTO sample_stat_slru(
        server_id,
        sample_id,
        name,
        blks_zeroed,
        blks_hit,
        blks_read,
        blks_written,
        blks_exists,
        flushes,
        truncates,
        stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.name,
        cur.blks_zeroed - COALESCE(lst.blks_zeroed, 0),
        cur.blks_hit - COALESCE(lst.blks_hit, 0),
        cur.blks_read - COALESCE(lst.blks_read, 0),
        cur.blks_written - COALESCE(lst.blks_written, 0),
        cur.blks_exists - COALESCE(lst.blks_exists, 0),
        cur.flushes - COALESCE(lst.flushes, 0),
        cur.truncates - COALESCE(lst.truncates, 0),
        cur.stats_reset
    FROM last_stat_slru cur
    LEFT OUTER JOIN last_stat_slru lst ON
      (lst.server_id, lst.sample_id, lst.name) =
      (sserver_id, s_id - 1, cur.name)
      AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
      GREATEST(
        cur.blks_zeroed - COALESCE(lst.blks_zeroed, 0),
        cur.blks_hit - COALESCE(lst.blks_hit, 0),
        cur.blks_read - COALESCE(lst.blks_read, 0),
        cur.blks_written - COALESCE(lst.blks_written, 0),
        cur.blks_exists - COALESCE(lst.blks_exists, 0),
        cur.flushes - COALESCE(lst.flushes, 0),
        cur.truncates - COALESCE(lst.truncates, 0)
      ) > 0;

    DELETE FROM last_stat_slru WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate SLRU stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate WAL stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc WAL stat diff
    INSERT INTO sample_stat_wal(
      server_id,
      sample_id,
      wal_records,
      wal_fpi,
      wal_bytes,
      wal_buffers_full,
      wal_write,
      wal_sync,
      wal_write_time,
      wal_sync_time,
      stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.wal_records - COALESCE(lst.wal_records,0),
        cur.wal_fpi - COALESCE(lst.wal_fpi,0),
        cur.wal_bytes - COALESCE(lst.wal_bytes,0),
        cur.wal_buffers_full - COALESCE(lst.wal_buffers_full,0),
        cur.wal_write - COALESCE(lst.wal_write,0),
        cur.wal_sync - COALESCE(lst.wal_sync,0),
        cur.wal_write_time - COALESCE(lst.wal_write_time,0),
        cur.wal_sync_time - COALESCE(lst.wal_sync_time,0),
        cur.stats_reset
    FROM last_stat_wal cur
    LEFT OUTER JOIN last_stat_wal lst ON
      (lst.server_id, lst.sample_id) = (sserver_id, s_id - 1)
      AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
    WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id);

    DELETE FROM last_stat_wal WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate WAL stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate archiver stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

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
      (lst.server_id, lst.sample_id) =
      (cur.server_id, cur.sample_id - 1)
      AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    DELETE FROM last_stat_archiver WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate archiver stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,delete obsolete samples}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Updating dictionary tables setting last_sample_id
    UPDATE tablespaces_list utl SET last_sample_id = s_id - 1
    FROM tablespaces_list tl LEFT JOIN sample_stat_tablespaces cur
      ON (cur.server_id, cur.sample_id, cur.tablespaceid) =
        (sserver_id, s_id, tl.tablespaceid)
    WHERE
      tl.last_sample_id IS NULL AND
      (utl.server_id, utl.tablespaceid) = (sserver_id, tl.tablespaceid) AND
      tl.server_id = sserver_id AND cur.server_id IS NULL;

    UPDATE funcs_list ufl SET last_sample_id = s_id - 1
    FROM funcs_list fl LEFT JOIN sample_stat_user_functions cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.funcid) =
        (sserver_id, s_id, fl.datid, fl.funcid)
    WHERE
      fl.last_sample_id IS NULL AND
      fl.server_id = sserver_id AND cur.server_id IS NULL AND
      (ufl.server_id, ufl.datid, ufl.funcid) =
      (sserver_id, fl.datid, fl.funcid);

    UPDATE indexes_list uil SET last_sample_id = s_id - 1
    FROM indexes_list il LEFT JOIN sample_stat_indexes cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.indexrelid) =
        (sserver_id, s_id, il.datid, il.indexrelid)
    WHERE
      il.last_sample_id IS NULL AND
      il.server_id = sserver_id AND cur.server_id IS NULL AND
      (uil.server_id, uil.datid, uil.indexrelid) =
      (sserver_id, il.datid, il.indexrelid);

    UPDATE tables_list utl SET last_sample_id = s_id - 1
    FROM tables_list tl LEFT JOIN sample_stat_tables cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.relid) =
        (sserver_id, s_id, tl.datid, tl.relid)
    WHERE
      tl.last_sample_id IS NULL AND
      tl.server_id = sserver_id AND cur.server_id IS NULL AND
      (utl.server_id, utl.datid, utl.relid) =
      (sserver_id, tl.datid, tl.relid);

    UPDATE stmt_list slu SET last_sample_id = s_id - 1
    FROM sample_statements ss RIGHT JOIN stmt_list sl
      ON (ss.server_id, ss.sample_id, ss.queryid_md5) =
        (sserver_id, s_id, sl.queryid_md5)
    WHERE
      sl.server_id = sserver_id AND
      sl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (slu.server_id, slu.queryid_md5) = (sserver_id, sl.queryid_md5);

    UPDATE roles_list rlu SET last_sample_id = s_id - 1
    FROM
        sample_statements ss
      RIGHT JOIN roles_list rl
      ON (ss.server_id, ss.sample_id, ss.userid) =
        (sserver_id, s_id, rl.userid)
    WHERE
      rl.server_id = sserver_id AND
      rl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (rlu.server_id, rlu.userid) = (sserver_id, rl.userid);

    -- Deleting obsolete baselines
    DELETE FROM baselines
    WHERE keep_until < now()
      AND server_id = sserver_id;

    -- Deleting obsolete samples
    PERFORM num_nulls(min(s.sample_id),max(s.sample_id)) > 0 OR
      delete_samples(sserver_id, min(s.sample_id), max(s.sample_id)) > 0
    FROM samples s JOIN
      servers n USING (server_id)
    WHERE s.server_id = sserver_id
        AND s.sample_time < now() - (COALESCE(n.max_sample_age,ret) || ' days')::interval
        AND (s.server_id,s.sample_id) NOT IN (SELECT server_id,sample_id FROM bl_samples WHERE server_id = sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,delete obsolete samples,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,total,end}',to_jsonb(clock_timestamp()));
      -- Save timing statistics of sample
      INSERT INTO sample_timings
      SELECT sserver_id, s_id, key,(value::jsonb #>> '{end}')::timestamp with time zone - (value::jsonb #>> '{start}')::timestamp with time zone as time_spent
      FROM jsonb_each_text(server_properties #> '{timings}');
    END IF;
    ASSERT server_properties IS NOT NULL, 'lost properties';

    -- Reset lock_timeout setting to its initial value
    EXECUTE format('SET lock_timeout TO %L', server_properties #>> '{properties,lock_timeout_init}');

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample(IN sserver_id integer, IN skip_sizes boolean) IS
  'Statistics sample creation function (by server_id)';