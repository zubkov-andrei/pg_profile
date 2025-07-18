/* ========= Sample functions ========= */
CREATE FUNCTION init_sample(IN sserver_id integer
) RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    server_properties jsonb = '{"extensions":[],"settings":[],"timings":{},"properties":{}}'; -- version, extensions, etc.
    qres              record;
    qres_subsample    record;
    server_connstr    text;

    server_query      text;
    server_host       text = NULL;
BEGIN
    server_properties := jsonb_set(server_properties, '{properties,in_sample}', to_jsonb(false));
    -- Conditionally set lock_timeout when it's not set
    server_properties := jsonb_set(server_properties,'{properties,lock_timeout_init}',
      to_jsonb(current_setting('lock_timeout')));
    IF (SELECT current_setting('lock_timeout')::interval = '0s'::interval) THEN
      SET lock_timeout TO '3s';
    END IF;
    server_properties := jsonb_set(server_properties,'{properties,lock_timeout_effective}',
      to_jsonb(current_setting('lock_timeout')));

    -- Get server connstr
    SELECT properties INTO server_properties FROM get_connstr(sserver_id, server_properties);

    -- Getting timing collection setting
    BEGIN
        SELECT current_setting('{pg_profile}.track_sample_timings')::boolean AS collect_timings
          INTO qres;
        server_properties := jsonb_set(server_properties,
          '{collect_timings}',
          to_jsonb(qres.collect_timings)
        );
    EXCEPTION
        WHEN OTHERS THEN
          server_properties := jsonb_set(server_properties,
            '{collect_timings}',
            to_jsonb(false)
          );
    END;

    -- Getting TopN setting
    BEGIN
        SELECT current_setting('{pg_profile}.topn')::integer AS topn INTO qres;
        server_properties := jsonb_set(server_properties,'{properties,topn}',to_jsonb(qres.topn));
    EXCEPTION
        WHEN OTHERS THEN
          server_properties := jsonb_set(server_properties,
            '{properties,topn}',
            to_jsonb(20)
          );
    END;

    -- Getting statement stats reset setting
    BEGIN
        server_properties := jsonb_set(server_properties,
          '{properties,statements_reset}',
          to_jsonb(current_setting('{pg_profile}.statements_reset')::boolean)
        );
    EXCEPTION
        WHEN OTHERS THEN
          server_properties := jsonb_set(server_properties,
            '{properties,statements_reset}',
            to_jsonb(true)
          );
    END;

    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;

    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    IF dblink_get_connections() @> ARRAY['server_connection'] THEN
        PERFORM dblink_disconnect('server_connection');
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,connect}',jsonb_build_object('start',clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,total}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Server connection
    PERFORM dblink_connect('server_connection', server_properties #>> '{properties,server_connstr}');
    -- Transaction
    PERFORM dblink('server_connection','BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    -- Setting application name
    PERFORM dblink('server_connection','SET application_name=''{pg_profile}''');
    -- Conditionally set lock_timeout
    IF (
      SELECT lock_timeout_unset
      FROM dblink('server_connection',
        $sql$SELECT current_setting('lock_timeout')::interval = '0s'::interval$sql$)
        AS probe(lock_timeout_unset boolean)
      )
    THEN
      -- Setting lock_timout prevents hanging due to DDL in long transaction
      PERFORM dblink('server_connection',
        format('SET lock_timeout TO %L',
          COALESCE(server_properties #>> '{properties,lock_timeout_effective}','3s')
        )
      );
    END IF;
    -- Reset search_path for security reasons
    PERFORM dblink('server_connection','SET search_path=''''');

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,connect,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,get server environment}',jsonb_build_object('start',clock_timestamp()));
    END IF;
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

    -- Is it PostgresPro?
    IF (SELECT pgpro_fxs = 3
        FROM dblink('server_connection',
          'select count(1) as pgpro_fxs '
          'from pg_catalog.pg_settings '
          'where name IN (''pgpro_build'',''pgpro_edition'',''pgpro_version'')'
        ) AS pgpro (pgpro_fxs integer))
    THEN
      server_properties := jsonb_set(server_properties,'{properties,pgpro}',to_jsonb(true));
    ELSE
      server_properties := jsonb_set(server_properties,'{properties,pgpro}',to_jsonb(false));
    END IF;

    -- Get extensions, that we need to perform statements stats collection
    FOR qres IN
      SELECT * FROM dblink('server_connection',
          'SELECT extname, '
          'extnamespace::regnamespace::name AS extnamespace, '
          'extversion '
          'FROM pg_catalog.pg_extension '
          'WHERE extname IN ('
            '''pg_stat_statements'','
            '''pg_wait_sampling'','
            '''pg_stat_kcache'''
          ')')
        AS dbl(extname name, extnamespace name, extversion text)
    LOOP
      server_properties := jsonb_insert(server_properties,'{"extensions",0}',to_jsonb(qres));
    END LOOP;

    -- Check system identifier
    WITH remote AS (
      SELECT
        dbl.system_identifier
      FROM dblink('server_connection',
        'SELECT system_identifier '
        'FROM pg_catalog.pg_control_system()'
      ) AS dbl (system_identifier bigint)
    )
    SELECT min(reset_val::bigint) != (
        SELECT
          system_identifier
        FROM remote
      ) AS sysid_changed,
      (
        SELECT
          s.server_name = 'local' AND cs.system_identifier != r.system_identifier
        FROM
          pg_catalog.pg_control_system() cs
          CROSS JOIN remote r
          JOIN servers s ON (s.server_id = sserver_id)
      ) AS local_missmatch
      INTO STRICT qres
    FROM sample_settings
    WHERE server_id = sserver_id AND name = 'system_identifier';
    IF qres.sysid_changed THEN
      RAISE 'Server system_identifier has changed! '
        'Ensure server connection string is correct. '
        'Consider creating a new server for this cluster.';
    END IF;
    IF qres.local_missmatch THEN
      RAISE 'Local system_identifier does not match '
        'with server specified by connection string of '
        '"local" server';
    END IF;

    -- Subsample settings collection
    -- Get last base sample identifier of a server
    SELECT
      last_sample_id,
      subsample_enabled,
      min_query_dur,
      min_xact_dur,
      min_xact_age,
      min_idle_xact_dur
      INTO STRICT qres_subsample
    FROM servers JOIN server_subsample USING (server_id)
    WHERE server_id = sserver_id;

    server_properties := jsonb_set(server_properties,
      '{properties,last_sample_id}',
      to_jsonb(qres_subsample.last_sample_id)
    );

    /* Getting subsample GUC thresholds used as defaults*/
    BEGIN
        SELECT current_setting('{pg_profile}.subsample_enabled')::boolean AS subsample_enabled
          INTO qres;
        server_properties := jsonb_set(
          server_properties,
          '{properties,subsample_enabled}',
          to_jsonb(COALESCE(qres_subsample.subsample_enabled, qres.subsample_enabled))
        );
    EXCEPTION
        WHEN OTHERS THEN
          server_properties := jsonb_set(server_properties,
            '{properties,subsample_enabled}',
            to_jsonb(COALESCE(qres_subsample.subsample_enabled, true))
          );
    END;

    -- Setup subsample settings when they are enabled
    IF (server_properties #>> '{properties,subsample_enabled}')::boolean THEN
      BEGIN
          SELECT current_setting('{pg_profile}.min_query_duration')::interval AS min_query_dur INTO qres;
          server_properties := jsonb_set(
            server_properties,
            '{properties,min_query_dur}',
            to_jsonb(COALESCE(qres_subsample.min_query_dur, qres.min_query_dur))
          );
      EXCEPTION
          WHEN OTHERS THEN
            server_properties := jsonb_set(server_properties,
              '{properties,min_query_dur}',
              COALESCE (
                to_jsonb(qres_subsample.min_query_dur)
                , 'null'::jsonb
              )
            );
      END;

      BEGIN
          SELECT current_setting('{pg_profile}.min_xact_duration')::interval AS min_xact_dur INTO qres;
          server_properties := jsonb_set(
            server_properties,
            '{properties,min_xact_dur}',
            to_jsonb(COALESCE(qres_subsample.min_xact_dur, qres.min_xact_dur))
          );
      EXCEPTION
          WHEN OTHERS THEN
            server_properties := jsonb_set(server_properties,
              '{properties,min_xact_dur}',
              COALESCE (
                to_jsonb(qres_subsample.min_xact_dur)
                , 'null'::jsonb
              )
            );
      END;

      BEGIN
          SELECT current_setting('{pg_profile}.min_xact_age')::integer AS min_xact_age INTO qres;
          server_properties := jsonb_set(
            server_properties,
            '{properties,min_xact_age}',
            to_jsonb(COALESCE(qres_subsample.min_xact_age, qres.min_xact_age))
          );
      EXCEPTION
          WHEN OTHERS THEN
            server_properties := jsonb_set(server_properties,
              '{properties,min_xact_age}',
              COALESCE (
                to_jsonb(qres_subsample.min_xact_age)
                , 'null'::jsonb
              )
            );
      END;

      BEGIN
          SELECT current_setting('{pg_profile}.min_idle_xact_duration')::interval AS min_idle_xact_dur INTO qres;
          server_properties := jsonb_set(
            server_properties,
            '{properties,min_idle_xact_dur}',
            to_jsonb(COALESCE(qres_subsample.min_idle_xact_dur, qres.min_idle_xact_dur))
          );
      EXCEPTION
          WHEN OTHERS THEN
            server_properties := jsonb_set(server_properties,
              '{properties,min_idle_xact_dur}',
              COALESCE (
                to_jsonb(qres_subsample.min_idle_xact_dur)
                , 'null'::jsonb
              )
            );
      END;
    END IF; -- when subsamples enabled
    RETURN server_properties;
END;
$$ LANGUAGE plpgsql;

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

CREATE FUNCTION take_sample(IN server name, IN skip_sizes boolean = NULL)
RETURNS TABLE (
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
DECLARE
    sserver_id          integer;
    server_sampleres    integer;
    etext               text := '';
    edetail             text := '';
    econtext            text := '';

    qres                record;
    conname             text;
    start_clock         timestamp (2) with time zone;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name = take_sample.server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found';
    ELSE
        /*
        * We should include dblink schema to perform disconnections
        * on exception conditions
        */
        SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
        IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
          EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
        END IF;

        BEGIN
            start_clock := clock_timestamp()::timestamp (2) with time zone;
            server_sampleres := take_sample(sserver_id, take_sample.skip_sizes);
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
                    /*
                      Cleanup dblink connections
                    */
                    FOREACH conname IN ARRAY dblink_get_connections()
                    LOOP
                        IF conname IN ('server_connection', 'server_db_connection') THEN
                            PERFORM dblink_disconnect(conname);
                        END IF;
                    END LOOP;
                END;
        END;
    END IF;
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample(IN server name, IN skip_sizes boolean) IS
  'Statistics sample creation function (by server name)';

CREATE FUNCTION take_sample_subset(IN sets_cnt integer = 1, IN current_set integer = 0) RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
DECLARE
    c_servers CURSOR FOR
      SELECT server_id,server_name FROM (
        SELECT server_id,server_name, row_number() OVER (ORDER BY server_id) AS srv_rn
        FROM servers WHERE enabled
        ) AS t1
      WHERE srv_rn % sets_cnt = current_set;
    server_sampleres    integer;
    etext               text := '';
    edetail             text := '';
    econtext            text := '';

    qres          RECORD;
    conname       text;
    start_clock   timestamp (2) with time zone;
BEGIN
    IF sets_cnt IS NULL OR sets_cnt < 1 THEN
      RAISE 'sets_cnt value is invalid. Must be positive';
    END IF;
    IF current_set IS NULL OR current_set < 0 OR current_set > sets_cnt - 1 THEN
      RAISE 'current_cnt value is invalid. Must be between 0 and sets_cnt - 1';
    END IF;
    /*
    * We should include dblink schema to perform disconnections
    * on exception conditions
    */
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    FOR qres IN c_servers LOOP
        BEGIN
            start_clock := clock_timestamp()::timestamp (2) with time zone;
            server := qres.server_name;
            server_sampleres := take_sample(qres.server_id, NULL);
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
                    /*
                      Cleanup dblink connections
                    */
                    FOREACH conname IN ARRAY dblink_get_connections()
                    LOOP
                        IF conname IN ('server_connection', 'server_db_connection') THEN
                            PERFORM dblink_disconnect(conname);
                        END IF;
                    END LOOP;
                END;
        END;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample_subset(IN sets_cnt integer, IN current_set integer) IS
  'Statistics sample creation function (for subset of enabled servers). Used for simplification of parallel sample collection.';

CREATE FUNCTION take_sample() RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
  SELECT * FROM take_sample_subset(1,0);
$$ LANGUAGE sql;

COMMENT ON FUNCTION take_sample() IS 'Statistics sample creation function (for all enabled servers). Must be explicitly called periodically.';

CREATE FUNCTION collect_obj_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer,
  IN skip_sizes boolean
) RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    --Cursor over databases
    c_dblist CURSOR FOR
    SELECT
      datid,
      datname,
      dattablespace AS tablespaceid
    FROM last_stat_database ldb
      JOIN servers n ON
        (n.server_id = sserver_id AND array_position(n.db_exclude,ldb.datname) IS NULL)
    WHERE
      NOT ldb.datistemplate AND ldb.datallowconn AND
      (ldb.server_id, ldb.sample_id) = (sserver_id, s_id);

    qres        record;
    db_connstr  text;
    t_query     text;
    analyze_list  text[] := array[]::text[];
    analyze_obj   text;
    result      jsonb := collect_obj_stats.properties;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Disconnecting existing connection
    IF dblink_get_connections() @> ARRAY['server_db_connection'] THEN
        PERFORM dblink_disconnect('server_db_connection');
    END IF;

    -- Load new data from statistic views of all cluster databases
    FOR qres IN c_dblist LOOP
      db_connstr := concat_ws(' ',properties #>> '{properties,server_connstr}',
        format($o$dbname='%s'$o$,replace(qres.datname,$o$'$o$,$o$\'$o$))
      );
      PERFORM dblink_connect('server_db_connection',db_connstr);
      -- Transaction
      PERFORM dblink('server_db_connection','BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
      -- Setting application name
      PERFORM dblink('server_db_connection','SET application_name=''{pg_profile}''');
      -- Conditionally set lock_timeout
      IF (
        SELECT lock_timeout_unset
        FROM dblink('server_db_connection',
          $sql$SELECT current_setting('lock_timeout')::interval = '0s'::interval$sql$)
          AS probe(lock_timeout_unset boolean)
        )
      THEN
        -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
        PERFORM dblink('server_db_connection',
          format('SET lock_timeout TO %L',
            COALESCE(properties #>> '{properties,lock_timeout_effective}','3s')
          )
        );
      END IF;
      -- Reset search_path for security reasons
      PERFORM dblink('server_db_connection','SET search_path=''''');

      IF (properties #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s get extensions version',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      t_query := 'SELECT '
        'extname,'
        'extversion '
        'FROM pg_extension';

      INSERT INTO last_extension_versions (
        server_id,
        datid,
        sample_id,
        extname,
        extversion
      )
      SELECT
        sserver_id as server_id,
        qres.datid,
        s_id as sample_id,
        dbl.extname,
        dbl.extversion
      FROM dblink('server_db_connection', t_query)
      AS dbl (
         extname    name,
         extversion text
      );

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s get extensions version',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect tables stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

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
            'NULL as total_vacuum_time,'
            'NULL as total_autovacuum_time,'
            'NULL as total_analyze_time,'
            'NULL as total_autoanalyze_time,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'NULL AS last_seq_scan,'
            'NULL AS last_idx_scan,'
            'NULL AS n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 160000
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
            'NULL as total_vacuum_time,'
            'NULL as total_autovacuum_time,'
            'NULL as total_analyze_time,'
            'NULL as total_autoanalyze_time,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'NULL AS last_seq_scan,'
            'NULL AS last_idx_scan,'
            'NULL AS n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 180000
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
            'NULL as total_vacuum_time,'
            'NULL as total_autovacuum_time,'
            'NULL as total_analyze_time,'
            'NULL as total_autoanalyze_time,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'st.last_seq_scan,'
            'st.last_idx_scan,'
            'st.n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer >= 180000
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
            'st.total_vacuum_time,'
            'st.total_autovacuum_time,'
            'st.total_analyze_time,'
            'st.total_autoanalyze_time,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'st.last_seq_scan,'
            'st.last_idx_scan,'
            'st.n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        ELSE
          RAISE 'Unsupported server version.';
      END CASE;

      IF skip_sizes THEN
        t_query := replace(t_query,'{relation_size}','NULL');
        t_query := replace(t_query,'{lock_join}','');
      ELSE
        t_query := replace(t_query,'{relation_size}','CASE locked.objid WHEN st.relid THEN NULL ELSE '
          'pg_catalog.pg_table_size(st.relid) - '
          'coalesce(pg_catalog.pg_relation_size(class.reltoastrelid),0) END');
        t_query := replace(t_query,'{lock_join}',
          'LEFT OUTER JOIN LATERAL '
            '(WITH RECURSIVE deps (objid) AS ('
              'SELECT relation FROM pg_catalog.pg_locks WHERE granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'' '
              'UNION '
              'SELECT refobjid FROM pg_catalog.pg_depend d JOIN deps dd ON (d.objid = dd.objid)'
            ') '
            'SELECT objid FROM deps) AS locked ON (st.relid = locked.objid)');
      END IF;

      IF COALESCE((properties #> '{collect,relations}')::boolean, true) THEN
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
          total_vacuum_time,
          total_autovacuum_time,
          total_analyze_time,
          total_autoanalyze_time,
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
          relkind,
          in_sample,
          relpages_bytes,
          relpages_bytes_diff,
          last_seq_scan,
          last_idx_scan,
          n_tup_newpage_upd,
          reloptions
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
          dbl.total_vacuum_time AS total_vacuum_time,
          dbl.total_autovacuum_time AS total_autovacuum_time,
          dbl.total_analyze_time AS total_analyze_time,
          dbl.total_autoanalyze_time AS total_autoanalyze_time,
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
          NULLIF(dbl.reltoastrelid, 0),
          dbl.relkind,
          false,
          dbl.relpages_bytes,
          dbl.relpages_bytes_diff,
          dbl.last_seq_scan,
          dbl.last_idx_scan,
          dbl.n_tup_newpage_upd,
          dbl.reloptions
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
            total_vacuum_time       double precision,
            total_autovacuum_time   double precision,
            total_analyze_time      double precision,
            total_autoanalyze_time  double precision,
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
            relkind               char,
            relpages_bytes        bigint,
            relpages_bytes_diff   bigint,
            last_seq_scan         timestamp with time zone,
            last_idx_scan         timestamp with time zone,
            n_tup_newpage_upd     bigint,
            reloptions            jsonb
        );

        IF NOT analyze_list @> ARRAY[format('last_stat_tables_srv%1$s', sserver_id)] THEN
          analyze_list := analyze_list ||
            format('last_stat_tables_srv%1$s', sserver_id);
        END IF;
      END IF; -- relation collection condition

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect tables stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect indexes stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate index stats query
      CASE
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 160000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.indexrelid,'
            'st.schemaname,'
            'st.relname,'
            'st.indexrelname,'
            'st.idx_scan,'
            'NULL AS last_idx_scan,'
            'st.idx_tup_read,'
            'st.idx_tup_fetch,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            '{relation_size} relsize,'
            '0,'
            'pg_class.reltablespace as tablespaceid,'
            '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique,'
            'pg_class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'to_jsonb(pg_class.reloptions) '
          'FROM pg_catalog.pg_stat_all_indexes st '
            'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
            'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
            'JOIN pg_catalog.pg_class ON (pg_class.oid = st.indexrelid) '
            'LEFT OUTER JOIN pg_catalog.pg_constraint con ON '
              '(con.conrelid, con.conindid) = (ix.indrelid, ix.indexrelid) AND con.contype in (''p'',''u'') '
            '{lock_join}'
            ;
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer >= 160000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.indexrelid,'
            'st.schemaname,'
            'st.relname,'
            'st.indexrelname,'
            'st.idx_scan,'
            'st.last_idx_scan,'
            'st.idx_tup_read,'
            'st.idx_tup_fetch,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            '{relation_size} relsize,'
            '0,'
            'pg_class.reltablespace as tablespaceid,'
            '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique,'
            'pg_class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'to_jsonb(pg_class.reloptions) '
          'FROM pg_catalog.pg_stat_all_indexes st '
            'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
            'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
            'JOIN pg_catalog.pg_class ON (pg_class.oid = st.indexrelid) '
            'LEFT OUTER JOIN pg_catalog.pg_constraint con ON '
              '(con.conrelid, con.conindid) = (ix.indrelid, ix.indexrelid) AND con.contype in (''p'',''u'') '
            '{lock_join}'
            ;
        ELSE
          RAISE 'Unsupported server version.';
      END CASE;

      IF skip_sizes THEN
        t_query := replace(t_query,'{relation_size}','NULL');
        t_query := replace(t_query,'{lock_join}','');
      ELSE
        t_query := replace(t_query,'{relation_size}',
          'CASE l.relation WHEN st.indexrelid THEN NULL ELSE pg_relation_size(st.indexrelid) END');
        t_query := replace(t_query,'{lock_join}',
          'LEFT OUTER JOIN LATERAL ('
            'SELECT relation '
            'FROM pg_catalog.pg_locks '
            'WHERE '
            '(relation = st.indexrelid AND granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'')'
          ') l ON (l.relation = st.indexrelid)');
      END IF;

      IF COALESCE((properties #> '{collect,relations}')::boolean, true) THEN
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
          last_idx_scan,
          idx_tup_read,
          idx_tup_fetch,
          idx_blks_read,
          idx_blks_hit,
          relsize,
          relsize_diff,
          tablespaceid,
          indisunique,
          in_sample,
          relpages_bytes,
          relpages_bytes_diff,
          reloptions
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
          dbl.last_idx_scan AS last_idx_scan,
          dbl.idx_tup_read AS idx_tup_read,
          dbl.idx_tup_fetch AS idx_tup_fetch,
          dbl.idx_blks_read AS idx_blks_read,
          dbl.idx_blks_hit AS idx_blks_hit,
          dbl.relsize AS relsize,
          dbl.relsize_diff AS relsize_diff,
          CASE WHEN tablespaceid=0 THEN qres.tablespaceid ELSE tablespaceid END tablespaceid,
          indisunique,
          false,
          dbl.relpages_bytes,
          dbl.relpages_bytes_diff,
          dbl.reloptions
        FROM dblink('server_db_connection', t_query)
        AS dbl (
           relid          oid,
           indexrelid     oid,
           schemaname     name,
           relname        name,
           indexrelname   name,
           idx_scan       bigint,
           last_idx_scan  timestamp with time zone,
           idx_tup_read   bigint,
           idx_tup_fetch  bigint,
           idx_blks_read  bigint,
           idx_blks_hit   bigint,
           relsize        bigint,
           relsize_diff   bigint,
           tablespaceid   oid,
           indisunique    bool,
           relpages_bytes bigint,
           relpages_bytes_diff  bigint,
           reloptions jsonb
        );

        IF NOT analyze_list @> ARRAY[format('last_stat_indexes_srv%1$s', sserver_id)] THEN
          analyze_list := analyze_list ||
            format('last_stat_indexes_srv%1$s', sserver_id);
        END IF;
      END IF; -- relation collection condition

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect indexes stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect functions stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

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
        'JOIN pg_catalog.pg_proc p ON (f.funcid = p.oid) '
      'WHERE pg_get_function_arguments(f.funcid) IS NOT NULL';

      IF COALESCE((properties #> '{collect,functions}')::boolean, true) THEN
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

        IF NOT analyze_list @> ARRAY[format('last_stat_user_functions_srv%1$s', sserver_id)] THEN
          analyze_list := analyze_list ||
            format('last_stat_user_functions_srv%1$s', sserver_id);
        END IF;
      END IF; -- functions collection condition

      PERFORM dblink('server_db_connection', 'COMMIT');
      PERFORM dblink_disconnect('server_db_connection');
      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect functions stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
      END IF;
    END LOOP; -- over databases

    -- Now we should preform ANALYZE on collected data
    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,ARRAY['timings', 'analyzing collected data'],jsonb_build_object('start',clock_timestamp()));
    END IF;

    FOREACH analyze_obj IN ARRAY analyze_list
    LOOP
      EXECUTE format('ANALYZE %1$I', analyze_obj);
    END LOOP;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,ARRAY['timings', 'analyzing collected data', 'end'],to_jsonb(clock_timestamp()));
    END IF;

   RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION sample_dbobj_delta(IN properties jsonb, IN sserver_id integer, IN s_id integer,
  IN topn integer, IN skip_sizes boolean) RETURNS jsonb AS $$
DECLARE
    result  jsonb := sample_dbobj_delta.properties;
BEGIN

    /* This function will calculate statistics increments for database objects
    * and store top objects values in sample.
    * Due to relations between objects we need to mark top objects (and their
    * dependencies) first, and calculate increments later
    */
    IF (properties #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(properties,'{timings,calculate tables stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Marking functions
    UPDATE last_stat_user_functions ulf
    SET in_sample = true
    FROM
        (SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.funcid,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.total_time - COALESCE(lst.total_time,0) DESC) time_rank,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.self_time - COALESCE(lst.self_time,0) DESC) stime_rank,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.calls - COALESCE(lst.calls,0) DESC) calls_rank
        FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
          LEFT OUTER JOIN last_stat_database dblst ON
            (dblst.server_id, dblst.datid, dblst.sample_id) =
            (sserver_id, dbcur.datid, s_id - 1)
            AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
          LEFT OUTER JOIN last_stat_user_functions lst ON
            (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
            (sserver_id, s_id - 1, dblst.datid, cur.funcid)
        WHERE
            (cur.server_id, cur.sample_id) =
            (sserver_id, s_id)
            AND cur.calls - COALESCE(lst.calls,0) > 0) diff
    WHERE
      least(
        time_rank,
        calls_rank,
        stime_rank
      ) <= topn
      AND (ulf.server_id, ulf.sample_id, ulf.datid, ulf.funcid) =
        (diff.server_id, diff.sample_id, diff.datid, diff.funcid);

    -- Marking indexes
    UPDATE last_stat_indexes uli
    SET in_sample = true
    FROM
      (SELECT
          cur.server_id,
          cur.sample_id,
          cur.datid,
          cur.indexrelid,
          -- Index ranks
          row_number() OVER (ORDER BY cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) DESC) read_rank,
          row_number() OVER (ORDER BY cur.idx_blks_read+cur.idx_blks_hit-
            COALESCE(lst.idx_blks_read+lst.idx_blks_hit,0) DESC) gets_rank,
          row_number() OVER (PARTITION BY cur.idx_scan - COALESCE(lst.idx_scan,0) = 0
            ORDER BY tblcur.n_tup_ins - COALESCE(tbllst.n_tup_ins,0) +
            tblcur.n_tup_upd - COALESCE(tbllst.n_tup_upd,0) +
            tblcur.n_tup_del - COALESCE(tbllst.n_tup_del,0) DESC) dml_unused_rank,
          row_number() OVER (ORDER BY (tblcur.vacuum_count - COALESCE(tbllst.vacuum_count,0) +
            tblcur.autovacuum_count - COALESCE(tbllst.autovacuum_count,0)) *
              -- Coalesce is used here in case of skipped size collection
              COALESCE(cur.relsize,lst.relsize) DESC) vacuum_bytes_rank
      FROM last_stat_indexes cur JOIN last_stat_tables tblcur USING (server_id, sample_id, datid, relid)
        JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
        LEFT OUTER JOIN last_stat_database dblst ON
          (dblst.server_id, dblst.datid, dblst.sample_id) =
          (sserver_id, dbcur.datid, s_id - 1)
          AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
        LEFT OUTER JOIN last_stat_indexes lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
          (sserver_id, s_id - 1, dblst.datid, cur.relid, cur.indexrelid)
        LEFT OUTER JOIN last_stat_tables tbllst ON
          (tbllst.server_id, tbllst.sample_id, tbllst.datid, tbllst.relid) =
          (sserver_id, s_id - 1, dblst.datid, lst.relid)
      WHERE
        (cur.server_id, cur.sample_id) =
        (sserver_id, s_id)
      ) diff
    WHERE
      (least(
        read_rank,
        gets_rank,
        vacuum_bytes_rank
      ) <= topn
      OR (dml_unused_rank <= topn AND idx_scan = 0))
      AND (uli.server_id, uli.sample_id, uli.datid, uli.indexrelid, uli.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, diff.indexrelid, false);

    -- Growth rank is to be calculated independently of database stats_reset value
    UPDATE last_stat_indexes uli
    SET in_sample = true
    FROM
      (SELECT
          cur.server_id,
          cur.sample_id,
          cur.datid,
          cur.indexrelid,
          cur.relsize IS NOT NULL AS relsize_avail,
          cur.relpages_bytes IS NOT NULL AS relpages_avail,
          -- Index ranks
          row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize,0) DESC NULLS LAST) growth_rank,
          row_number() OVER (ORDER BY cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) DESC NULLS LAST) pagegrowth_rank
      FROM last_stat_indexes cur
        LEFT OUTER JOIN last_stat_indexes lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
          (sserver_id, s_id - 1, cur.datid, cur.relid, cur.indexrelid)
      WHERE
        (cur.server_id, cur.sample_id) =
        (sserver_id, s_id)
      ) diff
    WHERE
      ((relsize_avail AND growth_rank <= topn) OR
      ((NOT relsize_avail) AND relpages_avail AND pagegrowth_rank <= topn))
      AND (uli.server_id, uli.sample_id, uli.datid, uli.indexrelid, uli.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, diff.indexrelid, false);

    -- Marking tables
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM (
      SELECT
          cur.server_id AS server_id,
          cur.sample_id AS sample_id,
          cur.datid AS datid,
          cur.relid AS relid,
          tcur.relid AS toastrelid,
          -- Seq. scanned blocks rank
          row_number() OVER (ORDER BY
            (cur.seq_scan - COALESCE(lst.seq_scan,0)) * cur.relsize +
            (tcur.seq_scan - COALESCE(tlst.seq_scan,0)) * tcur.relsize DESC) scan_rank,
          row_number() OVER (ORDER BY cur.n_tup_ins + cur.n_tup_upd + cur.n_tup_del -
            COALESCE(lst.n_tup_ins + lst.n_tup_upd + lst.n_tup_del, 0) +
            COALESCE(tcur.n_tup_ins + tcur.n_tup_upd + tcur.n_tup_del, 0) -
            COALESCE(tlst.n_tup_ins + tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) dml_rank,
          row_number() OVER (ORDER BY cur.n_tup_upd+cur.n_tup_del -
            COALESCE(lst.n_tup_upd + lst.n_tup_del, 0) +
            COALESCE(tcur.n_tup_upd + tcur.n_tup_del, 0) -
            COALESCE(tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) vacuum_dml_rank,
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
            cur.autovacuum_count - COALESCE(lst.autovacuum_count, 0) DESC) vacuum_count_rank,
          row_number() OVER (ORDER BY cur.analyze_count - COALESCE(lst.analyze_count,0) +
            cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) DESC) analyze_count_rank,
          row_number() OVER (ORDER BY cur.total_vacuum_time - COALESCE(lst.total_vacuum_time, 0) +
            cur.total_autovacuum_time - COALESCE(lst.total_autovacuum_time, 0) DESC) vacuum_time_rank,
          row_number() OVER (ORDER BY cur.total_analyze_time - COALESCE(lst.total_analyze_time,0) +
            cur.total_autoanalyze_time - COALESCE(lst.total_autoanalyze_time,0) DESC) analyze_time_rank,

          -- Newpage updates rank (since PG16)
          CASE WHEN cur.n_tup_newpage_upd IS NOT NULL THEN
            row_number() OVER (ORDER BY cur.n_tup_newpage_upd -
              COALESCE(lst.n_tup_newpage_upd, 0) DESC)
          ELSE
            NULL
          END newpage_upd_rank
      FROM
        -- main relations diff
        last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
        LEFT OUTER JOIN last_stat_database dblst ON
          (dblst.server_id, dblst.datid, dblst.sample_id) =
          (sserver_id, dbcur.datid, s_id - 1)
          AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
        LEFT OUTER JOIN last_stat_tables lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
          (sserver_id, s_id - 1, dblst.datid, cur.relid)
        -- toast relations diff
        LEFT OUTER JOIN last_stat_tables tcur ON
          (tcur.server_id, tcur.sample_id, tcur.datid, tcur.relid) =
          (sserver_id, s_id, dbcur.datid, cur.reltoastrelid)
        LEFT OUTER JOIN last_stat_tables tlst ON
          (tlst.server_id, tlst.sample_id, tlst.datid, tlst.relid) =
          (sserver_id, s_id - 1, dblst.datid, lst.reltoastrelid)
      WHERE
        (cur.server_id, cur.sample_id, cur.in_sample) =
        (sserver_id, s_id, false)
        AND cur.relkind IN ('r','m')) diff
    WHERE
      least(
        scan_rank,
        dml_rank,
        dead_pct_rank,
        mod_pct_rank,
        vacuum_dml_rank,
        read_rank,
        gets_rank,
        vacuum_count_rank,
        analyze_count_rank,
        vacuum_time_rank,
        analyze_time_rank,
        newpage_upd_rank
      ) <= topn
      AND (ulst.server_id, ulst.sample_id, ulst.datid, ulst.in_sample) =
        (sserver_id, s_id, diff.datid, false)
      AND (ulst.relid = diff.relid OR ulst.relid = diff.toastrelid);

    -- Growth rank is to be calculated independently of database stats_reset value
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM (
      SELECT
          cur.server_id AS server_id,
          cur.sample_id AS sample_id,
          cur.datid AS datid,
          cur.relid AS relid,
          tcur.relid AS toastrelid,
          cur.relsize IS NOT NULL AS relsize_avail,
          cur.relpages_bytes IS NOT NULL AS relpages_avail,
          row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize, 0) +
            COALESCE(tcur.relsize,0) - COALESCE(tlst.relsize, 0) DESC NULLS LAST) growth_rank,
          row_number() OVER (ORDER BY cur.relpages_bytes - COALESCE(lst.relpages_bytes, 0) +
            COALESCE(tcur.relpages_bytes,0) - COALESCE(tlst.relpages_bytes, 0) DESC NULLS LAST) pagegrowth_rank
      FROM
        -- main relations diff
        last_stat_tables cur
        LEFT OUTER JOIN last_stat_tables lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
          (sserver_id, s_id - 1, cur.datid, cur.relid)
        -- toast relations diff
        LEFT OUTER JOIN last_stat_tables tcur ON
          (tcur.server_id, tcur.sample_id, tcur.datid, tcur.relid) =
          (sserver_id, s_id, cur.datid, cur.reltoastrelid)
        LEFT OUTER JOIN last_stat_tables tlst ON
          (tlst.server_id, tlst.sample_id, tlst.datid, tlst.relid) =
          (sserver_id, s_id - 1, lst.datid, lst.reltoastrelid)
      WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
        AND cur.relkind IN ('r','m')) diff
    WHERE
      ((relsize_avail AND growth_rank <= topn) OR
      ((NOT relsize_avail) AND relpages_avail AND pagegrowth_rank <= topn))
      AND (ulst.server_id, ulst.sample_id, ulst.datid, in_sample) =
        (sserver_id, s_id, diff.datid, false)
      AND (ulst.relid = diff.relid OR ulst.relid = diff.toastrelid);

    /* Also mark tables having marked indexes on them including main
    * table in case of a TOAST index and TOAST table if index is on
    * main table
    */
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM
      last_stat_indexes ix
      JOIN last_stat_tables tbl ON
        (tbl.server_id, tbl.sample_id, tbl.datid, tbl.relid) =
        (sserver_id, s_id, ix.datid, ix.relid)
      LEFT JOIN last_stat_tables mtbl ON
        (mtbl.server_id, mtbl.sample_id, mtbl.datid, mtbl.reltoastrelid) =
        (sserver_id, s_id, tbl.datid, tbl.relid)
    WHERE
      (ix.server_id, ix.sample_id, ix.in_sample) =
      (sserver_id, s_id, true)
      AND (ulst.server_id, ulst.sample_id, ulst.datid, ulst.in_sample) =
        (sserver_id, s_id, tbl.datid, false)
      AND ulst.relid IN (tbl.relid, tbl.reltoastrelid, mtbl.relid);

    -- Insert marked objects statistics increments
    -- New table names
    INSERT INTO tables_list AS itl (
      server_id,
      last_sample_id,
      datid,
      relid,
      relkind,
      schemaname,
      relname
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.relid,
      cur.relkind,
      cur.schemaname,
      cur.relname
    FROM
      last_stat_tables cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_tables_list DO
      UPDATE SET
        (last_sample_id, schemaname, relname) =
        (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.relname)
      WHERE
        (itl.last_sample_id, itl.schemaname, itl.relname) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.relname);

    -- Tables
    INSERT INTO sample_stat_tables (
      server_id,
      sample_id,
      datid,
      relid,
      reltoastrelid,
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
      total_vacuum_time,
      total_autovacuum_time,
      total_analyze_time,
      total_autoanalyze_time,
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
      relpages_bytes,
      relpages_bytes_diff,
      last_seq_scan,
      last_idx_scan,
      n_tup_newpage_upd
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.relid AS relid,
      cur.reltoastrelid AS reltoastrelid,
      cur.tablespaceid AS tablespaceid,
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
      cur.total_vacuum_time - COALESCE(lst.total_vacuum_time,0) AS total_vacuum_time,
      cur.total_autovacuum_time - COALESCE(lst.total_autovacuum_time,0) AS total_autovacuum_time,
      cur.total_analyze_time - COALESCE(lst.total_analyze_time,0) AS total_analyze_time,
      cur.total_autoanalyze_time - COALESCE(lst.total_autoanalyze_time,0) AS total_autoanalyze_time,
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
      cur.relpages_bytes AS relpages_bytes,
      cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) AS relpages_bytes_diff,
      cur.last_seq_scan AS last_seq_scan,
      cur.last_idx_scan AS last_idx_scan,
      cur.n_tup_newpage_upd - COALESCE(lst.n_tup_newpage_upd,0) AS n_tup_newpage_upd
    FROM
      last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
    ORDER BY cur.reltoastrelid NULLS FIRST;

    -- Update incorrectly calculated relation growth in case of database stats reset
    UPDATE sample_stat_tables usst
    SET
      relsize_diff = cur.relsize - COALESCE(lst.relsize,0),
      relpages_bytes_diff = cur.relpages_bytes - COALESCE(lst.relpages_bytes,0)
    FROM
      last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
      AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
      AND (usst.server_id, usst.sample_id, usst.datid, usst.relid) =
        (sserver_id, s_id, cur.datid, cur.relid);

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
      total_vacuum_time,
      total_autovacuum_time,
      total_analyze_time,
      total_autoanalyze_time,
      heap_blks_read,
      heap_blks_hit,
      idx_blks_read,
      idx_blks_hit,
      toast_blks_read,
      toast_blks_hit,
      tidx_blks_read,
      tidx_blks_hit,
      relsize_diff,
      n_tup_newpage_upd
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
      sum(cur.total_vacuum_time - COALESCE(lst.total_vacuum_time,0)),
      sum(cur.total_autovacuum_time - COALESCE(lst.total_autovacuum_time,0)),
      sum(cur.total_analyze_time - COALESCE(lst.total_analyze_time,0)),
      sum(cur.total_autoanalyze_time - COALESCE(lst.total_autoanalyze_time,0)),
      sum(cur.heap_blks_read - COALESCE(lst.heap_blks_read,0)),
      sum(cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      sum(cur.toast_blks_read - COALESCE(lst.toast_blks_read,0)),
      sum(cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0)),
      sum(cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0)),
      sum(cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0)),
      CASE
        WHEN skip_sizes THEN NULL
        ELSE sum(cur.relsize - COALESCE(lst.relsize,0))
      END,
      sum(cur.n_tup_newpage_upd - COALESCE(lst.n_tup_newpage_upd,0))
    FROM last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.datid, dblst.sample_id) =
        (sserver_id, dbcur.datid, s_id - 1)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid;

    IF NOT skip_sizes THEN
    /* Update incorrectly calculated aggregated tables growth in case of
     * database statistics reset
     */
      UPDATE sample_stat_tables_total usstt
      SET relsize_diff = calc.relsize_diff
      FROM (
          SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.relkind,
            cur.tablespaceid,
            sum(cur.relsize - COALESCE(lst.relsize,0)) AS relsize_diff
          FROM last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
            JOIN last_stat_database dblst ON
              (dblst.server_id, dblst.sample_id, dblst.datid) =
              (sserver_id, s_id - 1, dbcur.datid)
            LEFT OUTER JOIN last_stat_tables lst ON
              (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
              (sserver_id, s_id - 1, dblst.datid, cur.relid)
          WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
            AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
          GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid
        ) calc
      WHERE (usstt.server_id, usstt.sample_id, usstt.datid, usstt.relkind, usstt.tablespaceid) =
        (sserver_id, s_id, calc.datid, calc.relkind, calc.tablespaceid);
    END IF;

    /*
    Preserve previous relation sizes in if we couldn't collect
    size this time (for example, due to locked relation)*/
    UPDATE last_stat_tables cur
    SET relsize = lst.relsize
    FROM last_stat_tables lst
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
      AND (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
      (cur.server_id, s_id - 1, cur.datid, cur.relid)
      AND cur.relsize IS NULL;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate tables stats,end}',to_jsonb(clock_timestamp()));
      result := jsonb_set(result,'{timings,calculate indexes stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- New index names
    INSERT INTO indexes_list AS iil (
      server_id,
      last_sample_id,
      datid,
      indexrelid,
      relid,
      schemaname,
      indexrelname
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.indexrelid,
      cur.relid,
      cur.schemaname,
      cur.indexrelname
    FROM
      last_stat_indexes cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_indexes_list DO
      UPDATE SET
        (last_sample_id, relid, schemaname, indexrelname) =
        (EXCLUDED.last_sample_id, EXCLUDED.relid, EXCLUDED.schemaname, EXCLUDED.indexrelname)
      WHERE
        (iil.last_sample_id, iil.relid, iil.schemaname, iil.indexrelname) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.relid, EXCLUDED.schemaname, EXCLUDED.indexrelname);

    -- Index stats
    INSERT INTO sample_stat_indexes (
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
      indisunique,
      relpages_bytes,
      relpages_bytes_diff,
      last_idx_scan
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.indexrelid AS indexrelid,
      cur.tablespaceid AS tablespaceid,
      cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
      cur.idx_tup_read - COALESCE(lst.idx_tup_read,0) AS idx_tup_read,
      cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
      cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
      cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
      cur.relsize,
      cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
      cur.indisunique,
      cur.relpages_bytes AS relpages_bytes,
      cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) AS relpages_bytes_diff,
      cur.last_idx_scan
    FROM
      last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Update incorrectly calculated relation growth in case of database stats reset
    UPDATE sample_stat_indexes ussi
    SET
      relsize_diff = cur.relsize - COALESCE(lst.relsize,0),
      relpages_bytes_diff = cur.relpages_bytes - COALESCE(lst.relpages_bytes,0)
    FROM
      last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
      AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
      AND (ussi.server_id, ussi.sample_id, ussi.datid, ussi.indexrelid) =
        (sserver_id, s_id, cur.datid, cur.indexrelid);

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
      CASE
        WHEN skip_sizes THEN NULL
        ELSE sum(cur.relsize - COALESCE(lst.relsize,0))
      END
    FROM last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.tablespaceid;

    /* Update incorrectly calculated aggregated index growth in case of
     * database statistics reset
     */
    IF NOT skip_sizes THEN
      UPDATE sample_stat_indexes_total ussit
      SET relsize_diff = calc.relsize_diff
      FROM (
          SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.tablespaceid,
            sum(cur.relsize - COALESCE(lst.relsize,0)) AS relsize_diff
          FROM last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
            JOIN last_stat_database dblst ON
              (dblst.server_id, dblst.sample_id, dblst.datid) =
              (sserver_id, s_id - 1, dbcur.datid)
            LEFT OUTER JOIN last_stat_indexes lst ON
              (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
              (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
          WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
            AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
          GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.tablespaceid
        ) calc
      WHERE (ussit.server_id, ussit.sample_id, ussit.datid, ussit.tablespaceid) =
        (sserver_id, s_id, calc.datid, calc.tablespaceid);
    END IF;
    /*
    Preserve previous relation sizes in if we couldn't collect
    size this time (for example, due to locked relation)*/
    UPDATE last_stat_indexes cur
    SET relsize = lst.relsize
    FROM last_stat_indexes lst
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
      AND (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
      (sserver_id, s_id - 1, cur.datid, cur.indexrelid)
      AND cur.relsize IS NULL;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate indexes stats,end}',to_jsonb(clock_timestamp()));
      result := jsonb_set(result,'{timings,calculate functions stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- New function names
    INSERT INTO funcs_list AS ifl (
      server_id,
      last_sample_id,
      datid,
      funcid,
      schemaname,
      funcname,
      funcargs
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.funcid,
      cur.schemaname,
      cur.funcname,
      cur.funcargs
    FROM
      last_stat_user_functions cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_funcs_list DO
      UPDATE SET
        (last_sample_id, funcid, schemaname, funcname, funcargs) =
        (EXCLUDED.last_sample_id, EXCLUDED.funcid, EXCLUDED.schemaname,
          EXCLUDED.funcname, EXCLUDED.funcargs)
      WHERE
        (ifl.last_sample_id, ifl.funcid, ifl.schemaname,
          ifl.funcname, ifl.funcargs) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.funcid, EXCLUDED.schemaname,
          EXCLUDED.funcname, EXCLUDED.funcargs);

    -- Function stats
    INSERT INTO sample_stat_user_functions (
      server_id,
      sample_id,
      datid,
      funcid,
      calls,
      total_time,
      self_time,
      trg_fn
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.funcid,
      cur.calls - COALESCE(lst.calls,0) AS calls,
      cur.total_time - COALESCE(lst.total_time,0) AS total_time,
      cur.self_time - COALESCE(lst.self_time,0) AS self_time,
      cur.trg_fn
    FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
        (sserver_id, s_id - 1, dblst.datid, cur.funcid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

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
    FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
        (sserver_id, s_id - 1, dblst.datid, cur.funcid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.trg_fn;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate functions stats,end}',to_jsonb(clock_timestamp()));
      result := jsonb_set(result,'{timings,merge new extensions version}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    UPDATE extension_versions ev
    SET last_sample_id = s_id - 1
    FROM last_extension_versions prev_lev
      LEFT JOIN last_extension_versions cur_lev ON
        (cur_lev.server_id, cur_lev.datid, cur_lev.sample_id, cur_lev.extname, cur_lev.extversion) =
        (sserver_id, prev_lev.datid, s_id, prev_lev.extname, prev_lev.extversion)
    WHERE
      (prev_lev.server_id, prev_lev.sample_id) = (sserver_id, s_id - 1) AND
      (ev.server_id, ev.datid, ev.extname, ev.extversion) =
      (sserver_id, prev_lev.datid, prev_lev.extname, prev_lev.extversion) AND
      ev.last_sample_id IS NULL AND
      cur_lev.extname IS NULL;

    INSERT INTO extension_versions (
      server_id,
      datid,
      first_seen,
      extname,
      extversion
    )
    SELECT
      cur_lev.server_id,
      cur_lev.datid,
      s.sample_time as first_seen,
      cur_lev.extname,
      cur_lev.extversion
    FROM last_extension_versions cur_lev
      JOIN samples s on (s.server_id, s.sample_id) = (sserver_id, s_id)
      LEFT JOIN last_extension_versions prev_lev ON
        (prev_lev.server_id, prev_lev.datid, prev_lev.sample_id, prev_lev.extname, prev_lev.extversion) =
        (sserver_id, cur_lev.datid, s_id - 1, cur_lev.extname, cur_lev.extversion)
    WHERE
      (cur_lev.server_id, cur_lev.sample_id) = (sserver_id, s_id) AND
      prev_lev.extname IS NULL;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,merge new extensions version,end}',to_jsonb(clock_timestamp()));
      result := jsonb_set(result,'{timings,merge new relation storage parameters}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    UPDATE table_storage_parameters tsp
    SET last_sample_id = s_id - 1
    FROM last_stat_tables prev_lst
      LEFT JOIN last_stat_tables cur_lst ON
        (cur_lst.server_id, cur_lst.datid, cur_lst.relid, cur_lst.sample_id, cur_lst.reloptions) =
        (sserver_id, prev_lst.datid, prev_lst.relid, s_id, prev_lst.reloptions)
    WHERE
      (prev_lst.server_id, prev_lst.sample_id) = (sserver_id, s_id - 1) AND
      prev_lst.reloptions IS NOT NULL AND
      (tsp.server_id, tsp.datid, tsp.relid, tsp.reloptions) =
      (sserver_id, prev_lst.datid, prev_lst.relid, prev_lst.reloptions) AND
      tsp.last_sample_id IS NULL AND
      cur_lst IS NULL;

    INSERT INTO table_storage_parameters (
      server_id,
      datid,
      relid,
      first_seen,
      reloptions
    )
    SELECT
      cur_lst.server_id,
      cur_lst.datid,
      cur_lst.relid,
      s.sample_time as first_seen,
      cur_lst.reloptions
    FROM last_stat_tables cur_lst
      JOIN samples s on (s.server_id, s.sample_id) = (sserver_id, s_id)
      LEFT JOIN last_stat_tables prev_lst ON
        (prev_lst.server_id, prev_lst.datid, prev_lst.relid, prev_lst.sample_id, prev_lst.in_sample, prev_lst.reloptions) =
        (sserver_id, cur_lst.datid, cur_lst.relid, s_id - 1, true, cur_lst.reloptions)
      LEFT JOIN table_storage_parameters tsp ON
        (tsp.server_id, tsp.datid, tsp.relid, tsp.reloptions) =
        (sserver_id, cur_lst.datid, cur_lst.relid, cur_lst.reloptions)
    WHERE
      (cur_lst.server_id, cur_lst.sample_id, cur_lst.in_sample) = (sserver_id, s_id, true) AND
      cur_lst.reloptions IS NOT NULL AND
      prev_lst IS NULL AND
      tsp IS NULL;

    UPDATE index_storage_parameters tsp
    SET last_sample_id = s_id - 1
      FROM last_stat_indexes prev_lsi
      LEFT JOIN last_stat_indexes cur_lsi ON
        (cur_lsi.server_id, cur_lsi.datid, cur_lsi.relid, cur_lsi.indexrelid, cur_lsi.sample_id, cur_lsi.reloptions) =
        (sserver_id, prev_lsi.datid, prev_lsi.relid, prev_lsi.indexrelid, s_id, prev_lsi.reloptions)
    WHERE (prev_lsi.server_id, prev_lsi.sample_id) = (sserver_id, s_id - 1)AND
      prev_lsi.reloptions IS NOT NULL AND
      (tsp.server_id, tsp.datid, tsp.relid, tsp.indexrelid, tsp.reloptions) =
      (sserver_id, prev_lsi.datid, prev_lsi.relid, prev_lsi.indexrelid, prev_lsi.reloptions) AND
      tsp.last_sample_id IS NULL AND
      cur_lsi IS NULL;

    INSERT INTO index_storage_parameters (
      server_id,
      datid,
      relid,
      indexrelid,
      first_seen,
      reloptions
    )
    SELECT
      cur_lsi.server_id,
      cur_lsi.datid,
      cur_lsi.relid,
      cur_lsi.indexrelid,
      s.sample_time as first_seen,
      cur_lsi.reloptions
    FROM last_stat_indexes cur_lsi
      JOIN samples s on (s.server_id, s.sample_id) = (sserver_id, s_id)
      LEFT JOIN last_stat_indexes prev_lsi ON
        (prev_lsi.server_id, prev_lsi.datid, prev_lsi.relid, prev_lsi.indexrelid, prev_lsi.sample_id, prev_lsi.in_sample, prev_lsi.reloptions) =
        (sserver_id, cur_lsi.datid, cur_lsi.relid, cur_lsi.indexrelid, s_id - 1, true, cur_lsi.reloptions)
      LEFT JOIN index_storage_parameters isp ON
        (isp.server_id, isp.datid, isp.relid, isp.indexrelid, isp.reloptions) =
        (sserver_id, cur_lsi.datid, cur_lsi.relid, cur_lsi.indexrelid, cur_lsi.reloptions)
    WHERE
      (cur_lsi.server_id, cur_lsi.sample_id, cur_lsi.in_sample) = (sserver_id, s_id, true) AND
      cur_lsi.reloptions IS NOT NULL AND
      prev_lsi IS NULL AND
      isp IS NULL;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,merge new relation storage parameters,end}',to_jsonb(clock_timestamp()));
    END IF;

    -- Clear data in last_ tables, holding data only for next diff sample
    DELETE FROM last_stat_tables WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_indexes WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_user_functions WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_extension_versions WHERE server_id = sserver_id AND sample_id != s_id;

    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION show_samples(IN server name,IN days integer = NULL)
RETURNS TABLE(
    sample integer,
    sample_time timestamp (0) with time zone,
    sizes_collected boolean,
    dbstats_reset timestamp (0) with time zone,
    bgwrstats_reset timestamp (0) with time zone,
    archstats_reset timestamp (0) with time zone)
SET search_path=@extschema@ AS $$
  SELECT
    s.sample_id,
    s.sample_time,
    count(relsize_diff) > 0 AS sizes_collected,
    max(nullif(db1.stats_reset,coalesce(db2.stats_reset,db1.stats_reset))) AS dbstats_reset,
    max(nullif(bgwr1.stats_reset,coalesce(bgwr2.stats_reset,bgwr1.stats_reset))) AS bgwrstats_reset,
    max(nullif(arch1.stats_reset,coalesce(arch2.stats_reset,arch1.stats_reset))) AS archstats_reset
  FROM samples s JOIN servers n USING (server_id)
    JOIN sample_stat_database db1 USING (server_id,sample_id)
    JOIN sample_stat_cluster bgwr1 USING (server_id,sample_id)
    JOIN sample_stat_tables_total USING (server_id,sample_id)
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

CREATE FUNCTION show_samples(IN days integer = NULL)
RETURNS TABLE(
    sample integer,
    sample_time timestamp (0) with time zone,
    sizes_collected boolean,
    dbstats_reset timestamp (0) with time zone,
    clustats_reset timestamp (0) with time zone,
    archstats_reset timestamp (0) with time zone)
SET search_path=@extschema@ AS $$
    SELECT * FROM show_samples('local',days);
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_samples(IN days integer) IS 'Display available samples for local server';

CREATE FUNCTION get_sized_bounds(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  left_bound    integer,
  right_bound   integer
)
SET search_path=@extschema@ AS $$
SELECT
  left_bound.sample_id AS left_bound,
  right_bound.sample_id AS right_bound
FROM (
    SELECT
      sample_id
    FROM
      sample_stat_tables_total
    WHERE
      server_id = sserver_id
      AND sample_id >= end_id
    GROUP BY
      sample_id
    HAVING
      count(relsize_diff) > 0
    ORDER BY sample_id ASC
    LIMIT 1
  ) right_bound,
  (
    SELECT
      sample_id
    FROM
      sample_stat_tables_total
    WHERE
      server_id = sserver_id
      AND sample_id <= start_id
    GROUP BY
      sample_id
    HAVING
      count(relsize_diff) > 0
    ORDER BY sample_id DESC
    LIMIT 1
  ) left_bound
$$ LANGUAGE sql;
