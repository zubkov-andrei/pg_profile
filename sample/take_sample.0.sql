CREATE FUNCTION take_sample(IN sserver_id integer, IN skip_sizes boolean
) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    s_id              integer;
    topn              integer;
    server_properties jsonb;
    qres              record;
    qres_settings     record;
    settings_refresh  boolean = true;

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
    server_properties := log_sample_timings(server_properties, 'get server environment', 'end');

    server_properties := collect_database_stats(server_properties, sserver_id, s_id);

    server_properties := log_sample_timings(server_properties, 'calculate database stats', 'start');
    perform calculate_database_stats(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'calculate database stats', 'end');

    server_properties := log_sample_timings(server_properties, 'collect tablespace stats', 'start');
    perform collect_tablespace_stats(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'collect tablespace stats', 'end');

    server_properties := log_sample_timings(server_properties, 'collect statement stats', 'start');
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
    server_properties := log_sample_timings(server_properties, 'collect statement stats', 'end');

    server_properties := log_sample_timings(server_properties, 'collect wait sampling stats', 'start');
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
    server_properties := log_sample_timings(server_properties, 'collect wait sampling stats', 'end');

    server_properties := query_pg_stat_bgwriter(server_properties, sserver_id, s_id);
    server_properties := query_pg_stat_wal(server_properties, sserver_id, s_id);
    server_properties := query_pg_stat_io(server_properties, sserver_id, s_id);
    server_properties := query_pg_stat_slru(server_properties, sserver_id, s_id);
    server_properties := query_pg_stat_archiver(server_properties, sserver_id, s_id);

    server_properties := log_sample_timings(server_properties, 'collect object stats', 'start');
    -- Collecting stat info for objects of all databases
    IF COALESCE((server_properties #> '{collect,objects}')::boolean, true) THEN
      server_properties := collect_obj_stats(server_properties, sserver_id, s_id, skip_sizes);
      ASSERT server_properties IS NOT NULL, 'lost properties';
    END IF;
    server_properties := log_sample_timings(server_properties, 'collect object stats', 'end');

    server_properties := log_sample_timings(server_properties, 'processing subsamples', 'start');
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

    server_properties := log_sample_timings(server_properties, 'processing subsamples', 'end');

    server_properties := log_sample_timings(server_properties, 'disconnect', 'start');
    PERFORM dblink('server_connection', 'COMMIT');
    PERFORM dblink_disconnect('server_connection');
    server_properties := log_sample_timings(server_properties, 'disconnect', 'end');

    server_properties := log_sample_timings(server_properties, 'maintain repository', 'start');
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

    server_properties := log_sample_timings(server_properties, 'maintain repository', 'end');

    server_properties := log_sample_timings(server_properties, 'calculate tablespace stats', 'start');
    perform calculate_tablespace_stats(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'calculate tablespace stats', 'end');

    server_properties := log_sample_timings(server_properties, 'calculate object stats', 'start');
    -- collect databases objects stats
    IF COALESCE((server_properties #> '{collect,objects}')::boolean, true) THEN
      server_properties := sample_dbobj_delta(server_properties,sserver_id,s_id,topn,skip_sizes);
      ASSERT server_properties IS NOT NULL, 'lost properties';
    END IF;

    DELETE FROM last_stat_tablespaces WHERE server_id = sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_database WHERE server_id = sserver_id AND sample_id != s_id;

    server_properties := log_sample_timings(server_properties, 'calculate object stats', 'end');

    server_properties := log_sample_timings(server_properties, 'calculate cluster stats', 'start');
    perform calculate_cluster_stats(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'calculate cluster stats', 'end');

    server_properties := log_sample_timings(server_properties, 'calculate IO stats', 'start');
    perform calculate_io_stats(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'calculate IO stats', 'end');

    server_properties := log_sample_timings(server_properties, 'calculate SLRU stats', 'start');
    perform calculate_slru_stats(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'calculate SLRU stats', 'end');

    server_properties := log_sample_timings(server_properties, 'calculate WAL stats', 'start');
    perform calculate_wal_stats(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'calculate WAL stats', 'end');

    server_properties := log_sample_timings(server_properties, 'calculate archiver stats', 'start');
    perform calculate_archiver_stats(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'calculate archiver stats', 'end');

    server_properties := log_sample_timings(server_properties, 'delete obsolete samples', 'start');
    perform delete_obsolete_samples(sserver_id, s_id);
    server_properties := log_sample_timings(server_properties, 'delete obsolete samples', 'end');

    server_properties := log_sample_timings(server_properties, 'total', 'end');
    IF (server_properties #>> '{collect_timings}')::boolean THEN
      -- Save timing statistics of sample
      INSERT INTO sample_timings (server_id, sample_id, "event", exec_point, event_ts)
      SELECT sserver_id, s_id, t.sampling_event, t.exec_point, t.event_tm
      FROM jsonb_to_recordset(server_properties -> 'timings') as t (sampling_event text, exec_point text, event_tm timestamptz);
    END IF;
    ASSERT server_properties IS NOT NULL, 'lost properties';

    -- Reset lock_timeout setting to its initial value
    EXECUTE format('SET lock_timeout TO %L', server_properties #>> '{properties,lock_timeout_init}');

    RETURN 0;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample(IN sserver_id integer, IN skip_sizes boolean) IS
  'Statistics sample creation function (by server_id)';