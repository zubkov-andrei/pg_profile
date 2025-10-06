CREATE FUNCTION init_sample(IN sserver_id integer
) RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    server_properties jsonb = '{"extensions":[],"settings":[],"timings":[],"properties":{}}'; -- version, extensions, etc.
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
        SELECT least(current_setting('{pg_profile}.topn')::integer, 100) AS topn INTO qres;
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

    server_properties := log_sample_timings(server_properties, 'connect', 'start');
    server_properties := log_sample_timings(server_properties, 'total', 'start');

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

    server_properties := log_sample_timings(server_properties, 'connect', 'end');
    server_properties := log_sample_timings(server_properties, 'get server environment', 'start');
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
