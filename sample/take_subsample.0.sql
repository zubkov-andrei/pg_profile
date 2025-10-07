CREATE FUNCTION take_subsample(IN sserver_id integer, IN properties jsonb = NULL)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
  server_query  text;

  session_rows  integer;
  qres          record;

  s_id          integer;  -- last base sample identifier
  srv_version   integer;

  guc_min_query_dur       interval hour to second;
  guc_min_xact_dur        interval hour to second;
  guc_min_xact_age        integer;
  guc_min_idle_xact_dur   interval hour to second;

BEGIN
  -- Adding dblink extension schema to search_path if it does not already there
  IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
    RAISE 'dblink extension must be installed';
  END IF;

  SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
  IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
    EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
  END IF;

  IF (properties IS NULL) THEN
    -- Initialization is not done yet
    properties := init_sample(sserver_id);
  END IF; -- empty properties

  -- Skip subsampling if it is disabled
  IF (NOT (properties #>> '{properties,subsample_enabled}')::boolean) THEN
    IF NOT (properties #>> '{properties,in_sample}')::boolean THEN
      -- Reset lock_timeout setting to its initial value
      EXECUTE format('SET lock_timeout TO %L', properties #>> '{properties,lock_timeout_init}');
    END IF;
    RETURN properties;
  END IF;

  -- Only one running take_subsample() function allowed per server!
  BEGIN
    PERFORM
    FROM server_subsample
    WHERE server_id = sserver_id
    FOR UPDATE NOWAIT;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE 'Can''t get lock on server. Is there another '
        'take_subsample() function running on this server?';
  END;

  s_id := (properties #>> '{properties,last_sample_id}')::integer;

  srv_version := (get_sp_setting(properties, 'server_version_num')).reset_val::integer;

  -- Current session states collection
  -- collect sessions and their states
  CASE
    WHEN srv_version >= 140000 THEN
      server_query :=
        'SELECT subsample_ts, datid, datname, pid, leader_pid, usesysid,'
          'usename, application_name, client_addr, client_hostname,'
          'client_port, backend_start, xact_start, query_start, state_change,'
          'state, backend_xid, backend_xmin, query_id, query, backend_type,'
          'backend_xmin_age '
        'FROM ('
            'SELECT '
              'clock_timestamp() as subsample_ts,'
              'datid,'
              'datname,'
              'pid,'
              'leader_pid,'
              'usesysid,'
              'usename,'
              'application_name,'
              'client_addr,'
              'client_hostname,'
              'client_port,'
              'backend_start,'
              'xact_start,'
              'query_start,'
              'state_change,'
              'state,'
              'backend_xid,'
              'backend_xmin,'
              'query_id,'
              'query,'
              'backend_type,'
              'age(backend_xmin) as backend_xmin_age,'
              'CASE '
                'WHEN xact_start <= now() - %1$L::interval THEN '
                  'row_number() OVER (ORDER BY xact_start ASC) '
                'ELSE NULL '
              'END as xact_ord, '
              'CASE '
                'WHEN query_start <= now() - %3$L::interval '
                  'AND state IN (''active'',''fastpath function call'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''active'',''fastpath function call'') ORDER BY query_start ASC) '
                'ELSE NULL '
              'END as query_ord, '
              'CASE '
                'WHEN state_change <= now() - %4$L::interval '
                'AND state IN (''idle in transaction'',''idle in transaction (aborted)'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''idle in transaction'',''idle in transaction (aborted)'') ORDER BY state_change ASC) '
                'ELSE NULL '
              'END as state_ord, '
              'CASE '
                'WHEN age(backend_xmin) >= %2$L THEN '
                  'row_number() OVER (ORDER BY age(backend_xmin) DESC) '
                'ELSE NULL '
              'END  as age_ord '
            'FROM pg_stat_activity '
            'WHERE state NOT IN (''idle'',''disabled'') '
          ') stat_activity '
        'WHERE least('
          'xact_ord,'
          'query_ord,'
          'state_ord,'
          'age_ord'
        ') <= %5$s';
    WHEN srv_version >= 130000 THEN
      server_query :=
        'SELECT subsample_ts, datid, datname, pid, leader_pid, usesysid,'
          'usename, application_name, client_addr, client_hostname,'
          'client_port, backend_start, xact_start, query_start, state_change,'
          'state, backend_xid, backend_xmin, query_id, query, backend_type,'
          'backend_xmin_age '
        'FROM ('
            'SELECT '
              'clock_timestamp() as subsample_ts,'
              'datid,'
              'datname,'
              'pid,'
              'leader_pid,'
              'usesysid,'
              'usename,'
              'application_name,'
              'client_addr,'
              'client_hostname,'
              'client_port,'
              'backend_start,'
              'xact_start,'
              'query_start,'
              'state_change,'
              'state,'
              'backend_xid,'
              'backend_xmin,'
              'NULL AS query_id,'
              'query,'
              'backend_type,'
              'age(backend_xmin) as backend_xmin_age,'
              'CASE '
                'WHEN xact_start <= now() - %1$L::interval THEN '
                  'row_number() OVER (ORDER BY xact_start ASC) '
                'ELSE NULL '
              'END as xact_ord, '
              'CASE '
                'WHEN query_start <= now() - %3$L::interval '
                  'AND state IN (''active'',''fastpath function call'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''active'',''fastpath function call'') ORDER BY query_start ASC) '
                'ELSE NULL '
              'END as query_ord, '
              'CASE '
                'WHEN state_change <= now() - %4$L::interval '
                'AND state IN (''idle in transaction'',''idle in transaction (aborted)'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''idle in transaction'',''idle in transaction (aborted)'') ORDER BY state_change ASC) '
                'ELSE NULL '
              'END as state_ord, '
              'CASE '
                'WHEN age(backend_xmin) >= %2$L THEN '
                  'row_number() OVER (ORDER BY age(backend_xmin) DESC) '
                'ELSE NULL '
              'END  as age_ord '
            'FROM pg_stat_activity '
            'WHERE state NOT IN (''idle'',''disabled'')'
          ') stat_activity '
        'WHERE least('
          'xact_ord,'
          'query_ord,'
          'state_ord,'
          'age_ord'
        ') <= %5$s';
    WHEN srv_version >= 100000 THEN
      server_query :=
        'SELECT subsample_ts, datid, datname, pid, leader_pid, usesysid,'
          'usename, application_name, client_addr, client_hostname,'
          'client_port, backend_start, xact_start, query_start, state_change,'
          'state, backend_xid, backend_xmin, query_id, query, backend_type,'
          'backend_xmin_age '
        'FROM ('
            'SELECT '
              'clock_timestamp() as subsample_ts,'
              'datid,'
              'datname,'
              'pid,'
              'NULL AS leader_pid,'
              'usesysid,'
              'usename,'
              'application_name,'
              'client_addr,'
              'client_hostname,'
              'client_port,'
              'backend_start,'
              'xact_start,'
              'query_start,'
              'state_change,'
              'state,'
              'backend_xid,'
              'backend_xmin,'
              'NULL AS query_id,'
              'query,'
              'backend_type,'
              'age(backend_xmin) as backend_xmin_age,'
              'CASE '
                'WHEN xact_start <= now() - %1$L::interval THEN '
                  'row_number() OVER (ORDER BY xact_start ASC) '
                'ELSE NULL '
              'END as xact_ord, '
              'CASE '
                'WHEN query_start <= now() - %3$L::interval '
                  'AND state IN (''active'',''fastpath function call'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''active'',''fastpath function call'') ORDER BY query_start ASC) '
                'ELSE NULL '
              'END as query_ord, '
              'CASE '
                'WHEN state_change <= now() - %4$L::interval '
                'AND state IN (''idle in transaction'',''idle in transaction (aborted)'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''idle in transaction'',''idle in transaction (aborted)'') ORDER BY state_change ASC) '
                'ELSE NULL '
              'END as state_ord, '
              'CASE '
                'WHEN age(backend_xmin) >= %2$L THEN '
                  'row_number() OVER (ORDER BY age(backend_xmin) DESC) '
                'ELSE NULL '
              'END  as age_ord '
            'FROM pg_stat_activity '
            'WHERE state NOT IN (''idle'',''disabled'')'
          ') stat_activity '
        'WHERE least('
          'xact_ord,'
          'query_ord,'
          'state_ord,'
          'age_ord'
        ') <= %5$s';
    ELSE
      RAISE 'Unsupported postgres version';
  END CASE;

  /*
   format() function will substitute defined values for us quoting
   them as it requested by the %L type. NULLs will be placed literally
   unquoted making threshold inactive. However we need the NULLIF()
   functions here to avoid errors in EDB instances having NULL strings
   to appear as the empty ones.
  */
  server_query := format(
      server_query,
      NULLIF(properties #>> '{properties,min_xact_dur}', ''),
      NULLIF(properties #>> '{properties,min_xact_age}', ''),
      NULLIF(properties #>> '{properties,min_query_dur}', ''),
      NULLIF(properties #>> '{properties,min_idle_xact_dur}', ''),
      properties #>> '{properties,topn}'
  );

  -- Save the current state of captured sessions satisfying thresholds
  INSERT INTO last_stat_activity
  SELECT
      sserver_id,
      s_id,
      dbl.subsample_ts,
      dbl.datid,
      dbl.datname,
      dbl.pid,
      dbl.leader_pid,
      dbl.usesysid,
      dbl.usename,
      dbl.application_name,
      dbl.client_addr,
      dbl.client_hostname,
      dbl.client_port,
      dbl.backend_start,
      dbl.xact_start,
      dbl.query_start,
      dbl.state_change,
      dbl.state,
      dbl.backend_xid,
      dbl.backend_xmin,
      dbl.query_id,
      dbl.query,
      dbl.backend_type,
      dbl.backend_xmin_age
  FROM
      dblink('server_connection', server_query) AS dbl(
          subsample_ts      timestamp with time zone,
          datid             oid,
          datname           name,
          pid               integer,
          leader_pid        integer,
          usesysid          oid,
          usename           name,
          application_name  text,
          client_addr       inet,
          client_hostname   text,
          client_port       integer,
          backend_start     timestamp with time zone,
          xact_start        timestamp with time zone,
          query_start       timestamp with time zone,
          state_change      timestamp with time zone,
          state             text,
          backend_xid       text,
          backend_xmin      text,
          query_id          bigint,
          query             text,
          backend_type      text,
          backend_xmin_age      bigint
      );
  GET DIAGNOSTICS session_rows = ROW_COUNT;

  IF session_rows > 0 THEN
    /*
      We have four thresholds probably defined for subsamples, so
      we'll delete the previous captured state when we don't need it
      anymore for all of them.
    */
    DELETE FROM last_stat_activity dlsa
    USING
      (
        SELECT pid, xact_start, max(subsample_ts) as subsample_ts
        FROM last_stat_activity
        WHERE (server_id, sample_id) = (sserver_id, s_id)
        GROUP BY pid, xact_start
      ) last_xact_state
      JOIN
      last_stat_activity lxs ON
        (sserver_id, s_id, last_xact_state.pid, last_xact_state.subsample_ts) =
        (lxs.server_id, lxs.sample_id, lxs.pid, lxs.subsample_ts)
    WHERE (dlsa.server_id, dlsa.sample_id, dlsa.pid, dlsa.xact_start) =
      (sserver_id, s_id, last_xact_state.pid, last_xact_state.xact_start)
      AND dlsa.subsample_ts < last_xact_state.subsample_ts
      AND
      /*
        As we are observing the same xact here (pid, xact_start)
        min_xact_dur threshold can't apply any limitation on deleting
        the old entry.
      */
      -- Can we delete dlsa state due to min_xact_age threshold?
        ((dlsa.backend_xmin IS NOT DISTINCT FROM lxs.backend_xmin)
        OR coalesce(
          dlsa.backend_xmin_age < (properties #>> '{properties,min_xact_age}')::integer,
          true)
        )
      AND
      -- Can we delete dlsa state due to min_query_dur threshold?
      CASE
        WHEN dlsa.state IN ('active', 'fastpath function call') THEN
        (
          ((dlsa.query_start, dlsa.state)
            IS NOT DISTINCT FROM
            (lxs.query_start, lxs.state)
          )
          OR coalesce(
            dlsa.subsample_ts - dlsa.query_start <
              (properties #>> '{properties,min_query_dur}')::interval,
            true)
        )
        ELSE true
      END
      AND
      -- Can we delete dlsa state due to min_idle_xact_dur threshold?
      CASE
        WHEN dlsa.state IN ('idle in transaction','idle in transaction (aborted)') THEN
        (
          ((dlsa.state_change)
            IS NOT DISTINCT FROM
            (lxs.state_change)
          )
          OR coalesce(
            dlsa.subsample_ts - dlsa.state_change <
              (properties #>> '{properties,min_idle_xact_dur}')::interval,
            true)
        )
        ELSE true
      END
    ;
  END IF;

  /* It seems we should avoid analyze here, hoping autoanalyze will do
  the trick */
  /*
  GET DIAGNOSTICS session_rows = ROW_COUNT; EXECUTE
  format('ANALYZE last_stat_activity_srv%1$s', sserver_id);
  */

  -- Collect sessions count by states and waits
  server_query :=
    'SELECT '
      'now() as subsample_ts,'
      'backend_type,'
      'datid,'
      'datname,'
      'usesysid,'
      'usename,'
      'application_name,'
      'client_addr,'
      'count(*) as total,'
      'count(*) FILTER (WHERE state = ''active'') as active,'
      'count(*) FILTER (WHERE state = ''idle'') as idle,'
      'count(*) FILTER (WHERE state = ''idle in transaction'') as idle_t,'
      'count(*) FILTER (WHERE state = ''idle in transaction (aborted)'') as idle_ta,'
      'count(*) FILTER (WHERE state IS NULL) as state_null,'
      'count(*) FILTER (WHERE wait_event_type = ''LWLock'') as lwlock,'
      'count(*) FILTER (WHERE wait_event_type = ''Lock'') as lock,'
      'count(*) FILTER (WHERE wait_event_type = ''BufferPin'') as bufferpin,'
      'count(*) FILTER (WHERE wait_event_type = ''Activity'') as activity,'
      'count(*) FILTER (WHERE wait_event_type = ''Extension'') as extension,'
      'count(*) FILTER (WHERE wait_event_type = ''Client'') as client,'
      'count(*) FILTER (WHERE wait_event_type = ''IPC'') as ipc,'
      'count(*) FILTER (WHERE wait_event_type = ''Timeout'') as timeout,'
      'count(*) FILTER (WHERE wait_event_type = ''IO'') as io '
    'FROM pg_stat_activity '
    'GROUP BY backend_type, datid, datname, usesysid, usename, application_name, client_addr';

  -- Save the current state of captured sessions satisfying thresholds
  INSERT INTO last_stat_activity_count
  SELECT
      sserver_id,
      s_id,
      dbl.subsample_ts,
      dbl.backend_type,
      dbl.datid,
      dbl.datname,
      dbl.usesysid,
      dbl.usename,
      dbl.application_name,
      dbl.client_addr,

      dbl.total,
      dbl.active,
      dbl.idle,
      dbl.idle_t,
      dbl.idle_ta,
      dbl.state_null,
      dbl.lwlock,
      dbl.lock,
      dbl.bufferpin,
      dbl.activity,
      dbl.extension,
      dbl.client,
      dbl.ipc,
      dbl.timeout,
      dbl.io
  FROM
      dblink('server_connection', server_query) AS dbl(
          subsample_ts      timestamp with time zone,
          backend_type      text,
          datid             oid,
          datname           name,
          usesysid          oid,
          usename           name,
          application_name  text,
          client_addr       inet,

          total             integer,
          active            integer,
          idle              integer,
          idle_t            integer,
          idle_ta           integer,
          state_null        integer,
          lwlock            integer,
          lock              integer,
          bufferpin         integer,
          activity          integer,
          extension         integer,
          client            integer,
          ipc               integer,
          timeout           integer,
          io                integer
      );

  IF NOT (properties #>> '{properties,in_sample}')::boolean THEN
    -- Reset lock_timeout setting to its initial value
    PERFORM dblink('server_connection', 'COMMIT');
    PERFORM dblink_disconnect('server_connection');
    EXECUTE format('SET lock_timeout TO %L', properties #>> '{properties,lock_timeout_init}');
  END IF;

  RETURN properties;
END
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_subsample(IN integer, IN jsonb) IS
  'Take a sub-sample for a server by server_id';
