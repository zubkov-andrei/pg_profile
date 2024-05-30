/* Subsampling functions */
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

  SELECT reset_val::integer INTO STRICT srv_version FROM jsonb_to_recordset(properties #> '{settings}')
    AS x(name text, reset_val text)
  WHERE name = 'server_version_num';

  -- Current session states collection
  PERFORM dblink_exec('server_connection', 'BEGIN TRANSACTION READ ONLY');
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
                'WHEN xact_start <= now() - %1$s::interval THEN '
                  'row_number() OVER (ORDER BY xact_start ASC) '
                'ELSE NULL '
              'END as xact_ord, '
              'CASE '
                'WHEN query_start <= now() - %3$s::interval '
                  'AND state IN (''active'',''fastpath function call'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''active'',''fastpath function call'') ORDER BY query_start ASC) '
                'ELSE NULL '
              'END as query_ord, '
              'CASE '
                'WHEN state_change <= now() - %4$s::interval '
                'AND state IN (''idle in transaction'',''idle in transaction (aborted)'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''idle in transaction'',''idle in transaction (aborted)'') ORDER BY state_change ASC) '
                'ELSE NULL '
              'END as state_ord, '
              'CASE '
                'WHEN age(backend_xmin) >= %2$s THEN '
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
                'WHEN xact_start <= now() - %1$s::interval THEN '
                  'row_number() OVER (ORDER BY xact_start ASC) '
                'ELSE NULL '
              'END as xact_ord, '
              'CASE '
                'WHEN query_start <= now() - %3$s::interval '
                  'AND state IN (''active'',''fastpath function call'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''active'',''fastpath function call'') ORDER BY query_start ASC) '
                'ELSE NULL '
              'END as query_ord, '
              'CASE '
                'WHEN state_change <= now() - %4$s::interval '
                'AND state IN (''idle in transaction'',''idle in transaction (aborted)'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''idle in transaction'',''idle in transaction (aborted)'') ORDER BY state_change ASC) '
                'ELSE NULL '
              'END as state_ord, '
              'CASE '
                'WHEN age(backend_xmin) >= %2$s THEN '
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
                'WHEN xact_start <= now() - %1$s::interval THEN '
                  'row_number() OVER (ORDER BY xact_start ASC) '
                'ELSE NULL '
              'END as xact_ord, '
              'CASE '
                'WHEN query_start <= now() - %3$s::interval '
                  'AND state IN (''active'',''fastpath function call'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''active'',''fastpath function call'') ORDER BY query_start ASC) '
                'ELSE NULL '
              'END as query_ord, '
              'CASE '
                'WHEN state_change <= now() - %4$s::interval '
                'AND state IN (''idle in transaction'',''idle in transaction (aborted)'') THEN '
                  'row_number() OVER (PARTITION BY state IN (''idle in transaction'',''idle in transaction (aborted)'') ORDER BY state_change ASC) '
                'ELSE NULL '
              'END as state_ord, '
              'CASE '
                'WHEN age(backend_xmin) >= %2$s THEN '
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

  server_query := format(
      server_query,
      COALESCE(''''||(properties #>> '{properties,min_xact_dur}')||'''', 'NULL'),
      COALESCE(''''||(properties #>> '{properties,min_xact_age}')||'''', 'NULL'),
      COALESCE(''''||(properties #>> '{properties,min_query_dur}')||'''', 'NULL'),
      COALESCE(''''||(properties #>> '{properties,min_idle_xact_dur}')||'''', 'NULL'),
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
          backend_xid       xid,
          backend_xmin      xid,
          query_id          bigint,
          query             text,
          backend_type      text,
          backend_xmin_age      bigint
      );
  GET DIAGNOSTICS session_rows = ROW_COUNT;

  IF session_rows > 0 THEN
    /*
      We should also old states
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

  GET DIAGNOSTICS session_rows = ROW_COUNT;
  EXECUTE format('ANALYZE last_stat_activity_srv%1$s',
    sserver_id);

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

  PERFORM dblink_exec('server_connection', 'COMMIT');

  RETURN properties;
END
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_subsample(IN integer, IN jsonb) IS
  'Take a sub-sample for a server by server_id';

CREATE FUNCTION take_subsample(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    sserver_id    integer;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name = server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found';
    ELSE
        PERFORM take_subsample(sserver_id);
        RETURN 0;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_subsample(IN name) IS
  'Statistics sub-sample taking function (by server name)';

CREATE FUNCTION take_subsample_subset(IN sets_cnt integer = 1, IN current_set integer = 0)
RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
DECLARE
    c_servers CURSOR FOR
      SELECT server_id,server_name FROM (
        SELECT server_id,server_name, row_number() OVER () AS srv_rn
        FROM servers
        WHERE enabled
        ) AS t1
      WHERE srv_rn % sets_cnt = current_set;
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
            PERFORM take_subsample(qres.server_id);
            elapsed := clock_timestamp()::timestamp (2) with time zone - start_clock;
            result := 'OK';
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

COMMENT ON FUNCTION take_subsample_subset(IN sets_cnt integer, IN current_set integer) IS
  'Statistics sub-sample taking function (for subset of enabled servers). Used for simplification of parallel sub-sample collection.';

CREATE FUNCTION take_subsample() RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
  SELECT * FROM take_subsample_subset(1,0);
$$ LANGUAGE sql;

COMMENT ON FUNCTION take_subsample() IS 'Subsample taking function (for all enabled servers).';

CREATE FUNCTION collect_subsamples(IN sserver_id integer, IN s_id integer, IN properties jsonb = NULL)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    qres  RECORD;

    c_statements CURSOR FOR
    WITH last_stmt_state AS (
      SELECT server_id, sample_id, pid, query_start, max(subsample_ts) as subsample_ts
      FROM last_stat_activity
      WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
      GROUP BY server_id, sample_id, pid, query_start
    )
    SELECT
      pid,
      leader_pid,
      state_change,
      query_start,
      query_id,
      query,
      subsample_ts
    FROM
      last_stat_activity
      JOIN last_stmt_state USING (server_id, sample_id, pid, query_start, subsample_ts)
    WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
    ;
BEGIN

    INSERT INTO sample_act_backend (
      server_id,
      sample_id,
      pid,
      backend_start,
      datid,
      datname,
      usesysid,
      usename,
      client_addr,
      client_hostname,
      client_port,
      backend_type,
      backend_last_ts
    )
    WITH last_backend_state AS (
      SELECT server_id, sample_id, pid, backend_start, max(subsample_ts) as subsample_ts
      FROM last_stat_activity
      WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
      GROUP BY server_id, sample_id, pid, backend_start
    )
    SELECT
      server_id,
      s_id as sample_id,
      pid,
      backend_start,
      datid,
      datname,
      usesysid,
      usename,
      client_addr,
      client_hostname,
      client_port,
      backend_type,
      subsample_ts
    FROM
      last_stat_activity
      JOIN last_backend_state
        USING (server_id, sample_id, pid, backend_start, subsample_ts)
    WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
    ;

    INSERT INTO sample_act_xact (
      server_id,
      sample_id,
      pid,
      backend_start,
      xact_start,
      backend_xid,
      xact_last_ts
    )
    WITH last_xact_state AS (
      SELECT server_id, sample_id, pid, xact_start, max(subsample_ts) as subsample_ts
      FROM last_stat_activity
      WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
      GROUP BY server_id, sample_id, pid, xact_start
    )
    SELECT
      server_id,
      s_id AS sample_id,
      pid,
      backend_start,
      xact_start,
      backend_xid,
      subsample_ts AS xact_last_ts
    FROM
      last_stat_activity
      JOIN last_xact_state
        USING (server_id, sample_id, pid, xact_start, subsample_ts)
    WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
    ;

    INSERT INTO sample_act_backend_state (
      server_id,
      sample_id,
      pid,
      backend_start,
      application_name,
      state_code,
      state_change,
      state_last_ts,
      xact_start,
      backend_xmin,
      backend_xmin_age
    )
    WITH last_backend_state AS (
      SELECT server_id, sample_id, pid, state_change, max(subsample_ts) as subsample_ts
      FROM last_stat_activity
      WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
        AND state IN ('idle in transaction', 'idle in transaction (aborted)', 'active')
      GROUP BY server_id, sample_id, pid, state_change
    )
    SELECT
      server_id,
      s_id AS sample_id,
      pid,
      backend_start,
      application_name,
      CASE state
        WHEN 'idle in transaction' THEN 1
        WHEN 'idle in transaction (aborted)' THEN 2
        WHEN 'active' THEN 3
        ELSE 0
      END state_code,
      state_change,
      subsample_ts AS state_last_ts,
      xact_start,
      backend_xmin,
      backend_xmin_age
    FROM
      last_stat_activity
      JOIN last_backend_state
        USING (server_id, sample_id, pid, state_change, subsample_ts)
    WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
    ;

    FOR qres IN c_statements
    LOOP
      INSERT INTO act_query (server_id, act_query_md5, act_query, last_sample_id)
      VALUES (sserver_id, md5(qres.query), qres.query, NULL)
      ON CONFLICT ON CONSTRAINT pk_act_query
      DO UPDATE SET last_sample_id = NULL;

      INSERT INTO sample_act_statement(
        server_id,
        sample_id,
        pid,
        leader_pid,
        state_change,
        query_start,
        query_id,
        act_query_md5,
        stmt_last_ts
      ) VALUES (
        sserver_id,
        s_id,
        qres.pid,
        qres.leader_pid,
        qres.state_change,
        qres.query_start,
        qres.query_id,
        md5(qres.query),
        qres.subsample_ts
      );
    END LOOP;

    -- Save session counters
    -- Insert new values of session attributes
    INSERT INTO session_attr AS isa (
      server_id,
      backend_type,
      datid,
      datname,
      usesysid,
      usename,
      application_name,
      client_addr
    )
    SELECT DISTINCT
      ls.server_id,
      ls.backend_type,
      ls.datid,
      ls.datname,
      ls.usesysid,
      ls.usename,
      ls.application_name,
      ls.client_addr
    FROM
      last_stat_activity_count ls LEFT JOIN session_attr sa ON
        -- ensure partition pruning
        (sa.server_id = sserver_id) AND
        (ls.server_id, ls.backend_type, ls.datid, ls.datname, ls.usesysid,
          ls.usename, ls.application_name, ls.client_addr)
        IS NOT DISTINCT FROM
        (sa.server_id, sa.backend_type, sa.datid, sa.datname, sa.usesysid,
          sa.usename, sa.application_name, sa.client_addr)
    WHERE
      (ls.server_id, ls.sample_id) = (sserver_id, s_id - 1)
      AND sa.server_id IS NULL
    ;

    INSERT INTO sample_stat_activity_cnt (
      server_id,
      sample_id,
      subsample_ts,

      sess_attr_id,

      total,
      active,
      idle,
      idle_t,
      idle_ta,
      state_null,
      lwlock,
      lock,
      bufferpin,
      activity,
      extension,
      client,
      ipc,
      timeout,
      io
    )
    SELECT
      sserver_id,
      s_id,
      l.subsample_ts,

      s.sess_attr_id,

      l.total,
      l.active,
      l.idle,
      l.idle_t,
      l.idle_ta,
      l.state_null,
      l.lwlock,
      l.lock,
      l.bufferpin,
      l.activity,
      l.extension,
      l.client,
      l.ipc,
      l.timeout,
      l.io
    FROM
      last_stat_activity_count l JOIN session_attr s ON
        (l.server_id, s.server_id, s.backend_type) = (sserver_id, sserver_id, l.backend_type) AND
        (s.datid, s.datname, s.usesysid, s.usename, s.application_name, s.client_addr)
          IS NOT DISTINCT FROM
        (l.datid, l.datname, l.usesysid, l.usename, l.application_name, l.client_addr)
    WHERE l.sample_id = s_id - 1;

    -- Mark disappered and returned entries
    UPDATE session_attr sau
    SET last_sample_id =
      CASE
        WHEN num_nulls(ss.server_id, sa.last_sample_id) = 2 THEN s_id - 1
        WHEN num_nulls(ss.server_id, sa.last_sample_id) = 0 THEN NULL
      END
    FROM sample_stat_activity_cnt ss RIGHT JOIN session_attr sa
      ON (sa.server_id, ss.server_id, ss.sample_id, ss.sess_attr_id) =
        (sserver_id, sserver_id, s_id, sa.sess_attr_id)
    WHERE
      num_nulls(ss.server_id, sa.last_sample_id) IN (0,2) AND
      (sau.server_id, sau.sess_attr_id) = (sserver_id, sa.sess_attr_id);

    UPDATE act_query squ SET last_sample_id = s_id - 1
    FROM sample_act_statement ss RIGHT JOIN act_query sq
      ON (ss.server_id, ss.sample_id, ss.act_query_md5) =
        (sserver_id, s_id, sq.act_query_md5)
    WHERE
      sq.server_id = sserver_id AND
      sq.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (squ.server_id, squ.act_query_md5) = (sserver_id, sq.act_query_md5);

    RETURN properties;
END;
$$ LANGUAGE plpgsql;
