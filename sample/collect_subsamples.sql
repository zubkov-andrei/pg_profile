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
      xact_start,
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

    /*
    Hash function md5() is not working when the FIPS mode is
    enabled. This can cause sampling falure in PG14+. SHA functions
    however are unavailable before PostgreSQL 11. We'll use md5()
    before PG11, and sha224 after PG11
    */
    IF current_setting('server_version_num')::integer < 110000 THEN
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
          xact_start,
          query_start,
          query_id,
          act_query_md5,
          stmt_last_ts
        ) VALUES (
          sserver_id,
          s_id,
          qres.pid,
          qres.leader_pid,
          qres.xact_start,
          qres.query_start,
          qres.query_id,
          md5(qres.query),
          qres.subsample_ts
        );
      END LOOP;
    ELSE
      FOR qres IN c_statements
      LOOP
        INSERT INTO act_query (server_id, act_query_md5, act_query, last_sample_id)
        VALUES (
          sserver_id,
          left(encode(sha224(convert_to(qres.query,'UTF8')), 'base64'), 32),
          qres.query,
          NULL
        )
        ON CONFLICT ON CONSTRAINT pk_act_query
        DO UPDATE SET last_sample_id = NULL;

        INSERT INTO sample_act_statement(
          server_id,
          sample_id,
          pid,
          leader_pid,
          xact_start,
          query_start,
          query_id,
          act_query_md5,
          stmt_last_ts
        ) VALUES (
          sserver_id,
          s_id,
          qres.pid,
          qres.leader_pid,
          qres.xact_start,
          qres.query_start,
          qres.query_id,
          left(encode(sha224(convert_to(qres.query,'UTF8')), 'base64'), 32),
          qres.subsample_ts
        );
      END LOOP;
    END IF;

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
      backend_xmin_age,
      query_start
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
      backend_xmin_age,
      query_start
    FROM
      last_stat_activity
      JOIN last_backend_state
        USING (server_id, sample_id, pid, state_change, subsample_ts)
    WHERE (server_id, sample_id) = (sserver_id, s_id - 1)
    ;

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
