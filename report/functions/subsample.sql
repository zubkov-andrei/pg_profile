CREATE FUNCTION stat_activity_states(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer = NULL, IN end2_id integer = NULL)
RETURNS TABLE(
    server_id         integer,
    datid             oid,
    datname           name,
    leader_pid        integer,
    pid               integer,
    usesysid          oid,
    usename           name,
    application_name  text,
    client_addr       inet,
    client_hostname   text,
    client_port       integer,
    backend_start     timestamp with time zone,
    backend_last_ts   timestamp with time zone,
    xact_start        timestamp with time zone,
    xact_last_ts      timestamp with time zone,
    state_code        integer,
    state             text,
    state_change      timestamp with time zone,
    state_last_ts     timestamp with time zone,
    backend_xid       text,
    backend_xmin      text,
    backend_xmin_age  bigint,
    backend_type      text,
    query_start       timestamp with time zone,
    stmt_last_ts      timestamp with time zone,
    query_id          bigint,
    act_query_md5     char(32),
    backend_duration  interval,
    xact_duration     interval,
    state_duration    interval,
    query_duration    interval
) SET search_path=@extschema@ AS $$
  SELECT
    server_id,
    datid,
    datname,
    leader_pid,
    pid,
    usesysid,
    usename,
    application_name,
    client_addr,
    client_hostname,
    client_port,
    backend_start,
    backend_last_ts,
    xact_start,
    xact_last_ts,
    state_code,
    CASE state_code
      WHEN 1 THEN 'idle in transaction'
      WHEN 2 THEN 'idle in transaction (aborted)'
      WHEN 3 THEN 'active'
      ELSE NULL
    END state,
    state_change,
    state_last_ts,
    backend_xid,
    backend_xmin,
    backend_xmin_age,
    backend_type,
    query_start,
    stmt_last_ts,
    query_id,
    act_query_md5,
    backend_last_ts - backend_start as backend_duration,
    xact_last_ts - xact_start as xact_duration,
    state_last_ts - state_change as state_duration,
    CASE
      WHEN state_code = 3 THEN stmt_last_ts - query_start
      ELSE state_change - query_start
    END as query_duration
  FROM
    (sample_act_backend b
    JOIN (
        SELECT server_id, pid, backend_start, max(backend_last_ts) as backend_last_ts
        FROM sample_act_backend
        WHERE server_id = sserver_id
          AND (
            sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id
          )
        GROUP BY server_id, pid, backend_start
      ) as act_backend USING (server_id, pid, backend_start, backend_last_ts))
    JOIN
    (sample_act_backend_state s JOIN (
        SELECT server_id, pid, state_change, max(state_last_ts) as state_last_ts
        FROM sample_act_backend_state
        WHERE server_id = sserver_id
          AND (
            sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id
          )
        GROUP BY server_id, pid, state_change
      ) as backend_state
      USING (server_id, pid, state_change, state_last_ts))
    USING (server_id, pid, backend_start)
    LEFT JOIN
    (sample_act_xact x JOIN (
        SELECT server_id, pid, xact_start, max(xact_last_ts) as xact_last_ts
        FROM sample_act_xact
        WHERE server_id = sserver_id
          AND (
            sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id
          )
        GROUP BY server_id, pid, xact_start
      ) as act_xact USING (server_id, pid, xact_start, xact_last_ts))
    USING (server_id, pid, backend_start, xact_start)
    LEFT JOIN
    (sample_act_statement q JOIN (
        SELECT server_id, pid, query_start, max(stmt_last_ts) as stmt_last_ts
        FROM sample_act_statement
        WHERE server_id = sserver_id
          AND (
            sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id
          )
        GROUP BY server_id, pid, query_start
      ) as act_stmt
      USING (server_id, pid, query_start, stmt_last_ts))
    USING (server_id, pid, xact_start, query_start)
  WHERE
    server_id = sserver_id
$$ LANGUAGE sql;

CREATE FUNCTION stat_activity_agg(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id         integer,
    datid             oid,
    datname           name,
    idle_xact_dur     interval,
    idle_xact_a_dur   interval,
    active_dur        interval,
    max_idle_xact_d   interval,
    max_idle_xact_a_d interval,
    max_active_dur    interval,
    max_xact_age      bigint
) SET search_path=@extschema@ AS $$
  SELECT
    server_id,
    datid,
    datname,
    sum(state_last_ts - state_change) FILTER (WHERE state = 'idle in transaction') as idle_xact_dur,
    sum(state_last_ts - state_change) FILTER (WHERE state = 'idle in transaction (aborted)') as idle_xact_a_dur,
    sum(state_last_ts - state_change) FILTER (WHERE state = 'active') as active_dur,
    max(state_last_ts - state_change) FILTER (WHERE state = 'idle in transaction') as max_idle_xact_d,
    max(state_last_ts - state_change) FILTER (WHERE state = 'idle in transaction (aborted)') as max_idle_xact_a_d,
    max(state_last_ts - state_change) FILTER (WHERE state = 'active') as max_active_dur,
    max(backend_xmin_age) as max_xact_age
  FROM
    stat_activity_states(sserver_id, start_id, end_id)
  GROUP BY server_id, datid, datname
$$ LANGUAGE sql;

CREATE FUNCTION stat_activity_agg_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid             oid,
    dbname            name,
    idle_xact_dur     text,
    idle_xact_a_dur   text,
    active_dur        text,
    max_idle_xact_d   text,
    max_idle_xact_a_d text,
    max_active_dur    text,
    max_xact_age      bigint,
    ord_db            integer
) SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        COALESCE(st.datname, 'Total') as dbname,
        NULLIF(date_trunc('second', sum(st.idle_xact_dur) + '0.5 second'),
          '0 second'::interval)::text as idle_xact_dur,
        NULLIF(date_trunc('second', sum(st.idle_xact_a_dur) + '0.5 second'),
          '0 second'::interval)::text as idle_xact_a_dur,
        NULLIF(date_trunc('second', sum(st.active_dur) + '0.5 second'),
          '0 second'::interval)::text as active_dur,
        NULLIF(date_trunc('second', max(st.max_idle_xact_d) + '0.5 second'),
          '0 second'::interval)::text as max_idle_xact_d,
        NULLIF(date_trunc('second', max(st.max_idle_xact_a_d) + '0.5 second'),
          '0 second'::interval)::text as max_idle_xact_a_d,
        NULLIF(date_trunc('second', max(st.max_active_dur) + '0.5 second'),
          '0 second'::interval)::text as max_active_dur,
        NULLIF(max(st.max_xact_age), 0) as max_xact_age,
        -- ordering fields
        row_number() OVER (ORDER BY st.datname NULLS LAST)::integer AS ord_db
    FROM
        stat_activity_agg(sserver_id, start_id, end_id) st
    GROUP BY GROUPING SETS ((st.datid, st.datname), ())
$$ LANGUAGE sql;

CREATE FUNCTION stat_activity_agg_format(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid               oid,
    dbname              name,
    idle_xact_dur1      text,
    idle_xact_a_dur1    text,
    active_dur1         text,
    max_idle_xact_d1    text,
    max_idle_xact_a_d1  text,
    max_active_dur1     text,
    max_xact_age1       bigint,
    idle_xact_dur2      text,
    idle_xact_a_dur2    text,
    active_dur2         text,
    max_idle_xact_d2    text,
    max_idle_xact_a_d2  text,
    max_active_dur2     text,
    max_xact_age2       bigint,
    ord_db              integer
) SET search_path=@extschema@ AS $$
    SELECT
        COALESCE(st1.datid,st2.datid) AS datid,
        COALESCE(COALESCE(st1.datname,st2.datname),'Total') AS dbname,
        NULLIF(date_trunc('second', sum(st1.idle_xact_dur) + '0.5 second'),
          '0 second'::interval)::text as idle_xact_dur1,
        NULLIF(date_trunc('second', sum(st1.idle_xact_a_dur) + '0.5 second'),
          '0 second'::interval)::text as idle_xact_a_dur1,
        NULLIF(date_trunc('second', sum(st1.active_dur) + '0.5 second'),
          '0 second'::interval)::text as active_dur1,
        NULLIF(date_trunc('second', max(st1.max_idle_xact_d) + '0.5 second'),
          '0 second'::interval)::text as max_idle_xact_d1,
        NULLIF(date_trunc('second', max(st1.max_idle_xact_a_d) + '0.5 second'),
          '0 second'::interval)::text as max_idle_xact_a_d1,
        NULLIF(date_trunc('second', max(st1.active_dur) + '0.5 second'),
          '0 second'::interval)::text as max_active_dur1,
        NULLIF(max(st1.max_xact_age), 0) as max_xact_age1,
        NULLIF(date_trunc('second', sum(st2.idle_xact_dur) + '0.5 second'),
          '0 second'::interval)::text as idle_xact_dur2,
        NULLIF(date_trunc('second', sum(st2.idle_xact_a_dur) + '0.5 second'),
          '0 second'::interval)::text as idle_xact_a_dur2,
        NULLIF(date_trunc('second', sum(st2.active_dur) + '0.5 second'),
          '0 second'::interval)::text as active_dur2,
        NULLIF(date_trunc('second', max(st2.max_idle_xact_d) + '0.5 second'),
          '0 second'::interval)::text as max_idle_xact_d2,
        NULLIF(date_trunc('second', max(st2.max_idle_xact_a_d) + '0.5 second'),
          '0 second'::interval)::text as max_idle_xact_a_d2,
        NULLIF(date_trunc('second', max(st2.active_dur) + '0.5 second'),
          '0 second'::interval)::text as max_active_dur2,
        NULLIF(max(st2.max_xact_age), 0) as max_xact_age2,
        -- ordering fields
        row_number() OVER (ORDER BY COALESCE(st1.datname,st2.datname) NULLS LAST)::integer AS ord_db
    FROM stat_activity_agg(sserver_id, start1_id, end1_id) st1
      FULL OUTER JOIN stat_activity_agg(sserver_id, start2_id, end2_id) st2
        USING (server_id, datid)
    GROUP BY GROUPING SETS ((COALESCE(st1.datid,st2.datid), COALESCE(st1.datname,st2.datname)),
      ())
$$ LANGUAGE sql;

CREATE FUNCTION stat_activity_states_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid             oid,
    dbname            name,
    pid               integer,
    userid            oid,
    username          name,
    client_addr       inet,
    client_hostname   text,
    client_port       integer,
    application_name  text,

    backend_start     timestamp with time zone,
    xact_start        timestamp with time zone,
    state_change      timestamp with time zone,

    backend_start_ut     numeric,
    xact_start_ut        numeric,
    state_change_ut      numeric,
    state_duration_ut    numeric,
    xact_duration_ut     numeric,
    backend_duration_ut  numeric,

    backend_start_format  timestamp with time zone,
    xact_start_format     timestamp with time zone,
    state_change_format   timestamp with time zone,

    flt_state_code          integer,
    state_code              integer,
    state                   text,
    state_duration_format   interval,
    xact_duration_format    interval,
    backend_duration_format interval,
    xmin_age                bigint,

    query_id              bigint,
    act_query_md5         char(32),

    ord_dur               integer,
    ord_age               integer,
    ord_xact              integer,
    flt_age               boolean
) SET search_path=@extschema@ AS $$
    SELECT
      datid,
      datname,
      pid,
      usesysid,
      usename,
      client_addr,
      client_hostname,
      client_port,
      application_name,

      backend_start AS backend_start,
      xact_start AS xact_start,
      state_change AS state_change,

      extract(EPOCH FROM backend_start)::numeric AS backend_start_ut,
      extract(EPOCH FROM xact_start)::numeric AS xact_start_ut,
      extract(EPOCH FROM state_change)::numeric AS state_change_ut,
      extract(EPOCH FROM state_duration)::numeric AS state_duration_ut,
      extract(EPOCH FROM xact_duration)::numeric AS xact_duration_ut,
      extract(EPOCH FROM backend_duration)::numeric AS backend_duration_ut,

      date_trunc('second',backend_start) AS backend_start_format,
      date_trunc('second',xact_start) AS xact_start_format,
      date_trunc('second',state_change) AS state_change_format,

      CASE
        WHEN state_code IN (1, 2, 3) AND act_query_md5 IS NOT NULL THEN state_code
        WHEN state_code NOT IN (1, 2, 3) THEN state_code
        ELSE NULL
      END AS flt_state_code,
      state_code,
      state,
      date_trunc('second',state_duration) AS state_duration_format,
      date_trunc('second',xact_duration) AS xact_duration_format,
      date_trunc('second',backend_duration) AS backend_duration_format,
      backend_xmin_age,

      query_id,
      act_query_md5,

      row_number() OVER (PARTITION BY state_code ORDER BY state_duration DESC)::integer AS ord_dur,
      row_number() OVER (ORDER BY backend_xmin_age DESC)::integer AS ord_age,
      row_number() OVER (ORDER BY xact_duration DESC)::integer AS ord_xact,
      coalesce(xact_last_ts = state_last_ts, false) AND backend_xmin_age > 0 as flt_age
    FROM
      stat_activity_states(sserver_id, start_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION stat_activity_states_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid             oid,
    dbname            name,
    pid               integer,
    userid            oid,
    username          name,
    client_addr       inet,
    client_hostname   text,
    client_port       integer,
    application_name  text,

    backend_start     timestamp with time zone,
    backend_last_ts   timestamp with time zone,
    xact_start        timestamp with time zone,
    state_change      timestamp with time zone,

    backend_start_ut    numeric,
    xact_start_ut       numeric,
    state_change_ut     numeric,
    state_duration_ut   numeric,
    xact_duration_ut    numeric,
    backend_duration_ut numeric,

    backend_start_format  timestamp with time zone,
    xact_start_format     timestamp with time zone,
    state_change_format   timestamp with time zone,

    flt_state_code        integer,
    state_code            integer,
    state                 text,
    state_duration_format interval,
    xact_duration_format  interval,
    xmin_age              bigint,

    query_id              bigint,
    act_query_md5         char(32),

    ord_dur               integer,
    ord_age               integer,
    ord_xact              integer,
    flt_age               boolean
) SET search_path=@extschema@ AS $$
    SELECT
      datid,
      datname,
      pid,
      usesysid,
      usename,
      client_addr,
      client_hostname,
      client_port,
      application_name,

      backend_start AS backend_start,
      backend_last_ts AS backend_last_ts,
      xact_start AS xact_start,
      state_change AS state_change,

      extract(EPOCH FROM backend_start)::numeric AS backend_start_ut,
      extract(EPOCH FROM xact_start)::numeric AS xact_start_ut,
      extract(EPOCH FROM state_change)::numeric AS state_change_ut,
      extract(EPOCH FROM
        max(state_duration) OVER (PARTITION BY pid, state_change)
      )::numeric AS state_duration_ut,
      extract(EPOCH FROM
        max(xact_duration) OVER (PARTITION BY pid, xact_start)
      )::numeric AS xact_duration_ut,
      extract(EPOCH FROM
        max(backend_duration) OVER (PARTITION BY pid, backend_start)
      )::numeric AS backend_duration_ut,

      date_trunc('second',backend_start) AS backend_start_format,
      date_trunc('second',xact_start) AS xact_start_format,
      date_trunc('second',state_change) AS state_change_format,

      CASE
        WHEN state_code IN (1, 2, 3) AND act_query_md5 IS NOT NULL THEN state_code
        WHEN state_code NOT IN (1, 2, 3) THEN state_code
        ELSE NULL
      END AS flt_state_code,
      state_code,
      state,
      date_trunc('second',
        max(state_duration) OVER (PARTITION BY pid, state_change)
      ) AS state_duration_format,
      date_trunc('second',
        max(xact_duration) OVER (PARTITION BY pid, xact_start)
      ) AS xact_duration_format,
      backend_xmin_age,

      query_id,
      act_query_md5,

      row_number() OVER (PARTITION BY state_code ORDER BY state_duration DESC)::integer AS ord_dur,
      row_number() OVER (ORDER BY backend_xmin_age DESC)::integer AS ord_age,
      row_number() OVER (ORDER BY xact_duration DESC)::integer AS ord_xact,
      coalesce(xact_last_ts = state_last_ts, false) AND backend_xmin_age > 0 as flt_age
    FROM
      stat_activity_states(sserver_id, start1_id, end1_id, start2_id, end2_id) st
$$ LANGUAGE sql;

CREATE FUNCTION report_active_queries_format(IN report_context jsonb, IN sserver_id integer,
  IN act_queries_list jsonb)
RETURNS TABLE (
  act_query_md5   text,
  query_texts     jsonb
)
SET search_path=@extschema@ AS $$
  SELECT
    act_query_md5,
    jsonb_build_array(
      replace(act_query, '<', '&lt;')
    )
  FROM
    jsonb_to_recordset(act_queries_list) AS ql (
      act_query_md5    text
    )
    JOIN act_query USING (act_query_md5)
  WHERE
    server_id = sserver_id
$$ LANGUAGE sql;
