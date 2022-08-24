/* pg_wait_sampling support */

CREATE FUNCTION collect_pg_wait_sampling_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
BEGIN
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_wait_sampling'
      )
      WHEN '1.1' THEN
        PERFORM collect_pg_wait_sampling_stats_11(properties, sserver_id, s_id, topn);
      ELSE
        NULL;
    END CASE;

END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION collect_pg_wait_sampling_stats_11(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  qres      record;

  st_query  text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    st_query := format('SELECT w.*,row_number() OVER () as weid '
      'FROM ( '
        'SELECT '
          'event_type,'
          'event,'
          'sum(count * current_setting(''pg_wait_sampling.profile_period'')::bigint) as tot_waited, '
          'sum(count * current_setting(''pg_wait_sampling.profile_period'')::bigint) '
            'FILTER (WHERE queryid IS NOT NULL AND queryid != 0) as stmt_waited '
        'FROM '
          '%1$I.pg_wait_sampling_profile '
        'GROUP BY '
          'event_type, '
          'event) as w',
      (
        SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extnamespace text)
        WHERE extname = 'pg_stat_statements'
      )
    );

    INSERT INTO wait_sampling_total(
      server_id,
      sample_id,
      sample_wevnt_id,
      event_type,
      event,
      tot_waited,
      stmt_waited
    )
    SELECT
      sserver_id,
      s_id,
      dbl.weid,
      dbl.event_type,
      dbl.event,
      dbl.tot_waited,
      dbl.stmt_waited
    FROM
      dblink('server_connection', st_query) AS dbl(
        event_type    text,
        event         text,
        tot_waited    bigint,
        stmt_waited   bigint,
        weid          integer
      );

    -- reset wait sampling profile
    SELECT * INTO qres FROM dblink('server_connection',
      format('SELECT %1$I.pg_wait_sampling_reset_profile()',
        (
          SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extnamespace text)
          WHERE extname = 'pg_wait_sampling'
        )
      )
    ) AS t(res char(1));

END;
$$ LANGUAGE plpgsql;
