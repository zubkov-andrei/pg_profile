/* ========= Baseline management functions ========= */

CREATE FUNCTION create_baseline(IN server name, IN baseline varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    baseline_id integer;
    sserver_id     integer;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name=server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found';
    END IF;

    INSERT INTO baselines(server_id,bl_name,keep_until)
    VALUES (sserver_id,baseline,now() + (days || ' days')::interval)
    RETURNING bl_id INTO baseline_id;

    INSERT INTO bl_samples (server_id,sample_id,bl_id)
    SELECT server_id,sample_id,baseline_id
    FROM samples s JOIN servers n USING (server_id)
    WHERE server_id=sserver_id AND sample_id BETWEEN start_id AND end_id;

    RETURN baseline_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_baseline(IN server name, IN baseline varchar(25), IN start_id integer, IN end_id integer, IN days integer) IS 'New baseline by ID''s';

CREATE FUNCTION create_baseline(IN baseline varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    RETURN create_baseline('local',baseline,start_id,end_id,days);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_baseline(IN baseline varchar(25), IN start_id integer, IN end_id integer, IN days integer) IS 'Local server new baseline by ID''s';

CREATE FUNCTION create_baseline(IN server name, IN baseline varchar(25), IN time_range tstzrange, IN days integer = NULL) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
  range_ids record;
BEGIN
  SELECT * INTO STRICT range_ids
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range);

  RETURN create_baseline(server,baseline,range_ids.start_id,range_ids.end_id,days);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_baseline(IN server name, IN baseline varchar(25), IN time_range tstzrange, IN days integer) IS 'New baseline by time range';

CREATE FUNCTION create_baseline(IN baseline varchar(25), IN time_range tstzrange, IN days integer = NULL) RETURNS integer
  SET search_path=@extschema@ AS $$
BEGIN
  RETURN create_baseline('local',baseline,time_range,days);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION create_baseline(IN baseline varchar(25), IN time_range tstzrange, IN days integer) IS 'Local server new baseline by time range';

CREATE FUNCTION drop_baseline(IN server name, IN baseline varchar(25)) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    del_rows integer;
BEGIN
    DELETE FROM baselines WHERE bl_name = baseline AND server_id IN (SELECT server_id FROM servers WHERE server_name = server);
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION drop_baseline(IN server name, IN baseline varchar(25)) IS 'Drop baseline on server';

CREATE FUNCTION drop_baseline(IN baseline varchar(25)) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    RETURN drop_baseline('local',baseline);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION drop_baseline(IN baseline varchar(25)) IS 'Drop baseline on local server';

CREATE FUNCTION keep_baseline(IN server name, IN baseline varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE baselines SET keep_until = now() + (days || ' days')::interval WHERE (baseline IS NULL OR bl_name = baseline) AND server_id IN (SELECT server_id FROM servers WHERE server_name = server);
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION  keep_baseline(IN server name, IN baseline varchar(25), IN days integer) IS 'Set new baseline retention on server';

CREATE FUNCTION keep_baseline(IN baseline varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    RETURN keep_baseline('local',baseline,days);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION keep_baseline(IN baseline varchar(25), IN days integer) IS 'Set new baseline retention on local server';

CREATE FUNCTION show_baselines(IN server name = 'local')
RETURNS TABLE (
       baseline varchar(25),
       min_sample integer,
       max_sample integer,
       keep_until_time timestamp (0) with time zone
) SET search_path=@extschema@ AS $$
    SELECT bl_name as baseline,min_sample_id,max_sample_id, keep_until
    FROM baselines b JOIN
        (SELECT server_id,bl_id,min(sample_id) min_sample_id,max(sample_id) max_sample_id FROM bl_samples GROUP BY server_id,bl_id) b_agg
    USING (server_id,bl_id)
    WHERE server_id IN (SELECT server_id FROM servers WHERE server_name = server)
    ORDER BY min_sample_id;
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_baselines(IN server name) IS 'Show server baselines (local server assumed if omitted)';
