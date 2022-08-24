/* ===== Main report function ===== */

CREATE FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report          text;
    report_context  jsonb;
BEGIN
    -- Interval expanding in case of growth stats requested
    IF with_growth THEN
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start_id, end_id
        FROM get_sized_bounds(sserver_id, start_id, end_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start_id, end_id);
      END;
    END IF;

    -- Getting report context and check conditions
    report_context := get_report_context(sserver_id, start_id, end_id, description);

    -- Create internal temporary tables for report
    PERFORM init_report_temp_tables(report_context, sserver_id);

    -- Prepare report template
    report := get_report_template(report_context, 1);
    -- Populate template with report tables
    report := template_populate_sections(report_context, sserver_id, report, 1);
    /*
    * Cleanup cache temporary tables
    * This is needed to avoid conflict with existing table if several
    * reports are collected in one session
    */
    PERFORM cleanup_report_temp_tables(report_context, sserver_id);

    RETURN report;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server_id and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN server name, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id,
    description, with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server name and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report('local',start_id,end_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function for local server. Takes IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN sserver_id integer, IN time_range tstzrange,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(sserver_id, start_id, end_id, description, with_growth)
  FROM get_sampleids_by_timerange(sserver_id, time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN sserver_id integer, IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server ID and time interval.';

CREATE FUNCTION get_report(IN server name, IN time_range tstzrange,
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id, description,with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server name and time interval.';

CREATE FUNCTION get_report(IN time_range tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(get_server_by_name('local'), start_id, end_id, description, with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name('local'), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN time_range tstzrange,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function for local server. Takes time interval.';

CREATE FUNCTION get_report(IN server name, IN baseline varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id, description, with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN baseline varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function for server baseline. Takes server name and baseline name.';

CREATE FUNCTION get_report(IN baseline varchar(25), IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN get_report('local',baseline,description,with_growth);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION get_report(IN baseline varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function for local server baseline. Takes baseline name.';

CREATE FUNCTION get_report_latest(IN server name = NULL)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_report(srv.server_id, s.sample_id, e.sample_id, NULL)
  FROM samples s JOIN samples e ON (s.server_id = e.server_id AND s.sample_id = e.sample_id - 1)
    JOIN servers srv ON (e.server_id = srv.server_id AND e.sample_id = srv.last_sample_id)
  WHERE srv.server_name = COALESCE(server, 'local')
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report_latest(IN server name) IS 'Statistics report generation function for last two samples';
