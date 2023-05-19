/* ===== Differential report functions ===== */

CREATE FUNCTION get_diffreport(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report          text;
    report_data     jsonb;
    report_context  jsonb;
BEGIN
    -- Interval expanding in case of growth stats requested
    IF with_growth THEN
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start1_id, end1_id
        FROM get_sized_bounds(sserver_id, start1_id, end1_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start1_id, end1_id);
      END;
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start2_id, end2_id
        FROM get_sized_bounds(sserver_id, start2_id, end2_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start2_id, end2_id);
      END;
    END IF;

    -- Getting report context and check conditions
    report_context := get_report_context(sserver_id, start1_id, end1_id, description,
      start2_id, end2_id);

    -- Prepare report template
    report := get_report_template(report_context, 2);
    -- Populate template with report data
    report_data := sections_jsonb(report_context, sserver_id, 2);
    report_data := jsonb_set(report_data, '{datasets}',
        get_report_datasets(report_context, sserver_id));
    report := replace(report, '{dynamic:data1}', report_data::text);

    RETURN report;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_diffreport(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server_id and IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),start1_id,end1_id,
    start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',start1_id,end1_id,start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;

COMMENT ON FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN start2_id integer,IN end2_id integer, IN description text,
  IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN server name, IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline1) bl1
    CROSS JOIN get_baseline_samples(get_server_by_name(server), baseline2) bl2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline1 varchar(25),
  IN baseline2 varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and two baselines to compare.';

CREATE FUNCTION get_diffreport(IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',baseline1,baseline2,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes two baselines to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    start2_id,end2_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl1
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text,
  IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, reference baseline name as first interval, start and end sample_ids of second interval.';

CREATE FUNCTION get_diffreport(IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',baseline,
    start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes reference baseline name as first interval, start and end sample_ids of second interval.';

CREATE FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),start1_id,end1_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, start and end sample_ids of first interval and reference baseline name as second interval.';

CREATE FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',start1_id,end1_id,
    baseline,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline varchar(25), IN start2_id integer,
  IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes start and end sample_ids of first interval and reference baseline name as second interval.';

CREATE FUNCTION get_diffreport(IN server name, IN time_range1 tstzrange,
  IN time_range2 tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),tm1.start_id,tm1.end_id,
    tm2.start_id,tm2.end_id,description,with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range1) tm1
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range2) tm2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN time_range1 tstzrange,
  IN time_range2 tstzrange, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and two time intervals to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN time_range tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    tm2.start_id,tm2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl1
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range) tm2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN time_range tstzrange, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, baseline and time interval to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN time_range tstzrange,
  IN baseline varchar(25), IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),tm1.start_id,tm1.end_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl2
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range) tm1
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN time_range tstzrange,
  IN baseline varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, time interval and baseline to compare.';
