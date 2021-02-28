/* ========= Internal functions ========= */

CREATE FUNCTION get_connstr(IN sserver_id integer) RETURNS text SET search_path=@extschema@ SET lock_timeout=300000 AS $$
DECLARE
    server_connstr text = null;
BEGIN
    --Getting server_connstr
    SELECT connstr INTO server_connstr FROM servers n WHERE n.server_id = sserver_id;
    IF (server_connstr IS NULL) THEN
        RAISE 'server_id not found';
    ELSE
        RETURN server_connstr;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION nodata_wrapper(IN section_text text) RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    IF section_text IS NULL OR section_text = '' THEN
        RETURN '<p>No data in this section</p>';
    ELSE
        RETURN section_text;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION jsonb_replace(IN dict jsonb, IN templates jsonb) RETURNS jsonb AS $$
DECLARE
    res_jsonb           jsonb;
    jsontemplkey        text;
    jsondictkey         text;
BEGIN
    res_jsonb := templates;
    FOR jsontemplkey IN SELECT jsonb_object_keys(res_jsonb) LOOP
      FOR jsondictkey IN SELECT jsonb_object_keys(dict) LOOP
        res_jsonb := jsonb_set(res_jsonb, ARRAY[jsontemplkey],
          to_jsonb(replace(res_jsonb #>> ARRAY[jsontemplkey], '{'||jsondictkey||'}', dict #>> ARRAY[jsondictkey])));
      END LOOP;
    END LOOP;

    RETURN res_jsonb;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_sampleids_by_timerange(IN sserver_id integer, IN time_range tstzrange)
RETURNS TABLE (
    start_id    integer,
    end_id      integer
) SET search_path=@extschema@ AS $$
BEGIN
  SELECT min(s1.sample_id),max(s2.sample_id) INTO start_id,end_id FROM
    samples s1 JOIN
    /* Here redundant join condition s1.sample_id < s2.sample_id is needed
     * Otherwise optimizer is using tstzrange(s1.sample_time,s2.sample_time) && time_range
     * as first join condition and some times failes with error
     * ERROR:  range lower bound must be less than or equal to range upper bound
     */
    samples s2 ON (s1.sample_id < s2.sample_id AND s1.server_id = s2.server_id AND s1.sample_id + 1 = s2.sample_id)
  WHERE s1.server_id = sserver_id AND tstzrange(s1.sample_time,s2.sample_time) && time_range;

    IF start_id IS NULL OR end_id IS NULL THEN
      RAISE 'Suitable samples not found';
    END IF;

    RETURN NEXT;
    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_server_by_name(IN server name)
RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    sserver_id     integer;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name=server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found.';
    END IF;

    RETURN sserver_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_baseline_samples(IN sserver_id integer, baseline varchar(25))
RETURNS TABLE (
    start_id    integer,
    end_id      integer
) SET search_path=@extschema@ AS $$
BEGIN
    SELECT min(sample_id), max(sample_id) INTO start_id,end_id
    FROM baselines JOIN bl_samples USING (bl_id,server_id)
    WHERE server_id = sserver_id AND bl_name = baseline;
    IF start_id IS NULL OR end_id IS NULL THEN
      RAISE 'Baseline not found';
    END IF;
    RETURN NEXT;
    RETURN;
END;
$$ LANGUAGE plpgsql;
