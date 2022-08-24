/* ========= Internal functions ========= */

CREATE FUNCTION get_connstr(IN sserver_id integer, INOUT properties jsonb)
SET search_path=@extschema@ SET lock_timeout=300000 AS $$
DECLARE
    server_connstr    text = NULL;
    server_host       text = NULL;
BEGIN
    ASSERT properties IS NOT NULL, 'properties must be not null';
    --Getting server_connstr
    SELECT connstr INTO server_connstr FROM servers n WHERE n.server_id = sserver_id;
    ASSERT server_connstr IS NOT NULL, 'server_id not found';
    /*
    When host= parameter is not specified, connection to unix socket is assumed.
    Unix socket can be in non-default location, so we need to specify it
    */
    IF (SELECT count(*) = 0 FROM regexp_matches(server_connstr,$o$((\s|^)host\s*=)$o$)) AND
      (SELECT count(*) != 0 FROM pg_catalog.pg_settings
      WHERE name = 'unix_socket_directories' AND boot_val != reset_val)
    THEN
      -- Get suitable socket name from available list
      server_host := (SELECT COALESCE(t[1],t[4])
        FROM pg_catalog.pg_settings,
          regexp_matches(reset_val,'("(("")|[^"])+")|([^,]+)','g') AS t
        WHERE name = 'unix_socket_directories' AND boot_val != reset_val
          -- libpq can't handle sockets with comma in their names
          AND position(',' IN COALESCE(t[1],t[4])) = 0
        LIMIT 1
      );
      -- quoted string processing
      IF left(server_host, 1) = '"' AND
        right(server_host, 1) = '"' AND
        (length(server_host) > 1)
      THEN
        server_host := replace(substring(server_host,2,length(server_host)-2),'""','"');
      END IF;
      -- append host parameter to the connection string
      IF server_host IS NOT NULL AND server_host != '' THEN
        server_connstr := concat_ws(server_connstr, format('host=%L',server_host), ' ');
      ELSE
        server_connstr := concat_ws(server_connstr, format('host=%L','localhost'), ' ');
      END IF;
    END IF;

    properties := jsonb_set(properties, '{properties, server_connstr}',
      to_jsonb(server_connstr));
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION jsonb_replace(IN settings jsonb, IN templates jsonb) RETURNS jsonb AS $$
DECLARE
    res_jsonb           jsonb;
    jsontemplkey        text;
    jsondictkey         text;

    c_replace_tab CURSOR(json_scope text) FOR
    SELECT
      '{'||template.key||'}' AS placeholder,
      CASE
        WHEN features.value::boolean THEN template.value
        ELSE COALESCE(template_not.value,'')
      END AS substitution
    FROM jsonb_each_text(settings #> ARRAY[json_scope]) AS features
      JOIN jsonb_each_text(templates) AS template ON (strpos(template.key,features.key||'?') = 1)
      LEFT JOIN jsonb_each_text(templates) AS template_not ON (template_not.key = '!'||template.key)
    ;

    r_replace RECORD;
    jscope    text;
BEGIN
    res_jsonb := templates;
    /* Conditional template placeholders processing
    * based on available report features
    */
    FOREACH jscope IN ARRAY ARRAY['report_properties', 'report_features'] LOOP
      FOR r_replace IN c_replace_tab(jscope) LOOP
        FOR jsontemplkey IN SELECT jsonb_object_keys(res_jsonb) LOOP
          res_jsonb := jsonb_set(res_jsonb, ARRAY[jsontemplkey],
            to_jsonb(replace(res_jsonb #>> ARRAY[jsontemplkey],
            r_replace.placeholder, r_replace.substitution)));
        END LOOP; -- over table templates
      END LOOP; -- over feature-based replacements
    END LOOP; -- over json settings scope
    -- Replacing common html/css placeholders
    FOR jsontemplkey IN SELECT jsonb_object_keys(res_jsonb) LOOP
      FOR jsondictkey IN SELECT jsonb_object_keys(settings #> ARRAY['htbl']) LOOP
        res_jsonb := jsonb_set(res_jsonb, ARRAY[jsontemplkey],
          to_jsonb(replace(res_jsonb #>> ARRAY[jsontemplkey], '{'||jsondictkey||'}',
            settings #> ARRAY['htbl'] #>> ARRAY[jsondictkey])));
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
