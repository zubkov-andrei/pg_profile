/* ========= Internal functions ========= */

CREATE OR REPLACE FUNCTION get_connstr(IN snode_id integer) RETURNS text SET search_path=@extschema@,public SET lock_timeout=300000 AS $$
DECLARE
    node_connstr text = null;
BEGIN
    --Getting node_connstr
    SELECT connstr INTO node_connstr FROM nodes n WHERE n.node_id = snode_id;
    IF (node_connstr IS NULL) THEN
        RAISE 'node_id not found';
    ELSE
        RETURN node_connstr;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nodata_wrapper(IN section_text text) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    IF section_text IS NULL OR section_text = '' THEN
        RETURN '<p>No data in this section</p>';
    ELSE
        RETURN section_text;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION jsonb_replace(IN dict jsonb, IN templates jsonb) RETURNS jsonb AS $$
DECLARE
    res_jsonb           jsonb;
    jsontemplkey        varchar(20);
    jsondictkey         varchar(20);
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

CREATE OR REPLACE FUNCTION get_snapids_by_timerange(IN snode_id integer, IN time_range tstzrange)
RETURNS TABLE (
    start_id    integer,
    end_id      integer
) SET search_path=@extschema@,public AS $$
BEGIN
  SELECT min(s1.snap_id),max(s2.snap_id) INTO start_id,end_id FROM
    snapshots s1 JOIN
    snapshots s2 ON (s1.node_id = s2.node_id AND s1.snap_id + 1 = s2.snap_id)
  WHERE s1.node_id = snode_id AND tstzrange(s1.snap_time,s2.snap_time) && time_range;
  
    IF start_id IS NULL OR end_id IS NULL THEN
      RAISE 'Suitable snapshots not found';
    END IF;

    RETURN NEXT;
    RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_node_by_name(IN node name)
RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    snode_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found.';
    END IF;

    RETURN snode_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_baseline_snapshots(IN snode_id integer, baseline varchar(25))
RETURNS TABLE (
    start_id    integer,
    end_id      integer
) SET search_path=@extschema@,public AS $$
BEGIN
    SELECT min(snap_id), max(snap_id) INTO start_id,end_id
    FROM baselines JOIN bl_snaps USING (bl_id,node_id)
    WHERE node_id = snode_id AND bl_name = baseline;
    IF start_id IS NULL OR end_id IS NULL THEN
      RAISE 'Baseline not found';
    END IF;
    RETURN NEXT;
    RETURN;
END;
$$ LANGUAGE plpgsql;
