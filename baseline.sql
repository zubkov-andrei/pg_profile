/* ========= Baseline management functions ========= */

CREATE OR REPLACE FUNCTION baseline_new(IN node name, IN name varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    baseline_id integer;
    snode_id     integer;
BEGIN
    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NULL THEN
        RAISE 'Node not found';
    END IF;

    INSERT INTO baselines(node_id,bl_name,keep_until)
    VALUES (snode_id,name,now() + (days || ' days')::interval)
    RETURNING bl_id INTO baseline_id;

    INSERT INTO bl_snaps (node_id,snap_id,bl_id)
    SELECT node_id,snap_id,baseline_id
    FROM snapshots s JOIN nodes n USING (node_id)
    WHERE node_id=snode_id AND snap_id BETWEEN start_id AND end_id;

    RETURN baseline_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_new(IN name varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    RETURN baseline_new('local',name,start_id,end_id,days);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION baseline_drop(IN node name, IN name varchar(25)) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    del_rows integer;
BEGIN
    DELETE FROM baselines WHERE bl_name = name AND node_id IN (SELECT node_id FROM nodes WHERE node_name = node);
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_drop(IN name varchar(25)) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    RETURN baseline_drop('local',name);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_keep(IN node name, IN name varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE baselines SET keep_until = now() + (days || ' days')::interval WHERE (name IS NULL OR bl_name = name) AND node_id IN (SELECT node_id FROM nodes WHERE node_name = node);
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_keep(IN name varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    RETURN baseline_keep('local',name,days);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_show(IN node name = 'local')
RETURNS TABLE (
       baseline varchar(25),
       min_snap integer,
       max_snap integer,
       keep_until_time timestamp (0) with time zone
) SET search_path=@extschema@,public AS $$
    SELECT bl_name as baseline,min_snap_id,max_snap_id, keep_until
    FROM baselines b JOIN
        (SELECT node_id,bl_id,min(snap_id) min_snap_id,max(snap_id) max_snap_id FROM bl_snaps GROUP BY node_id,bl_id) b_agg
    USING (node_id,bl_id)
    WHERE node_id IN (SELECT node_id FROM nodes WHERE node_name = node)
    ORDER BY min_snap_id;
$$ LANGUAGE sql;
