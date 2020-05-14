/* ========= Node functions ========= */

CREATE OR REPLACE FUNCTION node_new(IN node name, IN node_connstr text, IN node_enabled boolean = TRUE,
IN data_retention integer = NULL) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    snode_id     integer;
BEGIN

    SELECT node_id INTO snode_id FROM nodes WHERE node_name=node;
    IF snode_id IS NOT NULL THEN
        RAISE 'Node already exists.';
    END IF;

    INSERT INTO nodes(node_name,connstr,enabled,retention)
    VALUES (node,node_connstr,node_enabled,data_retention)
    RETURNING node_id INTO snode_id;

    RETURN snode_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_new(IN node name, IN node_connstr text, IN node_enabled boolean,
IN data_retention integer) IS 'Create a new node';

CREATE OR REPLACE FUNCTION node_drop(IN node name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    del_rows integer;
BEGIN
    DELETE FROM nodes WHERE node_name = node;
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_drop(IN node name) IS 'Drop a node';

CREATE OR REPLACE FUNCTION node_rename(IN node name, IN node_new_name name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET node_name = node_new_name WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_rename(IN node name, IN node_new_name name) IS 'Rename existing node';

CREATE OR REPLACE FUNCTION node_connstr(IN node name, IN node_connstr text) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET connstr = node_connstr WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_connstr(IN node name, IN node_connstr text) IS 'Update node connection string';

CREATE OR REPLACE FUNCTION node_retention(IN node name, IN data_retention integer) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET retention = data_retention WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_retention(IN node name, IN data_retention integer) IS 'Update node retention period';

CREATE OR REPLACE FUNCTION node_enable(IN node name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET enabled = TRUE WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_enable(IN node name) IS 'Enable existing node (will be included in snapshot() call)';

CREATE OR REPLACE FUNCTION node_disable(IN node name) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET enabled = FALSE WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_disable(IN node name) IS 'Disable existing node (will be excluded from snapshot() call)';

CREATE OR REPLACE FUNCTION node_set_db_exclude(IN node name, IN exclude_db name[]) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE nodes SET db_exclude = exclude_db WHERE node_name = node;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION node_set_db_exclude(IN node name, IN exclude_db name[]) IS 'Excude databases from object stats collection. Useful in RDS.';

CREATE OR REPLACE FUNCTION node_show() RETURNS TABLE(node_name name, connstr text, enabled boolean) SET search_path=@extschema@,public AS $$
    SELECT node_name,connstr,enabled FROM nodes;
$$ LANGUAGE sql;

COMMENT ON FUNCTION node_show() IS 'Displays all nodes';
