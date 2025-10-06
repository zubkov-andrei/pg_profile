CREATE FUNCTION take_subsample(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    sserver_id    integer;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name = server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found';
    ELSE
        PERFORM take_subsample(sserver_id);
        RETURN 0;
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_subsample(IN name) IS
  'Statistics sub-sample taking function (by server name)';
