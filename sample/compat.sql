/* ==== Backward compatibility functions ====*/
CREATE FUNCTION snapshot() RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
SELECT * FROM take_sample()
$$ LANGUAGE SQL;

CREATE FUNCTION snapshot(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
BEGIN
    RETURN take_sample(server);
END;
$$ LANGUAGE plpgsql;
