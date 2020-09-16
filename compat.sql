/* ==== Backward compatibility functions ====*/
CREATE OR REPLACE FUNCTION snapshot() RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@,public AS $$
SELECT * FROM take_sample()
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION snapshot(IN server name) RETURNS integer SET search_path=@extschema@,public AS $$
BEGIN
    RETURN take_sample(server);
END;
$$ LANGUAGE plpgsql;
