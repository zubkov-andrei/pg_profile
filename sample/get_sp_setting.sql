CREATE FUNCTION get_sp_setting(IN server_properties jsonb, IN setting_name text, out reset_val text, out unit text, out pending_restart boolean
) RETURNS record SET search_path=@extschema@ AS $$
SELECT x.reset_val,
       x.unit,
       x.pending_restart
  FROM jsonb_to_recordset(server_properties #> '{settings}')
    AS x (name text, reset_val text, unit text, pending_restart boolean)
 WHERE x.name = setting_name;
$$ LANGUAGE sql IMMUTABLE;