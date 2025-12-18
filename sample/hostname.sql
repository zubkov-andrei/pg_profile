/* System information support - hostname and network information */

-- Function to collect system information from remote server via dblink
CREATE FUNCTION collect_system_info(IN properties jsonb, IN sserver_id integer, IN s_id integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  qres record;
  server_query text;
  extension_schema text;
BEGIN
  -- Get the schema where pg_profile is installed on the remote server
  extension_schema := COALESCE(
    (
      SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
        AS x(extname text, extnamespace text)
      WHERE extname = 'pg_profile'
    ),
    'public'
  );

  -- Build query to call get_system_info() on remote server
  server_query := format(
    'SELECT hostname, server_ip, server_port FROM %I.get_system_info()',
    extension_schema
  );

  -- Execute query on remote server and update servers table
  BEGIN
    FOR qres IN
      SELECT * FROM dblink('server_connection', server_query) AS dbl(
        hostname text,
        server_ip inet,
        server_port integer
      )
    LOOP
      -- Update servers table with collected info
      UPDATE servers SET
        server_hostname = qres.hostname,
        server_ip = qres.server_ip,
        server_port = qres.server_port
      WHERE server_id = sserver_id;

      -- Update samples table with collected info
      UPDATE samples SET
        server_hostname = qres.hostname,
        server_ip = qres.server_ip,
        server_port = qres.server_port
      WHERE server_id = sserver_id AND sample_id = s_id;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      -- If get_system_info() doesn't exist on remote server or fails, continue silently
      RAISE WARNING 'Failed to collect system information from remote server: %', SQLERRM;
  END;
END;
$$ LANGUAGE plpgsql;

-- Function to get comprehensive system information
CREATE FUNCTION get_system_info()
RETURNS TABLE(
    hostname text,
    server_ip inet,
    server_port integer
)
SET search_path=@extschema@ AS $$
DECLARE
    current_hostname text := NULL;
    current_ip inet := NULL;
    current_port integer := NULL;
BEGIN
    -- Get hostname if hostname extension is available
    IF EXISTS (
        SELECT 1 FROM pg_extension
        WHERE extname = 'hostname'
    ) THEN
        BEGIN
            SELECT hostname() INTO current_hostname;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to retrieve hostname: %', SQLERRM;
                current_hostname := NULL;
        END;
    END IF;

    -- Get IP address using inet_server_addr()
    BEGIN
        SELECT inet_server_addr() INTO current_ip;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Failed to retrieve server IP address: %', SQLERRM;
            current_ip := NULL;
    END;

    -- Get port using inet_server_port()
    BEGIN
        SELECT inet_server_port() INTO current_port;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Failed to retrieve server port: %', SQLERRM;
            current_port := NULL;
    END;

    -- Return all system information
    RETURN QUERY SELECT
        current_hostname,
        current_ip,
        current_port;
END;
$$ LANGUAGE plpgsql;

-- Convenience function to get only network information
CREATE FUNCTION get_network_info()
RETURNS TABLE(hostname text, server_ip inet, server_port integer)
SET search_path=@extschema@ AS $$
BEGIN
    RETURN QUERY SELECT s.hostname, s.server_ip, s.server_port FROM get_system_info() s;
END;
$$ LANGUAGE plpgsql;

-- Convenience function to get only hostname
CREATE FUNCTION get_current_hostname()
RETURNS text SET search_path=@extschema@ AS $$
BEGIN
    RETURN (SELECT hostname FROM get_system_info());
END;
$$ LANGUAGE plpgsql;
