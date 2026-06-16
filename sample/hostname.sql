/* System information support - hostname and network information */

-- Collector (runs on the pg_profile host). Pulls the remote's hostname,
-- listen address, and port via dblink without requiring pg_profile on
-- the remote (Method B):
--   * Hostname: when the 'hostname' extension is present on the remote,
--     call its hostname() function directly via dblink.
--   * IP/port: query the remote session's inet_server_addr() and
--     inet_server_port() (which report the remote's listening endpoint),
--     with a pg_settings fallback for Unix-socket connections.
CREATE FUNCTION collect_system_info(IN properties jsonb, IN sserver_id integer, IN s_id integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  qres record;
  hostname_schema text;
  hostname_query text;
  netinfo_query text;
  current_hostname text;
  current_ip text;
  current_port integer;
BEGIN
  -- Adding dblink extension schema to search_path if it is not already there
  SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres
  FROM pg_catalog.pg_extension WHERE extname = 'dblink';
  IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
    EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
  END IF;

  -- 1. Hostname (only if the hostname extension is on the remote).
  SELECT extnamespace INTO hostname_schema
  FROM jsonb_to_recordset(properties #> '{extensions}')
    AS x(extname text, extnamespace text)
  WHERE extname = 'hostname';

  IF hostname_schema IS NOT NULL THEN
    hostname_query := format('SELECT %I.hostname()::text', hostname_schema);
    BEGIN
      SELECT h INTO current_hostname
      FROM dblink('server_connection', hostname_query) AS dbl(h text);
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'Failed to retrieve hostname from remote: %', SQLERRM;
      current_hostname := NULL;
    END;
  END IF;

  -- 2. IP/port from the remote's perspective.
  -- inet_server_addr()/inet_server_port() report the remote's listening
  -- endpoint for the dblink connection. They return NULL for Unix-socket
  -- connections (typical of self-monitoring), in which case we fall back
  -- to pg_settings.
  netinfo_query :=
    'SELECT '
    'COALESCE(host(inet_server_addr()), '
    '  (SELECT setting FROM pg_catalog.pg_settings WHERE name = ''listen_addresses'')), '
    'COALESCE(inet_server_port(), '
    '  (SELECT setting::integer FROM pg_catalog.pg_settings WHERE name = ''port''))';

  BEGIN
    SELECT ip, port INTO current_ip, current_port
    FROM dblink('server_connection', netinfo_query) AS dbl(ip text, port integer);
    -- Normalize listen_addresses wildcards.
    IF current_ip = '*' THEN
      current_ip := '0.0.0.0';
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Failed to retrieve network info from remote: %', SQLERRM;
    current_ip := NULL;
    current_port := NULL;
  END;

  -- 3. Write to local tables.
  UPDATE servers SET
    server_hostname = current_hostname,
    server_ip = current_ip,
    server_port = current_port
  WHERE server_id = sserver_id;

  UPDATE samples SET
    server_hostname = current_hostname,
    server_ip = current_ip,
    server_port = current_port
  WHERE server_id = sserver_id AND sample_id = s_id;
END;
$$ LANGUAGE plpgsql;

