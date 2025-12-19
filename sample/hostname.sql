/* System information support - hostname, network, and CPU */

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
    server_port integer,
    cpu_cores integer,
    cpu_model text,
    cpu_mhz numeric,
    cpu_sockets integer,
    memory_total_mb bigint
)
SET search_path=@extschema@ AS $$
DECLARE
    current_hostname text := NULL;
    current_ip inet := NULL;
    current_port integer := NULL;
    current_cpu_cores integer := NULL;
    current_cpu_model text := NULL;
    current_cpu_mhz numeric := NULL;
    current_cpu_sockets integer := NULL;
    current_memory_total_mb bigint := NULL;
    temp_table_name text;
    is_linux boolean := false;
BEGIN
    -- Detect if running on Linux by checking PostgreSQL version string
    BEGIN
        SELECT version() ILIKE '%linux%' INTO is_linux;
    EXCEPTION
        WHEN OTHERS THEN
            is_linux := false;
    END;
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

    -- Get CPU information using system commands (Linux only)
    IF is_linux THEN
        temp_table_name := 'temp_cpu_' || extract(epoch from now())::bigint || '_' || pg_backend_pid();

        -- Get logical CPU cores
        BEGIN
            EXECUTE format('CREATE TEMP TABLE %I (output text)', temp_table_name);
            EXECUTE format('COPY %I FROM PROGRAM ''nproc''', temp_table_name);
            EXECUTE format('SELECT output::integer FROM %I LIMIT 1', temp_table_name) INTO current_cpu_cores;
            EXECUTE format('DROP TABLE %I', temp_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to retrieve CPU core count: %', SQLERRM;
                current_cpu_cores := NULL;
                -- Clean up table if it exists
                BEGIN
                    EXECUTE format('DROP TABLE IF EXISTS %I', temp_table_name);
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
        END;

        -- Get CPU model
        BEGIN
            EXECUTE format('CREATE TEMP TABLE %I (output text)', temp_table_name);
            EXECUTE format('COPY %I FROM PROGRAM ''grep "model name" /proc/cpuinfo | head -1 | cut -d: -f2''', temp_table_name);
            EXECUTE format('SELECT trim(output) FROM %I LIMIT 1', temp_table_name) INTO current_cpu_model;
            EXECUTE format('DROP TABLE %I', temp_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to retrieve CPU model: %', SQLERRM;
                current_cpu_model := NULL;
                BEGIN
                    EXECUTE format('DROP TABLE IF EXISTS %I', temp_table_name);
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
        END;

        -- Get CPU frequency
        BEGIN
            EXECUTE format('CREATE TEMP TABLE %I (output text)', temp_table_name);
            EXECUTE format('COPY %I FROM PROGRAM ''grep "cpu MHz" /proc/cpuinfo | head -1 | cut -d: -f2''', temp_table_name);
            EXECUTE format('SELECT trim(output)::numeric FROM %I LIMIT 1', temp_table_name) INTO current_cpu_mhz;
            EXECUTE format('DROP TABLE %I', temp_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to retrieve CPU frequency: %', SQLERRM;
                current_cpu_mhz := NULL;
                BEGIN
                    EXECUTE format('DROP TABLE IF EXISTS %I', temp_table_name);
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
        END;

        -- Get CPU sockets
        BEGIN
            EXECUTE format('CREATE TEMP TABLE %I (output text)', temp_table_name);
            EXECUTE format('COPY %I FROM PROGRAM ''grep "physical id" /proc/cpuinfo | sort -u | wc -l''', temp_table_name);
            EXECUTE format('SELECT output::integer FROM %I LIMIT 1', temp_table_name) INTO current_cpu_sockets;
            EXECUTE format('DROP TABLE %I', temp_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to retrieve CPU socket count: %', SQLERRM;
                current_cpu_sockets := NULL;
                BEGIN
                    EXECUTE format('DROP TABLE IF EXISTS %I', temp_table_name);
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
        END;

        -- Get total memory (convert from KB to MB)
        BEGIN
            EXECUTE format('CREATE TEMP TABLE %I (output text)', temp_table_name);
            EXECUTE format('COPY %I FROM PROGRAM ''cat /proc/meminfo | grep MemTotal | cut -d: -f2 | tr -d " kB"''', temp_table_name);
            EXECUTE format('SELECT (output::bigint / 1024) FROM %I LIMIT 1', temp_table_name) INTO current_memory_total_mb;
            EXECUTE format('DROP TABLE %I', temp_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to retrieve total memory: %', SQLERRM;
                current_memory_total_mb := NULL;
                BEGIN
                    EXECUTE format('DROP TABLE IF EXISTS %I', temp_table_name);
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
        END;

        -- Get load averages
        BEGIN
            EXECUTE format('CREATE TEMP TABLE %I (output text)', temp_table_name);
            EXECUTE format('COPY %I FROM PROGRAM ''cat /proc/loadavg | cut -d" " -f1-3''', temp_table_name);
            EXECUTE format('SELECT split_part(output, '' '', 1)::numeric FROM %I LIMIT 1', temp_table_name) INTO current_load_avg_1min;
            EXECUTE format('SELECT split_part(output, '' '', 2)::numeric FROM %I LIMIT 1', temp_table_name) INTO current_load_avg_5min;
            EXECUTE format('SELECT split_part(output, '' '', 3)::numeric FROM %I LIMIT 1', temp_table_name) INTO current_load_avg_15min;
            EXECUTE format('DROP TABLE %I', temp_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to retrieve load averages: %', SQLERRM;
                current_load_avg_1min := NULL;
                current_load_avg_5min := NULL;
                current_load_avg_15min := NULL;
                BEGIN
                    EXECUTE format('DROP TABLE IF EXISTS %I', temp_table_name);
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
        END;

        -- Get CPU utilization from /proc/stat (Linux only)
        BEGIN
            cpu_stat_table_name := 'temp_cpu_stat_' || extract(epoch from now())::bigint || '_' || pg_backend_pid();
            EXECUTE format('CREATE TEMP TABLE %I (output text)', cpu_stat_table_name);
            EXECUTE format('COPY %I FROM PROGRAM ''head -1 /proc/stat''', cpu_stat_table_name);

            -- Parse /proc/stat format: cpu user nice system idle iowait irq softirq steal guest guest_nice
            -- Calculate percentages based on total CPU time
            EXECUTE format('
                WITH cpu_values AS (
                    SELECT
                        split_part(output, '' '', 3)::bigint AS user_time,
                        split_part(output, '' '', 4)::bigint AS nice_time,
                        split_part(output, '' '', 5)::bigint AS system_time,
                        split_part(output, '' '', 6)::bigint AS idle_time,
                        split_part(output, '' '', 7)::bigint AS iowait_time,
                        split_part(output, '' '', 8)::bigint AS irq_time,
                        split_part(output, '' '', 9)::bigint AS softirq_time
                    FROM %I
                ),
                cpu_totals AS (
                    SELECT
                        user_time,
                        nice_time,
                        system_time,
                        idle_time,
                        iowait_time,
                        irq_time,
                        softirq_time,
                        (user_time + nice_time + system_time + idle_time +
                         COALESCE(iowait_time, 0) + COALESCE(irq_time, 0) + COALESCE(softirq_time, 0)) AS total_time
                    FROM cpu_values
                )
                SELECT
                    ROUND((user_time + nice_time) * 100.0 / NULLIF(total_time, 0), 2) AS user_pct,
                    ROUND((system_time + COALESCE(irq_time, 0) + COALESCE(softirq_time, 0)) * 100.0 / NULLIF(total_time, 0), 2) AS system_pct,
                    ROUND(idle_time * 100.0 / NULLIF(total_time, 0), 2) AS idle_pct
                FROM cpu_totals
            ', cpu_stat_table_name)
            INTO current_cpu_user_pct, current_cpu_system_pct, current_cpu_idle_pct;

            EXECUTE format('DROP TABLE %I', cpu_stat_table_name);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to retrieve CPU utilization: %', SQLERRM;
                current_cpu_user_pct := NULL;
                current_cpu_system_pct := NULL;
                current_cpu_idle_pct := NULL;
                -- Clean up if we actually created the table
                BEGIN
                    EXECUTE format('DROP TABLE IF EXISTS %I', cpu_stat_table_name);
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
        END;
    END IF;

    -- Return all system information
    RETURN QUERY SELECT
        current_hostname,
        current_ip,
        current_port,
        current_cpu_cores,
        current_cpu_model,
        current_cpu_mhz,
        current_cpu_sockets,
        current_memory_total_mb;
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
