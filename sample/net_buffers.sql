/* Network Buffer Statistics Collection (Linux only) */

-- Function to collect network buffer configuration from sysctl
CREATE FUNCTION collect_net_buffer_stats(
    IN sserver_id integer,
    IN s_id integer,
    IN server_properties jsonb
)
RETURNS void
SET search_path=@extschema@ AS $$
DECLARE
    temp_table_name text;
    is_linux boolean := false;
    sysctl_output text;

    v_rmem_default bigint := NULL;
    v_rmem_max bigint := NULL;
    v_wmem_default bigint := NULL;
    v_wmem_max bigint := NULL;
    v_optmem_max bigint := NULL;
BEGIN
    -- Detect if running on Linux by checking PostgreSQL version string
    BEGIN
        SELECT version() ILIKE '%linux%' INTO is_linux;
    EXCEPTION
        WHEN OTHERS THEN
            is_linux := false;
    END;

    -- Only collect on Linux systems
    IF NOT is_linux THEN
        RETURN;
    END IF;

    -- Create temp table for sysctl output
    temp_table_name := 'temp_net_buffers_' || extract(epoch from now())::bigint || '_' || pg_backend_pid();

    BEGIN
        -- Collect all net.core.*mem values in one command
        EXECUTE format('CREATE TEMP TABLE %I (output text)', temp_table_name);
        EXECUTE format('COPY %I FROM PROGRAM ''sysctl -a -r "net\.core\..*mem"''', temp_table_name);

        -- Parse the output and extract values
        FOR sysctl_output IN EXECUTE format('SELECT output FROM %I WHERE output IS NOT NULL', temp_table_name)
        LOOP
            -- Parse format: "net.core.rmem_default = 212992"
            IF sysctl_output ~ 'net\.core\.rmem_default' THEN
                v_rmem_default := (regexp_replace(sysctl_output, '^.*= *', ''))::bigint;
            ELSIF sysctl_output ~ 'net\.core\.rmem_max' THEN
                v_rmem_max := (regexp_replace(sysctl_output, '^.*= *', ''))::bigint;
            ELSIF sysctl_output ~ 'net\.core\.wmem_default' THEN
                v_wmem_default := (regexp_replace(sysctl_output, '^.*= *', ''))::bigint;
            ELSIF sysctl_output ~ 'net\.core\.wmem_max' THEN
                v_wmem_max := (regexp_replace(sysctl_output, '^.*= *', ''))::bigint;
            ELSIF sysctl_output ~ 'net\.core\.optmem_max' THEN
                v_optmem_max := (regexp_replace(sysctl_output, '^.*= *', ''))::bigint;
            END IF;
        END LOOP;

        -- Insert the collected values
        INSERT INTO sample_stat_net_buffers (
            server_id,
            sample_id,
            rmem_default,
            rmem_max,
            wmem_default,
            wmem_max,
            optmem_max
        ) VALUES (
            sserver_id,
            s_id,
            v_rmem_default,
            v_rmem_max,
            v_wmem_default,
            v_wmem_max,
            v_optmem_max
        );

        -- Clean up temp table
        EXECUTE format('DROP TABLE %I', temp_table_name);

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Failed to collect network buffer statistics: %', SQLERRM;
            -- Clean up on error
            BEGIN
                EXECUTE format('DROP TABLE IF EXISTS %I', temp_table_name);
            EXCEPTION
                WHEN OTHERS THEN NULL;
            END;
    END;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION collect_net_buffer_stats(integer, integer, jsonb) IS 'Collect Linux network buffer configuration from sysctl (net.core.*mem). Linux-only feature using sysctl command via COPY FROM PROGRAM.';
