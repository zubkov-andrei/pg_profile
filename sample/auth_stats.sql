/* Authentication Statistics Collection (pg_auth_mon) */

-- Function to collect authentication statistics from pg_auth_mon extension
CREATE FUNCTION collect_auth_stats(
    IN sserver_id integer,
    IN s_id integer,
    IN server_properties jsonb
)
RETURNS void
SET search_path=@extschema@ AS $$
DECLARE
    qres RECORD;
    pg_auth_mon_available boolean := false;
BEGIN
    -- Check if pg_auth_mon extension exists
    BEGIN
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = 'pg_auth_mon'
        ) INTO pg_auth_mon_available;
    EXCEPTION
        WHEN OTHERS THEN
            pg_auth_mon_available := false;
    END;

    -- Only collect if pg_auth_mon is available
    IF NOT pg_auth_mon_available THEN
        RETURN;
    END IF;

    -- Collect current authentication statistics from pg_auth_mon
    -- and calculate deltas from last sample
    INSERT INTO sample_stat_auth (
        server_id,
        sample_id,
        rolname,
        successful_logins,
        failed_logins,
        hba_conflicts
    )
    SELECT
        sserver_id,
        s_id,
        cur.rolname,  -- Store NULL for invalid users
        GREATEST(cur.successful_attempts - COALESCE(lst.successful_attempts, 0), 0) as successful_logins,
        GREATEST(cur.other_auth_failures - COALESCE(lst.other_auth_failures, 0), 0) as failed_logins,
        GREATEST(cur.total_hba_conflicts - COALESCE(lst.total_hba_conflicts, 0), 0) as hba_conflicts
    FROM
        pg_auth_mon cur
    LEFT JOIN last_stat_auth lst ON (
        lst.server_id = sserver_id AND
        lst.rolname IS NOT DISTINCT FROM cur.rolname
    )
    WHERE
        -- Only include rows with activity in this interval
        cur.successful_attempts - COALESCE(lst.successful_attempts, 0) > 0 OR
        cur.other_auth_failures - COALESCE(lst.other_auth_failures, 0) > 0 OR
        cur.total_hba_conflicts - COALESCE(lst.total_hba_conflicts, 0) > 0;

    -- Update last_stat_auth with current cumulative values
    -- Delete old entries first
    DELETE FROM last_stat_auth WHERE server_id = sserver_id;

    -- Insert current values
    INSERT INTO last_stat_auth (
        server_id,
        sample_id,
        rolname,
        successful_attempts,
        other_auth_failures,
        total_hba_conflicts
    )
    SELECT
        sserver_id,
        s_id,
        rolname,  -- Store NULL for invalid users
        successful_attempts,
        other_auth_failures,
        total_hba_conflicts
    FROM pg_auth_mon;

    -- Calculate and store aggregated totals
    INSERT INTO sample_stat_auth_total (
        server_id,
        sample_id,
        total_successful,
        total_failed,
        total_hba_conflicts,
        unique_users
    )
    SELECT
        sserver_id,
        s_id,
        COALESCE(SUM(successful_logins), 0),
        COALESCE(SUM(failed_logins), 0),
        COALESCE(SUM(hba_conflicts), 0),
        COUNT(DISTINCT rolname)
    FROM sample_stat_auth
    WHERE server_id = sserver_id AND sample_id = s_id;

EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'Failed to collect authentication statistics: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION collect_auth_stats(integer, integer, jsonb) IS 'Collect authentication statistics from pg_auth_mon extension. Calculates deltas from cumulative counters. Only collects if pg_auth_mon extension is installed.';
