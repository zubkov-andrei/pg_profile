/* ===== Authentication Statistics Report Functions ===== */

-- Get authentication statistics with rates
CREATE FUNCTION auth_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id           integer,
    rolname             text,
    successful_logins   bigint,
    failed_logins       bigint,
    total_attempts      bigint,
    hba_conflicts       bigint,
    has_data            boolean
)
SET search_path=@extschema@ AS $$
    SELECT
        sserver_id as server_id,
        rolname::text,
        SUM(successful_logins)::bigint as successful_logins,
        SUM(failed_logins)::bigint as failed_logins,
        SUM(successful_logins + failed_logins)::bigint as total_attempts,
        SUM(hba_conflicts)::bigint as hba_conflicts,
        (SUM(successful_logins) > 0 OR SUM(failed_logins) > 0 OR SUM(hba_conflicts) > 0) as has_data
    FROM sample_stat_auth
    WHERE server_id = sserver_id
      AND sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY rolname
$$ LANGUAGE sql;

-- Get aggregated authentication statistics with rates
CREATE FUNCTION auth_stats_total(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id           integer,
    total_successful    bigint,
    total_failed        bigint,
    total_attempts      bigint,
    total_hba_conflicts bigint,
    max_unique_users    integer,
    has_data            boolean
)
SET search_path=@extschema@ AS $$
    SELECT
        sserver_id as server_id,
        SUM(total_successful)::bigint as total_successful,
        SUM(total_failed)::bigint as total_failed,
        SUM(total_successful + total_failed)::bigint as total_attempts,
        SUM(total_hba_conflicts)::bigint as total_hba_conflicts,
        MAX(unique_users)::integer as max_unique_users,
        (SUM(total_successful) > 0 OR SUM(total_failed) > 0 OR SUM(total_hba_conflicts) > 0) as has_data
    FROM sample_stat_auth_total
    WHERE server_id = sserver_id
      AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

-- Format authentication statistics for reports with per-second rates
CREATE FUNCTION auth_stats_format(
    IN sserver_id integer,
    IN start_id integer,
    IN end_id integer,
    IN interval_duration_sec numeric
)
RETURNS TABLE(
    username                text,
    successful_logins       numeric,
    failed_logins           numeric,
    total_attempts          numeric,
    hba_conflicts           numeric,
    success_rate_pct        numeric,
    logins_per_sec          numeric,
    failures_per_sec        numeric,
    attempts_per_sec        numeric
)
SET search_path=@extschema@ AS $$
    SELECT
        CASE
            WHEN rolname IS NULL THEN '<invalid users>'
            ELSE rolname
        END as username,
        successful_logins,
        failed_logins,
        total_attempts,
        hba_conflicts,
        CASE
            WHEN total_attempts > 0
            THEN ROUND((successful_logins::numeric / total_attempts::numeric * 100), 2)
            ELSE NULL
        END as success_rate_pct,
        ROUND(successful_logins::numeric / NULLIF(interval_duration_sec, 0), 3) as logins_per_sec,
        ROUND(failed_logins::numeric / NULLIF(interval_duration_sec, 0), 3) as failures_per_sec,
        ROUND(total_attempts::numeric / NULLIF(interval_duration_sec, 0), 3) as attempts_per_sec
    FROM auth_stats(sserver_id, start_id, end_id)
    WHERE has_data
    ORDER BY total_attempts DESC
$$ LANGUAGE sql;

-- Format aggregated authentication statistics for reports
CREATE FUNCTION auth_stats_total_format(
    IN sserver_id integer,
    IN start_id integer,
    IN end_id integer,
    IN interval_duration_sec numeric
)
RETURNS TABLE(
    metric                  text,
    value                   text
)
SET search_path=@extschema@ AS $$
    SELECT metric, value
    FROM (
        SELECT 'Total Successful Logins' as metric, total_successful::text as value, 1 as ord
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data
        UNION ALL
        SELECT 'Total Failed Logins', total_failed::text, 2
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data
        UNION ALL
        SELECT 'Total Attempts', total_attempts::text, 3
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data
        UNION ALL
        SELECT 'Success Rate',
            ROUND((total_successful::numeric / NULLIF(total_attempts, 0)::numeric * 100), 2)::text || '%', 4
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data AND total_attempts > 0
        UNION ALL
        SELECT 'Logins per Second',
            ROUND(total_successful::numeric / NULLIF(interval_duration_sec, 0), 3)::text, 5
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data
        UNION ALL
        SELECT 'Failures per Second',
            ROUND(total_failed::numeric / NULLIF(interval_duration_sec, 0), 3)::text, 6
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data
        UNION ALL
        SELECT 'Attempts per Second',
            ROUND(total_attempts::numeric / NULLIF(interval_duration_sec, 0), 3)::text, 7
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data
        UNION ALL
        SELECT 'Unique Users', max_unique_users::text, 8
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data
        UNION ALL
        SELECT 'HBA Conflicts', total_hba_conflicts::text, 9
        FROM auth_stats_total(sserver_id, start_id, end_id)
        WHERE has_data AND total_hba_conflicts > 0
    ) AS t
    ORDER BY ord
$$ LANGUAGE sql;

-- Differential report format
CREATE FUNCTION auth_stats_format_diff(
    IN sserver_id integer,
    IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer,
    IN interval1_duration_sec numeric,
    IN interval2_duration_sec numeric
)
RETURNS TABLE(
    username                text,
    successful_logins1      numeric,
    successful_logins2      numeric,
    failed_logins1          numeric,
    failed_logins2          numeric,
    total_attempts1         numeric,
    total_attempts2         numeric,
    logins_per_sec1         numeric,
    logins_per_sec2         numeric,
    failures_per_sec1       numeric,
    failures_per_sec2       numeric,
    attempts_per_sec1       numeric,
    attempts_per_sec2       numeric,
    success_rate_pct1       numeric,
    success_rate_pct2       numeric
)
SET search_path=@extschema@ AS $$
    SELECT
        COALESCE(t1.username, t2.username) as username,
        t1.successful_logins as successful_logins1,
        t2.successful_logins as successful_logins2,
        t1.failed_logins as failed_logins1,
        t2.failed_logins as failed_logins2,
        t1.total_attempts as total_attempts1,
        t2.total_attempts as total_attempts2,
        t1.logins_per_sec as logins_per_sec1,
        t2.logins_per_sec as logins_per_sec2,
        t1.failures_per_sec as failures_per_sec1,
        t2.failures_per_sec as failures_per_sec2,
        t1.attempts_per_sec as attempts_per_sec1,
        t2.attempts_per_sec as attempts_per_sec2,
        t1.success_rate_pct as success_rate_pct1,
        t2.success_rate_pct as success_rate_pct2
    FROM
        auth_stats_format(sserver_id, start1_id, end1_id, interval1_duration_sec) t1
    FULL OUTER JOIN
        auth_stats_format(sserver_id, start2_id, end2_id, interval2_duration_sec) t2
        USING (username)
    ORDER BY GREATEST(
        COALESCE(t1.total_attempts, 0),
        COALESCE(t2.total_attempts, 0)
    ) DESC
$$ LANGUAGE sql;
