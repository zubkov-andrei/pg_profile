/* ===== Network Buffer Stats functions (Linux only) ===== */

CREATE FUNCTION net_buffer_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id        integer,
    rmem_default     bigint,
    rmem_max         bigint,
    wmem_default     bigint,
    wmem_max         bigint,
    optmem_max       bigint,
    has_data         boolean
)
SET search_path=@extschema@ AS $$
    -- Network buffer settings are configuration values, not counters
    -- Return the most recent values from the sample range
    SELECT
        server_id,
        rmem_default,
        rmem_max,
        wmem_default,
        wmem_max,
        optmem_max,
        (rmem_default IS NOT NULL OR rmem_max IS NOT NULL OR
         wmem_default IS NOT NULL OR wmem_max IS NOT NULL OR
         optmem_max IS NOT NULL) as has_data
    FROM sample_stat_net_buffers
    WHERE server_id = sserver_id
      AND sample_id = end_id
$$ LANGUAGE sql;

CREATE FUNCTION net_buffer_stats_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    parameter text,
    value     text
)
SET search_path=@extschema@ AS $$
    SELECT parameter, value
    FROM (
        SELECT 'net.core.rmem_default' as parameter, pg_size_pretty(rmem_default) as value, 1 as ord
        FROM net_buffer_stats(sserver_id, start_id, end_id)
        WHERE has_data AND rmem_default IS NOT NULL
        UNION ALL
        SELECT 'net.core.rmem_max', pg_size_pretty(rmem_max), 2
        FROM net_buffer_stats(sserver_id, start_id, end_id)
        WHERE has_data AND rmem_max IS NOT NULL
        UNION ALL
        SELECT 'net.core.wmem_default', pg_size_pretty(wmem_default), 3
        FROM net_buffer_stats(sserver_id, start_id, end_id)
        WHERE has_data AND wmem_default IS NOT NULL
        UNION ALL
        SELECT 'net.core.wmem_max', pg_size_pretty(wmem_max), 4
        FROM net_buffer_stats(sserver_id, start_id, end_id)
        WHERE has_data AND wmem_max IS NOT NULL
        UNION ALL
        SELECT 'net.core.optmem_max', pg_size_pretty(optmem_max), 5
        FROM net_buffer_stats(sserver_id, start_id, end_id)
        WHERE has_data AND optmem_max IS NOT NULL
    ) AS t
    ORDER BY ord
$$ LANGUAGE sql;

CREATE FUNCTION net_buffer_stats_htbl(IN sserver_id integer, IN start_id integer, IN end_id integer, IN description text)
RETURNS text
SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;
    c_net_buf CURSOR FOR
    SELECT * FROM net_buffer_stats_format(sserver_id, start_id, end_id);
    r_result RECORD;
BEGIN
    -- Check if we have any data
    IF NOT EXISTS(
        SELECT 1 FROM net_buffer_stats(sserver_id, start_id, end_id)
        WHERE has_data
    ) THEN
        RETURN '';
    END IF;

    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Parameter</th><th>Value</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td {value}>%s</td></tr>'
    );

    report := replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', '');

    FOR r_result IN c_net_buf LOOP
        report := report || format(jtab_tpl #>> ARRAY['row_tpl'], r_result.parameter, r_result.value);
    END LOOP;

    IF report != replace(jtab_tpl #>> ARRAY['tab_hdr'], '{rows}', '') THEN
        report := '<h3>' || description || '</h3>' || report;
    ELSE
        report := '';
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

-- Differential report format function
CREATE FUNCTION net_buffer_stats_format_diff(
    IN sserver_id integer,
    IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer
)
RETURNS TABLE(
    parameter text,
    value1    text,
    value2    text
)
SET search_path=@extschema@ AS $$
    SELECT
        COALESCE(t1.parameter, t2.parameter) as parameter,
        t1.value as value1,
        t2.value as value2
    FROM
        (SELECT * FROM net_buffer_stats_format(sserver_id, start1_id, end1_id)) t1
    FULL OUTER JOIN
        (SELECT * FROM net_buffer_stats_format(sserver_id, start2_id, end2_id)) t2
        USING (parameter)
    ORDER BY parameter
$$ LANGUAGE sql;
