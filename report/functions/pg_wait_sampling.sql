/* pg_wait_sampling reporting functions */
CREATE FUNCTION profile_checkavail_wait_sampling_total(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there is table sizes collected in both bounds
  SELECT
    count(*) > 0
  FROM wait_sampling_total
  WHERE
    server_id = sserver_id
    AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION wait_sampling_total_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        event_type text,
        event      text,
        tot_waited      numeric,
        stmt_waited     numeric
)
SET search_path=@extschema@ AS $$
    SELECT
        st.event_type,
        st.event,
        sum(st.tot_waited)::numeric / 1000 AS tot_waited,
        sum(st.stmt_waited)::numeric / 1000 AS stmt_waited
    FROM wait_sampling_total st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.event_type, st.event;
$$ LANGUAGE sql;

CREATE FUNCTION wait_sampling_totals_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for wait stats
    c_stats CURSOR
    FOR
    WITH tot AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats1)
    SELECT
        event_type,
        sum(st.tot_waited) as tot_waited,
        sum(st.tot_waited) * 100 / NULLIF(min(tot.tot_waited),0) as tot_waited_pct,
        sum(st.stmt_waited) as stmt_waited,
        sum(st.stmt_waited) * 100 / NULLIF(min(tot.stmt_waited),0) as stmt_waited_pct
    FROM wait_sampling_total_stats1 st CROSS JOIN tot
    GROUP BY ROLLUP(event_type)
    ORDER BY event_type NULLS LAST;

    r_result RECORD;
BEGIN
    -- wait stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th title="Time, waited in events of wait event type executing statements in seconds">Statements Waited (s)</th>'
            '<th title="Time, waited in events of wait event type as a percentage of total time waited in a cluster executing statements">%Total</th>'
            '<th title="Time, waited in events of wait event type by all backends (including background activity) in seconds">All Waited (s)</th>'
            '<th title="Time, waited in events of wait event type as a percentage of total time waited in a cluster by all backends (including background activity)">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wait_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'wait_tot_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td></td>'
          '<td {value}>%s</td>'
          '<td></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary wait stats
    FOR r_result IN c_stats
    LOOP
      IF r_result.event_type IS NOT NULL THEN
        report := report||format(
            jtab_tpl #>> ARRAY['wait_tpl'],
            r_result.event_type,
            round(r_result.stmt_waited, 2),
            round(r_result.stmt_waited_pct,2),
            round(r_result.tot_waited, 2),
            round(r_result.tot_waited_pct,2)
        );
      ELSE
        IF COALESCE(r_result.tot_waited,r_result.stmt_waited) IS NOT NULL THEN
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tot_tpl'],
              'Total',
              round(r_result.stmt_waited, 2),
              round(r_result.tot_waited, 2)
          );
        END IF;
      END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION wait_sampling_totals_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for wait stats
    c_stats CURSOR
    FOR
    WITH tot1 AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats1),
    tot2 AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats2)
    SELECT
        event_type,
        sum(st1.tot_waited) as tot_waited1,
        sum(st1.tot_waited) * 100 / NULLIF(min(tot1.tot_waited),0) as tot_waited_pct1,
        sum(st1.stmt_waited) as stmt_waited1,
        sum(st1.stmt_waited) * 100 / NULLIF(min(tot1.stmt_waited),0) as stmt_waited_pct1,
        sum(st2.tot_waited) as tot_waited2,
        sum(st2.tot_waited) * 100 / NULLIF(min(tot2.tot_waited),0) as tot_waited_pct2,
        sum(st2.stmt_waited) as stmt_waited2,
        sum(st2.stmt_waited) * 100 / NULLIF(min(tot2.stmt_waited),0) as stmt_waited_pct2
    FROM (wait_sampling_total_stats1 st1 CROSS JOIN tot1)
      FULL JOIN
        (wait_sampling_total_stats2 st2 CROSS JOIN tot2)
      USING (event_type, event)
    GROUP BY ROLLUP(event_type)
    ORDER BY event_type NULLS LAST;

    r_result RECORD;
BEGIN
    -- wait stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>I</th>'
            '<th title="Time, waited in events of wait event type executing statements in seconds">Statements Waited (s)</th>'
            '<th title="Time, waited in events of wait event type as a percentage of total time waited in a cluster executing statements">%Total</th>'
            '<th title="Time, waited in events of wait event type by all backends (including background activity) in seconds">All Waited (s)</th>'
            '<th title="Time, waited in events of wait event type as a percentage of total time waited in a cluster by all backends (including background activity)">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wait_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>',
      'wait_tot_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td></td>'
          '<td {value}>%s</td>'
          '<td></td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td></td>'
          '<td {value}>%s</td>'
          '<td></td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>'
        );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting summary wait stats
    FOR r_result IN c_stats
    LOOP
      IF r_result.event_type IS NOT NULL THEN
        report := report||format(
            jtab_tpl #>> ARRAY['wait_tpl'],
            r_result.event_type,
            round(r_result.stmt_waited1, 2),
            round(r_result.stmt_waited_pct1,2),
            round(r_result.tot_waited1, 2),
            round(r_result.tot_waited_pct1,2),
            round(r_result.stmt_waited2, 2),
            round(r_result.stmt_waited_pct2,2),
            round(r_result.tot_waited2, 2),
            round(r_result.tot_waited_pct2,2)
        );
      ELSE
        IF COALESCE(r_result.tot_waited1,r_result.stmt_waited1,r_result.tot_waited2,r_result.stmt_waited2) IS NOT NULL
        THEN
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tot_tpl'],
              'Total',
              round(r_result.stmt_waited1, 2),
              round(r_result.tot_waited1, 2),
              round(r_result.stmt_waited2, 2),
              round(r_result.tot_waited2, 2)
          );
        END IF;
      END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_wait_sampling_events_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for wait stats
    c_all_stats CURSOR(topn integer)
    FOR
    WITH tot AS (
      SELECT sum(tot_waited) AS tot_waited
      FROM wait_sampling_total_stats1)
    SELECT
        event_type,
        event,
        st.tot_waited,
        st.tot_waited * 100 / NULLIF(tot.tot_waited,0) as tot_waited_pct
    FROM wait_sampling_total_stats1 st CROSS JOIN tot
    WHERE st.tot_waited IS NOT NULL AND st.tot_waited > 0
    ORDER BY st.tot_waited DESC, st.event_type, st.event
    LIMIT topn;

    c_stmt_stats CURSOR(topn integer)
    FOR
    WITH tot AS (
      SELECT sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats1)
    SELECT
        event_type,
        event,
        st.stmt_waited,
        st.stmt_waited * 100 / NULLIF(tot.stmt_waited,0) as stmt_waited_pct
    FROM wait_sampling_total_stats1 st CROSS JOIN tot
    WHERE st.stmt_waited IS NOT NULL AND st.stmt_waited > 0
    ORDER BY st.stmt_waited DESC, st.event_type, st.event
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- wait stats TPLs
    jtab_tpl := jsonb_build_object(
      'wt_smp_all_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>Wait event</th>'
            '<th title="Time, waited in event by all backends (including background activity) in seconds">Waited (s)</th>'
            '<th title="Time, waited in event by all backends as a percentage of total time waited in a cluster by all backends (including background activity)">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wt_smp_stmt_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>Wait event</th>'
            '<th title="Time, waited in event executing statements in seconds">Waited (s)</th>'
            '<th title="Time, waited in event as a percentage of total time waited in a cluster executing statements">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wait_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting wait event stats
    CASE report_context #>> '{report_properties,sect_href}'
      WHEN 'wt_smp_all' THEN
        FOR r_result IN c_all_stats(
          (report_context #>> '{report_properties,topn}')::integer
        )
        LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tpl'],
              r_result.event_type,
              r_result.event,
              round(r_result.tot_waited, 2),
              round(r_result.tot_waited_pct,2)
          );
        END LOOP;
      WHEN 'wt_smp_stmt' THEN
        FOR r_result IN c_stmt_stats(
          (report_context #>> '{report_properties,topn}')::integer
        )
        LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tpl'],
              r_result.event_type,
              r_result.event,
              round(r_result.stmt_waited, 2),
              round(r_result.stmt_waited_pct,2)
          );
        END LOOP;
      ELSE
        RAISE 'Incorrect report context';
    END CASE;

    IF report != '' THEN
        report := replace(
          jtab_tpl #>> ARRAY[concat(report_context #>> '{report_properties,sect_href}','_hdr')],
          '{rows}',
          report
        );
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_wait_sampling_events_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for wait stats
    c_all_stats CURSOR(topn integer)
    FOR
    WITH tot1 AS (
      SELECT sum(tot_waited) AS tot_waited
      FROM wait_sampling_total_stats1),
    tot2 AS (
      SELECT sum(tot_waited) AS tot_waited
      FROM wait_sampling_total_stats2)
    SELECT
        event_type,
        event,
        st1.tot_waited as tot_waited1,
        st1.tot_waited * 100 / NULLIF(tot1.tot_waited,0) as tot_waited_pct1,
        st2.tot_waited as tot_waited2,
        st2.tot_waited * 100 / NULLIF(tot2.tot_waited,0) as tot_waited_pct2
    FROM (wait_sampling_total_stats1 st1 CROSS JOIN tot1)
      FULL JOIN
    (wait_sampling_total_stats2 st2 CROSS JOIN tot2)
      USING (event_type, event)
    WHERE num_nulls(st1.tot_waited,st2.tot_waited) < 2 AND
      COALESCE(st1.tot_waited,0) + COALESCE(st2.tot_waited,0) > 0
    ORDER BY COALESCE(st1.tot_waited,0) + COALESCE(st2.tot_waited,0) DESC, event_type, event
    LIMIT topn;

    c_stmt_stats CURSOR(topn integer)
    FOR
    WITH tot1 AS (
      SELECT sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats1),
    tot2 AS (
      SELECT sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats2)
    SELECT
        event_type,
        event,
        st1.stmt_waited as stmt_waited1,
        st1.stmt_waited * 100 / NULLIF(tot1.stmt_waited,0) as stmt_waited_pct1,
        st2.stmt_waited as stmt_waited2,
        st2.stmt_waited * 100 / NULLIF(tot2.stmt_waited,0) as stmt_waited_pct2
    FROM (wait_sampling_total_stats1 st1 CROSS JOIN tot1)
      FULL JOIN
    (wait_sampling_total_stats2 st2 CROSS JOIN tot2)
      USING (event_type, event)
    WHERE num_nulls(st1.stmt_waited,st2.stmt_waited) < 2 AND
      COALESCE(st1.stmt_waited,0) + COALESCE(st2.stmt_waited,0) > 0
    ORDER BY COALESCE(st1.stmt_waited,0) + COALESCE(st2.stmt_waited,0) DESC, event_type, event
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- wait stats TPLs
    jtab_tpl := jsonb_build_object(
      'wt_smp_all_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>Wait event</th>'
            '<th>I</th>'
            '<th title="Time, waited in event by all backends (including background activity) in seconds">Waited (s)</th>'
            '<th title="Time, waited in event by all backends as a percentage of total time waited in a cluster by all backends (including background activity)">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wt_smp_stmt_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Wait event type</th>'
            '<th>Wait event</th>'
            '<th>I</th>'
            '<th title="Time, waited in event executing statements in seconds">Waited (s)</th>'
            '<th title="Time, waited in event as a percentage of total time waited in a cluster executing statements">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'wait_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>'
    );
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting wait event stats
    CASE report_context #>> '{report_properties,sect_href}'
      WHEN 'wt_smp_all' THEN
        FOR r_result IN c_all_stats(
          (report_context #>> '{report_properties,topn}')::integer
        )
        LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tpl'],
              r_result.event_type,
              r_result.event,
              round(r_result.tot_waited1, 2),
              round(r_result.tot_waited_pct1,2),
              round(r_result.tot_waited2, 2),
              round(r_result.tot_waited_pct2,2)
          );
        END LOOP;
      WHEN 'wt_smp_stmt' THEN
        FOR r_result IN c_stmt_stats(
          (report_context #>> '{report_properties,topn}')::integer
        )
        LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['wait_tpl'],
              r_result.event_type,
              r_result.event,
              round(r_result.stmt_waited1, 2),
              round(r_result.stmt_waited_pct1,2),
              round(r_result.stmt_waited2, 2),
              round(r_result.stmt_waited_pct2,2)
          );
        END LOOP;
      ELSE
        RAISE 'Incorrect report context';
    END CASE;

    IF report != '' THEN
        report := replace(
          jtab_tpl #>> ARRAY[concat(report_context #>> '{report_properties,sect_href}','_hdr')],
          '{rows}',
          report
        );
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;
