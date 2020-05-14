/* ===== Function stats functions ===== */

CREATE OR REPLACE FUNCTION top_functions(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    node_id integer,
    datid oid,
    funcid oid,
    dbname name,
    schemaname name,
    funcname name,
    funcargs text,
    calls bigint,
    total_time double precision,
    self_time double precision,
    m_time double precision,
    m_stime double precision
)
SET search_path=@extschema@,public AS $$
    SELECT
        st.node_id,
        st.datid,
        st.funcid,
        snap_db.datname AS dbname,
        st.schemaname,
        st.funcname,
        st.funcargs,
        sum(st.calls)::bigint AS calls,
        sum(st.total_time) AS total_time,
        sum(st.self_time) AS self_time,
        sum(st.total_time)/sum(st.calls) AS m_time,
        sum(st.self_time)/sum(st.calls) AS m_stime
    FROM v_snap_stat_user_functions st
        -- Database name
        JOIN snap_stat_database snap_db
        ON (st.node_id=snap_db.node_id AND st.snap_id=snap_db.snap_id AND st.datid=snap_db.datid)
        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
    WHERE
      st.node_id = snode_id
      AND snap_db.datname NOT LIKE 'template_'
      AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY st.node_id,st.datid,st.funcid,snap_db.datname,st.schemaname,st.funcname,st.funcargs
    --HAVING min(snap_db.stats_reset) = max(snap_db.stats_reset)
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION func_top_time_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        calls,
        total_time,
        self_time,
        m_time,
        m_stime
    FROM top_functions(snode_id, start_id, end_id)
    ORDER BY total_time DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td title="%s">%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_top_time_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        COALESCE(f1.funcargs,f2.funcargs) as funcargs,
        f1.calls as calls1,
        f1.total_time as total_time1,
        f1.self_time as self_time1,
        f1.m_time as m_time1,
        f1.m_stime as m_stime1,
        f2.calls as calls2,
        f2.total_time as total_time2,
        f2.self_time as self_time2,
        f2.m_time as m_time2,
        f2.m_stime as m_stime2,
        row_number() OVER (ORDER BY f1.total_time DESC NULLS LAST) as rn_time1,
        row_number() OVER (ORDER BY f2.total_time DESC NULLS LAST) as rn_time2
    FROM top_functions(snode_id, start1_id, end1_id) f1
        FULL OUTER JOIN top_functions(snode_id, start2_id, end2_id) f2 USING (node_id, datid, funcid)
    ORDER BY COALESCE(f1.total_time,0) + COALESCE(f2.total_time,0) DESC) t1
    WHERE rn_time1 <= topn OR rn_time2 <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>DB</th><th>Schema</th><th>Function</th><th>I</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>',
      'row_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr} title="%s">%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            round(CAST(r_result.self_time1 AS numeric),2),
            round(CAST(r_result.m_time1 AS numeric),3),
            round(CAST(r_result.m_stime1 AS numeric),3),
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            round(CAST(r_result.self_time2 AS numeric),2),
            round(CAST(r_result.m_time2 AS numeric),3),
            round(CAST(r_result.m_stime2 AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_top_calls_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        calls,
        total_time,
        self_time,
        m_time,
        m_stime
    FROM top_functions(snode_id, start_id, end_id)
    ORDER BY calls DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td title="%s">%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls,
            round(CAST(r_result.total_time AS numeric),2),
            round(CAST(r_result.self_time AS numeric),2),
            round(CAST(r_result.m_time AS numeric),3),
            round(CAST(r_result.m_stime AS numeric),3)
        );
    END LOOP;

   IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
   ELSE
        RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_top_calls_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        COALESCE(f1.funcargs,f2.funcargs) as funcargs,
        f1.calls as calls1,
        f1.total_time as total_time1,
        f1.self_time as self_time1,
        f1.m_time as m_time1,
        f1.m_stime as m_stime1,
        f2.calls as calls2,
        f2.total_time as total_time2,
        f2.self_time as self_time2,
        f2.m_time as m_time2,
        f2.m_stime as m_stime2,
        row_number() OVER (ORDER BY f1.calls DESC NULLS LAST) as rn_calls1,
        row_number() OVER (ORDER BY f2.calls DESC NULLS LAST) as rn_calls2
    FROM top_functions(snode_id, start1_id, end1_id) f1
        FULL OUTER JOIN top_functions(snode_id, start2_id, end2_id) f2 USING (node_id, datid, funcid)
    ORDER BY COALESCE(f1.calls,0) + COALESCE(f2.calls,0) DESC) t1
    WHERE rn_calls1 <= topn OR rn_calls2 <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>DB</th><th>Schema</th><th>Function</th><th>I</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>',
      'row_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr} title="%s">%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    FOR r_result IN c_fun_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.funcargs,
            r_result.funcname,
            r_result.calls1,
            round(CAST(r_result.total_time1 AS numeric),2),
            round(CAST(r_result.self_time1 AS numeric),2),
            round(CAST(r_result.m_time1 AS numeric),3),
            round(CAST(r_result.m_stime1 AS numeric),3),
            r_result.calls2,
            round(CAST(r_result.total_time2 AS numeric),2),
            round(CAST(r_result.self_time2 AS numeric),2),
            round(CAST(r_result.m_time2 AS numeric),3),
            round(CAST(r_result.m_stime2 AS numeric),3)
        );
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;
