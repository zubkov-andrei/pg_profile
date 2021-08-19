/* ===== Function stats functions ===== */
CREATE FUNCTION profile_checkavail_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(calls), 0) > 0
  FROM sample_stat_user_func_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_trg_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have planning times collected for report interval
  SELECT COALESCE(sum(calls), 0) > 0
  FROM sample_stat_user_func_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
    AND sn.trg_fn
$$ LANGUAGE sql;
/* ===== Function stats functions ===== */

CREATE FUNCTION top_functions(IN sserver_id integer, IN start_id integer, IN end_id integer, IN trigger_fn boolean)
RETURNS TABLE(
    server_id     integer,
    datid       oid,
    funcid      oid,
    dbname      name,
    schemaname  name,
    funcname    name,
    funcargs    text,
    calls       bigint,
    total_time  double precision,
    self_time   double precision,
    m_time      double precision,
    m_stime     double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.datid,
        st.funcid,
        sample_db.datname AS dbname,
        st.schemaname,
        st.funcname,
        st.funcargs,
        sum(st.calls)::bigint AS calls,
        sum(st.total_time)/1000 AS total_time,
        sum(st.self_time)/1000 AS self_time,
        sum(st.total_time)/NULLIF(sum(st.calls),0)/1000 AS m_time,
        sum(st.self_time)/NULLIF(sum(st.calls),0)/1000 AS m_stime
    FROM v_sample_stat_user_functions st
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
    WHERE
      st.server_id = sserver_id
      AND st.trg_fn = trigger_fn
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.funcid,sample_db.datname,st.schemaname,st.funcname,st.funcargs
$$ LANGUAGE sql;

CREATE FUNCTION func_top_time_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions
    WHERE total_time > 0
    ORDER BY
      total_time DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
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

CREATE FUNCTION func_top_time_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        COALESCE(f1.funcargs,f2.funcargs) as funcargs,
        NULLIF(f1.calls, 0) as calls1,
        NULLIF(f1.total_time, 0.0) as total_time1,
        NULLIF(f1.self_time, 0.0) as self_time1,
        NULLIF(f1.m_time, 0.0) as m_time1,
        NULLIF(f1.m_stime, 0.0) as m_stime1,
        NULLIF(f2.calls, 0) as calls2,
        NULLIF(f2.total_time, 0.0) as total_time2,
        NULLIF(f2.self_time, 0.0) as self_time2,
        NULLIF(f2.m_time, 0.0) as m_time2,
        NULLIF(f2.m_stime, 0.0) as m_stime2,
        row_number() OVER (ORDER BY f1.total_time DESC NULLS LAST) as rn_time1,
        row_number() OVER (ORDER BY f2.total_time DESC NULLS LAST) as rn_time2
    FROM top_functions1 f1
        FULL OUTER JOIN top_functions2 f2 USING (server_id, datid, funcid)
    ORDER BY
      COALESCE(f1.total_time, 0.0) + COALESCE(f2.total_time, 0.0) DESC,
      COALESCE(f1.datid,f2.datid) ASC,
      COALESCE(f1.funcid,f2.funcid) ASC
    ) t1
    WHERE COALESCE(total_time1, 0.0) + COALESCE(total_time2, 0.0) > 0.0
      AND least(
        rn_time1,
        rn_time2
      ) <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr} title="%s">%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
        '</tr>'
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

CREATE FUNCTION func_top_calls_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions
    WHERE calls > 0
    ORDER BY
      calls DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
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

CREATE FUNCTION func_top_calls_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        COALESCE(f1.funcargs,f2.funcargs) as funcargs,
        NULLIF(f1.calls, 0) as calls1,
        NULLIF(f1.total_time, 0.0) as total_time1,
        NULLIF(f1.self_time, 0.0) as self_time1,
        NULLIF(f1.m_time, 0.0) as m_time1,
        NULLIF(f1.m_stime, 0.0) as m_stime1,
        NULLIF(f2.calls, 0) as calls2,
        NULLIF(f2.total_time, 0.0) as total_time2,
        NULLIF(f2.self_time, 0.0) as self_time2,
        NULLIF(f2.m_time, 0.0) as m_time2,
        NULLIF(f2.m_stime, 0.0) as m_stime2,
        row_number() OVER (ORDER BY f1.calls DESC NULLS LAST) as rn_calls1,
        row_number() OVER (ORDER BY f2.calls DESC NULLS LAST) as rn_calls2
    FROM top_functions1 f1
        FULL OUTER JOIN top_functions2 f2 USING (server_id, datid, funcid)
    ORDER BY
      COALESCE(f1.calls, 0) + COALESCE(f2.calls, 0) DESC,
      COALESCE(f1.datid,f2.datid) ASC,
      COALESCE(f1.funcid,f2.funcid) ASC
    ) t1
    WHERE COALESCE(calls1, 0) + COALESCE(calls2, 0) > 0
      AND least(
        rn_calls1,
        rn_calls2
      ) <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr} title="%s">%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
        '</tr>'
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

/* ==== Trigger report functions ==== */

CREATE FUNCTION func_top_trg_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT
        dbname,
        schemaname,
        funcname,
        funcargs,
        NULLIF(calls, 0) as calls,
        NULLIF(total_time, 0.0) as total_time,
        NULLIF(self_time, 0.0) as self_time,
        NULLIF(m_time, 0.0) as m_time,
        NULLIF(m_stime, 0.0) as m_stime
    FROM top_functions(sserver_id, start_id, end_id, true)
    WHERE total_time > 0
    ORDER BY
      total_time DESC,
      datid ASC,
      funcid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
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

CREATE FUNCTION func_top_trg_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    c_fun_stats CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(f1.dbname,f2.dbname) as dbname,
        COALESCE(f1.schemaname,f2.schemaname) as schemaname,
        COALESCE(f1.funcname,f2.funcname) as funcname,
        COALESCE(f1.funcargs,f2.funcargs) as funcargs,
        NULLIF(f1.calls, 0) as calls1,
        NULLIF(f1.total_time, 0.0) as total_time1,
        NULLIF(f1.self_time, 0.0) as self_time1,
        NULLIF(f1.m_time, 0.0) as m_time1,
        NULLIF(f1.m_stime, 0.0) as m_stime1,
        NULLIF(f2.calls, 0) as calls2,
        NULLIF(f2.total_time, 0.0) as total_time2,
        NULLIF(f2.self_time, 0.0) as self_time2,
        NULLIF(f2.m_time, 0.0) as m_time2,
        NULLIF(f2.m_stime, 0.0) as m_stime2,
        row_number() OVER (ORDER BY f1.total_time DESC NULLS LAST) as rn_time1,
        row_number() OVER (ORDER BY f2.total_time DESC NULLS LAST) as rn_time2
    FROM top_functions(sserver_id, start1_id, end1_id, true) f1
        FULL OUTER JOIN top_functions(sserver_id, start2_id, end2_id, true) f2 USING (server_id, datid, funcid)
    ORDER BY
      COALESCE(f1.total_time, 0.0) + COALESCE(f2.total_time, 0.0) DESC,
      COALESCE(f1.datid,f2.datid) ASC,
      COALESCE(f1.funcid,f2.funcid) ASC
    ) t1
    WHERE COALESCE(total_time1, 0.0) + COALESCE(total_time2, 0.0) > 0.0
      AND least(
        rn_time1,
        rn_time2
      ) <= topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Function</th>'
            '<th rowspan="2">I</th>'
            '<th rowspan="2" title="Number of times this function has been called">Executions</th>'
            '<th colspan="4" title="Function execution timing statistics">Time (s)</th>'
          '</tr>'
          '<tr>'
            '<th title="Total time spent in this function and all other functions called by it">Total</th>'
            '<th title="Total time spent in this function itself, not including other functions called by it">Self</th>'
            '<th title="Mean total time per execution">Mean</th>'
            '<th title="Mean self time per execution">Mean self</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr} title="%s">%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
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
          '<td {value}>%s</td>'
        '</tr>'
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
