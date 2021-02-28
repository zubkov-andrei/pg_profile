CREATE FUNCTION tbl_top_dead_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR (n_id integer, e_id integer, cnt integer) FOR
    SELECT
        sample_db.datname AS dbname,
        schemaname,
        relname,
        NULLIF(n_live_tup, 0) as n_live_tup,
        n_dead_tup as n_dead_tup,
        n_dead_tup * 100 / NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) AS dead_pct,
        last_autovacuum,
        pg_size_pretty(relsize) AS relsize
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
    WHERE st.server_id=n_id AND NOT sample_db.datistemplate AND sample_id = e_id
        -- Min 5 MB in size
        AND st.relsize > 5 * 1024^2
        AND st.n_dead_tup > 0
    ORDER BY n_dead_tup*100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) DESC,
      st.datid ASC, st.relid ASC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>DB</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Estimated number of live rows">Live</th>'
            '<th title="Estimated number of dead rows">Dead</th>'
            '<th title="Dead rows count as a percentage of total rows count">%Dead</th>'
            '<th title="Last autovacuum ran time">Last AV</th>'
            '<th title="Table size without indexes and TOAST">Size</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(sserver_id, end_id, topn) LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_live_tup,
            r_result.n_dead_tup,
            r_result.dead_pct,
            r_result.last_autovacuum,
            r_result.relsize
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION tbl_top_mods_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR (n_id integer, e_id integer, cnt integer) FOR
    SELECT
        sample_db.datname AS dbname,
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze AS mods,
        n_mod_since_analyze*100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) AS mods_pct,
        last_autoanalyze,
        pg_size_pretty(relsize) AS relsize
    FROM v_sample_stat_tables st
        -- Database name and existance condition
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
    WHERE st.server_id = n_id AND NOT sample_db.datistemplate AND sample_id = e_id
        AND st.relkind IN ('r','m')
        -- Min 5 MB in size
        AND relsize > 5 * 1024^2
        AND n_mod_since_analyze > 0
        AND n_live_tup + n_dead_tup > 0
    ORDER BY n_mod_since_analyze*100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) DESC,
      st.datid ASC, st.relid ASC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>DB</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Estimated number of live rows">Live</th>'
            '<th title="Estimated number of dead rows">Dead</th>'
            '<th title="Estimated number of rows modified since this table was last analyzed">Mod</th>'
            '<th title="Modified rows count as a percentage of total rows count">%Mod</th>'
            '<th title="Last autoanalyze ran time">Last AA</th>'
            '<th title="Table size without indexes and TOAST">Size</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(sserver_id, end_id, topn) LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.schemaname,
            r_result.relname,
            r_result.n_live_tup,
            r_result.n_dead_tup,
            r_result.mods,
            r_result.mods_pct,
            r_result.last_autoanalyze,
            r_result.relsize
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
