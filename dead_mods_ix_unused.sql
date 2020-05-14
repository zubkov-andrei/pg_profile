CREATE OR REPLACE FUNCTION tbl_top_dead_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR (n_id integer, e_id integer, cnt integer) FOR
    SELECT
        snap_db.datname AS dbname,
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        n_dead_tup*100/(n_live_tup + n_dead_tup) AS dead_pct,
        last_autovacuum,
        pg_size_pretty(relsize) AS relsize
    FROM v_snap_stat_tables st
        -- Database name
        JOIN snap_stat_database snap_db USING (node_id, snap_id, datid)
    WHERE st.node_id=n_id AND snap_db.datname not like 'template_' AND snap_id = e_id
        -- Min 5 MB in size
        AND st.relsize > 5 * 1024^2
        AND st.n_dead_tup > 0
    ORDER BY n_dead_tup*100/(n_live_tup + n_dead_tup) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Live</th><th>Dead</th><th>%Dead</th><th>Last AV</th><th>Size</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(snode_id, end_id, topn) LOOP
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

CREATE OR REPLACE FUNCTION tbl_top_mods_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR (n_id integer, e_id integer, cnt integer) FOR
    SELECT
        snap_db.datname AS dbname,
        schemaname,
        relname,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze AS mods,
        n_mod_since_analyze*100/(n_live_tup + n_dead_tup) AS mods_pct,
        last_autoanalyze,
        pg_size_pretty(relsize) AS relsize
    FROM v_snap_stat_tables st
        -- Database name and existance condition
        JOIN snap_stat_database snap_db USING (node_id, snap_id, datid)
    WHERE st.node_id = n_id AND snap_db.datname NOT LIKE 'template_' AND snap_id = e_id
        AND st.relkind IN ('r','m')
        -- Min 5 MB in size
        AND relsize > 5 * 1024^2
        AND n_mod_since_analyze > 0
        AND n_live_tup + n_dead_tup > 0
    ORDER BY n_mod_since_analyze*100/(n_live_tup + n_dead_tup) DESC
    LIMIT cnt;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Live</th><th>Dead</th><th>Mods</th><th>%Mod</th><th>Last AA</th><th>Size</th></tr>{rows}</table>',
      'row_tpl','<tr><td>%s</td><td>%s</td><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting vacuum stats
    FOR r_result IN c_tbl_stats(snode_id, end_id, topn) LOOP
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
