/* ===== Main report function ===== */

CREATE OR REPLACE FUNCTION report(IN snode_id integer, IN start_id integer, IN end_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    topn        integer;
    stmt_all_cnt    integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile report {snaps}</title></head><body><H1>Postgres profile report {snaps}</H1>'||
    '<p>pg_profile version: {pgprofile_version}</p>'||
    '<p>Report interval: {report_start} - {report_end}</p>{report_description}{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} '||
    'table tr td.value, table tr td.mono {font-family: Monospace;} '||
    'table tr.parent td:not(.relhdr) {background-color: #D8E8C2;} '||
    'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} '||
    'table tr:nth-child(even) {background-color: #eee;} '||
    'table tr:nth-child(odd) {background-color: #fff;} '||
    'table tr:hover td:not(.relhdr) {background-color:#d9ffcc} '||
    'table th {color: black; background-color: #ffcc99;}'||
    'table tr:target td {background-color: #EBEDFF;}'||
    'table tr:target td:first-of-type {font-weight: bold;}';
    description_tpl CONSTANT text := '<h2>Report description</h2><p>{description_text}</p>';
    --Cursor and variable for checking existance of snapshots
    c_snap CURSOR (snapshot_id integer) FOR SELECT * FROM snapshots WHERE node_id = snode_id AND snap_id = snapshot_id;
    snap_rec snapshots%rowtype;
    jreportset  jsonb;

    r_result RECORD;
BEGIN
    -- Creating temporary table for reported queries
    CREATE TEMPORARY TABLE IF NOT EXISTS queries_list (
      queryid_md5       char(10),
      CONSTRAINT pk_queries_list PRIMARY KEY (queryid_md5))
    ON COMMIT DELETE ROWS;

    -- CSS
    report := replace(report_tpl,'{css}',report_css);

    -- Add provided description
    IF description IS NOT NULL THEN
      report := replace(report,'{report_description}',replace(description_tpl,'{description_text}',description));
    ELSE
      report := replace(report,'{report_description}','');
    END IF;

    -- pg_profile version
    SELECT extversion INTO STRICT r_result FROM pg_catalog.pg_extension WHERE extname='pg_profile';
    report := replace(report,'{pgprofile_version}',r_result.extversion);

    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Checking snapshot existance, header generation
    OPEN c_snap(start_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'Start snapshot % does not exists', start_id;
        END IF;
        report := replace(report,'{report_start}',snap_rec.snap_time::timestamp(0) without time zone::text);
        tmp_text := '(StartID: ' || snap_rec.snap_id ||', ';
    CLOSE c_snap;

    OPEN c_snap(end_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'End snapshot % does not exists', end_id;
        END IF;
        report := replace(report,'{report_end}',snap_rec.snap_time::timestamp(0) without time zone::text);
        tmp_text := tmp_text || 'EndID: ' || snap_rec.snap_id ||')';
    CLOSE c_snap;
    report := replace(report,'{snaps}',tmp_text);
    tmp_text := '';

    -- Populate report settings
    jreportset := jsonb_build_object(
    'htbl',jsonb_build_object(
      'reltr','class="parent"',
      'toasttr','class="child"',
      'reltdhdr','class="relhdr"',
      'value','class="value"',
      'mono','class="mono"',
      'reltdspanhdr','rowspan="2" class="relhdr"'
    ),
    'report_features',jsonb_build_object(
      'statstatements',profile_checkavail_statstatements(snode_id, start_id, end_id)
    ));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(snode_id, start_id, end_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>This interval contains snapshot(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    -- pg_stat_statements.tarck warning
    stmt_all_cnt := check_stmt_all_setting(snode_id, start_id, end_id);
    tmp_report := '';
    IF stmt_all_cnt > 0 THEN
        tmp_report := 'Report includes '||stmt_all_cnt||' snapshot(s) with setting <i>pg_stat_statements.track = all</i>.'||
        'Value of %Total columns may be incorrect.';
    END IF;
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b>'||tmp_report||'</p>';
    END IF;

    -- Table of Contents
    tmp_text := tmp_text ||'<H2>Report sections</H2><ul>';
    tmp_text := tmp_text || '<li><a HREF=#cl_stat>Cluster statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#db_stat>Databases statistics</a></li>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#st_stat>Statements statistics by database</a></li>';
    END IF;
    tmp_text := tmp_text || '<li><a HREF=#clu_stat>Cluster statistics</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#tablespace_stat>Tablespaces statistics</a></li>';
    tmp_text := tmp_text || '</ul>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#sql_stat>SQL Query statistics</a></li>';
      tmp_text := tmp_text || '<ul>';
      tmp_text := tmp_text || '<li><a HREF=#top_ela>Top SQL by elapsed time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_calls>Top SQL by executions</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_iowait>Top SQL by I/O wait time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_gets>Top SQL by gets</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_reads>Top SQL by shared reads</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_dirtied>Top SQL by shared dirtied</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_written>Top SQL by shared written</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_temp>Top SQL by temp usage</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#sql_list>Complete List of SQL Text</a></li>';
      tmp_text := tmp_text || '</ul>';
    END IF;

    tmp_text := tmp_text || '<li><a HREF=#schema_stat>Schema objects statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#scanned_tbl>Most scanned tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#gets_tbl>Top tables by gets</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#dml_tbl>Top DML tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#vac_tbl>Top Delete/Update tables with vacuum run count</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_tbl>Top growing tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#gets_idx>Top indexes by gets</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_idx>Top growing indexes</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#ix_unused>Unused indexes</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '<li><a HREF=#io_stat>I/O Schema objects statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#tbl_io_stat>Top tables by read I/O</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#ix_io_stat>Top indexes by read I/O</a></li>';
    tmp_text := tmp_text || '</ul>';

    tmp_text := tmp_text || '<li><a HREF=#func_stat>User function statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#funs_time_stat>Top functions by total time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#funs_calls_stat>Top functions by executions</a></li>';
    tmp_text := tmp_text || '</ul>';


    tmp_text := tmp_text || '<li><a HREF=#vacuum_stats>Vacuum related statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#dead_tbl>Tables ordered by dead tuples ratio</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#mod_tbl>Tables ordered by modified tuples ratio</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '<li><a HREF=#pg_settings>Cluster settings during report interval</a></li>';
    tmp_text := tmp_text || '</ul>';


    --Reporting cluster stats
    tmp_text := tmp_text || '<H2><a NAME=cl_stat>Cluster statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=db_stat>Databases statistics</a></H3>';
    tmp_report := dbstats_reset_htbl(jreportset, snode_id, start_id, end_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Database statistics reset detected during report period!</p>'||tmp_report||
        '<p>Statistics for listed databases and contained objects might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(dbstats_htbl(jreportset, snode_id, start_id, end_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=st_stat>Statements statistics by database</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(statements_stats_htbl(jreportset, snode_id, start_id, end_id, topn));
    END IF;

    tmp_text := tmp_text || '<H3><a NAME=clu_stat>Cluster statistics</a></H3>';
    tmp_report := cluster_stats_reset_htbl(jreportset, snode_id, start_id, end_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Cluster statistics reset detected during report period!</p>'||tmp_report||
        '<p>Cluster statistics might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_htbl(jreportset, snode_id, start_id, end_id));

    tmp_text := tmp_text || '<H3><a NAME=tablespace_stat>Tablespaces statistics</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tablespaces_stats_htbl(jreportset, snode_id, start_id, end_id));

    --Reporting on top queries by elapsed time
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H2><a NAME=sql_stat>SQL Query statistics</a></H2>';
      tmp_text := tmp_text || '<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_elapsed_htbl(jreportset, snode_id, start_id, end_id, topn));

      -- Reporting on top queries by executions
      tmp_text := tmp_text || '<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_htbl(jreportset, snode_id, start_id, end_id, topn));

      -- Reporting on top queries by I/O wait time
      tmp_text := tmp_text || '<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_iowait_htbl(jreportset, snode_id, start_id, end_id, topn));

      -- Reporting on top queries by gets
      tmp_text := tmp_text || '<H3><a NAME=top_gets>Top SQL by gets</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_gets_htbl(jreportset, snode_id, start_id, end_id, topn));

      -- Reporting on top queries by shared reads
      tmp_text := tmp_text || '<H3><a NAME=top_shared_reads>Top SQL by shared reads</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_reads_htbl(jreportset, snode_id, start_id, end_id, topn));

      -- Reporting on top queries by shared dirtied
      tmp_text := tmp_text || '<H3><a NAME=top_shared_dirtied>Top SQL by shared dirtied</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_dirtied_htbl(jreportset, snode_id, start_id, end_id, topn));

      -- Reporting on top queries by shared written
      tmp_text := tmp_text || '<H3><a NAME=top_shared_written>Top SQL by shared written</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_written_htbl(jreportset, snode_id, start_id, end_id, topn));

      -- Reporting on top queries by temp usage
      tmp_text := tmp_text || '<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_temp_htbl(jreportset, snode_id, start_id, end_id, topn));

      -- Listing queries
      tmp_text := tmp_text || '<H3><a NAME=sql_list>Complete List of SQL Text</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(report_queries(jreportset));
    END IF;

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text || '<H2><a NAME=schema_stat>Schema objects statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=scanned_tbl>Most seq. scanned tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=gets_tbl>Top tables by gets</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_gets_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=vac_tbl>Top Delete/Update tables with vacuum run count</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_upd_vac_tables_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=growth_tbl>Top growing tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_tables_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=gets_idx>Top indexes by gets</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_gets_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=growth_idx>Top growing indexes</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_indexes_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=ix_unused>Unused indexes</a></H3>';
    tmp_text := tmp_text || '<p>This table contains not-scanned indexes (during report period), ordered by number of DML operations on underlying tables. Constraint indexes are excluded.</p>';
    tmp_text := tmp_text || nodata_wrapper(ix_unused_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=io_stat>I/O Schema objects statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=tbl_io_stat>Top tables by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=ix_io_stat>Top indexes by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_io_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=func_stat>User function statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=funs_time_stat>Top functions by total time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_time_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=funs_calls_stat>Top functions by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_calls_htbl(jreportset, snode_id, start_id, end_id, topn));

    -- Reporting vacuum related stats
    tmp_text := tmp_text || '<H2><a NAME=vacuum_stats>Vacuum related statistics</a></H2>';
    tmp_text := tmp_text || '<p>Data in this section is not differential. This data is valid for ending snapshot only.</p>';
    tmp_text := tmp_text || '<H3><a NAME=dead_tbl>Tables ordered by dead tuples ratio</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_dead_htbl(jreportset, snode_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=mod_tbl>Tables ordered by modified tuples ratio</a></H3>';
    tmp_text := tmp_text || '<p>Table shows modified tuples statistics since last analyze.</p>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_mods_htbl(jreportset, snode_id, start_id, end_id, topn));

    -- Database settings report
    tmp_text := tmp_text || '<H2><a NAME=pg_settings>Cluster settings during report interval</a></H2>';
    tmp_text := tmp_text || nodata_wrapper(settings_and_changes_htbl(jreportset, snode_id, start_id, end_id));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(snode_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>Snapshot repository contains snapshots with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    RETURN replace(report,'{report}',tmp_text);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION report(IN snode_id integer, IN start_id integer, IN end_id integer, IN description text) IS 'Statistics report generation function. Takes node_id and IDs of start and end snapshot (inclusive).';

CREATE OR REPLACE FUNCTION report(IN node name, IN start_id integer, IN end_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report(get_node_by_name(node), start_id, end_id, description);
$$ LANGUAGE sql;
COMMENT ON FUNCTION report(IN node name, IN start_id integer, IN end_id integer, IN description text) IS 'Statistics report generation function. Takes node name and IDs of start and end snapshot (inclusive).';

CREATE OR REPLACE FUNCTION report(IN start_id integer, IN end_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report('local',start_id,end_id,description);
$$ LANGUAGE sql;
COMMENT ON FUNCTION report(IN start_id integer, IN end_id integer, IN description text) IS 'Statistics report generation function for local node. Takes IDs of start and end snapshot (inclusive).';

CREATE OR REPLACE FUNCTION report(IN snode_id integer, IN time_range tstzrange, IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report(snode_id, start_id, end_id, description)
  FROM get_snapids_by_timerange(snode_id, time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION report(IN snode_id integer, IN time_range tstzrange, IN description text) IS 'Statistics report generation function. Takes node ID and time interval.';

CREATE OR REPLACE FUNCTION report(IN node name, IN time_range tstzrange, IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report(get_node_by_name(node), start_id, end_id, description)
  FROM get_snapids_by_timerange(get_node_by_name(node), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION report(IN node name, IN time_range tstzrange, IN description text) IS 'Statistics report generation function. Takes node name and time interval.';

CREATE OR REPLACE FUNCTION report(IN time_range tstzrange, IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report(get_node_by_name('local'), start_id, end_id, description)
  FROM get_snapids_by_timerange(get_node_by_name('local'), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION report(IN time_range tstzrange, IN description text) IS 'Statistics report generation function for local node. Takes time interval.';

CREATE OR REPLACE FUNCTION report(IN node name, IN baseline varchar(25), IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report(get_node_by_name(node), start_id, end_id, description)
  FROM get_baseline_snapshots(get_node_by_name(node), baseline)
$$ LANGUAGE sql;
COMMENT ON FUNCTION report(IN node name, IN baseline varchar(25), IN description text) IS 'Statistics report generation function for node baseline. Takes node name and baseline name.';

CREATE OR REPLACE FUNCTION report(IN baseline varchar(25), IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    RETURN report('local',baseline,description);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION report(IN baseline varchar(25), IN description text) IS 'Statistics report generation function for local node baseline. Takes baseline name.';
