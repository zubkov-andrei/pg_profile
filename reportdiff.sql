/* ===== Differential report functions ===== */

CREATE OR REPLACE FUNCTION report_diff(IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    i1_title    text;
    i2_title    text;
    topn        integer;
    stmt_all_cnt    integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile differential report {snaps}</title></head><body><H1>Postgres profile differential report {snaps}</H1><p>First interval (1): {i1_title}</p><p>Second interval (2): {i2_title}</p>{report_description}{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} '||
    'table .value, table .mono {font-family: Monospace;} '||
    '.int1 td:not(.hdr), td.int1 {background-color: #FFEEEE;} '||
    '.int2 td:not(.hdr), td.int2 {background-color: #EEEEFF;} '||
    'table.diff tr.int2 td {border-top: hidden;} '||
    'table tr:nth-child(even) {background-color: #eee;} '||
    'table tr:nth-child(odd) {background-color: #fff;} '||
    'table tr:hover td:not(.hdr) {background-color:#d9ffcc} '||
    'table th {color: black; background-color: #ffcc99;}'||
    '.label {color: grey;}'||
    'table tr:target td {background-color: #EBEDFF;}'||
    'table tr:target td:first-of-type {font-weight: bold;}'||
    'table tr.parent td {background-color: #D8E8C2;} '||
    'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} ';
    description_tpl CONSTANT text := '<h2>Report description</h2><p>{description_text}</p>';
    --Cursor and variable for checking existance of snapshots
    c_snap CURSOR (snapshot_id integer) FOR SELECT * FROM snapshots WHERE node_id = snode_id AND snap_id = snapshot_id;
    snap_rec snapshots%rowtype;
    jreportset  jsonb;
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

    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Checking snapshot existance, header generation
    OPEN c_snap(start1_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'Start snapshot % does not exists', start_id;
        END IF;
        i1_title := snap_rec.snap_time::timestamp(0) without time zone::text|| ' - ';
        tmp_text := '(1): [' || snap_rec.snap_id ||' - ';
    CLOSE c_snap;

    OPEN c_snap(end1_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'End snapshot % does not exists', end_id;
        END IF;
        i1_title := i1_title||snap_rec.snap_time::timestamp(0) without time zone::text;
        tmp_text := tmp_text || snap_rec.snap_id ||'] with ';
    CLOSE c_snap;

    OPEN c_snap(start2_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'Start snapshot % does not exists', start_id;
        END IF;
        i2_title := snap_rec.snap_time::timestamp(0) without time zone::text|| ' - ';
        tmp_text := tmp_text|| '(2): [' || snap_rec.snap_id ||' - ';
    CLOSE c_snap;

    OPEN c_snap(end2_id);
        FETCH c_snap INTO snap_rec;
        IF snap_rec IS NULL THEN
            RAISE 'End snapshot % does not exists', end_id;
        END IF;
        i2_title := i2_title||snap_rec.snap_time::timestamp(0) without time zone::text;
        tmp_text := tmp_text || snap_rec.snap_id ||']';
    CLOSE c_snap;
    report := replace(report,'{snaps}',tmp_text);
    tmp_text := '';

    -- Insert report intervals
    report := replace(report,'{i1_title}',i1_title);
    report := replace(report,'{i2_title}',i2_title);

    -- Populate report settings
    jreportset := jsonb_build_object(
    'htbl',jsonb_build_object(
      'value','class="value"',
      'interval1','class="int1"',
      'interval2','class="int2"',
      'label','class="label"',
      'difftbl','class="diff"',
      'rowtdspanhdr','rowspan="2" class="hdr"',
      'rowtdspanhdr_mono','rowspan="2" class="hdr mono"',
      'title1',format('title="%s"',i1_title),
      'title2',format('title="%s"',i2_title)
      ),
    'report_features',jsonb_build_object(
      'statstatements',profile_checkavail_statstatements(snode_id, start1_id, end1_id) OR profile_checkavail_statstatements(snode_id, start2_id, end2_id)
    ));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(snode_id, start1_id, end1_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>Interval (1) contains snapshot(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;
    tmp_report := check_stmt_cnt(snode_id, start2_id, end2_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p>Interval (2) contains snapshot(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    -- pg_stat_statements.track warning
    tmp_report := '';
    stmt_all_cnt := check_stmt_all_setting(snode_id, start1_id, end1_id);
    IF stmt_all_cnt > 0 THEN
        tmp_report := tmp_report||'<p>Interval (1) includes '||stmt_all_cnt||' snapshot(s) with setting <i>pg_stat_statements.track = all</i>. '||
        'Value of %Total columns may be incorrect.</p>';
    END IF;
    stmt_all_cnt := check_stmt_all_setting(snode_id, start2_id, end2_id);
    IF stmt_all_cnt > 0 THEN
        tmp_report := tmp_report||'Interval (2) includes '||stmt_all_cnt||' snapshot(s) with setting <i>pg_stat_statements.track = all</i>. '||
        'Value of %Total columns may be incorrect.';
    END IF;
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b></p>'||tmp_report;
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
     tmp_text := tmp_text || '<li><a HREF=#tablespace_stat>Tablespace statistics</a></li>';
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

    tmp_text := tmp_text || '<li><a HREF=#pg_settings>Cluster settings during report interval</a></li>';

    tmp_text := tmp_text || '</ul>';


    --Reporting cluster stats
    tmp_text := tmp_text || '<H2><a NAME=cl_stat>Cluster statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=db_stat>Databases statistics</a></H3>';
    tmp_report := dbstats_reset_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Database statistics reset detected during report period!</p>'||tmp_report||
        '<p>Statistics for listed databases and contained objects might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(dbstats_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=st_stat>Statements statistics by database</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(statements_stats_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));
    END IF;

    tmp_text := tmp_text || '<H3><a NAME=clu_stat>Cluster statistics</a></H3>';
    tmp_report := cluster_stats_reset_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Cluster statistics reset detected during report period!</p>'||tmp_report||
        '<p>Cluster statistics might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id));

    tmp_text := tmp_text || '<H3><a NAME=tablespace_stat>Tablespace statistics</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tablespaces_stats_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      --Reporting on top queries by elapsed time
      tmp_text := tmp_text || '<H2><a NAME=sql_stat>SQL Query statistics</a></H2>';
      tmp_text := tmp_text || '<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_elapsed_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));
      -- Reporting on top queries by executions
      tmp_text := tmp_text || '<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by I/O wait time
      tmp_text := tmp_text || '<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_iowait_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by gets
      tmp_text := tmp_text || '<H3><a NAME=top_gets>Top SQL by gets</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_gets_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by shared reads
      tmp_text := tmp_text || '<H3><a NAME=top_shared_reads>Top SQL by shared reads</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_reads_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by shared dirtied
      tmp_text := tmp_text || '<H3><a NAME=top_shared_dirtied>Top SQL by shared dirtied</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_dirtied_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by shared written
      tmp_text := tmp_text || '<H3><a NAME=top_shared_written>Top SQL by shared written</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_written_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by temp usage
      tmp_text := tmp_text || '<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_temp_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));
      -- Listing queries
      tmp_text := tmp_text || '<H3><a NAME=sql_list>Complete List of SQL Text</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(report_queries(jreportset));
    END IF;

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text || '<H2><a NAME=schema_stat>Schema objects statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=scanned_tbl>Most seq. scanned tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=gets_tbl>Top tables by gets</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_gets_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=vac_tbl>Top Delete/Update tables with vacuum run count</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_upd_vac_tables_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=growth_tbl>Top growing tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_tables_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=gets_idx>Top indexes by gets</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_gets_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=growth_idx>Top growing indexes</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_indexes_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=io_stat>I/O Schema objects statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=tbl_io_stat>Top tables by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=ix_io_stat>Top indexes by read I/O</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_io_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=func_stat>User function statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=funs_time_stat>Top functions by total time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_time_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=funs_calls_stat>Top functions by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_calls_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id, topn));

    -- Database settings report
    tmp_text := tmp_text || '<H2><a NAME=pg_settings>Cluster settings during report intervals</a></H2>';
    tmp_text := tmp_text || nodata_wrapper(settings_and_changes_diff_htbl(jreportset, snode_id, start1_id, end1_id, start2_id, end2_id));

    report := replace(report,'{report}',tmp_text);
    RETURN report;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION report_diff(IN snode_id integer, IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer, IN description text)
IS 'Statistics differential report generation function. Takes node_id and IDs of start and end snapshot for first and second intervals';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff(get_node_by_name(node),start1_id,end1_id,start2_id,end2_id,description);
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN node name, IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer, IN description text)
IS 'Statistics differential report generation function. Takes node name and IDs of start and end snapshot for first and second intervals';

CREATE OR REPLACE FUNCTION report_diff(IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff('local',start1_id,end1_id,start2_id,end2_id,description);
$$ LANGUAGE sql;

COMMENT ON FUNCTION report_diff(IN start1_id integer, IN end1_id integer, IN start2_id integer,
IN end2_id integer, IN description text)
IS 'Statistics differential report generation function for local node. Takes IDs of start and end snapshot for first and second intervals';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN baseline1 varchar(25), IN baseline2 varchar(25),
IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff(get_node_by_name(node),bl1.start_id,bl1.end_id,bl2.start_id,bl2.end_id,description)
  FROM get_baseline_snapshots(get_node_by_name(node), baseline1) bl1
    CROSS JOIN get_baseline_snapshots(get_node_by_name(node), baseline2) bl2
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN node name, IN baseline1 varchar(25), IN baseline2 varchar(25),
IN description text)
IS 'Statistics differential report generation function. Takes node name and two baselines to compare.';

CREATE OR REPLACE FUNCTION report_diff(IN baseline1 varchar(25), IN baseline2 varchar(25),
IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff('local',baseline1,baseline2,description);
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN baseline1 varchar(25), IN baseline2 varchar(25),
IN description text) IS 'Statistics differential report generation function for local node. Takes two baselines to compare.';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN baseline varchar(25), IN start2_id integer,
IN end2_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff(get_node_by_name(node),bl1.start_id,bl1.end_id,start2_id,end2_id,description)
  FROM get_baseline_snapshots(get_node_by_name(node), baseline) bl1
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN node name, IN baseline varchar(25), IN start2_id integer,
IN end2_id integer, IN description text)
IS 'Statistics differential report generation function. Takes node name, reference baseline name as first interval, start and end snapshot_ids of second interval.';

CREATE OR REPLACE FUNCTION report_diff(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer, IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff('local',baseline,start2_id,end2_id,description);
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer, IN description text)
IS 'Statistics differential report generation function for local node. Takes reference baseline name as first interval, start and end snapshot_ids of second interval.';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN start1_id integer, IN end1_id integer,
IN baseline varchar(25), IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff(get_node_by_name(node),start1_id,end1_id,bl2.start_id,bl2.end_id,description)
  FROM get_baseline_snapshots(get_node_by_name(node), baseline) bl2
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN node name, IN start1_id integer, IN end1_id integer,
IN baseline varchar(25), IN description text)
IS 'Statistics differential report generation function. Takes node name, start and end snapshot_ids of first interval and reference baseline name as second interval.';

CREATE OR REPLACE FUNCTION report_diff(IN start1_id integer, IN end1_id integer,
IN baseline varchar(25), IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff('local',start1_id,end1_id,baseline,description);
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer, IN description text)
IS 'Statistics differential report generation function for local node. Takes start and end snapshot_ids of first interval and reference baseline name as second interval.';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN time_range1 tstzrange,
  IN time_range2 tstzrange,
  IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff(get_node_by_name(node),tm1.start_id,tm1.end_id,tm2.start_id,tm2.end_id,description)
  FROM get_snapids_by_timerange(get_node_by_name(node), time_range1) tm1
    CROSS JOIN get_snapids_by_timerange(get_node_by_name(node), time_range2) tm2
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN node name, IN time_range1 tstzrange, IN time_range2 tstzrange,
IN description text)
IS 'Statistics differential report generation function. Takes node name and two time intervals to compare.';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN baseline varchar(25),
  IN time_range tstzrange,
  IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff(get_node_by_name(node),bl1.start_id,bl1.end_id,tm2.start_id,tm2.end_id,description)
  FROM get_baseline_snapshots(get_node_by_name(node), baseline) bl1
    CROSS JOIN get_snapids_by_timerange(get_node_by_name(node), time_range) tm2
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN node name, IN baseline varchar(25), IN time_range tstzrange,
IN description text)
IS 'Statistics differential report generation function. Takes node name, baseline and time interval to compare.';

CREATE OR REPLACE FUNCTION report_diff(IN node name, IN time_range tstzrange,
  IN baseline varchar(25),
  IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT report_diff(get_node_by_name(node),tm1.start_id,tm1.end_id,bl2.start_id,bl2.end_id,description)
  FROM get_baseline_snapshots(get_node_by_name(node), baseline) bl2
    CROSS JOIN get_snapids_by_timerange(get_node_by_name(node), time_range) tm1
$$ LANGUAGE sql;
COMMENT ON FUNCTION report_diff(IN node name, IN time_range tstzrange, IN baseline varchar(25),
IN description text)
IS 'Statistics differential report generation function. Takes node name, time interval and baseline to compare.';
