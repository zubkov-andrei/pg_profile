/* ===== Main report function ===== */

CREATE FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    topn        integer;
    stmt_all_cnt    integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile report {samples}</title></head><body><H1>Postgres profile report {samples}</H1>'
    '<p>{pg_profile} version {pgprofile_version}</p>'
    '<p>Server name: <strong>{server_name}</strong></p>'
    '<p>Report interval: <strong>{report_start} - {report_end}</strong></p>'
    '{report_description}{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} '
    'table tr td.value, table tr td.mono {font-family: Monospace;} '
    'table tr td.value {text-align: right;} '
    'table p {margin: 0.2em;}'
    'table tr.parent td:not(.relhdr) {background-color: #D8E8C2;} '
    'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} '
    'table tr:nth-child(even) {background-color: #eee;} '
    'table tr:nth-child(odd) {background-color: #fff;} '
    'table tr:hover td:not(.relhdr) {background-color:#d9ffcc} '
    'table th {color: black; background-color: #ffcc99;}'
    'table tr:target td {background-color: #EBEDFF;}'
    'table tr:target td:first-of-type {font-weight: bold;}';
    description_tpl CONSTANT text := '<h2>Report description</h2><p>{description_text}</p>';
    --Cursor and variable for checking existance of samples
    c_sample CURSOR (csample_id integer) FOR SELECT * FROM samples WHERE server_id = sserver_id AND sample_id = csample_id;
    sample_rec samples%rowtype;
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

    -- {pg_profile} version
    IF (SELECT count(*) = 1 FROM pg_catalog.pg_extension WHERE extname = '{pg_profile}') THEN
      SELECT extversion INTO STRICT r_result FROM pg_catalog.pg_extension WHERE extname = '{pg_profile}';
      report := replace(report,'{pgprofile_version}',r_result.extversion);
    ELSE
      report := replace(report,'{pgprofile_version}','{extension_version}');
    END IF;

    -- Server name substitution
    SELECT server_name INTO STRICT r_result FROM servers WHERE server_id = sserver_id;
    report := replace(report,'{server_name}',r_result.server_name);

    -- Getting TopN setting
    BEGIN
        topn := current_setting('{pg_profile}.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Checking sample existance, header generation
    OPEN c_sample(start_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'Start sample % does not exists', start_id;
        END IF;
        report := replace(report,'{report_start}',sample_rec.sample_time::timestamp(0) without time zone::text);
        tmp_text := '(StartID: ' || sample_rec.sample_id ||', ';
    CLOSE c_sample;

    OPEN c_sample(end_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'End sample % does not exists', end_id;
        END IF;
        report := replace(report,'{report_end}',sample_rec.sample_time::timestamp(0) without time zone::text);
        tmp_text := tmp_text || 'EndID: ' || sample_rec.sample_id ||')';
    CLOSE c_sample;
    report := replace(report,'{samples}',tmp_text);
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
      'statstatements',profile_checkavail_statstatements(sserver_id, start_id, end_id),
      'statstatements_v1.8',profile_checkavail_statements_v18(sserver_id, start_id, end_id),
      'kcachestatements',profile_checkavail_kcachestatements(sserver_id,start_id,end_id)
    ));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(sserver_id, start_id, end_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>This interval contains sample(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    -- pg_stat_statements.tarck warning
    stmt_all_cnt := check_stmt_all_setting(sserver_id, start_id, end_id);
    tmp_report := '';
    IF stmt_all_cnt > 0 THEN
        tmp_report := 'Report includes '||stmt_all_cnt||' sample(s) with setting <i>pg_stat_statements.track = all</i>.'||
        'Value of %Total columns may be incorrect.';
    END IF;
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b>'||tmp_report||'</p>';
    END IF;

    -- Table of Contents
    tmp_text := tmp_text ||'<H2>Report sections</H2><ul>';
    tmp_text := tmp_text || '<li><a HREF=#cl_stat>Server statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#db_stat>Database statistics</a></li>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#st_stat>Statement statistics by database</a></li>';
    END IF;
    tmp_text := tmp_text || '<li><a HREF=#clu_stat>Cluster statistics</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#tablespace_stat>Tablespace statistics</a></li>';
    tmp_text := tmp_text || '</ul>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#sql_stat>SQL Query statistics</a></li>';
      tmp_text := tmp_text || '<ul>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#top_ela>Top SQL by elapsed time</a></li>';
        tmp_text := tmp_text || '<li><a HREF=#top_plan>Top SQL by planning time</a></li>';
      END IF;
      tmp_text := tmp_text || '<li><a HREF=#top_exec>Top SQL by execution time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_calls>Top SQL by executions</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_iowait>Top SQL by I/O wait time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_pgs_fetched>Top SQL by shared blocks fetched</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_reads>Top SQL by shared blocks read</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_dirtied>Top SQL by shared blocks dirtied</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_written>Top SQL by shared blocks written</a></li>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#top_wal_bytes>Top SQL by WAL size</a></li>';
      END IF;
      tmp_text := tmp_text || '<li><a HREF=#top_temp>Top SQL by temp usage</a></li>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#kcache_stat>rusage statistics</a></li>';
        tmp_text := tmp_text || '<ul>';
        tmp_text := tmp_text || '<li><a HREF=#kcache_time>Top SQL by system and user time </a></li>';
        tmp_text := tmp_text || '<li><a HREF=#kcache_reads_writes>Top SQL by reads/writes done by filesystem layer </a></li>';
        tmp_text := tmp_text || '</ul>';
      END IF;
      -- SQL texts
      tmp_text := tmp_text || '<li><a HREF=#sql_list>Complete list of SQL texts</a></li>';
      tmp_text := tmp_text || '</ul>';
    END IF;

    tmp_text := tmp_text || '<li><a HREF=#schema_stat>Schema object statisctics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#scanned_tbl>Top tables by estimated number of sequentially scanned blocks</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#fetch_tbl>Top tables by blocks fetched</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#read_tbl>Top tables by blocks read</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#dml_tbl>Top DML tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#vac_tbl>Top tables by Delete/Update operations</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_tbl>Top growing tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#fetch_idx>Top indexes by blocks fetched</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#read_idx>Top indexes by blocks read</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_idx>Top growing indexes</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#ix_unused>Unused indexes</a></li>';
    tmp_text := tmp_text || '</ul>';

    tmp_text := tmp_text || '<li><a HREF=#func_stat>User function statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#funcs_time_stat>Top functions by total time</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#funcs_calls_stat>Top functions by executions</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#trg_funcs_time_stat>Top trigger functions by total time</a></li>';
    tmp_text := tmp_text || '</ul>';


    tmp_text := tmp_text || '<li><a HREF=#vacuum_stats>Vacuum-related statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#top_vacuum_cnt_tbl>Top tables by vacuum operations</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_analyze_cnt_tbl>Top tables by analyze operations</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_ix_vacuum_bytes_cnt_tbl>Top indexes by estimated vacuum I/O load</a></li>';

    tmp_text := tmp_text || '<li><a HREF=#dead_tbl>Top tables by dead tuples ratio</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#mod_tbl>Top tables by modified tuples ratio</a></li>';
    tmp_text := tmp_text || '</ul>';
    tmp_text := tmp_text || '<li><a HREF=#pg_settings>Cluster settings during the report interval</a></li>';
    tmp_text := tmp_text || '</ul>';


    --Reporting cluster stats
    tmp_text := tmp_text || '<H2><a NAME=cl_stat>Server statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=db_stat>Database statistics</a></H3>';
    tmp_report := dbstats_reset_htbl(jreportset, sserver_id, start_id, end_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Database statistics reset detected during report period!</p>'||tmp_report||
        '<p>Statistics for listed databases and contained objects might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(dbstats_htbl(jreportset, sserver_id, start_id, end_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=st_stat>Statement statistics by database</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(statements_stats_htbl(jreportset, sserver_id, start_id, end_id, topn));
    END IF;

    tmp_text := tmp_text || '<H3><a NAME=clu_stat>Cluster statistics</a></H3>';
    tmp_report := cluster_stats_reset_htbl(jreportset, sserver_id, start_id, end_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Cluster statistics reset detected during report period!</p>'||tmp_report||
        '<p>Cluster statistics might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_htbl(jreportset, sserver_id, start_id, end_id));

    tmp_text := tmp_text || '<H3><a NAME=tablespace_stat>Tablespace statistics</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tablespaces_stats_htbl(jreportset, sserver_id, start_id, end_id));

    --Reporting on top queries by elapsed time
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H2><a NAME=sql_stat>SQL Query statistics</a></H2>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_elapsed_htbl(jreportset, sserver_id, start_id, end_id, topn));
        tmp_text := tmp_text || '<H3><a NAME=top_plan>Top SQL by planning time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_plan_time_htbl(jreportset, sserver_id, start_id, end_id, topn));
      END IF;
      tmp_text := tmp_text || '<H3><a NAME=top_exec>Top SQL by execution time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_time_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by executions
      tmp_text := tmp_text || '<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by I/O wait time
      tmp_text := tmp_text || '<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_iowait_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by fetched blocks
      tmp_text := tmp_text || '<H3><a NAME=top_pgs_fetched>Top SQL by shared blocks fetched</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_blks_fetched_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by shared reads
      tmp_text := tmp_text || '<H3><a NAME=top_shared_reads>Top SQL by shared blocks read</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_reads_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by shared dirtied
      tmp_text := tmp_text || '<H3><a NAME=top_shared_dirtied>Top SQL by shared blocks dirtied</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_dirtied_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by shared written
      tmp_text := tmp_text || '<H3><a NAME=top_shared_written>Top SQL by shared blocks written</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_written_htbl(jreportset, sserver_id, start_id, end_id, topn));

      -- Reporting on top queries by WAL bytes
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements_v1.8')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=top_wal_bytes>Top SQL by WAL size</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_wal_size_htbl(jreportset, sserver_id, start_id, end_id, topn));
      END IF;

      -- Reporting on top queries by temp usage
      tmp_text := tmp_text || '<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_temp_htbl(jreportset, sserver_id, start_id, end_id, topn));

      --Kcache section
     IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean THEN
      -- Reporting kcache queries
        tmp_text := tmp_text||'<H3><a NAME=kcache_stat>rusage statistics</a></H3>';
        tmp_text := tmp_text||'<H4><a NAME=kcache_time>Top SQL by system and user time </a></H4>';
        tmp_text := tmp_text || nodata_wrapper(top_cpu_time_htbl(jreportset, sserver_id, start_id, end_id, topn));
        tmp_text := tmp_text||'<H4><a NAME=kcache_reads_writes>Top SQL by reads/writes done by filesystem layer </a></H4>';
        tmp_text := tmp_text || nodata_wrapper(top_io_filesystem_htbl(jreportset, sserver_id, start_id, end_id, topn));
     END IF;

      -- Listing queries
      tmp_text := tmp_text || '<H3><a NAME=sql_list>Complete list of SQL texts</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(report_queries(jreportset));
    END IF;

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text || '<H2><a NAME=schema_stat>Schema object statisctics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=scanned_tbl>Top tables by estimated number of sequentially scanned blocks</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=fetch_tbl>Top tables by blocks fetched</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_fetch_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=read_tbl>Top tables by blocks read</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=vac_tbl>Top tables by Delete/Update operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_upd_vac_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=growth_tbl>Top growing tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=fetch_idx>Top indexes by blocks fetched</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_fetch_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=read_idx>Top indexes by blocks read</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_io_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=growth_idx>Top growing indexes</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_indexes_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=ix_unused>Unused indexes</a></H3>';
    tmp_text := tmp_text || '<p>This table contains non-scanned indexes (during report period), ordered by number of DML operations on underlying tables. Constraint indexes are excluded.</p>';
    tmp_text := tmp_text || nodata_wrapper(ix_unused_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H2><a NAME=func_stat>User function statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=funcs_time_stat>Top functions by total time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_time_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=funcs_calls_stat>Top functions by executions</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_calls_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=trg_funcs_time_stat>Top trigger functions by total time</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(func_top_trg_htbl(jreportset, sserver_id, start_id, end_id, topn));

    -- Reporting vacuum related stats
    tmp_text := tmp_text || '<H2><a NAME=vacuum_stats>Vacuum-related statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=top_vacuum_cnt_tbl>Top tables by vacuum operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_vacuumed_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_analyze_cnt_tbl>Top tables by analyze operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_analyzed_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_ix_vacuum_bytes_cnt_tbl>Top indexes by estimated vacuum I/O load</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_vacuumed_indexes_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=dead_tbl>Top tables by dead tuples ratio</a></H3>';
    tmp_text := tmp_text || '<p>Data in this section is not differential. This data is valid for last report sample only.</p>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_dead_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=mod_tbl>Top tables by modified tuples ratio</a></H3>';
    tmp_text := tmp_text || '<p>Table shows modified tuples statistics since last analyze.</p>';
    tmp_text := tmp_text || '<p>Data in this section is not differential. This data is valid for last report sample only.</p>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_mods_htbl(jreportset, sserver_id, start_id, end_id, topn));

    -- Database settings report
    tmp_text := tmp_text || '<H2><a NAME=pg_settings>Cluster settings during the report interval</a></H2>';
    tmp_text := tmp_text || nodata_wrapper(settings_and_changes_htbl(jreportset, sserver_id, start_id, end_id));

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(sserver_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>Sample repository contains samples with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    RETURN replace(report,'{report}',tmp_text);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer, IN description text) IS 'Statistics report generation function. Takes server_id and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN server name, IN start_id integer, IN end_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id, description);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN start_id integer, IN end_id integer, IN description text) IS 'Statistics report generation function. Takes server name and IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN start_id integer, IN end_id integer, IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
  SELECT get_report('local',start_id,end_id,description);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN start_id integer, IN end_id integer, IN description text) IS 'Statistics report generation function for local server. Takes IDs of start and end sample (inclusive).';

CREATE FUNCTION get_report(IN sserver_id integer, IN time_range tstzrange, IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT get_report(sserver_id, start_id, end_id, description)
  FROM get_sampleids_by_timerange(sserver_id, time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN sserver_id integer, IN time_range tstzrange, IN description text) IS 'Statistics report generation function. Takes server ID and time interval.';

CREATE FUNCTION get_report(IN server name, IN time_range tstzrange, IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id, description)
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN time_range tstzrange, IN description text) IS 'Statistics report generation function. Takes server name and time interval.';

CREATE FUNCTION get_report(IN time_range tstzrange, IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT get_report(get_server_by_name('local'), start_id, end_id, description)
  FROM get_sampleids_by_timerange(get_server_by_name('local'), time_range)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN time_range tstzrange, IN description text) IS 'Statistics report generation function for local server. Takes time interval.';

CREATE FUNCTION get_report(IN server name, IN baseline varchar(25), IN description text = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT get_report(get_server_by_name(server), start_id, end_id, description)
  FROM get_baseline_samples(get_server_by_name(server), baseline)
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report(IN server name, IN baseline varchar(25), IN description text) IS 'Statistics report generation function for server baseline. Takes server name and baseline name.';

CREATE FUNCTION get_report(IN baseline varchar(25), IN description text = NULL) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    RETURN get_report('local',baseline,description);
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION get_report(IN baseline varchar(25), IN description text) IS 'Statistics report generation function for local server baseline. Takes baseline name.';

CREATE FUNCTION get_report_latest(IN server name = NULL)
RETURNS text SET search_path=@extschema@,public AS $$
  SELECT get_report(srv.server_id, s.sample_id, e.sample_id, NULL)
  FROM samples s JOIN samples e ON (s.server_id = e.server_id AND s.sample_id = e.sample_id - 1)
    JOIN servers srv ON (e.server_id = srv.server_id AND e.sample_id = srv.last_sample_id)
  WHERE srv.server_name = COALESCE(server, 'local')
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_report_latest(IN server name) IS 'Statistics report generation function for last two samples.';
