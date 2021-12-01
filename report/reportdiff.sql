/* ===== Differential report functions ===== */

CREATE FUNCTION get_diffreport(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    i1_title    text;
    i2_title    text;
    topn        integer;
    qlen_limit  integer;
    stmt_all_cnt    integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile differential report {samples}</title></head><body><H1>Postgres profile differential report {samples}</H1>'
    '<p>{pg_profile} version {pgprofile_version}</p>'
    '<p>Server name: <strong>{server_name}</strong></p>'
    '{server_description}'
    '<p>First interval (1): <strong>{i1_title}</strong></p>'
    '<p>Second interval (2): <strong>{i2_title}</strong></p>'
    '{report_description}{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} '
    'table .value, table .mono {font-family: Monospace;} '
    'table .value {text-align: right;} '
    'table p {margin: 0.2em;}'
    '.int1 td:not(.hdr), td.int1 {background-color: #FFEEEE;} '
    '.int2 td:not(.hdr), td.int2 {background-color: #EEEEFF;} '
    'table.diff tr.int2 td {border-top: hidden;} '
    'table.stat tr:nth-child(even), table.setlist tr:nth-child(even) {background-color: #eee;} '
    'table.stat tr:nth-child(odd), table.setlist tr:nth-child(odd) {background-color: #fff;} '
    'table tr:hover td:not(.hdr) {background-color:#d9ffcc} '
    'table th {color: black; background-color: #ffcc99;}'
    '.label {color: grey;}'
    'table tr:target,td:target {border: solid; border-width: medium; border-color: limegreen;}'
    'table tr:target td:first-of-type, table td:target {font-weight: bold;}'
    'table tr.parent td {background-color: #D8E8C2;} '
    'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} ';
    description_tpl CONSTANT text := '<h2>Report description</h2><p>{description_text}</p>';
    --Cursor and variable for checking existance of samples
    c_sample CURSOR (csample_id integer) FOR SELECT * FROM samples WHERE server_id = sserver_id AND sample_id = csample_id;
    sample_rec samples%rowtype;
    jreportset  jsonb;

    r_result RECORD;
BEGIN
    -- Interval expanding in case of growth stats requested
    IF with_growth THEN
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start1_id, end1_id
        FROM get_sized_bounds(sserver_id, start1_id, end1_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start1_id, end1_id);
      END;
      BEGIN
        SELECT left_bound, right_bound INTO STRICT start2_id, end2_id
        FROM get_sized_bounds(sserver_id, start2_id, end2_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start2_id, end2_id);
      END;
    END IF;

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

    -- Server name and description substitution
    SELECT server_name,server_description INTO STRICT r_result
    FROM servers WHERE server_id = sserver_id;
    report := replace(report,'{server_name}',r_result.server_name);
    IF r_result.server_description IS NOT NULL AND r_result.server_description != ''
    THEN
      report := replace(report,'{server_description}','<p>'||r_result.server_description||'</p>');
    ELSE
      report := replace(report,'{server_description}','');
    END IF;

    -- Getting TopN setting
    BEGIN
        topn := current_setting('{pg_profile}.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Getting query length limit setting
    BEGIN
        qlen_limit := current_setting('{pg_profile}.max_query_length')::integer;
    EXCEPTION
        WHEN OTHERS THEN qlen_limit := 20000;
    END;

    -- Check if all samples of requested intervals are available
    IF (
      SELECT count(*) != end1_id - start1_id + 1 FROM samples
      WHERE server_id = sserver_id AND sample_id BETWEEN start1_id AND end1_id
    ) THEN
      RAISE 'Not enough samples between %',
        format('%s AND %s', start1_id, end1_id);
    END IF;
    IF (
      SELECT count(*) != end2_id - start2_id + 1 FROM samples
      WHERE server_id = sserver_id AND sample_id BETWEEN start2_id AND end2_id
    ) THEN
      RAISE 'Not enough samples between %',
        format('%s AND %s', start2_id, end2_id);
    END IF;
    -- Checking sample existance, header generation
    OPEN c_sample(start1_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'Start sample % does not exists', start_id;
        END IF;
        i1_title := sample_rec.sample_time::text|| ' - ';
        tmp_text := '(1): [' || sample_rec.sample_id ||' - ';
    CLOSE c_sample;

    OPEN c_sample(end1_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'End sample % does not exists', end_id;
        END IF;
        i1_title := i1_title||sample_rec.sample_time::text;
        tmp_text := tmp_text || sample_rec.sample_id ||'] with ';
    CLOSE c_sample;

    OPEN c_sample(start2_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'Start sample % does not exists', start_id;
        END IF;
        i2_title := sample_rec.sample_time::text|| ' - ';
        tmp_text := tmp_text|| '(2): [' || sample_rec.sample_id ||' - ';
    CLOSE c_sample;

    OPEN c_sample(end2_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'End sample % does not exists', end_id;
        END IF;
        i2_title := i2_title||sample_rec.sample_time::text;
        tmp_text := tmp_text || sample_rec.sample_id ||']';
    CLOSE c_sample;
    report := replace(report,'{samples}',tmp_text);
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
      'stattbl','class="stat"',
      'difftbl','class="stat diff"',
      'rowtdspanhdr','rowspan="2" class="hdr"',
      'rowtdspanhdr_mono','rowspan="2" class="hdr mono"',
      'mono','class="mono"',
      'title1',format('title="%s"',i1_title),
      'title2',format('title="%s"',i2_title)
      ),
    'report_features',jsonb_build_object(
      'statstatements',profile_checkavail_statstatements(sserver_id, start1_id, end1_id) OR
        profile_checkavail_statstatements(sserver_id, start2_id, end2_id),
      'planning_times',profile_checkavail_planning_times(sserver_id, start1_id, end1_id) OR
        profile_checkavail_planning_times(sserver_id, start2_id, end2_id),
      'stmt_io_times',profile_checkavail_stmt_io_times(sserver_id, start1_id, end1_id) OR
        profile_checkavail_stmt_io_times(sserver_id, start2_id, end2_id),
      'statement_wal_bytes',profile_checkavail_stmt_wal_bytes(sserver_id, start1_id, end1_id) OR
        profile_checkavail_stmt_wal_bytes(sserver_id, start2_id, end2_id),
      'wal_stats',profile_checkavail_walstats(sserver_id, start1_id, end1_id) OR
        profile_checkavail_walstats(sserver_id, start2_id, end2_id),
      'sess_stats',profile_checkavail_sessionstats(sserver_id, start1_id, end1_id) OR
        profile_checkavail_sessionstats(sserver_id, start2_id, end2_id),
      'function_stats',profile_checkavail_functions(sserver_id, start1_id, end1_id) OR
        profile_checkavail_functions(sserver_id, start2_id, end2_id),
      'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start1_id, end1_id) OR
        profile_checkavail_trg_functions(sserver_id, start2_id, end2_id),
      'kcachestatements',profile_checkavail_rusage(sserver_id, start1_id, end1_id) OR
        profile_checkavail_rusage(sserver_id, start2_id, end2_id),
      'rusage_planstats',profile_checkavail_rusage_planstats(sserver_id, start1_id, end1_id) OR
        profile_checkavail_rusage_planstats(sserver_id, start2_id, end2_id)
      ),
    'report_properties',jsonb_build_object(
      'interval1_duration_sec',
        (SELECT extract(epoch FROM e.sample_time - s.sample_time)
        FROM samples s JOIN samples e USING (server_id)
        WHERE e.sample_id=end1_id and s.sample_id=start1_id
          AND server_id = sserver_id),
      'interval2_duration_sec',
        (SELECT extract(epoch FROM e.sample_time - s.sample_time)
        FROM samples s JOIN samples e USING (server_id)
        WHERE e.sample_id=end2_id and s.sample_id=start2_id
          AND server_id = sserver_id),
      'max_query_length', qlen_limit
      )
    );

    -- Report internal temporary tables
    -- Creating temporary table for reported queries
    CREATE TEMPORARY TABLE IF NOT EXISTS queries_list (
      userid              oid,
      datid               oid,
      queryid             bigint,
      CONSTRAINT pk_queries_list PRIMARY KEY (userid, datid, queryid))
    ON COMMIT DELETE ROWS;
    /*
    * Caching temporary tables, containing object stats cache
    * used several times in a report functions
    */
    CREATE TEMPORARY TABLE top_statements1 AS
    SELECT * FROM top_statements(sserver_id, start1_id, end1_id);

    /* table size is collected in a sample when relsize field is not null
    In a report we can use relsize-based growth calculated as a sum of
    relsize increments only when sizes was collected
    in the both first and last sample, otherwise we only can use
    pg_class.relpages
    */
    CREATE TEMPORARY TABLE top_tables1 AS
    SELECT tt.*,
      rs.relsize_growth_avail AS relsize_growth_avail,
      CASE WHEN rs.relsize_growth_avail THEN
        tt.growth
      ELSE
        tt.relpagegrowth_bytes
      END AS best_growth,
      rs.relsize_toastgrowth_avail AS relsize_toastgrowth_avail,
      CASE WHEN rs.relsize_toastgrowth_avail THEN
        tt.toastgrowth
      ELSE
        tt.toastrelpagegrowth_bytes
      END AS best_toastgrowth,
      CASE WHEN tt.seqscan_relsize_avail THEN
        tt.seqscan_bytes_relsize
      ELSE
        tt.seqscan_bytes_relpages
      END AS best_seqscan_bytes,
      CASE WHEN tt.t_seqscan_relsize_avail THEN
        tt.t_seqscan_bytes_relsize
      ELSE
        tt.t_seqscan_bytes_relpages
      END AS best_t_seqscan_bytes
    FROM top_tables(sserver_id, start1_id, end1_id) tt
    JOIN (
      SELECT rel.server_id, rel.datid, rel.relid,
          COALESCE(
              max(rel.sample_id) = max(rel.sample_id) FILTER (WHERE rel.relsize IS NOT NULL)
              AND min(rel.sample_id) = min(rel.sample_id) FILTER (WHERE rel.relsize IS NOT NULL)
          , false) AS relsize_growth_avail,
          COALESCE(
              max(reltoast.sample_id) = max(reltoast.sample_id) FILTER (WHERE reltoast.relsize IS NOT NULL)
              AND min(reltoast.sample_id) = min(reltoast.sample_id) FILTER (WHERE reltoast.relsize IS NOT NULL)
          , false) AS relsize_toastgrowth_avail
      FROM sample_stat_tables rel
          JOIN tables_list tl USING (server_id, datid, relid)
          LEFT JOIN sample_stat_tables reltoast ON
              (rel.server_id, rel.sample_id, rel.datid, tl.reltoastrelid) =
              (reltoast.server_id, reltoast.sample_id, reltoast.datid, reltoast.relid)
      WHERE
          rel.server_id = sserver_id
          AND rel.sample_id BETWEEN start1_id AND end1_id
      GROUP BY rel.server_id, rel.datid, rel.relid
    ) rs USING (server_id, datid, relid);

    CREATE TEMPORARY TABLE top_indexes1 AS
    SELECT ti.*,
      rs.relsize_growth_avail AS relsize_growth_avail,
      CASE WHEN rs.relsize_growth_avail THEN
        ti.growth
      ELSE
        ti.relpagegrowth_bytes
      END AS best_growth
    FROM top_indexes(sserver_id, start1_id, end1_id) ti
    JOIN (
      SELECT server_id, datid, indexrelid,
          COALESCE(
              max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL)
              AND min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL)
          , false) AS relsize_growth_avail
      FROM sample_stat_indexes
      WHERE
          server_id = sserver_id
          AND sample_id BETWEEN start1_id AND end1_id
      GROUP BY server_id, datid, indexrelid
    ) rs USING (server_id, datid, indexrelid);

    CREATE TEMPORARY TABLE top_io_tables1 AS
    SELECT * FROM top_io_tables(sserver_id, start1_id, end1_id);
    CREATE TEMPORARY TABLE top_io_indexes1 AS
    SELECT * FROM top_io_indexes(sserver_id, start1_id, end1_id);
    CREATE TEMPORARY TABLE top_functions1 AS
    SELECT * FROM top_functions(sserver_id, start1_id, end1_id, false);
    CREATE TEMPORARY TABLE top_kcache_statements1 AS
    SELECT * FROM top_kcache_statements(sserver_id, start1_id, end1_id);
    CREATE TEMPORARY TABLE top_statements2 AS
    SELECT * FROM top_statements(sserver_id, start2_id, end2_id);

    CREATE TEMPORARY TABLE top_tables2 AS
    SELECT tt.*,
      rs.relsize_growth_avail AS relsize_growth_avail,
      CASE WHEN rs.relsize_growth_avail THEN
        tt.growth
      ELSE
        tt.relpagegrowth_bytes
      END AS best_growth,
      rs.relsize_toastgrowth_avail AS relsize_toastgrowth_avail,
      CASE WHEN rs.relsize_toastgrowth_avail THEN
        tt.toastgrowth
      ELSE
        tt.toastrelpagegrowth_bytes
      END AS best_toastgrowth,
      CASE WHEN tt.seqscan_relsize_avail THEN
        tt.seqscan_bytes_relsize
      ELSE
        tt.seqscan_bytes_relpages
      END AS best_seqscan_bytes,
      CASE WHEN tt.t_seqscan_relsize_avail THEN
        tt.t_seqscan_bytes_relsize
      ELSE
        tt.t_seqscan_bytes_relpages
      END AS best_t_seqscan_bytes
    FROM top_tables(sserver_id, start2_id, end2_id) tt
    JOIN (
      SELECT rel.server_id, rel.datid, rel.relid,
          COALESCE(
              max(rel.sample_id) = max(rel.sample_id) FILTER (WHERE rel.relsize IS NOT NULL)
              AND min(rel.sample_id) = min(rel.sample_id) FILTER (WHERE rel.relsize IS NOT NULL)
          , false) AS relsize_growth_avail,
          COALESCE(
              max(reltoast.sample_id) = max(reltoast.sample_id) FILTER (WHERE reltoast.relsize IS NOT NULL)
              AND min(reltoast.sample_id) = min(reltoast.sample_id) FILTER (WHERE reltoast.relsize IS NOT NULL)
          , false) AS relsize_toastgrowth_avail
      FROM sample_stat_tables rel
          JOIN tables_list tl USING (server_id, datid, relid)
          LEFT JOIN sample_stat_tables reltoast ON
              (rel.server_id, rel.sample_id, rel.datid, tl.reltoastrelid) =
              (reltoast.server_id, reltoast.sample_id, reltoast.datid, reltoast.relid)
      WHERE
          rel.server_id = sserver_id
          AND rel.sample_id BETWEEN start2_id AND end2_id
      GROUP BY rel.server_id, rel.datid, rel.relid
    ) rs USING (server_id, datid, relid);

    CREATE TEMPORARY TABLE top_indexes2 AS
    SELECT ti.*,
      rs.relsize_growth_avail AS relsize_growth_avail,
      CASE WHEN rs.relsize_growth_avail THEN
        ti.growth
      ELSE
        ti.relpagegrowth_bytes
      END AS best_growth
    FROM top_indexes(sserver_id, start2_id, end2_id) ti
    JOIN (
      SELECT server_id, datid, indexrelid,
          COALESCE(
              max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL)
              AND min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL)
          , false) AS relsize_growth_avail
      FROM sample_stat_indexes
      WHERE
          server_id = sserver_id
          AND sample_id BETWEEN start2_id AND end2_id
      GROUP BY server_id, datid, indexrelid
    ) rs USING (server_id, datid, indexrelid);

    CREATE TEMPORARY TABLE top_io_tables2 AS
    SELECT * FROM top_io_tables(sserver_id, start2_id, end2_id);
    CREATE TEMPORARY TABLE top_io_indexes2 AS
    SELECT * FROM top_io_indexes(sserver_id, start2_id, end2_id);
    CREATE TEMPORARY TABLE top_functions2 AS
    SELECT * FROM top_functions(sserver_id, start2_id, end2_id, false);
    CREATE TEMPORARY TABLE top_kcache_statements2 AS
    SELECT * FROM top_kcache_statements(sserver_id, start2_id, end2_id);
    ANALYZE top_statements1;
    ANALYZE top_tables1;
    ANALYZE top_indexes1;
    ANALYZE top_io_tables1;
    ANALYZE top_io_indexes1;
    ANALYZE top_functions1;
    ANALYZE top_kcache_statements1;
    ANALYZE top_statements2;
    ANALYZE top_tables2;
    ANALYZE top_indexes2;
    ANALYZE top_io_tables2;
    ANALYZE top_io_indexes2;
    ANALYZE top_functions2;
    ANALYZE top_kcache_statements2;

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(sserver_id, start1_id, end1_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>Interval (1) contains sample(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;
    tmp_report := check_stmt_cnt(sserver_id, start2_id, end2_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p>Interval (2) contains sample(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    -- pg_stat_statements.track warning
    tmp_report := '';
    stmt_all_cnt := check_stmt_all_setting(sserver_id, start1_id, end1_id);
    IF stmt_all_cnt > 0 THEN
        tmp_report := tmp_report||'<p>Interval (1) includes '||stmt_all_cnt||' sample(s) with setting <i>pg_stat_statements.track = all</i>. '||
        'Value of %Total columns may be incorrect.</p>';
    END IF;
    stmt_all_cnt := check_stmt_all_setting(sserver_id, start2_id, end2_id);
    IF stmt_all_cnt > 0 THEN
        tmp_report := tmp_report||'Interval (2) includes '||stmt_all_cnt||' sample(s) with setting <i>pg_stat_statements.track = all</i>. '||
        'Value of %Total columns may be incorrect.';
    END IF;
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b></p>'||tmp_report;
    END IF;

    -- Table of Contents
    tmp_text := tmp_text ||'<H2>Report sections</H2><ul>';
    tmp_text := tmp_text || '<li><a HREF=#cl_stat>Server statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#db_stat>Database statistics</a></li>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'sess_stats')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#db_stat_sessions>Session statistics by database</a></li>';
    END IF;
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#st_stat>Statement statistics by database</a></li>';
    END IF;
    tmp_text := tmp_text || '<li><a HREF=#clu_stat>Cluster statistics</a></li>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'wal_stats')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#wal_stat>WAL statistics</a></li>';
    END IF;
    tmp_text := tmp_text || '<li><a HREF=#tablespace_stat>Tablespace statistics</a></li>';
    tmp_text := tmp_text || '</ul>';
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#sql_stat>SQL query statistics</a></li>';
      tmp_text := tmp_text || '<ul>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#top_ela>Top SQL by elapsed time</a></li>';
        tmp_text := tmp_text || '<li><a HREF=#top_plan>Top SQL by planning time</a></li>';
      END IF;
      tmp_text := tmp_text || '<li><a HREF=#top_exec>Top SQL by execution time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_calls>Top SQL by executions</a></li>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'stmt_io_times')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#top_iowait>Top SQL by I/O wait time</a></li>';
      END IF;
      tmp_text := tmp_text || '<li><a HREF=#top_pgs_fetched>Top SQL by shared blocks fetched</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_reads>Top SQL by shared blocks read</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_dirtied>Top SQL by shared blocks dirtied</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#top_shared_written>Top SQL by shared blocks written</a></li>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statement_wal_bytes')::boolean THEN
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
      tmp_text := tmp_text || '<li><a HREF=#sql_list>Complete list of SQL texts</a></li>';
      tmp_text := tmp_text || '</ul>';
    END IF;
    tmp_text := tmp_text || '<li><a HREF=#schema_stat>Schema object statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#scanned_tbl>Top tables by estimated sequentially scanned volume</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#fetch_tbl>Top tables by blocks fetched</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#read_tbl>Top tables by blocks read</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#dml_tbl>Top DML tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#vac_tbl>Top tables by updated/deleted tuples</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_tbl>Top growing tables</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#fetch_idx>Top indexes by blocks fetched</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#read_idx>Top indexes by blocks read</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#growth_idx>Top growing indexes</a></li>';
    tmp_text := tmp_text || '</ul>';

    IF jsonb_extract_path_text(jreportset, 'report_features', 'function_stats')::boolean THEN
      tmp_text := tmp_text || '<li><a HREF=#func_stat>User function statistics</a></li>';
      tmp_text := tmp_text || '<ul>';
      tmp_text := tmp_text || '<li><a HREF=#funcs_time_stat>Top functions by total time</a></li>';
      tmp_text := tmp_text || '<li><a HREF=#funcs_calls_stat>Top functions by executions</a></li>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'trigger_function_stats')::boolean THEN
        tmp_text := tmp_text || '<li><a HREF=#trg_funcs_time_stat>Top trigger functions by total time</a></li>';
      END IF;
      tmp_text := tmp_text || '</ul>';
    END IF;

    tmp_text := tmp_text || '<li><a HREF=#vacuum_stats>Vacuum-related statistics</a></li>';
    tmp_text := tmp_text || '<ul>';
    tmp_text := tmp_text || '<li><a HREF=#top_vacuum_cnt_tbl>Top tables by vacuum operations</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_analyze_cnt_tbl>Top tables by analyze operations</a></li>';
    tmp_text := tmp_text || '<li><a HREF=#top_ix_vacuum_bytes_cnt_tbl>Top indexes by estimated vacuum load</a></li>';
    tmp_text := tmp_text || '</ul>';

    tmp_text := tmp_text || '<li><a HREF=#pg_settings>Cluster settings during the report interval</a></li>';

    tmp_text := tmp_text || '</ul>';


    --Reporting cluster stats
    tmp_text := tmp_text || '<H2><a NAME=cl_stat>Server statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=db_stat>Database statistics</a></H3>';
    tmp_report := dbstats_reset_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Database statistics reset detected during report period!</p>'||tmp_report||
        '<p>Statistics for listed databases and contained objects might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(dbstats_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'sess_stats')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=db_stat_sessions>Session statistics by database</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(dbstats_sessions_diff_htbl(jreportset, sserver_id,
        start1_id, end1_id, start2_id, end2_id, topn));
    END IF;

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=st_stat>Statement statistics by database</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(statements_stats_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
    END IF;

    tmp_text := tmp_text || '<div>';
    tmp_text := tmp_text || '<div style="display:inline-block; margin-right:2em;">'
      '<H3><a NAME=clu_stat>Cluster statistics</a></H3>';
    tmp_report := cluster_stats_reset_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Cluster statistics reset detected during report period!</p>'||tmp_report||
        '<p>Cluster statistics might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id)) ||
      '</div>';

    IF jsonb_extract_path_text(jreportset, 'report_features', 'wal_stats')::boolean THEN
      tmp_text := tmp_text || '<div style="display:inline-block"><H3><a NAME=wal_stat>WAL statistics</a></H3>';
      tmp_report := wal_stats_reset_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id);
      IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b> WAL statistics reset detected during report period!</p>'||tmp_report||
          '<p>WAL statistics might be affected</p>';
      END IF;
      tmp_text := tmp_text || nodata_wrapper(wal_stats_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id)) ||
        '</div>';
    END IF;
    tmp_text := tmp_text || '</div>';

    tmp_text := tmp_text || '<H3><a NAME=tablespace_stat>Tablespace statistics</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tablespaces_stats_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      --Reporting on top queries by elapsed time
      tmp_text := tmp_text || '<H2><a NAME=sql_stat>SQL query statistics</a></H2>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_elapsed_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
        tmp_text := tmp_text || '<H3><a NAME=top_plan>Top SQL by planning time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_plan_time_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
      END IF;
      tmp_text := tmp_text || '<H3><a NAME=top_exec>Top SQL by execution time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_time_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
      -- Reporting on top queries by executions
      tmp_text := tmp_text || '<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_exec_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by I/O wait time
      IF jsonb_extract_path_text(jreportset, 'report_features', 'stmt_io_times')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_iowait_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
      END IF;

      -- Reporting on top queries by gets
      tmp_text := tmp_text || '<H3><a NAME=top_pgs_fetched>Top SQL by shared blocks fetched</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_blks_fetched_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by shared reads
      tmp_text := tmp_text || '<H3><a NAME=top_shared_reads>Top SQL by shared blocks read</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_reads_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by shared dirtied
      tmp_text := tmp_text || '<H3><a NAME=top_shared_dirtied>Top SQL by shared blocks dirtied</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_dirtied_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by shared written
      tmp_text := tmp_text || '<H3><a NAME=top_shared_written>Top SQL by shared blocks written</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_shared_written_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

      -- Reporting on top queries by WAL bytes
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statement_wal_bytes')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=top_wal_bytes>Top SQL by WAL size</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_wal_size_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
      END IF;

      -- Reporting on top queries by temp usage
      tmp_text := tmp_text || '<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(top_temp_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

      IF jsonb_extract_path_text(jreportset, 'report_features', 'kcachestatements')::boolean THEN
        --Reporting kcache queries
        tmp_text := tmp_text || '<H3><a NAME=kcache_stat>rusage statistics</a></H3>';
        tmp_text := tmp_text||'<H4><a NAME=kcache_time>Top SQL by system and user time </a></H4>';
        tmp_text := tmp_text || nodata_wrapper(top_cpu_time_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
        tmp_text := tmp_text||'<H4><a NAME=kcache_reads_writes>Top SQL by reads/writes done by filesystem layer </a></H4>';
        tmp_text := tmp_text || nodata_wrapper(top_io_filesystem_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
      END IF;
      -- Listing queries
      tmp_text := tmp_text || '<H3><a NAME=sql_list>Complete list of SQL texts</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(report_queries(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id));
    END IF;

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text || '<H2><a NAME=schema_stat>Schema object statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=scanned_tbl>Top tables by estimated sequentially scanned volume</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=fetch_tbl>Top tables by blocks fetched</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_fetch_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=read_tbl>Top tables by blocks read</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=vac_tbl>Top tables by updated/deleted tuples</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_upd_vac_tables_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=growth_tbl>Top growing tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_tables_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=fetch_idx>Top indexes by blocks fetched</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_fetch_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=read_idx>Top indexes by blocks read</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(ix_top_io_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=growth_idx>Top growing indexes</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_growth_indexes_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    IF jsonb_extract_path_text(jreportset, 'report_features', 'function_stats')::boolean THEN
      tmp_text := tmp_text || '<H2><a NAME=func_stat>User function statistics</a></H2>';
      tmp_text := tmp_text || '<H3><a NAME=funcs_time_stat>Top functions by total time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(func_top_time_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

      tmp_text := tmp_text || '<H3><a NAME=funcs_calls_stat>Top functions by executions</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(func_top_calls_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

      IF jsonb_extract_path_text(jreportset, 'report_features', 'trigger_function_stats')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=trg_funcs_time_stat>Top trigger functions by total time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(func_top_trg_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
      END IF;
    END IF;

    -- Reporting vacuum related stats
    tmp_text := tmp_text || '<H2><a NAME=vacuum_stats>Vacuum-related statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=top_vacuum_cnt_tbl>Top tables by vacuum operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_vacuumed_tables_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_analyze_cnt_tbl>Top tables by analyze operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_analyzed_tables_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_ix_vacuum_bytes_cnt_tbl>Top indexes by estimated vacuum load</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_vacuumed_indexes_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id, topn));

    -- Database settings report
    tmp_text := tmp_text || '<H2><a NAME=pg_settings>Cluster settings during the report intervals</a></H2>';
    tmp_text := tmp_text || nodata_wrapper(settings_and_changes_diff_htbl(jreportset, sserver_id, start1_id, end1_id, start2_id, end2_id));

    /*
    * Dropping cache temporary tables
    * This is needed to avoid conflict with existing table if several
    * reports are collected in one session
    */
    DROP TABLE top_statements1;
    DROP TABLE top_tables1;
    DROP TABLE top_indexes1;
    DROP TABLE top_io_tables1;
    DROP TABLE top_io_indexes1;
    DROP TABLE top_functions1;
    DROP TABLE top_kcache_statements1;
    DROP TABLE top_statements2;
    DROP TABLE top_tables2;
    DROP TABLE top_indexes2;
    DROP TABLE top_io_tables2;
    DROP TABLE top_io_indexes2;
    DROP TABLE top_functions2;
    DROP TABLE top_kcache_statements2;

    report := replace(report,'{report}',tmp_text);
    RETURN report;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_diffreport(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN start2_id integer,IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server_id and IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),start1_id,end1_id,
    start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',start1_id,end1_id,start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;

COMMENT ON FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN start2_id integer,IN end2_id integer, IN description text,
  IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes IDs of start and end sample for first and second intervals';

CREATE FUNCTION get_diffreport(IN server name, IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline1) bl1
    CROSS JOIN get_baseline_samples(get_server_by_name(server), baseline2) bl2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline1 varchar(25),
  IN baseline2 varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and two baselines to compare.';

CREATE FUNCTION get_diffreport(IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',baseline1,baseline2,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline1 varchar(25), IN baseline2 varchar(25),
  IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes two baselines to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    start2_id,end2_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl1
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text,
  IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, reference baseline name as first interval, start and end sample_ids of second interval.';

CREATE FUNCTION get_diffreport(IN baseline varchar(25),
  IN start2_id integer, IN end2_id integer, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',baseline,
    start2_id,end2_id,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline varchar(25), IN start2_id integer,
IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes reference baseline name as first interval, start and end sample_ids of second interval.';

CREATE FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),start1_id,end1_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, start and end sample_ids of first interval and reference baseline name as second interval.';

CREATE FUNCTION get_diffreport(IN start1_id integer, IN end1_id integer,
  IN baseline varchar(25), IN description text = NULL, IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport('local',start1_id,end1_id,
    baseline,description,with_growth);
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN baseline varchar(25), IN start2_id integer,
  IN end2_id integer, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function for local server. Takes start and end sample_ids of first interval and reference baseline name as second interval.';

CREATE FUNCTION get_diffreport(IN server name, IN time_range1 tstzrange,
  IN time_range2 tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),tm1.start_id,tm1.end_id,
    tm2.start_id,tm2.end_id,description,with_growth)
  FROM get_sampleids_by_timerange(get_server_by_name(server), time_range1) tm1
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range2) tm2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN time_range1 tstzrange,
  IN time_range2 tstzrange, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name and two time intervals to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN time_range tstzrange, IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),bl1.start_id,bl1.end_id,
    tm2.start_id,tm2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl1
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range) tm2
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN baseline varchar(25),
  IN time_range tstzrange, IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, baseline and time interval to compare.';

CREATE FUNCTION get_diffreport(IN server name, IN time_range tstzrange,
  IN baseline varchar(25), IN description text = NULL,
  IN with_growth boolean = false)
RETURNS text SET search_path=@extschema@ AS $$
  SELECT get_diffreport(get_server_by_name(server),tm1.start_id,tm1.end_id,
    bl2.start_id,bl2.end_id,description,with_growth)
  FROM get_baseline_samples(get_server_by_name(server), baseline) bl2
    CROSS JOIN get_sampleids_by_timerange(get_server_by_name(server), time_range) tm1
$$ LANGUAGE sql;
COMMENT ON FUNCTION get_diffreport(IN server name, IN time_range tstzrange,
  IN baseline varchar(25), IN description text, IN with_growth boolean)
IS 'Statistics differential report generation function. Takes server name, time interval and baseline to compare.';
