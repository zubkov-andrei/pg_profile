/* === Data of reports === */

INSERT INTO report_static(static_name, static_text)
VALUES
('css1',
  '* {font-family: Century Gothic, CenturyGothic, AppleGothic, sans-serif;} '
  'table, th, td {border: 1px solid black; border-collapse: collapse; padding:4px;} '
  'table tr td.value, table tr td.mono {font-family: consolas, monaco, Monospace;} '
  'table tr td.value {text-align: right;} '
  'table p {margin: 0.2em;}'
  'table tr.parent td:not(.hdr) {background-color: #D8E8C2;} '
  'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} '
  'table.stat tr:nth-child(even), table.setlist tr:nth-child(even) {background-color: #eee;} '
  'table.stat tr:nth-child(odd), table.setlist tr:nth-child(odd) {background-color: #fff;} '
  'table tr:hover td:not(.hdr) {background-color:#d9ffcc} '
  'table th {color: black; background-color: #ffcc99;}'
  'table tr:target,td:target {border: solid; border-width: medium; border-color:limegreen;}'
  'table tr:target td:first-of-type, table td:target {font-weight: bold;}'
  '{static:css1_post}'
),
('version',
  '<p>{pg_profile} version {properties:pgprofile_version}</p>'),
('report',
  '<html><head><style>{static:css1}</style>'
  '<title>Postgres profile report ({properties:start1_id} -'
  ' {properties:end1_id})</title></head><body>'
  '<H1>Postgres profile report ({properties:start1_id} -'
  ' {properties:end1_id})</H1>'
  '{static:version}'
  '<p>Server name: <strong>{properties:server_name}</strong></p>'
  '{properties:server_description}'
  '<p>Report interval: <strong>{properties:report_start1} -'
  ' {properties:report_end1}</strong></p>'
  '{properties:description}'
  '<h2>Report sections</h2>'
  '{report:toc}{report:sect}</body></html>'),
('css2',
  '* {font-family: Century Gothic, CenturyGothic, AppleGothic, sans-serif;} '
  'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} '
  'table .value, table .mono {font-family: consolas, monaco, Monospace;} '
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
  'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} '
  '{static:css2_post}'
),
('diffreport',
  '<html><head><style>{static:css2}</style>'
  '<title>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</title></head><body>'
  '<H1>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</H1>'
  '{static:version}'
  '<p>Server name: <strong>{properties:server_name}</strong></p>'
  '{properties:server_description}'
  '<p>First interval (1): <strong>{properties:report_start1} -'
  ' {properties:report_end1}</strong></p>'
  '<p>Second interval (2): <strong>{properties:report_start2} -'
  ' {properties:report_end2}</strong></p>'
  '{properties:description}'
  '<h2>Report sections</h2>'
  '{report:toc}{report:sect}</body></html>')
;

INSERT INTO report(report_id, report_name, report_description, template)
VALUES
(1, 'report', 'Regular single interval report', 'report'),
(2, 'diffreport', 'Differential report on two intervals', 'diffreport')
;

-- Regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'stmt_cmt1', NULL, 100, NULL, NULL, NULL, 'check_stmt_cnt_first_htbl', NULL, '<p><strong>Warning!</strong></p>'
  '<p>This interval contains sample(s) with captured statements count more than 90% of <i>pg_stat_statements.max</i> parameter.</p>'
  '{func_output}'
  '<p> Consider increasing <i>pg_stat_statements.max</i> parameter.</p>'),
(1, 'srvstat', NULL, 200, 'Server statistics', 'Server statistics', NULL, NULL, 'cl_stat', NULL),
(1, 'sqlsthdr', NULL, 300, 'SQL query statistics', 'SQL query statistics', 'statstatements', NULL, 'sql_stat', NULL),
(1, 'objects', NULL, 400, 'Schema object statistics', 'Schema object statistics', NULL, NULL, 'schema_stat', NULL),
(1, 'funchdr', NULL, 500, 'User function statistics', 'User function statistics', 'function_stats', NULL, 'func_stat', NULL),
(1, 'vachdr', NULL, 600, 'Vacuum-related statistics', 'Vacuum-related statistics', NULL, NULL, 'vacuum_stats', NULL),
(1, 'setings', NULL, 700, 'Cluster settings during the report interval', 'Cluster settings during the report interval', NULL, 'settings_and_changes_htbl', 'pg_settings', NULL),
(1, 'stmt_warn', NULL, 800, NULL, 'Warning!', NULL, 'check_stmt_cnt_all_htbl', NULL, NULL)
;

-- Server section of regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'dbstat1', 'srvstat', 100, 'Database statistics', 'Database statistics', NULL, NULL, 'db_stat', NULL),
(1, 'dbstat2', 'srvstat', 200, NULL, NULL, NULL, 'dbstats_reset_htbl', NULL,
  '<p><b>Warning!</b> Database statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Statistics for listed databases and contained objects might be affected</p>'),
(1, 'dbstat3', 'srvstat', 300, NULL, NULL, NULL, 'dbstats_htbl', NULL, NULL),
(1, 'sesstat', 'srvstat', 400, 'Session statistics by database', 'Session statistics by database', 'sess_stats', 'dbstats_sessions_htbl', 'db_stat_sessions', NULL),
(1, 'stmtstat', 'srvstat', 500, 'Statement statistics by database', 'Statement statistics by database', 'statstatements', 'statements_stats_htbl', 'st_stat', NULL),
(1, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', 'dbagg_jit_stats_htbl', 'dbagg_jit_stat', NULL),
(1, 'div1', 'srvstat', 600, NULL, NULL, NULL, NULL, NULL, '<div>'),
(1, 'clusthdr', 'srvstat', 700, 'Cluster statistics', 'Cluster statistics', NULL, NULL, 'clu_stat',
  '<div style="display:inline-block; margin-right:2em;">{header}'),
(1, 'clustrst', 'srvstat', 800, NULL, NULL, NULL, 'cluster_stats_reset_htbl', NULL,
  '<p><b>Warning!</b> Cluster statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Cluster statistics might be affected</p>'),
(1, 'clust', 'srvstat', 900, NULL, NULL, NULL, 'cluster_stats_htbl', NULL, '{func_output}</div>'),
(1, 'walsthdr', 'srvstat', 1000, 'WAL statistics', 'WAL statistics', 'wal_stats', NULL, 'wal_stat',
  '<div style="display:inline-block; margin-right:2em;">{header}'),
(1, 'walstrst', 'srvstat', 1100, NULL, NULL, 'wal_stats', 'wal_stats_reset_htbl', NULL,
  '<p><b>Warning!</b> WAL statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>WAL statistics might be affected</p>'),
(1, 'walst', 'srvstat', 1200, NULL, NULL, 'wal_stats', 'wal_stats_htbl', NULL, '{func_output}</div>'),
(1, 'div2', 'srvstat', 1300, NULL, NULL, NULL, NULL, NULL, '</div>'),
(1, 'tbspst', 'srvstat', 1400, 'Tablespace statistics', 'Tablespace statistics', NULL, 'tablespaces_stats_htbl', 'tablespace_stat', NULL),
(1, 'wait_sampling_srvstats', 'srvstat', 1500, 'Wait sampling', 'Wait sampling', 'wait_sampling_tot', NULL, 'wait_sampling', NULL),
(1, 'wait_sampling_total', 'wait_sampling_srvstats', 100, 'Wait events types', 'Wait events types', 'wait_sampling_tot', 'wait_sampling_totals_htbl', 'wait_sampling_total', NULL),
(1, 'wait_sampling_statements', 'wait_sampling_srvstats', 200, 'Top wait events (statements)', 'Top wait events (statements)', 'wait_sampling_tot', 'top_wait_sampling_events_htbl', 'wt_smp_stmt', '<p>Top wait events detected in statements execution</p>'),
(1, 'wait_sampling_all', 'wait_sampling_srvstats', 300, 'Top wait events (All)', 'Top wait events (All)', 'wait_sampling_tot', 'top_wait_sampling_events_htbl', 'wt_smp_all', '<p>Top wait events detected in all backends</p>')
;

-- Query section of regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'sqlela_t', 'sqlsthdr', 100, 'Top SQL by elapsed time', 'Top SQL by elapsed time', 'planning_times', 'top_elapsed_htbl', 'top_ela', NULL),
(1, 'sqlplan_t', 'sqlsthdr', 200, 'Top SQL by planning time', 'Top SQL by planning time', 'planning_times', 'top_plan_time_htbl', 'top_plan', NULL),
(1, 'sqlexec_t', 'sqlsthdr', 300, 'Top SQL by execution time', 'Top SQL by execution time', NULL, 'top_exec_time_htbl', 'top_exec', NULL),
(1, 'sqlcalls', 'sqlsthdr', 400, 'Top SQL by executions', 'Top SQL by executions', NULL, 'top_exec_htbl', 'top_calls', NULL),
(1, 'sqlio_t', 'sqlsthdr', 500, 'Top SQL by I/O wait time', 'Top SQL by I/O wait time', 'io_times', 'top_iowait_htbl', 'top_iowait', NULL),
(1, 'sqlfetch', 'sqlsthdr', 600, 'Top SQL by shared blocks fetched', 'Top SQL by shared blocks fetched', NULL, 'top_shared_blks_fetched_htbl', 'top_pgs_fetched', NULL),
(1, 'sqlshrd', 'sqlsthdr', 700, 'Top SQL by shared blocks read', 'Top SQL by shared blocks read', NULL, 'top_shared_reads_htbl', 'top_shared_reads', NULL),
(1, 'sqlshdir', 'sqlsthdr', 800, 'Top SQL by shared blocks dirtied', 'Top SQL by shared blocks dirtied', NULL, 'top_shared_dirtied_htbl', 'top_shared_dirtied', NULL),
(1, 'sqlshwr', 'sqlsthdr', 900, 'Top SQL by shared blocks written', 'Top SQL by shared blocks written', NULL, 'top_shared_written_htbl', 'top_shared_written', NULL),
(1, 'sqlwalsz', 'sqlsthdr', 1000, 'Top SQL by WAL size', 'Top SQL by WAL size', 'statement_wal_bytes', 'top_wal_size_htbl', 'top_wal_bytes', NULL),
(1, 'sqltmp', 'sqlsthdr', 1100, 'Top SQL by temp usage', 'Top SQL by temp usage', NULL, 'top_temp_htbl', 'top_temp', NULL),
(1, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', 'top_jit_htbl', 'top_jit', NULL),
(1, 'sqlkcachehdr', 'sqlsthdr', 1200, 'rusage statistics', 'rusage statistics', 'kcachestatements', NULL, 'kcache_stat', NULL),
(1, 'sqllist', 'sqlsthdr', 1300, 'Complete list of SQL texts', 'Complete list of SQL texts', NULL, 'report_queries', 'sql_list', NULL)
;

-- rusage section of regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'sqlrusgcpu_t', 'sqlkcachehdr', 100, 'Top SQL by system and user time', 'Top SQL by system and user time', NULL, 'top_cpu_time_htbl', 'kcache_time', NULL),
(1, 'sqlrusgio', 'sqlkcachehdr', 200, 'Top SQL by reads/writes done by filesystem layer', 'Top SQL by reads/writes done by filesystem layer', NULL, 'top_io_filesystem_htbl', 'kcache_reads_writes', NULL)
;

-- Schema objects section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'tblscan', 'objects', 100, 'Top tables by estimated sequentially scanned volume', 'Top tables by estimated sequentially scanned volume', NULL, 'top_scan_tables_htbl', 'scanned_tbl', NULL),
(1, 'tblfetch', 'objects', 200, 'Top tables by blocks fetched', 'Top tables by blocks fetched', NULL, 'tbl_top_fetch_htbl', 'fetch_tbl', NULL),
(1, 'tblrd', 'objects', 300, 'Top tables by blocks read', 'Top tables by blocks read', NULL, 'tbl_top_io_htbl', 'read_tbl', NULL),
(1, 'tbldml', 'objects', 400, 'Top DML tables', 'Top DML tables', NULL, 'top_dml_tables_htbl', 'dml_tbl', NULL),
(1, 'tblud', 'objects', 500, 'Top tables by updated/deleted tuples', 'Top tables by updated/deleted tuples', NULL, 'top_upd_vac_tables_htbl', 'vac_tbl', NULL),
(1, 'tblgrw', 'objects', 600, 'Top growing tables', 'Top growing tables', NULL, 'top_growth_tables_htbl', 'growth_tbl',
  '<ul><li>Sizes in square brackets is based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}'),
(1, 'ixfetch', 'objects', 700, 'Top indexes by blocks fetched', 'Top indexes by blocks fetched', NULL, 'ix_top_fetch_htbl', 'fetch_idx', NULL),
(1, 'ixrd', 'objects', 800, 'Top indexes by blocks read', 'Top indexes by blocks read', NULL, 'ix_top_io_htbl', 'read_idx', NULL),
(1, 'ixgrw', 'objects', 900, 'Top growing indexes', 'Top growing indexes', NULL, 'top_growth_indexes_htbl', 'growth_idx',
  '<ul><li>Sizes in square brackets is based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}'),
(1, 'ixunused', 'objects', 1000, 'Unused indexes', 'Unused indexes', NULL, 'ix_unused_htbl', 'ix_unused',
  '<p>This table contains non-scanned indexes (during report interval), ordered by number of DML '
  'operations on underlying tables. Constraint indexes are excluded.</p>{func_output}')
;

-- Functions section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'func_t', 'funchdr', 100, 'Top functions by total time', 'Top functions by total time', NULL, 'func_top_time_htbl', 'funcs_time_stat', NULL),
(1, 'func_c', 'funchdr', 200, 'Top functions by executions', 'Top functions by executions', NULL, 'func_top_calls_htbl', 'funcs_calls_stat', NULL),
(1, 'func_trg', 'funchdr', 300, 'Top trigger functions by total time', 'Top trigger functions by total time', 'trigger_function_stats', 'func_top_trg_htbl', 'trg_funcs_time_stat', NULL)
;

-- Vacuum section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(1, 'vacops', 'vachdr', 100, 'Top tables by vacuum operations', 'Top tables by vacuum operations', NULL, 'top_vacuumed_tables_htbl', 'top_vacuum_cnt_tbl', NULL),
(1, 'anops', 'vachdr', 200, 'Top tables by analyze operations', 'Top tables by analyze operations', NULL, 'top_analyzed_tables_htbl', 'top_analyze_cnt_tbl', NULL),
(1, 'ixvacest', 'vachdr', 300, 'Top indexes by estimated vacuum load', 'Top indexes by estimated vacuum load', NULL, 'top_vacuumed_indexes_htbl', 'top_ix_vacuum_bytes_cnt_tbl', NULL),
(1, 'tblbydead', 'vachdr', 400, 'Top tables by dead tuples ratio', 'Top tables by dead tuples ratio', NULL, 'tbl_top_dead_htbl', 'dead_tbl',
  '<p>Data in this section is not differential. This data is valid for last report sample only.</p>{func_output}'),
(1, 'tblbymod', 'vachdr', 500, 'Top tables by modified tuples ratio', 'Top tables by modified tuples ratio', NULL, 'tbl_top_mods_htbl', 'mod_tbl',
  '<p>Data in this section is not differential. This data is valid for last report sample only.</p>{func_output}')
;

-- Differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'stmt_cmt1', NULL, 100, NULL, NULL, NULL, 'check_stmt_cnt_first_htbl', NULL, '<p><strong>Warning!</strong></p>'
  '<p>First interval contains sample(s) with captured statements count more than 90% of '
  '<i>pg_stat_statements.max</i> parameter.</p>'
  '{func_output}'
  '<p> Consider increasing <i>pg_stat_statements.max</i> parameter.</p>'),
(2, 'stmt_cmt2', NULL, 200, NULL, NULL, NULL, 'check_stmt_cnt_second_htbl', NULL, '<p><strong>Warning!</strong></p>'
  '<p>Second interval contains sample(s) with captured statements count more than 90% of '
  '<i>pg_stat_statements.max</i> parameter.</p>'
  '{func_output}'
  '<p> Consider increasing <i>pg_stat_statements.max</i> parameter.</p>'),
(2, 'srvstat', NULL, 300, 'Server statistics', 'Server statistics', NULL, NULL, 'cl_stat', NULL),
(2, 'sqlsthdr', NULL, 400, 'SQL query statistics', 'SQL query statistics', 'statstatements', NULL, 'sql_stat', NULL),
(2, 'objects', NULL, 500, 'Schema object statistics', 'Schema object statistics', NULL, NULL, 'schema_stat', NULL),
(2, 'funchdr', NULL, 600, 'User function statistics', 'User function statistics', 'function_stats', NULL, 'func_stat', NULL),
(2, 'vachdr', NULL, 700, 'Vacuum-related statistics', 'Vacuum-related statistics', NULL, NULL, 'vacuum_stats', NULL),
(2, 'setings', NULL, 800, 'Cluster settings during the report interval', 'Cluster settings during the report interval', NULL, 'settings_and_changes_diff_htbl', 'pg_settings', NULL),
(2, 'stmt_warn', NULL, 900, NULL, 'Warning!', NULL, 'check_stmt_cnt_all_htbl', NULL, NULL)
;

-- Server section of differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'dbstat1', 'srvstat', 100, 'Database statistics', 'Database statistics', NULL, NULL, 'db_stat', NULL),
(2, 'dbstat2', 'srvstat', 200, NULL, NULL, NULL, 'dbstats_reset_diff_htbl', NULL,
  '<p><b>Warning!</b> Database statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Statistics for listed databases and contained objects might be affected</p>'),
(2, 'dbstat3', 'srvstat', 300, NULL, NULL, NULL, 'dbstats_diff_htbl', NULL, NULL),
(2, 'sesstat', 'srvstat', 400, 'Session statistics by database', 'Session statistics by database', 'sess_stats', 'dbstats_sessions_diff_htbl', 'db_stat_sessions', NULL),
(2, 'stmtstat', 'srvstat', 500, 'Statement statistics by database', 'Statement statistics by database', 'statstatements', 'statements_stats_diff_htbl', 'st_stat', NULL),
(2, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', 'dbagg_jit_stats_diff_htbl', 'dbagg_jit_stat', NULL),
(2, 'div1', 'srvstat', 600, NULL, NULL, NULL, NULL, NULL, '<div>'),
(2, 'clusthdr', 'srvstat', 700, 'Cluster statistics', 'Cluster statistics', NULL, NULL, 'clu_stat',
  '<div style="display:inline-block; margin-right:2em;">{header}'),
(2, 'clustrst', 'srvstat', 800, NULL, NULL, NULL, 'cluster_stats_reset_diff_htbl', NULL,
  '<p><b>Warning!</b> Cluster statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Cluster statistics might be affected</p>'),
(2, 'clust', 'srvstat', 900, NULL, NULL, NULL, 'cluster_stats_diff_htbl', NULL, '{func_output}</div>'),
(2, 'walsthdr', 'srvstat', 1000, 'WAL statistics', 'WAL statistics', 'wal_stats', NULL, 'wal_stat',
  '<div style="display:inline-block; margin-right:2em;">{header}'),
(2, 'walstrst', 'srvstat', 1100, NULL, NULL, 'wal_stats', 'wal_stats_reset_diff_htbl', NULL,
  '<p><b>Warning!</b> WAL statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>WAL statistics might be affected</p>'),
(2, 'walst', 'srvstat', 1200, NULL, NULL, 'wal_stats', 'wal_stats_diff_htbl', NULL, '{func_output}</div>'),
(2, 'div2', 'srvstat', 1300, NULL, NULL, NULL, NULL, NULL, '</div>'),
(2, 'tbspst', 'srvstat', 1400, 'Tablespace statistics', 'Tablespace statistics', NULL, 'tablespaces_stats_diff_htbl', 'tablespace_stat', NULL),
(2, 'wait_sampling_srvstats', 'srvstat', 1500, 'Wait sampling', 'Wait sampling', 'wait_sampling_tot', NULL, 'wait_sampling', NULL),
(2, 'wait_sampling_total', 'wait_sampling_srvstats', 100, 'Wait events types', 'Wait events types', 'wait_sampling_tot', 'wait_sampling_totals_diff_htbl', 'wait_sampling_total', NULL),
(2, 'wait_sampling_statements', 'wait_sampling_srvstats', 200, 'Top wait events (statements)', 'Top wait events (statements)', 'wait_sampling_tot', 'top_wait_sampling_events_diff_htbl', 'wt_smp_stmt', '<p>Top wait events detected in statements execution</p>'),
(2, 'wait_sampling_all', 'wait_sampling_srvstats', 300, 'Top wait events (All)', 'Top wait events (All)', 'wait_sampling_tot', 'top_wait_sampling_events_diff_htbl', 'wt_smp_all', '<p>Top wait events detected in all backends</p>')
;

-- Query section of differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'sqlela_t', 'sqlsthdr', 100, 'Top SQL by elapsed time', 'Top SQL by elapsed time', 'planning_times', 'top_elapsed_diff_htbl', 'top_ela', NULL),
(2, 'sqlplan_t', 'sqlsthdr', 200, 'Top SQL by planning time', 'Top SQL by planning time', 'planning_times', 'top_plan_time_diff_htbl', 'top_plan', NULL),
(2, 'sqlexec_t', 'sqlsthdr', 300, 'Top SQL by execution time', 'Top SQL by execution time', NULL, 'top_exec_time_diff_htbl', 'top_exec', NULL),
(2, 'sqlcalls', 'sqlsthdr', 400, 'Top SQL by executions', 'Top SQL by executions', NULL, 'top_exec_diff_htbl', 'top_calls', NULL),
(2, 'sqlio_t', 'sqlsthdr', 500, 'Top SQL by I/O wait time', 'Top SQL by I/O wait time', 'io_times', 'top_iowait_diff_htbl', 'top_iowait', NULL),
(2, 'sqlfetch', 'sqlsthdr', 600, 'Top SQL by shared blocks fetched', 'Top SQL by shared blocks fetched', NULL, 'top_shared_blks_fetched_diff_htbl', 'top_pgs_fetched', NULL),
(2, 'sqlshrd', 'sqlsthdr', 700, 'Top SQL by shared blocks read', 'Top SQL by shared blocks read', NULL, 'top_shared_reads_diff_htbl', 'top_shared_reads', NULL),
(2, 'sqlshdir', 'sqlsthdr', 800, 'Top SQL by shared blocks dirtied', 'Top SQL by shared blocks dirtied', NULL, 'top_shared_dirtied_diff_htbl', 'top_shared_dirtied', NULL),
(2, 'sqlshwr', 'sqlsthdr', 900, 'Top SQL by shared blocks written', 'Top SQL by shared blocks written', NULL, 'top_shared_written_diff_htbl', 'top_shared_written', NULL),
(2, 'sqlwalsz', 'sqlsthdr', 1000, 'Top SQL by WAL size', 'Top SQL by WAL size', 'statement_wal_bytes', 'top_wal_size_diff_htbl', 'top_wal_bytes', NULL),
(2, 'sqltmp', 'sqlsthdr', 1100, 'Top SQL by temp usage', 'Top SQL by temp usage', NULL, 'top_temp_diff_htbl', 'top_temp', NULL),
(2, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', 'top_jit_diff_htbl', 'top_jit', NULL),
(2, 'sqlkcachehdr', 'sqlsthdr', 1200, 'rusage statistics', 'rusage statistics', 'kcachestatements', NULL, 'kcache_stat', NULL),
(2, 'sqllist', 'sqlsthdr', 1300, 'Complete list of SQL texts', 'Complete list of SQL texts', NULL, 'report_queries', 'sql_list', NULL)
;

-- rusage section of differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'sqlrusgcpu_t', 'sqlkcachehdr', 100, 'Top SQL by system and user time', 'Top SQL by system and user time', NULL, 'top_cpu_time_diff_htbl', 'kcache_time', NULL),
(2, 'sqlrusgio', 'sqlkcachehdr', 200, 'Top SQL by reads/writes done by filesystem layer', 'Top SQL by reads/writes done by filesystem layer', NULL, 'top_io_filesystem_diff_htbl', 'kcache_reads_writes', NULL)
;

-- Schema objects section of a differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'tblscan', 'objects', 100, 'Top tables by estimated sequentially scanned volume', 'Top tables by estimated sequentially scanned volume', NULL, 'top_scan_tables_diff_htbl', 'scanned_tbl', NULL),
(2, 'tblfetch', 'objects', 200, 'Top tables by blocks fetched', 'Top tables by blocks fetched', NULL, 'tbl_top_fetch_diff_htbl', 'fetch_tbl', NULL),
(2, 'tblrd', 'objects', 300, 'Top tables by blocks read', 'Top tables by blocks read', NULL, 'tbl_top_io_diff_htbl', 'read_tbl', NULL),
(2, 'tbldml', 'objects', 400, 'Top DML tables', 'Top DML tables', NULL, 'top_dml_tables_diff_htbl', 'dml_tbl', NULL),
(2, 'tblud', 'objects', 500, 'Top tables by updated/deleted tuples', 'Top tables by updated/deleted tuples', NULL, 'top_upd_vac_tables_diff_htbl', 'vac_tbl', NULL),
(2, 'tblgrw', 'objects', 600, 'Top growing tables', 'Top growing tables', NULL, 'top_growth_tables_diff_htbl', 'growth_tbl',
  '<ul><li>Sizes in square brackets is based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}'),
(2, 'ixfetch', 'objects', 700, 'Top indexes by blocks fetched', 'Top indexes by blocks fetched', NULL, 'ix_top_fetch_diff_htbl', 'fetch_idx', NULL),
(2, 'ixrd', 'objects', 800, 'Top indexes by blocks read', 'Top indexes by blocks read', NULL, 'ix_top_io_diff_htbl', 'read_idx', NULL),
(2, 'ixgrw', 'objects', 900, 'Top growing indexes', 'Top growing indexes', NULL, 'top_growth_indexes_diff_htbl', 'growth_idx',
  '<ul><li>Sizes in square brackets is based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}')
;

-- Functions section of a differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'func_t', 'funchdr', 100, 'Top functions by total time', 'Top functions by total time', NULL, 'func_top_time_diff_htbl', 'funcs_time_stat', NULL),
(2, 'func_c', 'funchdr', 200, 'Top functions by executions', 'Top functions by executions', NULL, 'func_top_calls_diff_htbl', 'funcs_calls_stat', NULL),
(2, 'func_trg', 'funchdr', 300, 'Top trigger functions by total time', 'Top trigger functions by total time', 'trigger_function_stats', 'func_top_trg_diff_htbl', 'trg_funcs_time_stat', NULL)
;

-- Vacuum section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content)
VALUES
(2, 'vacops', 'vachdr', 100, 'Top tables by vacuum operations', 'Top tables by vacuum operations', NULL, 'top_vacuumed_tables_diff_htbl', 'top_vacuum_cnt_tbl', NULL),
(2, 'anops', 'vachdr', 200, 'Top tables by analyze operations', 'Top tables by analyze operations', NULL, 'top_analyzed_tables_diff_htbl', 'top_analyze_cnt_tbl', NULL),
(2, 'ixvacest', 'vachdr', 300, 'Top indexes by estimated vacuum load', 'Top indexes by estimated vacuum load', NULL, 'top_vacuumed_indexes_diff_htbl', 'top_ix_vacuum_bytes_cnt_tbl', NULL)
;
