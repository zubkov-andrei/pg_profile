\echo Use "UPDATE EXTENSION pg_profile" to load this file. \quit

CREATE TABLE baselines (
    bl_id SERIAL PRIMARY KEY,
    bl_name varchar (25) UNIQUE,
    keep_until timestamp (0) with time zone
);
COMMENT ON TABLE baselines IS 'Baselines list';

CREATE TABLE bl_snaps (
    snap_id integer REFERENCES snapshots (snap_id) ON DELETE RESTRICT,
    bl_id integer REFERENCES baselines (bl_id) ON DELETE CASCADE,
    CONSTRAINT bl_snaps_pk PRIMARY KEY (snap_id,bl_id)
);
COMMENT ON TABLE bl_snaps IS 'Snapshots in baselines';

CREATE OR REPLACE FUNCTION baseline_new(IN name varchar(25), IN start_id integer, IN end_id integer, IN days integer = NULL) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
   baseline_id integer;
BEGIN
   INSERT INTO baselines(bl_name,keep_until)
   VALUES (name,now() + (days || ' days')::interval)
   RETURNING bl_id INTO baseline_id;
   
   INSERT INTO bl_snaps (snap_id,bl_id)
   SELECT snap_id, baseline_id
   FROM snapshots
   WHERE snap_id BETWEEN start_id AND end_id;
   
	RETURN baseline_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_drop(IN name varchar(25) = null) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
   del_rows integer;
BEGIN
   DELETE FROM baselines WHERE name IS NULL OR bl_name = name;
   GET DIAGNOSTICS del_rows = ROW_COUNT;
	RETURN del_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_keep(IN name varchar(25) = null, IN days integer = null) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
   upd_rows integer;
BEGIN
   UPDATE baselines SET keep_until = now() + (days || ' days')::interval WHERE name IS NULL OR bl_name = name;
   GET DIAGNOSTICS upd_rows = ROW_COUNT;
	RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION baseline_show() RETURNS TABLE(baseline varchar(25), min_snap integer, max_snap integer, keep_until_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
   SELECT bl_name as baseline,min_snap_id,max_snap_id, keep_until 
   FROM baselines b JOIN 
   (SELECT bl_id,min(snap_id) min_snap_id,max(snap_id) max_snap_id FROM bl_snaps GROUP BY bl_id) b_agg
   USING (bl_id)
   ORDER BY min_snap_id;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION snapshot_show(IN days integer = NULL) RETURNS TABLE(snapshot integer, date_time timestamp (0) with time zone) SET search_path=@extschema@,public AS $$
   SELECT snap_id, snap_time
   FROM snapshots
   WHERE days IS NULL OR snap_time > now() - (days || ' days')::interval
   ORDER BY snap_id;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION snapshot() RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
	id    integer;
   topn  integer;
   ret   integer;
   b_local_db boolean;
BEGIN
   -- Getting TopN setting
   BEGIN
      topn := current_setting('pg_profile.topn')::integer;
   EXCEPTION
      WHEN OTHERS THEN topn := 20;
   END;
   -- Getting retention setting
   BEGIN
      ret := current_setting('pg_profile.retention')::integer;
   EXCEPTION
      WHEN OTHERS THEN ret := 7;
   END;

   -- Deleting obsolete baselines
   DELETE FROM baselines WHERE keep_until < now();
   -- Deleting obsolote snapshots
	DELETE FROM snapshots WHERE snap_time < now() - (ret || ' days')::interval
      AND snap_id NOT IN (SELECT snap_id FROM bl_snaps);

   -- Creating a new snapshot record
	INSERT INTO snapshots(snap_time) 
   VALUES (now())
   RETURNING snap_id INTO id;
   
   -- Collecting postgres parameters
   INSERT INTO snap_params
   SELECT id,name,setting
   FROM pg_catalog.pg_settings
   WHERE name IN ('pg_stat_statements.max');
   
   -- Snapshot data from pg_stat_statements for top whole cluster statements
   INSERT INTO snap_statements 
   SELECT id,st.* FROM pg_stat_statements st
   JOIN
     (SELECT userid,dbid,queryid,
      row_number() over (PARTITION BY dbid ORDER BY total_time DESC) AS time_p, 
      row_number() over (PARTITION BY dbid ORDER BY calls DESC) AS calls_p,
      row_number() over (PARTITION BY dbid ORDER BY (blk_read_time + blk_write_time) DESC) AS io_time_p,
      row_number() over (PARTITION BY dbid ORDER BY (shared_blks_hit + shared_blks_read) DESC) AS gets_p,
      row_number() over (PARTITION BY dbid ORDER BY (temp_blks_read + temp_blks_written) DESC) AS temp_p
      FROM pg_stat_statements) tops
   USING (userid,dbid,queryid)
      WHERE
        (tops.time_p <= topn or 
        tops.calls_p <= topn or 
        tops.io_time_p <= topn or 
        tops.gets_p <= topn or 
        tops.temp_p <= topn);

   -- Aggregeted statistics data
   INSERT INTO snap_statements_total
   SELECT id,dbid,sum(calls),sum(total_time),sum(rows),sum(shared_blks_hit),sum(shared_blks_read),sum(shared_blks_dirtied),sum(shared_blks_written),
     sum(local_blks_hit),sum(local_blks_read),sum(local_blks_dirtied),sum(local_blks_written),sum(temp_blks_read),sum(temp_blks_written),sum(blk_read_time),
     sum(blk_write_time),count(*)
   FROM pg_stat_statements
   GROUP BY dbid;
   -- Flushing pg_stat_statements
   PERFORM pg_stat_statements_reset();
   -- pg_stat_database data
   INSERT INTO snap_stat_database 
   SELECT 
      id,
      rs.datid,
      rs.datname,
      rs.xact_commit-ls.xact_commit,
      rs.xact_rollback-ls.xact_rollback,
      rs.blks_read-ls.blks_read,
      rs.blks_hit-ls.blks_hit,
      rs.tup_returned-ls.tup_returned,
      rs.tup_fetched-ls.tup_fetched,
      rs.tup_inserted-ls.tup_inserted,
      rs.tup_updated-ls.tup_updated,
      rs.tup_deleted-ls.tup_deleted,
      rs.conflicts-ls.conflicts,
      rs.temp_files-ls.temp_files,
      rs.temp_bytes-ls.temp_bytes,
      rs.deadlocks-ls.deadlocks,
      rs.blk_read_time-ls.blk_read_time,
      rs.blk_write_time-ls.blk_write_time,
      rs.stats_reset
   FROM pg_stat_database rs 
   JOIN ONLY(last_stat_database) ls ON (rs.datid = ls.datid AND rs.datname = ls.datname AND rs.stats_reset = ls.stats_reset AND ls.snap_id = id - 1);

	PERFORM snapshot_dbobj_delta(id,topn);
   
   TRUNCATE TABLE last_stat_database;
   
   INSERT INTO last_stat_database (
      snap_id,
      datid,
      datname,
      xact_commit,
      xact_rollback,
      blks_read,
      blks_hit,
      tup_returned,
      tup_fetched,
      tup_inserted,
      tup_updated,
      tup_deleted,
      conflicts,
      temp_files,
      temp_bytes,
      deadlocks,
      blk_read_time,
      blk_write_time,
      stats_reset)
   SELECT 
      id,
      datid,
      datname,
      xact_commit,
      xact_rollback,
      blks_read,
      blks_hit,
      tup_returned,
      tup_fetched,
      tup_inserted,
      tup_updated,
      tup_deleted,
      conflicts,
      temp_files,
      temp_bytes,
      deadlocks,
      blk_read_time,
      blk_write_time,
      stats_reset
   FROM pg_catalog.pg_stat_database;
   
   RETURN id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION snapshot() IS 'Statistics snapshot creation function. Must be explicitly called periodically.';

CREATE OR REPLACE FUNCTION dbstats_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	-- Database stats TPLs
	tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Committs</th><th>Rollbacks</th><th>BlkHit%(read/hit)</th><th>Tup Ret/Fet</th><th>Tup Ins</th><th>Tup Del</th><th>Temp Bytes(Files)</th><th>Deadlocks</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s%%(%s/%s)</td><td>%s/%s</td><td>%s</td><td>%s</td><td>%s(%s)</td><td>%s</td></tr>';

   --Cursor for db stats
   c_dbstats CURSOR (s_id integer, e_id integer) FOR
   SELECT 
      datname as dbname,
      sum(xact_commit) as xact_commit,
      sum(xact_rollback) as xact_rollback,
      sum(blks_read) as blks_read,
      sum(blks_hit) as blks_hit,
      sum(tup_returned) as tup_returned,
      sum(tup_fetched) as tup_fetched,
      sum(tup_inserted) as tup_inserted,
      sum(tup_updated) as tup_updated,
      sum(tup_deleted) as tup_deleted,
      sum(temp_files) as temp_files,
      sum(temp_bytes) as temp_bytes,
      sum(deadlocks) as deadlocks, 
      sum(blks_hit)*100/GREATEST(sum(blks_hit)+sum(blks_read),1) as blks_hit_pct
   FROM ONLY(snap_stat_database)
   WHERE datname not like 'template_' and snap_id between s_id + 1 and e_id
   GROUP BY datid,datname
   HAVING max(stats_reset)=min(stats_reset);

	r_result RECORD;
BEGIN
	-- Reporting summary databases stats
	FOR r_result IN c_dbstats(start_id, end_id) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
			r_result.xact_commit,
         r_result.xact_rollback,
			round(CAST(r_result.blks_hit_pct AS numeric),2),
			r_result.blks_read,
         r_result.blks_hit,
			r_result.tup_returned,
			r_result.tup_fetched,
         r_result.tup_inserted,
			r_result.tup_deleted,
			r_result.temp_bytes,
         r_result.temp_files,
         r_result.deadlocks
         );
	END LOOP;
   
   IF report != '' THEN
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION statements_stats_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	-- Database stats TPLs
	tab_tpl CONSTANT text := '<table><tr><th>Database</th><th>Calls</th><th>Total time(s)</th><th>Shared gets</th><th>Local gets</th><th>Shared dirtied</th><th>Local dirtied</th><th>Temp_r (blk)</th><th>Temp_w (blk)</th><th>Statements</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   --Cursor for db stats
   c_dbstats CURSOR (s_id integer, e_id integer) FOR
   SELECT db_s.datname as dbname,
      sum(st.calls) as calls,
      sum(st.total_time)/1000 as total_time,
      sum(st.shared_blks_hit + st.shared_blks_read) as shared_gets,
      sum(st.local_blks_hit + st.local_blks_read) as local_gets,
      sum(st.shared_blks_dirtied) as shared_blks_dirtied,
      sum(st.local_blks_dirtied) as local_blks_dirtied,
      sum(st.temp_blks_read) as temp_blks_read,
      sum(st.temp_blks_written) as temp_blks_written,
      sum(st.statements) as statements
	FROM ONLY(snap_statements_total) st 
      -- Database name and existance condition
      JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id)
      JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
	WHERE st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
   GROUP BY ROLLUP(db_s.datname);

	r_result RECORD;
BEGIN
	-- Reporting summary databases stats
	FOR r_result IN c_dbstats(start_id, end_id) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
			r_result.calls,
         round(CAST(r_result.total_time AS numeric),2),
			r_result.shared_gets,
         r_result.local_gets,
			r_result.shared_blks_dirtied,
			r_result.local_blks_dirtied,
         r_result.temp_blks_read,
			r_result.temp_blks_written,
			r_result.statements
         );
	END LOOP;
   
   IF report != '' THEN
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_scan_tables_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	-- Tables stats template
	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>SeqScan</th><th>SeqFet</th><th>IxScan</th><th>IxFet</th><th>Ins</th><th>Upd</th><th>Del</th><th>HUpd</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   --Cursor for tables stats
   c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
   SELECT 
      db_s.datname as dbname,
      schemaname,
      relname,
      sum(seq_scan) as seq_scan,
      sum(seq_tup_read) as seq_tup_read,
      sum(idx_scan) as idx_scan,
      sum(idx_tup_fetch) as idx_tup_fetch,
      sum(n_tup_ins) as n_tup_ins,
      sum(n_tup_upd) as n_tup_upd,
      sum(n_tup_del) as n_tup_del,
      sum(n_tup_hot_upd) as n_tup_hot_upd
   FROM ONLY(snap_stat_user_tables) st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
   GROUP BY db_s.datid,relid,db_s.datname,schemaname,relname
   ORDER BY sum(seq_scan) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	-- Reporting table stats
	FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
         r_result.schemaname,
         r_result.relname,
         r_result.seq_scan,
         r_result.seq_tup_read,
         r_result.idx_scan,
         r_result.idx_tup_fetch,
         r_result.n_tup_ins,
         r_result.n_tup_upd,
         r_result.n_tup_del,
         r_result.n_tup_hot_upd
         );
	END LOOP;
   
   IF report != '' THEN
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_dml_tables_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	-- Tables stats template
	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Ins</th><th>Upd</th><th>Del</th><th>HUpd</th><th>SeqScan</th><th>SeqFet</th><th>IxScan</th><th>IxFet</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   --Cursor for tables stats
   c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
   SELECT 
      db_s.datname as dbname,
      schemaname,
      relname,
      sum(seq_scan) as seq_scan,
      sum(seq_tup_read) as seq_tup_read,
      sum(idx_scan) as idx_scan,
      sum(idx_tup_fetch) as idx_tup_fetch,
      sum(n_tup_ins) as n_tup_ins,
      sum(n_tup_upd) as n_tup_upd,
      sum(n_tup_del) as n_tup_del,
      sum(n_tup_hot_upd) as n_tup_hot_upd
   FROM ONLY(snap_stat_user_tables) st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
   GROUP BY db_s.datid,relid,db_s.datname,schemaname,relname
   ORDER BY sum(n_tup_ins)+sum(n_tup_upd)+sum(n_tup_del)+sum(n_tup_hot_upd) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	-- Reporting table stats
	FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
         r_result.schemaname,
         r_result.relname,
         r_result.n_tup_ins,
         r_result.n_tup_upd,
         r_result.n_tup_del,
         r_result.n_tup_hot_upd,
         r_result.seq_scan,
         r_result.seq_tup_read,
         r_result.idx_scan,
         r_result.idx_tup_fetch
         );
	END LOOP;
	
   IF report != '' THEN
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_growth_tables_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	-- Tables stats template
	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Size</th><th>Growth</th><th>Ins</th><th>Upd</th><th>Del</th><th>HUpd</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   --Cursor for tables stats
   c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
   SELECT 
      db_s.datname as dbname,
      st.schemaname,
      st.relname,
      sum(st.seq_scan) as seq_scan,
      sum(st.seq_tup_read) as seq_tup_read,
      sum(st.idx_scan) as idx_scan,
      sum(st.idx_tup_fetch) as idx_tup_fetch,
      sum(st.n_tup_ins) as n_tup_ins,
      sum(st.n_tup_upd) as n_tup_upd,
      sum(st.n_tup_del) as n_tup_del,
      sum(st.n_tup_hot_upd) as n_tup_hot_upd,
      pg_size_pretty(sum(st.relsize_diff)) as growth,
      pg_size_pretty(max(st_last.relsize)) as relsize
   FROM ONLY(snap_stat_user_tables) st
   JOIN ONLY(snap_stat_user_tables) st_last using (dbid,relid)
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
      AND st_last.snap_id=db_e.snap_id
   GROUP BY db_s.datid,relid,db_s.datname,st.schemaname,st.relname
   ORDER BY sum(st.relsize_diff) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	-- Reporting table stats
	FOR r_result IN c_tbl_stats(start_id, end_id,topn) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
         r_result.schemaname,
         r_result.relname,
         r_result.relsize,
         r_result.growth,
         r_result.n_tup_ins,
         r_result.n_tup_upd,
         r_result.n_tup_del,
         r_result.n_tup_hot_upd
         );
	END LOOP;
   
   IF report != '' THEN
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION check_stmt_cnt(IN start_id integer = 0, IN end_id integer = 0) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	tab_tpl CONSTANT text := '<table><tr><th>Snapshot ID</th><th>Snapshot Time</th><th>Stmts Captured</th><th>pg_stat_statements.max</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';
   
   report text := '';

   c_stmt_all_stats CURSOR FOR
   SELECT snap_id,snap_time,stmt_cnt,prm.setting AS max_cnt FROM
   snap_params prm JOIN
   (SELECT snap_id,sum(statements) stmt_cnt
   FROM snap_statements_total
   GROUP BY snap_id
   ) snap_stmt_cnt USING(snap_id)
   JOIN snapshots USING (snap_id)
   WHERE prm.p_name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer);

   c_stmt_stats CURSOR (s_id integer, e_id integer) FOR
   SELECT snap_id,snap_time,stmt_cnt,prm.setting AS max_cnt FROM
   snap_params prm JOIN
   (SELECT snap_id,sum(statements) stmt_cnt
   FROM snap_statements_total
   WHERE snap_id BETWEEN s_id AND e_id
   GROUP BY snap_id
   ) snap_stmt_cnt USING(snap_id)
   JOIN snapshots USING (snap_id)
   WHERE prm.p_name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer);

	r_result RECORD;
BEGIN
   IF start_id = 0 THEN
      FOR r_result IN c_stmt_all_stats LOOP
         report := report||format(row_tpl,
            r_result.snap_id,
            r_result.snap_time,
            r_result.stmt_cnt,
            r_result.max_cnt
            );
      END LOOP;
   ELSE
      FOR r_result IN c_stmt_stats(start_id,end_id) LOOP
         report := report||format(row_tpl,
            r_result.snap_id,
            r_result.snap_time,
            r_result.stmt_cnt,
            r_result.max_cnt
            );
      END LOOP;
   END IF; 
   
   IF report != '' THEN
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_dead_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	-- Top dead tuples table
	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Live</th><th>Dead</th><th>%Dead</th><th>Last AV</th><th>Size</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   --Cursor for tables stats
   c_tbl_stats CURSOR (e_id integer, cnt integer) FOR
   SELECT 
      db_e.datname as dbname,
      schemaname,
      relname,
      n_live_tup,
      n_dead_tup,
      n_dead_tup*100/GREATEST(n_live_tup,1) as dead_pct,
      last_autovacuum,
      pg_size_pretty(relsize) as relsize
   FROM ONLY(snap_stat_user_tables) st
   -- Database name and existance condition
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id)
   WHERE db_e.datname not like 'template_' AND st.snap_id = db_e.snap_id
   -- Min 5 MB in size
      AND st.relsize > 5 * 1024^2
      AND st.n_dead_tup > 0
   ORDER BY n_dead_tup/GREATEST(n_live_tup,1) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	-- Reporting vacuum stats
	FOR r_result IN c_tbl_stats(end_id, topn) LOOP
		report := report||format(row_tpl,
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
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_mods_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	-- Top modified tuples table
	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Live</th><th>Dead</th><th>Mods</th><th>%Mod</th><th>Last AA</th><th>Size</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   --Cursor for tables stats
   c_tbl_stats CURSOR (e_id integer, cnt integer) FOR
   SELECT 
      db_e.datname as dbname,
      schemaname,
      relname,
      n_live_tup,
      n_dead_tup,
      n_mod_since_analyze as mods,
      n_mod_since_analyze*100/GREATEST(n_live_tup,1) as mods_pct,
      last_autoanalyze,
      pg_size_pretty(relsize) as relsize
   FROM ONLY(snap_stat_user_tables) st
   -- Database name and existance condition
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id)
   WHERE db_e.datname not like 'template_' AND st.snap_id = db_e.snap_id
   -- Min 5 MB in size
      AND relsize > 5 * 1024^2
      AND n_mod_since_analyze > 0
   ORDER BY n_mod_since_analyze/GREATEST(n_live_tup,1) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	-- Reporting vacuum stats
	FOR r_result IN c_tbl_stats(end_id, topn) LOOP
		report := report||format(row_tpl,
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
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_unused_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>ixSize</th><th>Table DML ops (w/o HOT)</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   c_ix_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
   SELECT 
      db_e.datname as dbname,
      schemaname,
      relname,
      indexrelname,
      pg_size_pretty(max(ix_last.relsize)) as relsize,
      sum(tab.n_tup_ins+tab.n_tup_upd+tab.n_tup_del) as dml_ops
   FROM ONLY(snap_stat_user_indexes) ix
      JOIN ONLY(snap_stat_user_tables) tab USING (snap_id,dbid,relid,schemaname,relname)
      JOIN ONLY(snap_stat_user_indexes) ix_last USING (dbid,relid,indexrelid,schemaname,relname,indexrelname)
   -- Database name and existance condition
      JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=ix.dbid and db_s.snap_id=s_id) 
      JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=ix.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   WHERE ix_last.snap_id = db_e.snap_id 
      AND ix.snap_id BETWEEN db_s.snap_id + 1 and db_e.snap_id
      AND NOT ix.indisunique
      AND ix.idx_scan = 0
   GROUP BY dbid,relid,indexrelid,dbname,schemaname,relname,indexrelname
   ORDER BY sum(tab.n_tup_ins+tab.n_tup_upd+tab.n_tup_del) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	FOR r_result IN c_ix_stats(start_id, end_id, topn) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
         r_result.schemaname,
         r_result.relname,
         r_result.indexrelname,
         r_result.relsize,
         r_result.dml_ops
         );
	END LOOP;
   
   IF report != '' THEN
      report := replace(tab_tpl,'{rows}',report);
   END IF;
   
	RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tbl_top_io_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Heap</th><th>Ix</th><th>TOAST</th><th>TOAST-Ix</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
   SELECT 
      db_s.datname as dbname,
      st.schemaname,
      st.relname,
      sum(st.heap_blks_read) as heap_blks_read,
      sum(st.idx_blks_read) as idx_blks_read,
      sum(st.toast_blks_read) as toast_blks_read,
      sum(st.tidx_blks_read) as tidx_blks_read
   FROM ONLY(snap_statio_user_tables) st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
   GROUP BY db_s.datid,relid,db_s.datname,st.schemaname,st.relname
   ORDER BY sum(st.heap_blks_read + st.idx_blks_read + st.toast_blks_read + st.tidx_blks_read) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
         r_result.schemaname,
         r_result.relname,
         r_result.heap_blks_read,
         r_result.idx_blks_read,
         r_result.toast_blks_read,
         r_result.tidx_blks_read
         );
	END LOOP;
   
   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ix_top_io_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Table</th><th>Index</th><th>Blk Reads</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
   SELECT 
      db_s.datname as dbname,
      st.schemaname,
      st.relname,
      st.indexrelname,
      sum(st.idx_blks_read) as idx_blks_read
   FROM ONLY(snap_statio_user_indexes) st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
   GROUP BY db_s.datid,relid,indexrelid,db_s.datname,st.schemaname,st.relname,st.indexrelname
   ORDER BY sum(st.idx_blks_read) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
         r_result.schemaname,
         r_result.relname,
         r_result.indexrelname,
         r_result.idx_blks_read
         );
	END LOOP;
   
   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_top_time_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
   SELECT 
      db_s.datname as dbname,
      st.schemaname,
      st.funcname,
      sum(st.calls) as calls,
      sum(st.total_time) as total_time,
      sum(st.self_time) as self_time,
      sum(st.total_time)/sum(st.calls) as m_time,
      sum(st.self_time)/sum(st.calls) as m_stime
   FROM ONLY(snap_stat_user_functions) st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
   GROUP BY db_s.datid,funcid,db_s.datname,st.schemaname,st.funcname
   ORDER BY sum(st.total_time) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
         r_result.schemaname,
         r_result.funcname,
         r_result.calls,
			round(CAST(r_result.total_time AS numeric),2),
			round(CAST(r_result.self_time AS numeric),2),
			round(CAST(r_result.m_time AS numeric),3),
			round(CAST(r_result.m_stime AS numeric),3)
         );
	END LOOP;
   
   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION func_top_calls_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';

	tab_tpl CONSTANT text := '<table><tr><th>DB</th><th>Schema</th><th>Function</th><th>Executions</th><th>Total time</th><th>Self time</th><th>Mean time</th><th>Mean self time</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

   c_tbl_stats CURSOR (s_id integer, e_id integer, cnt integer) FOR
   SELECT 
      db_s.datname as dbname,
      st.schemaname,
      st.funcname,
      sum(st.calls) as calls,
      sum(st.total_time) as total_time,
      sum(st.self_time) as self_time,
      sum(st.total_time)/sum(st.calls) as m_time,
      sum(st.self_time)/sum(st.calls) as m_stime
   FROM ONLY(snap_stat_user_functions) st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   WHERE db_s.datname not like 'template_' AND st.snap_id BETWEEN db_s.snap_id + 1 AND db_e.snap_id
   GROUP BY db_s.datid,funcid,db_s.datname,st.schemaname,st.funcname
   ORDER BY sum(st.calls) DESC
   LIMIT cnt;

	r_result RECORD;
BEGIN
	FOR r_result IN c_tbl_stats(start_id, end_id, topn) LOOP
		report := report||format(row_tpl,
			r_result.dbname,
         r_result.schemaname,
         r_result.funcname,
         r_result.calls,
			round(CAST(r_result.total_time AS numeric),2),
			round(CAST(r_result.self_time AS numeric),2),
			round(CAST(r_result.m_time AS numeric),3),
			round(CAST(r_result.m_stime AS numeric),3)
         );
	END LOOP;
   
   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_elapsed_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';
   
	-- Elapsed time sorted list TPLs
	tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Elapsed(s)</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Executions</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

	--Cursor for top(cnt) queries ordered by epapsed time 
	c_elapsed_time CURSOR (s_id integer, e_id integer, cnt integer) FOR 
   WITH tot AS (SELECT GREATEST(sum(total_time),1) AS total_time
         FROM snap_statements_total
         WHERE snap_id BETWEEN s_id AND e_id
         )
	SELECT st.queryid,
	st.query,db_s.datname as dbname,sum(st.calls) as calls,sum(st.total_time)/1000 as total_time,sum(st.total_time/tot.total_time)*100 as total_pct,
   min(st.min_time) as min_time,max(st.max_time) as max_time,sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
	sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
	sum(st.rows) as rows, sum(st.shared_blks_hit) as shared_blks_hit, sum(st.shared_blks_read) as shared_blks_read,
	sum(st.shared_blks_dirtied) as shared_blks_dirtied, sum(st.shared_blks_written) as shared_blks_written,
	sum(st.local_blks_hit) as local_blks_hit, sum(st.local_blks_read) as local_blks_read, sum(st.local_blks_dirtied) as local_blks_dirtied,
	sum(st.local_blks_written) as local_blks_written, sum(st.temp_blks_read) as temp_blks_read, sum(st.temp_blks_written) as temp_blks_written,
	sum(st.blk_read_time) as blk_read_time, sum(st.blk_write_time) as blk_write_time
	FROM snap_statements st 
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   -- Total stats
   CROSS JOIN tot
	WHERE st.snap_id BETWEEN s_id + 1 AND e_id
	GROUP BY st.queryid,st.query,db_s.datname
	ORDER BY total_time DESC
	LIMIT cnt;
   
	r_result RECORD;
BEGIN
	-- Reporting on top 10 queries by elapsed time
	FOR r_result IN c_elapsed_time(start_id, end_id,topn) LOOP
		report := report||format(row_tpl,
			to_hex(r_result.queryid),
			to_hex(r_result.queryid),
         r_result.dbname,
			round(CAST(r_result.total_time AS numeric),1),
			round(CAST(r_result.total_pct AS numeric),2),
			r_result.rows,
			round(CAST(r_result.mean_time AS numeric),3),
			round(CAST(r_result.min_time AS numeric),3),
			round(CAST(r_result.max_time AS numeric),3),
			round(CAST(r_result.stddev_time AS numeric),3),
			r_result.calls);
      PERFORM collect_queries(r_result.queryid,r_result.query);
	END LOOP;
   
   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_exec_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';
   
	-- Executions sorted list TPLs
	tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Executions</th><th>%Total</th><th>Rows</th><th>Mean(ms)</th><th>Min(ms)</th><th>Max(ms)</th><th>StdErr(ms)</th><th>Total(s)</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

	--Cursor for top(cnt) querues ordered by executions 
	c_calls CURSOR (s_id integer, e_id integer, cnt integer) FOR 
   WITH tot AS (SELECT GREATEST(sum(calls),1) AS calls
         FROM snap_statements_total
         WHERE snap_id BETWEEN s_id AND e_id
         )
	SELECT st.queryid,
	st.query,db_s.datname as dbname,sum(st.calls) as calls,sum(st.calls/tot.calls)*100 as total_pct,sum(st.total_time)/1000 as total_time,min(st.min_time) as min_time,
   max(st.max_time) as max_time,sum(st.mean_time*st.calls)/sum(st.calls) as mean_time,
	sqrt(sum((power(st.stddev_time,2)+power(st.mean_time,2))*st.calls)/sum(st.calls)-power(sum(st.mean_time*st.calls)/sum(st.calls),2)) as stddev_time,
	sum(st.rows) as rows
	FROM snap_statements st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   -- Total stats
   CROSS JOIN tot
	WHERE st.snap_id BETWEEN s_id + 1 AND e_id
	GROUP BY st.queryid,st.query,db_s.datname
	ORDER BY calls DESC
	LIMIT cnt;
   
	r_result RECORD;
BEGIN
	-- Reporting on top 10 queries by executions
	FOR r_result IN c_calls(start_id, end_id,topn) LOOP
		report := report||format(row_tpl,
			to_hex(r_result.queryid),
			to_hex(r_result.queryid),
         r_result.dbname,
			r_result.calls,
			round(CAST(r_result.total_pct AS numeric),2),
			r_result.rows,
			round(CAST(r_result.mean_time AS numeric),3),
			round(CAST(r_result.min_time AS numeric),3),
			round(CAST(r_result.max_time AS numeric),3),
			round(CAST(r_result.stddev_time AS numeric),3),
			round(CAST(r_result.total_time AS numeric),1));
      PERFORM collect_queries(r_result.queryid,r_result.query);
	END LOOP;
   
   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_iowait_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';
   
	-- IOWait time sorted list TPLs
	tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>IO wait(s)</th><th>%Total</th><th>Reads</th><th>Writes</th><th>Executions</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

	--Cursor for top(cnt) querues ordered by I/O Wait time 
	c_iowait_time CURSOR (s_id integer, e_id integer, cnt integer) FOR 
   WITH tot AS (SELECT CASE WHEN sum(blk_read_time) = 0 THEN 1 ELSE sum(blk_read_time) END AS blk_read_time,
         CASE WHEN sum(blk_write_time) = 0 THEN 1 ELSE sum(blk_write_time) END AS blk_write_time
         FROM snap_statements_total
         WHERE snap_id BETWEEN s_id + 1 AND e_id
         )
	SELECT st.queryid,
	st.query,db_s.datname as dbname,sum(st.calls) as calls,sum(st.total_time)/1000 as total_time,
	sum(st.rows) as rows, sum(st.shared_blks_hit) as shared_blks_hit, sum(st.shared_blks_read) as shared_blks_read,
	sum(st.shared_blks_dirtied) as shared_blks_dirtied, sum(st.shared_blks_written) as shared_blks_written,
	sum(st.local_blks_hit) as local_blks_hit, sum(st.local_blks_read) as local_blks_read, sum(st.local_blks_dirtied) as local_blks_dirtied,
	sum(st.local_blks_written) as local_blks_written, sum(st.temp_blks_read) as temp_blks_read, sum(st.temp_blks_written) as temp_blks_written,
	sum(st.blk_read_time) as blk_read_time, sum(st.blk_write_time) as blk_write_time, (sum(st.blk_read_time + st.blk_write_time))/1000 as io_time,
   (sum(st.blk_read_time/tot.blk_read_time) + sum(st.blk_write_time/tot.blk_write_time))*100 as total_pct
	FROM snap_statements st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   -- Total stats
   CROSS JOIN tot
	WHERE st.snap_id BETWEEN s_id + 1 AND e_id
	GROUP BY st.queryid,st.query,db_s.datname
	HAVING sum(st.blk_read_time) + sum(st.blk_write_time) > 0
	ORDER BY io_time DESC
	LIMIT cnt;

	r_result RECORD;
BEGIN
	-- Reporting on top 10 queries by I/O wait time
	FOR r_result IN c_iowait_time(start_id, end_id,topn) LOOP
		report := report||format(row_tpl,
			to_hex(r_result.queryid),
			to_hex(r_result.queryid),
         r_result.dbname,
			round(CAST(r_result.total_time AS numeric),1),
			round(CAST(r_result.io_time AS numeric),3),
			round(CAST(r_result.total_pct AS numeric),2),
			round(CAST(r_result.shared_blks_read AS numeric),3),
			round(CAST(r_result.shared_blks_written AS numeric),3),
			r_result.calls);
      PERFORM collect_queries(r_result.queryid,r_result.query);
	END LOOP;

   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_gets_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';
   
	-- Gets sorted list TPLs
	tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>%Total</th><th>Hits(%)</th><th>Executions</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

	--Cursor for top(cnt) querues ordered by gets
	c_gets CURSOR (s_id integer, e_id integer, cnt integer) FOR 
   WITH tot AS (SELECT GREATEST(sum(shared_blks_hit),1) AS shared_blks_hit,
         GREATEST(sum(shared_blks_read),1) AS shared_blks_read
         FROM snap_statements_total
         WHERE snap_id BETWEEN s_id + 1 AND e_id
         )
	SELECT st.queryid,
	st.query,db_s.datname as dbname,sum(st.calls) as calls,sum(st.total_time)/1000 as total_time,
	sum(st.rows) as rows,
	sum(st.shared_blks_hit) + sum(st.shared_blks_read) as gets,
   (sum(st.shared_blks_hit/tot.shared_blks_hit) + sum(st.shared_blks_read/tot.shared_blks_read))*100 as total_pct,
	sum(st.shared_blks_hit) * 100 / CASE WHEN (sum(st.shared_blks_hit)+sum(st.shared_blks_read)) = 0 THEN 1
		ELSE (sum(st.shared_blks_hit)+sum(st.shared_blks_read)) END as hit_pct
	FROM snap_statements st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   -- Total stats
   CROSS JOIN tot
	WHERE st.snap_id BETWEEN s_id + 1 AND e_id
	GROUP BY st.queryid,st.query,db_s.datname
	HAVING sum(st.shared_blks_hit) + sum(st.shared_blks_read) > 0
	ORDER BY gets DESC
	LIMIT cnt;

	r_result RECORD;
BEGIN
	-- Reporting on top queries by gets
	FOR r_result IN c_gets(start_id, end_id,topn) LOOP
		report := report||format(row_tpl,
			to_hex(r_result.queryid),
			to_hex(r_result.queryid),
         r_result.dbname,
			round(CAST(r_result.total_time AS numeric),1),
			r_result.rows,
			r_result.gets,
			round(CAST(r_result.total_pct AS numeric),2),
			round(CAST(r_result.hit_pct AS numeric),2),
			r_result.calls);
      PERFORM collect_queries(r_result.queryid,r_result.query);
	END LOOP;
   
   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_temp_htbl(IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	report text := '';
   
	-- Temp usage sorted list TPLs
	tab_tpl CONSTANT text := '<table><tr><th>Query ID</th><th>Database</th><th>Total(s)</th><th>Rows</th><th>Gets</th><th>Hits(%)</th><th>Temp_w(blk)</th><th>%Total</th><th>Temp_r(blk)</th><th>%Total</th><th>Executions</th></tr>{rows}</table>';
	row_tpl CONSTANT text := '<tr><td><a HREF="#%s">%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

	--Cursor for top(cnt) querues ordered by temp usage 
	c_temp CURSOR (s_id integer, e_id integer, cnt integer) FOR 
   WITH tot AS (SELECT GREATEST(sum(temp_blks_read),1) AS temp_blks_read,
         GREATEST(sum(temp_blks_written),1) AS temp_blks_written
         FROM snap_statements_total
         WHERE snap_id BETWEEN s_id + 1 AND e_id
         )
	SELECT st.queryid,
	st.query,db_s.datname as dbname,sum(st.calls) as calls,sum(st.total_time)/1000 as total_time,
	sum(st.rows) as rows, sum(st.shared_blks_hit) + sum(st.shared_blks_read) as gets,
	sum(st.shared_blks_hit) * 100 / GREATEST(sum(st.shared_blks_hit)+sum(st.shared_blks_read),1) as hit_pct,
	sum(st.temp_blks_read) as temp_blks_read, sum(st.temp_blks_written) as temp_blks_written,
   sum(st.temp_blks_read/tot.temp_blks_read)*100 as read_total_pct,
   sum(st.temp_blks_written/tot.temp_blks_written)*100 as write_total_pct
	FROM snap_statements st
   -- Database name and existance condition
   JOIN ONLY(snap_stat_database) db_s ON (db_s.datid=st.dbid and db_s.snap_id=s_id) 
	JOIN ONLY(snap_stat_database) db_e ON (db_e.datid=st.dbid and db_e.snap_id=e_id and db_s.datname=db_e.datname)
   -- Total stats
   CROSS JOIN tot
	WHERE st.snap_id BETWEEN s_id + 1 AND e_id
	GROUP BY st.queryid,st.query,db_s.datname
	HAVING sum(st.temp_blks_read) + sum(st.temp_blks_written) > 0
	ORDER BY sum(st.temp_blks_read) + sum(st.temp_blks_written) DESC
	LIMIT cnt;

	r_result RECORD;
BEGIN
	-- Reporting on top queries by temp usage
	FOR r_result IN c_temp(start_id, end_id,topn) LOOP
		report := report||format(row_tpl,
			to_hex(r_result.queryid),
			to_hex(r_result.queryid),
         r_result.dbname,
			round(CAST(r_result.total_time AS numeric),1),
			r_result.rows,
			r_result.gets,
			round(CAST(r_result.hit_pct AS numeric),2),
			r_result.temp_blks_written,
			round(CAST(r_result.write_total_pct AS numeric),2),
			r_result.temp_blks_read,
			round(CAST(r_result.read_total_pct AS numeric),2),
			r_result.calls);
      PERFORM collect_queries(r_result.queryid,r_result.query);
	END LOOP;

   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION report_queries() RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
   c_queries CURSOR FOR SELECT queryid, querytext FROM queries_list;
   qr_result RECORD;
   report text := '';
   tab_tpl CONSTANT text := '<table><tr><th>QueryID</th><th>Query Text</th></tr>{rows}</table>';
   row_tpl CONSTANT text := '<tr><td><a NAME=%s>%s</a></td><td>%s</td></tr>';
BEGIN
   FOR qr_result IN c_queries LOOP
		report := report||format(row_tpl,
			to_hex(qr_result.queryid),
			to_hex(qr_result.queryid),
			qr_result.querytext);
	END LOOP;

   IF report != '' THEN
      RETURN replace(tab_tpl,'{rows}',report);
   ELSE
      RETURN '';
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nodata_wrapper(IN section_text text) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
   IF section_text IS NULL OR section_text = '' THEN
      RETURN '<p>No data in this section</p>';
   ELSE
      RETURN section_text;
   END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION report(IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
	tmp_text text;
   tmp_report text;
	report   text;
   topn     integer;
	-- HTML elements templates
   report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile report {snaps}</title></head><body><H1>Postgres profile report {snaps}</H1><p>Report interval: {report_start} - {report_end}</p>{report}</body></html>';
   report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} table tr:nth-child(even) {background-color: #eee;} table tr:nth-child(odd) {background-color: #fff;} table tr:hover{background-color:#d9ffcc} table th {color: black; background-color: #ffcc99;}';
	--Cursor and variable for checking existance of snapshots
	c_snap CURSOR (snapshot_id integer) FOR SELECT * FROM snapshots WHERE snap_id = snapshot_id;
	snap_rec snapshots%rowtype;
BEGIN
   -- Creating temporary table for reported queries
   CREATE TEMPORARY TABLE IF NOT EXISTS queries_list (queryid bigint PRIMARY KEY, querytext text) ON COMMIT DELETE ROWS;
   
   -- CSS
   report := replace(report_tpl,'{css}',report_css);
   
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
   report := replace(report,'{report_start}',cast(snap_rec.snap_time as text));
	tmp_text := '(StartID: ' || snap_rec.snap_id ||', ';
	CLOSE c_snap;

	OPEN c_snap(end_id);
	FETCH c_snap INTO snap_rec;
	IF snap_rec IS NULL THEN
		RAISE 'End snapshot % does not exists', end_id;
	END IF;
   report := replace(report,'{report_end}',cast(snap_rec.snap_time as text));
	tmp_text := tmp_text || 'EndID: ' || snap_rec.snap_id ||')';
	CLOSE c_snap;
	report := replace(report,'{snaps}',tmp_text);
   tmp_text := '';
   
   -- Reporting possible statements overflow
   tmp_report := check_stmt_cnt(start_id, end_id);
   IF tmp_report != '' THEN
      tmp_text := tmp_text || '<H2>Warning!</H2>';
      tmp_text := tmp_text || '<p>This interval contains snapshots with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
      tmp_text := tmp_text || tmp_report;
   END IF;

   -- Table of Contents
   tmp_text := tmp_text ||'<H2>Report sections</H2><ul>';
   tmp_text := tmp_text || '<li><a HREF=#cl_stat>Cluster statistics</a></li>';
   tmp_text := tmp_text || '<ul>';
   tmp_text := tmp_text || '<li><a HREF=#db_stat>Databases stats</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#st_stat>Statements stats by database</a></li>';
   tmp_text := tmp_text || '</ul>';
   tmp_text := tmp_text || '<li><a HREF=#sql_stat>SQL Query stats</a></li>';
   tmp_text := tmp_text || '<ul>';
   tmp_text := tmp_text || '<li><a HREF=#top_ela>Top SQL by elapsed time</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#top_calls>Top SQL by elapsed time</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#top_iowait>Top SQL by I/O wait time</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#top_gets>Top SQL by gets</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#top_temp>Top SQL by temp usage</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#sql_list>Complete List of SQL Text</a></li>';
   tmp_text := tmp_text || '</ul>';
   
   tmp_text := tmp_text || '<li><a HREF=#schema_stat>Schema objects stats</a></li>';
   tmp_text := tmp_text || '<ul>';
   tmp_text := tmp_text || '<li><a HREF=#scanned_tbl>Most scanned tables</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#dml_tbl>Top DML tables</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#growth_tbl>Top growth tables</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#ix_unused>Unused indexes</a></li>';
   tmp_text := tmp_text || '</ul>';
   tmp_text := tmp_text || '<li><a HREF=#io_stat>I/O Schema objects stats</a></li>';
   tmp_text := tmp_text || '<ul>';
   tmp_text := tmp_text || '<li><a HREF=#tbl_io_stat>Top tables by I/O</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#ix_io_stat>Top indexes by I/O</a></li>';
   tmp_text := tmp_text || '</ul>';
   
   tmp_text := tmp_text || '<li><a HREF=#func_stat>User function stats</a></li>';
   tmp_text := tmp_text || '<ul>';
   tmp_text := tmp_text || '<li><a HREF=#funs_time_stat>Top functions by total time</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#funs_calls_stat>Top functions by executions</a></li>';
   tmp_text := tmp_text || '</ul>';

   
   tmp_text := tmp_text || '<li><a HREF=#vacuum_stats>Vacuum related stats</a></li>';
   tmp_text := tmp_text || '<ul>';
   tmp_text := tmp_text || '<li><a HREF=#dead_tbl>Tables ordered by dead tuples ratio</a></li>';
   tmp_text := tmp_text || '<li><a HREF=#mod_tbl>Tables ordered by modified tuples ratio</a></li>';
   tmp_text := tmp_text || '</ul>';
   tmp_text := tmp_text || '</ul>';
   
   
   --Reporting cluster stats
	tmp_text := tmp_text || '<H2><a NAME=cl_stat>Cluster statistics</a></H2>';
	tmp_text := tmp_text || '<H3><a NAME=db_stat>Databases stats</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(dbstats_htbl(start_id, end_id, topn));

	tmp_text := tmp_text || '<H3><a NAME=st_stat>Statements stats by database</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(statements_stats_htbl(start_id, end_id, topn));
   
   --Reporting on top queries by elapsed time
	tmp_text := tmp_text||'<H2><a NAME=sql_stat>SQL Query stats</a></H2>';
   tmp_text := tmp_text||'<H3><a NAME=top_ela>Top SQL by elapsed time</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(top_elapsed_htbl(start_id, end_id, topn));

	-- Reporting on top queries by executions
	tmp_text := tmp_text||'<H3><a NAME=top_calls>Top SQL by executions</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(top_exec_htbl(start_id, end_id, topn));

	-- Reporting on top queries by I/O wait time
	tmp_text := tmp_text||'<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(top_iowait_htbl(start_id, end_id, topn));

	-- Reporting on top queries by gets
	tmp_text := tmp_text||'<H3><a NAME=top_gets>Top SQL by gets</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(top_gets_htbl(start_id, end_id, topn));

	-- Reporting on top queries by temp usage
	tmp_text := tmp_text||'<H3><a NAME=top_temp>Top SQL by temp usage</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(top_temp_htbl(start_id, end_id, topn));

   -- Listing queries
	tmp_text := tmp_text||'<H3><a NAME=sql_list>Complete List of SQL Text</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(report_queries());
   
   -- Reporting Object stats
   -- Reporting scanned table
	tmp_text := tmp_text||'<H2><a NAME=schema_stat>Schema objects stats</a></H2>';
   tmp_text := tmp_text||'<H3><a NAME=scanned_tbl>Most scanned tables</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(top_scan_tables_htbl(start_id, end_id, topn));
   
   tmp_text := tmp_text||'<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(top_dml_tables_htbl(start_id, end_id, topn));

   tmp_text := tmp_text||'<H3><a NAME=growth_tbl>Top growth tables</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(top_growth_tables_htbl(start_id, end_id, topn));
   
   tmp_text := tmp_text||'<H3><a NAME=ix_unused>Unused indexes</a></H3>';
   tmp_text := tmp_text||'<p>This table contains not-scanned indexes (during report period), ordered by number of DML operations on underlying tables. Constraint indexes are excluded.</p>';
   tmp_text := tmp_text || nodata_wrapper(ix_unused_htbl(start_id, end_id, topn));
   
   tmp_text := tmp_text || '<H2><a NAME=io_stat>I/O Schema objects stats</a></H2>';
   tmp_text := tmp_text || '<H3><a NAME=tbl_io_stat>Top tables by read I/O</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(tbl_top_io_htbl(start_id, end_id, topn));

   tmp_text := tmp_text || '<H3><a NAME=ix_io_stat>Top indexes by read I/O</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(ix_top_io_htbl(start_id, end_id, topn));

   tmp_text := tmp_text || '<H2><a NAME=func_stat>User function stats</a></H2>';
   tmp_text := tmp_text || '<H3><a NAME=funs_time_stat>Top functions by total time</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(func_top_time_htbl(start_id, end_id, topn));
   
   tmp_text := tmp_text || '<H3><a NAME=funs_calls_stat>Top functions by executions</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(func_top_calls_htbl(start_id, end_id, topn));
   
   -- Reporting vacuum related stats
	tmp_text := tmp_text||'<H2><a NAME=vacuum_stats>Vacuum related stats</a></H2>';
	tmp_text := tmp_text||'<p>Data in this section is not incremental. This data is valid for endind snapshot only.</p>';
   tmp_text := tmp_text||'<H3><a NAME=dead_tbl>Tables ordered by dead tuples ratio</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(tbl_top_dead_htbl(start_id, end_id, topn));

   tmp_text := tmp_text||'<H3><a NAME=mod_tbl>Tables ordered by modified tuples ratio</a></H3>';
   tmp_text := tmp_text || nodata_wrapper(tbl_top_mods_htbl(start_id, end_id, topn));
   
   -- Reporting possible statements overflow
   tmp_report := check_stmt_cnt();
   IF tmp_report != '' THEN
      tmp_text := tmp_text || '<H2>Warning!</H2>';
      tmp_text := tmp_text || '<p>Snapshot repository contains snapshots with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
      tmp_text := tmp_text || tmp_report;
   END IF;
   
	RETURN replace(report,'{report}',tmp_text);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION report(IN start_id integer, IN end_id integer) IS 'Statistics report generation function. Takes IDs of start and end snapshot (inclusive)';