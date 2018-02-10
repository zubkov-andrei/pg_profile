\echo Use "ALTER EXTENSION pg_profile UPDATE" to load this file. \quit

CREATE OR REPLACE FUNCTION collect_obj_stats(IN s_id integer) RETURNS integer SET search_path=@extschema@,public AS $$
DECLARE
   --Cursor for db stats
   c_dblist CURSOR FOR
   select dbs.datid,dbs.datname,s1.setting as port from pg_catalog.pg_stat_database dbs, pg_catalog.pg_settings s1
   where dbs.datname not like 'template_' and s1.name='port';

	r_result RECORD;
BEGIN
   -- Creating temporary tables, holding data for objects of all cluster databases
   CREATE TEMPORARY TABLE IF NOT EXISTS temp_stat_user_tables () INHERITS (snap_stat_user_tables) ON COMMIT DROP;
   CREATE TEMPORARY TABLE IF NOT EXISTS temp_stat_user_functions () INHERITS (snap_stat_user_functions) ON COMMIT DROP;
   CREATE TEMPORARY TABLE IF NOT EXISTS temp_stat_user_indexes () INHERITS (snap_stat_user_indexes) ON COMMIT DROP;
   CREATE TEMPORARY TABLE IF NOT EXISTS temp_statio_user_tables () INHERITS (snap_statio_user_tables) ON COMMIT DROP;
   CREATE TEMPORARY TABLE IF NOT EXISTS temp_statio_user_indexes () INHERITS (snap_statio_user_indexes) ON COMMIT DROP;

   -- Load new data from statistic views of all cluster databases
	FOR r_result IN c_dblist LOOP
      INSERT INTO temp_stat_user_tables
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select *,pg_relation_size(relid) relsize,0 relsize_diff from pg_catalog.pg_stat_user_tables')
      AS t (
         relid oid,
         schemaname name,
         relname name,
         seq_scan bigint,
         seq_tup_read bigint,
         idx_scan bigint,
         idx_tup_fetch bigint,
         n_tup_ins bigint,
         n_tup_upd bigint,
         n_tup_del bigint,
         n_tup_hot_upd bigint,
         n_live_tup bigint,
         n_dead_tup bigint,
         n_mod_since_analyze bigint,
         last_vacuum timestamp with time zone,
         last_autovacuum timestamp with time zone,
         last_analyze timestamp with time zone,
         last_autoanalyze timestamp with time zone,
         vacuum_count bigint,
         autovacuum_count bigint,
         analyze_count bigint,
         autoanalyze_count bigint,
         relsize bigint,
         relsize_diff bigint
      );
      
      INSERT INTO temp_stat_user_indexes
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select st.*,pg_relation_size(st.relid),0,(ix.indisunique or con.conindid IS NOT NULL) as indisunique
from pg_catalog.pg_stat_user_indexes st 
join pg_catalog.pg_index ix on (ix.indexrelid = st.indexrelid) 
left join pg_catalog.pg_constraint con on(con.conindid = ix.indexrelid and con.contype in (''p'',''u''))')
      AS t (
         relid oid,
         indexrelid oid,
         schemaname name,
         relname name,
         indexrelname name,
         idx_scan bigint,
         idx_tup_read bigint,
         idx_tup_fetch bigint,
         relsize bigint,
         relsize_diff bigint,
         indisunique bool
      );
      
      INSERT INTO temp_stat_user_functions
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select * from pg_catalog.pg_stat_user_functions')
      AS t (
         funcid oid,
         schemaname name,
         funcname name,
         calls bigint,
         total_time double precision,
         self_time double precision
      );

      INSERT INTO temp_statio_user_tables
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select *,pg_relation_size(relid),0 from pg_catalog.pg_statio_user_tables')
      AS t (
         relid oid,
         schemaname name,
         relname name,
         heap_blks_read bigint,
         heap_blks_hit bigint,
         idx_blks_read bigint,
         idx_blks_hit bigint,
         toast_blks_read bigint,
         toast_blks_hit bigint,
         tidx_blks_read bigint,
         tidx_blks_hit bigint,
         relsize bigint,
         relsize_diff bigint
      );
      
      INSERT INTO temp_statio_user_indexes
      SELECT s_id,r_result.datid,t.*
      FROM dblink('dbname='||r_result.datname||' port='||r_result.port, 'select *,pg_relation_size(relid),0 from pg_catalog.pg_statio_user_indexes')
      AS t (
         relid oid,
         indexrelid oid,
         schemaname name,
         relname name,
         indexrelname name,
         idx_blks_read bigint,
         idx_blks_hit bigint,
         relsize bigint,
         relsize_diff bigint
      );
	END LOOP;
   RETURN 0;
END;
$$ LANGUAGE plpgsql; 
