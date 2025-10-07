CREATE FUNCTION collect_obj_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer,
  IN skip_sizes boolean
) RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    --Cursor over databases
    c_dblist CURSOR FOR
    SELECT
      datid,
      datname,
      dattablespace AS tablespaceid
    FROM last_stat_database ldb
      JOIN servers n ON
        (n.server_id = sserver_id AND array_position(n.db_exclude,ldb.datname) IS NULL)
    WHERE
      NOT ldb.datistemplate AND ldb.datallowconn AND
      (ldb.server_id, ldb.sample_id) = (sserver_id, s_id);

    qres        record;
    db_connstr  text;
    t_query     text;
    analyze_list  text[] := array[]::text[];
    analyze_obj   text;
    result      jsonb := collect_obj_stats.properties;
    pg_version int := (get_sp_setting(properties, 'server_version_num')).reset_val::integer;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Disconnecting existing connection
    IF dblink_get_connections() @> ARRAY['server_db_connection'] THEN
        PERFORM dblink_disconnect('server_db_connection');
    END IF;

    -- Load new data from statistic views of all cluster databases
    FOR qres IN c_dblist LOOP
      db_connstr := concat_ws(' ',properties #>> '{properties,server_connstr}',
        format($o$dbname='%s'$o$,replace(qres.datname,$o$'$o$,$o$\'$o$)) --'
      );
      PERFORM dblink_connect('server_db_connection',db_connstr);
      -- Transaction
      PERFORM dblink('server_db_connection','BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
      -- Setting application name
      PERFORM dblink('server_db_connection','SET application_name=''{pg_profile}''');
      -- Conditionally set lock_timeout
      IF (
        SELECT lock_timeout_unset
        FROM dblink('server_db_connection',
          $sql$SELECT current_setting('lock_timeout')::interval = '0s'::interval$sql$)
          AS probe(lock_timeout_unset boolean)
        )
      THEN
        -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
        PERFORM dblink('server_db_connection',
          format('SET lock_timeout TO %L',
            COALESCE(properties #>> '{properties,lock_timeout_effective}','3s')
          )
        );
      END IF;
      -- Reset search_path for security reasons
      PERFORM dblink('server_db_connection','SET search_path=''''');

      result := log_sample_timings(result, format('db:%s get extensions version',qres.datname), 'start');

      t_query := 'SELECT '
        'extname,'
        'extversion '
        'FROM pg_extension';

      INSERT INTO last_extension_versions (
        server_id,
        datid,
        sample_id,
        extname,
        extversion
      )
      SELECT
        sserver_id as server_id,
        qres.datid,
        s_id as sample_id,
        dbl.extname,
        dbl.extversion
      FROM dblink('server_db_connection', t_query)
      AS dbl (
         extname    name,
         extversion text
      );

      result := log_sample_timings(result, format('db:%s get extensions version',qres.datname), 'end');
      result := log_sample_timings(result, format('db:%s collect tables stats',qres.datname), 'start');

      -- Generate Table stats query
      CASE
        WHEN pg_version < 130000 THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'NULL as n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'NULL as total_vacuum_time,'
            'NULL as total_autovacuum_time,'
            'NULL as total_analyze_time,'
            'NULL as total_autoanalyze_time,'
            'pg_catalog.pg_stat_get_blocks_fetched(class.oid) -
                    pg_catalog.pg_stat_get_blocks_hit(class.oid),'
            'pg_catalog.pg_stat_get_blocks_hit(class.oid),'
            'I.idx_blks_read,'
            'I.idx_blks_hit,'
            'pg_catalog.pg_stat_get_blocks_fetched(class.reltoastrelid) - '
                'pg_catalog.pg_stat_get_blocks_hit(class.reltoastrelid) '
                'AS toast_blks_read,'
            'pg_catalog.pg_stat_get_blocks_hit(class.reltoastrelid) '
                'AS toast_blks_hit,'
            'X.idx_blks_read AS tidx_blks_read,'
            'X.idx_blks_hit AS tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'NULL AS last_seq_scan,'
            'NULL AS last_idx_scan,'
            'NULL AS n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          'LEFT JOIN ixstat I ON I.indrelid = class.oid '
          'LEFT JOIN ixstat X ON I.indrelid = class.reltoastrelid '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 150000
        )
        THEN
          t_query :=
          'WITH ixstat AS ( '
            'SELECT '
              'indrelid, '
              'sum(pg_catalog.pg_stat_get_blocks_fetched(indexrelid) -'
                  'pg_catalog.pg_stat_get_blocks_hit(indexrelid))::bigint '
                  'AS idx_blks_read,'
              'sum(pg_stat_get_blocks_hit(indexrelid))::bigint '
                  'AS idx_blks_hit '
            'FROM '
              'pg_catalog.pg_index '
            'GROUP BY indrelid'
          ') '
          'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'st.n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'NULL as total_vacuum_time,'
            'NULL as total_autovacuum_time,'
            'NULL as total_analyze_time,'
            'NULL as total_autoanalyze_time,'
            'pg_catalog.pg_stat_get_blocks_fetched(class.oid) -
                    pg_catalog.pg_stat_get_blocks_hit(class.oid),'
            'pg_catalog.pg_stat_get_blocks_hit(class.oid),'
            'I.idx_blks_read,'
            'I.idx_blks_hit,'
            'pg_catalog.pg_stat_get_blocks_fetched(class.reltoastrelid) - '
                'pg_catalog.pg_stat_get_blocks_hit(class.reltoastrelid) '
                'AS toast_blks_read,'
            'pg_catalog.pg_stat_get_blocks_hit(class.reltoastrelid) '
                'AS toast_blks_hit,'
            'X.idx_blks_read AS tidx_blks_read,'
            'X.idx_blks_hit AS tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'NULL AS last_seq_scan,'
            'NULL AS last_idx_scan,'
            'NULL AS n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          'LEFT JOIN ixstat I ON I.indrelid = class.oid '
          'LEFT JOIN ixstat X ON I.indrelid = class.reltoastrelid '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN pg_version < 160000 THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'st.n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'NULL as total_vacuum_time,'
            'NULL as total_autovacuum_time,'
            'NULL as total_analyze_time,'
            'NULL as total_autoanalyze_time,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'NULL AS last_seq_scan,'
            'NULL AS last_idx_scan,'
            'NULL AS n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN pg_version < 180000 THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'st.n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'NULL as total_vacuum_time,'
            'NULL as total_autovacuum_time,'
            'NULL as total_analyze_time,'
            'NULL as total_autoanalyze_time,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'st.last_seq_scan,'
            'st.last_idx_scan,'
            'st.n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN pg_version >= 180000 THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'st.n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'st.total_vacuum_time,'
            'st.total_autovacuum_time,'
            'st.total_analyze_time,'
            'st.total_autoanalyze_time,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'st.last_seq_scan,'
            'st.last_idx_scan,'
            'st.n_tup_newpage_upd,'
            'to_jsonb(class.reloptions) '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        ELSE
          RAISE 'Unsupported server version.';
      END CASE;

      IF skip_sizes THEN
        t_query := replace(t_query,'{relation_size}','NULL');
        t_query := replace(t_query,'{lock_join}','');
      ELSE
        t_query := replace(t_query,'{relation_size}','CASE locked.objid WHEN st.relid THEN NULL ELSE '
          'pg_catalog.pg_table_size(st.relid) - '
          'coalesce(pg_catalog.pg_relation_size(class.reltoastrelid),0) END');
        t_query := replace(t_query,'{lock_join}',
          'LEFT OUTER JOIN LATERAL '
            '(WITH RECURSIVE deps (objid) AS ('
              'SELECT relation FROM pg_catalog.pg_locks WHERE granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'' '
              'UNION '
              'SELECT refobjid FROM pg_catalog.pg_depend d JOIN deps dd ON (d.objid = dd.objid)'
            ') '
            'SELECT objid FROM deps) AS locked ON (st.relid = locked.objid)');
      END IF;

      IF COALESCE((properties #> '{collect,relations}')::boolean, true) THEN
        INSERT INTO last_stat_tables(
          server_id,
          sample_id,
          datid,
          relid,
          schemaname,
          relname,
          seq_scan,
          seq_tup_read,
          idx_scan,
          idx_tup_fetch,
          n_tup_ins,
          n_tup_upd,
          n_tup_del,
          n_tup_hot_upd,
          n_live_tup,
          n_dead_tup,
          n_mod_since_analyze,
          n_ins_since_vacuum,
          last_vacuum,
          last_autovacuum,
          last_analyze,
          last_autoanalyze,
          vacuum_count,
          autovacuum_count,
          analyze_count,
          autoanalyze_count,
          total_vacuum_time,
          total_autovacuum_time,
          total_analyze_time,
          total_autoanalyze_time,
          heap_blks_read,
          heap_blks_hit,
          idx_blks_read,
          idx_blks_hit,
          toast_blks_read,
          toast_blks_hit,
          tidx_blks_read,
          tidx_blks_hit,
          relsize,
          relsize_diff,
          tablespaceid,
          reltoastrelid,
          relkind,
          in_sample,
          relpages_bytes,
          relpages_bytes_diff,
          last_seq_scan,
          last_idx_scan,
          n_tup_newpage_upd,
          reloptions
        )
        SELECT
          sserver_id,
          s_id,
          qres.datid,
          dbl.relid,
          dbl.schemaname,
          dbl.relname,
          dbl.seq_scan AS seq_scan,
          dbl.seq_tup_read AS seq_tup_read,
          dbl.idx_scan AS idx_scan,
          dbl.idx_tup_fetch AS idx_tup_fetch,
          dbl.n_tup_ins AS n_tup_ins,
          dbl.n_tup_upd AS n_tup_upd,
          dbl.n_tup_del AS n_tup_del,
          dbl.n_tup_hot_upd AS n_tup_hot_upd,
          dbl.n_live_tup AS n_live_tup,
          dbl.n_dead_tup AS n_dead_tup,
          dbl.n_mod_since_analyze AS n_mod_since_analyze,
          dbl.n_ins_since_vacuum AS n_ins_since_vacuum,
          dbl.last_vacuum,
          dbl.last_autovacuum,
          dbl.last_analyze,
          dbl.last_autoanalyze,
          dbl.vacuum_count AS vacuum_count,
          dbl.autovacuum_count AS autovacuum_count,
          dbl.analyze_count AS analyze_count,
          dbl.autoanalyze_count AS autoanalyze_count,
          dbl.total_vacuum_time AS total_vacuum_time,
          dbl.total_autovacuum_time AS total_autovacuum_time,
          dbl.total_analyze_time AS total_analyze_time,
          dbl.total_autoanalyze_time AS total_autoanalyze_time,
          dbl.heap_blks_read AS heap_blks_read,
          dbl.heap_blks_hit AS heap_blks_hit,
          dbl.idx_blks_read AS idx_blks_read,
          dbl.idx_blks_hit AS idx_blks_hit,
          dbl.toast_blks_read AS toast_blks_read,
          dbl.toast_blks_hit AS toast_blks_hit,
          dbl.tidx_blks_read AS tidx_blks_read,
          dbl.tidx_blks_hit AS tidx_blks_hit,
          dbl.relsize AS relsize,
          dbl.relsize_diff AS relsize_diff,
          CASE WHEN dbl.tablespaceid=0 THEN qres.tablespaceid ELSE dbl.tablespaceid END AS tablespaceid,
          NULLIF(dbl.reltoastrelid, 0),
          dbl.relkind,
          false,
          dbl.relpages_bytes,
          dbl.relpages_bytes_diff,
          dbl.last_seq_scan,
          dbl.last_idx_scan,
          dbl.n_tup_newpage_upd,
          dbl.reloptions
        FROM dblink('server_db_connection', t_query)
        AS dbl (
            relid                 oid,
            schemaname            name,
            relname               name,
            seq_scan              bigint,
            seq_tup_read          bigint,
            idx_scan              bigint,
            idx_tup_fetch         bigint,
            n_tup_ins             bigint,
            n_tup_upd             bigint,
            n_tup_del             bigint,
            n_tup_hot_upd         bigint,
            n_live_tup            bigint,
            n_dead_tup            bigint,
            n_mod_since_analyze   bigint,
            n_ins_since_vacuum    bigint,
            last_vacuum           timestamp with time zone,
            last_autovacuum       timestamp with time zone,
            last_analyze          timestamp with time zone,
            last_autoanalyze      timestamp with time zone,
            vacuum_count          bigint,
            autovacuum_count      bigint,
            analyze_count         bigint,
            autoanalyze_count     bigint,
            total_vacuum_time       double precision,
            total_autovacuum_time   double precision,
            total_analyze_time      double precision,
            total_autoanalyze_time  double precision,
            heap_blks_read        bigint,
            heap_blks_hit         bigint,
            idx_blks_read         bigint,
            idx_blks_hit          bigint,
            toast_blks_read       bigint,
            toast_blks_hit        bigint,
            tidx_blks_read        bigint,
            tidx_blks_hit         bigint,
            relsize               bigint,
            relsize_diff          bigint,
            tablespaceid          oid,
            reltoastrelid         oid,
            relkind               char,
            relpages_bytes        bigint,
            relpages_bytes_diff   bigint,
            last_seq_scan         timestamp with time zone,
            last_idx_scan         timestamp with time zone,
            n_tup_newpage_upd     bigint,
            reloptions            jsonb
        );

        IF NOT analyze_list @> ARRAY[format('last_stat_tables_srv%1$s', sserver_id)] THEN
          analyze_list := analyze_list ||
            format('last_stat_tables_srv%1$s', sserver_id);
        END IF;
      END IF; -- relation collection condition

      result := log_sample_timings(result, format('db:%s collect tables stats',qres.datname), 'end');
      result := log_sample_timings(result, format('db:%s collect indexes stats',qres.datname), 'start');

      -- Generate index stats query
      CASE
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 160000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.indexrelid,'
            'st.schemaname,'
            'st.relname,'
            'st.indexrelname,'
            'st.idx_scan,'
            'NULL AS last_idx_scan,'
            'st.idx_tup_read,'
            'st.idx_tup_fetch,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            '{relation_size} relsize,'
            '0,'
            'pg_class.reltablespace as tablespaceid,'
            '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique,'
            'pg_class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'to_jsonb(pg_class.reloptions) '
          'FROM pg_catalog.pg_stat_all_indexes st '
            'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
            'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
            'JOIN pg_catalog.pg_class ON (pg_class.oid = st.indexrelid) '
            'LEFT OUTER JOIN pg_catalog.pg_constraint con ON '
              '(con.conrelid, con.conindid) = (ix.indrelid, ix.indexrelid) AND con.contype in (''p'',''u'') '
            '{lock_join}'
            ;
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer >= 160000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.indexrelid,'
            'st.schemaname,'
            'st.relname,'
            'st.indexrelname,'
            'st.idx_scan,'
            'st.last_idx_scan,'
            'st.idx_tup_read,'
            'st.idx_tup_fetch,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            '{relation_size} relsize,'
            '0,'
            'pg_class.reltablespace as tablespaceid,'
            '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique,'
            'pg_class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'to_jsonb(pg_class.reloptions) '
          'FROM pg_catalog.pg_stat_all_indexes st '
            'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
            'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
            'JOIN pg_catalog.pg_class ON (pg_class.oid = st.indexrelid) '
            'LEFT OUTER JOIN pg_catalog.pg_constraint con ON '
              '(con.conrelid, con.conindid) = (ix.indrelid, ix.indexrelid) AND con.contype in (''p'',''u'') '
            '{lock_join}'
            ;
        ELSE
          RAISE 'Unsupported server version.';
      END CASE;

      IF skip_sizes THEN
        t_query := replace(t_query,'{relation_size}','NULL');
        t_query := replace(t_query,'{lock_join}','');
      ELSE
        t_query := replace(t_query,'{relation_size}',
          'CASE l.relation WHEN st.indexrelid THEN NULL ELSE pg_relation_size(st.indexrelid) END');
        t_query := replace(t_query,'{lock_join}',
          'LEFT OUTER JOIN LATERAL ('
            'SELECT relation '
            'FROM pg_catalog.pg_locks '
            'WHERE '
            '(relation = st.indexrelid AND granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'')'
          ') l ON (l.relation = st.indexrelid)');
      END IF;

      IF COALESCE((properties #> '{collect,relations}')::boolean, true) THEN
        INSERT INTO last_stat_indexes(
          server_id,
          sample_id,
          datid,
          relid,
          indexrelid,
          schemaname,
          relname,
          indexrelname,
          idx_scan,
          last_idx_scan,
          idx_tup_read,
          idx_tup_fetch,
          idx_blks_read,
          idx_blks_hit,
          relsize,
          relsize_diff,
          tablespaceid,
          indisunique,
          in_sample,
          relpages_bytes,
          relpages_bytes_diff,
          reloptions
        )
        SELECT
          sserver_id,
          s_id,
          qres.datid,
          relid,
          indexrelid,
          schemaname,
          relname,
          indexrelname,
          dbl.idx_scan AS idx_scan,
          dbl.last_idx_scan AS last_idx_scan,
          dbl.idx_tup_read AS idx_tup_read,
          dbl.idx_tup_fetch AS idx_tup_fetch,
          dbl.idx_blks_read AS idx_blks_read,
          dbl.idx_blks_hit AS idx_blks_hit,
          dbl.relsize AS relsize,
          dbl.relsize_diff AS relsize_diff,
          CASE WHEN tablespaceid=0 THEN qres.tablespaceid ELSE tablespaceid END tablespaceid,
          indisunique,
          false,
          dbl.relpages_bytes,
          dbl.relpages_bytes_diff,
          dbl.reloptions
        FROM dblink('server_db_connection', t_query)
        AS dbl (
           relid          oid,
           indexrelid     oid,
           schemaname     name,
           relname        name,
           indexrelname   name,
           idx_scan       bigint,
           last_idx_scan  timestamp with time zone,
           idx_tup_read   bigint,
           idx_tup_fetch  bigint,
           idx_blks_read  bigint,
           idx_blks_hit   bigint,
           relsize        bigint,
           relsize_diff   bigint,
           tablespaceid   oid,
           indisunique    bool,
           relpages_bytes bigint,
           relpages_bytes_diff  bigint,
           reloptions jsonb
        );

        IF NOT analyze_list @> ARRAY[format('last_stat_indexes_srv%1$s', sserver_id)] THEN
          analyze_list := analyze_list ||
            format('last_stat_indexes_srv%1$s', sserver_id);
        END IF;
      END IF; -- relation collection condition

      result := log_sample_timings(result, format('db:%s collect indexes stats',qres.datname), 'end');
      result := log_sample_timings(result, format('db:%s collect functions stats',qres.datname), 'start');

      -- Generate Function stats query
      t_query := 'SELECT f.funcid,'
        'f.schemaname,'
        'f.funcname,'
        'pg_get_function_arguments(f.funcid) AS funcargs,'
        'f.calls,'
        'f.total_time,'
        'f.self_time,'
        'p.prorettype::regtype::text =''trigger'' AS trg_fn '
      'FROM pg_catalog.pg_stat_user_functions f '
        'JOIN pg_catalog.pg_proc p ON (f.funcid = p.oid) '
      'WHERE pg_get_function_arguments(f.funcid) IS NOT NULL';

      IF COALESCE((properties #> '{collect,functions}')::boolean, true) THEN
        INSERT INTO last_stat_user_functions(
          server_id,
          sample_id,
          datid,
          funcid,
          schemaname,
          funcname,
          funcargs,
          calls,
          total_time,
          self_time,
          trg_fn
        )
        SELECT
          sserver_id,
          s_id,
          qres.datid,
          funcid,
          schemaname,
          funcname,
          funcargs,
          dbl.calls AS calls,
          dbl.total_time AS total_time,
          dbl.self_time AS self_time,
          dbl.trg_fn
        FROM dblink('server_db_connection', t_query)
        AS dbl (
           funcid       oid,
           schemaname   name,
           funcname     name,
           funcargs     text,
           calls        bigint,
           total_time   double precision,
           self_time    double precision,
           trg_fn       boolean
        );

        IF NOT analyze_list @> ARRAY[format('last_stat_user_functions_srv%1$s', sserver_id)] THEN
          analyze_list := analyze_list ||
            format('last_stat_user_functions_srv%1$s', sserver_id);
        END IF;
      END IF; -- functions collection condition

      PERFORM dblink('server_db_connection', 'COMMIT');
      PERFORM dblink_disconnect('server_db_connection');
      result := log_sample_timings(result, format('db:%s collect functions stats',qres.datname), 'end');
    END LOOP; -- over databases

    -- Now we should preform ANALYZE on collected data
    result := log_sample_timings(result, 'analyzing collected data', 'start');

    FOREACH analyze_obj IN ARRAY analyze_list
    LOOP
      EXECUTE format('ANALYZE %1$I', analyze_obj);
    END LOOP;

    result := log_sample_timings(result, 'analyzing collected data', 'end');

   RETURN result;
END;
$$ LANGUAGE plpgsql;
