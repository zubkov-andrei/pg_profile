/* ===== import queries update ===== */
INSERT INTO import_queries_version_order VALUES
  ('pg_profile','0.3.6','pg_profile','0.3.5');

DROP FUNCTION collect_obj_stats(jsonb,integer,integer,text,boolean);
CREATE FUNCTION collect_obj_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN connstr text,
  IN skip_sizes boolean
) RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    --Cursor for db stats
    c_dblist CURSOR FOR
    SELECT datid,datname,tablespaceid FROM dblink('server_connection',
    'select dbs.oid,dbs.datname,dbs.dattablespace from pg_catalog.pg_database dbs '
    'where not dbs.datistemplate and dbs.datallowconn') AS dbl (
        datid oid,
        datname name,
        tablespaceid oid
    ) JOIN servers n ON (n.server_id = sserver_id AND array_position(n.db_exclude,dbl.datname) IS NULL);

    qres        record;
    db_connstr  text;
    t_query     text;
    result      jsonb := collect_obj_stats.properties;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Disconnecting existing connection
    IF dblink_get_connections() @> ARRAY['server_db_connection'] THEN
        PERFORM dblink_disconnect('server_db_connection');
    END IF;

    -- Load new data from statistic views of all cluster databases
    FOR qres IN c_dblist LOOP
      db_connstr := concat_ws(' ',connstr,
        format($o$dbname='%s'$o$,replace(qres.datname,$o$'$o$,$o$\'$o$))
      );
      PERFORM dblink_connect('server_db_connection',db_connstr);
      -- Setting application name
      PERFORM dblink('server_connection','SET application_name=''{pg_profile}''');
      -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
      PERFORM dblink('server_db_connection','SET lock_timeout=3000');
      -- Reset search_path for security reasons
      PERFORM dblink('server_connection','SET search_path=''''');

      IF (properties #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect tables stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate Table stats query
      CASE
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 130000
        )
        THEN
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
            '0 AS relpages_bytes_diff '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer >= 130000
        )
        THEN
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
            '0 AS relpages_bytes_diff '
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
        relpages_bytes_diff
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
        dbl.reltoastrelid,
        dbl.relkind,
        false,
        dbl.relpages_bytes,
        dbl.relpages_bytes_diff
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
          relpages_bytes_diff   bigint
      );

      ANALYZE last_stat_tables;

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect tables stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect indexes stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate index stats query
      t_query := 'SELECT st.*,'
        'stio.idx_blks_read,'
        'stio.idx_blks_hit,'
        '{relation_size} relsize,'
        '0,'
        'pg_class.reltablespace as tablespaceid,'
        '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique,'
        'pg_class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
        '0 AS relpages_bytes_diff '
      'FROM pg_catalog.pg_stat_all_indexes st '
        'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
        'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
        'JOIN pg_catalog.pg_class ON (pg_class.oid = st.indexrelid) '
        'LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.conindid = ix.indexrelid AND con.contype in (''p'',''u'')) '
        '{lock_join}'
        ;

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
        relpages_bytes_diff
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
        dbl.relpages_bytes_diff
      FROM dblink('server_db_connection', t_query)
      AS dbl (
         relid          oid,
         indexrelid     oid,
         schemaname     name,
         relname        name,
         indexrelname   name,
         idx_scan       bigint,
         idx_tup_read   bigint,
         idx_tup_fetch  bigint,
         idx_blks_read  bigint,
         idx_blks_hit   bigint,
         relsize        bigint,
         relsize_diff   bigint,
         tablespaceid   oid,
         indisunique    bool,
         relpages_bytes bigint,
         relpages_bytes_diff  bigint
      );

      ANALYZE last_stat_indexes;

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect indexes stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect functions stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

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
        'JOIN pg_catalog.pg_proc p ON (f.funcid = p.oid)';

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

      ANALYZE last_stat_user_functions;

      PERFORM dblink_disconnect('server_db_connection');
      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect functions stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
      END IF;
    END LOOP;
   RETURN result;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION get_report(integer, integer, integer, text, boolean);
CREATE FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text = NULL, IN with_growth boolean = false) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    tmp_text    text;
    tmp_report  text;
    report      text;
    topn        integer;
    qlen_limit  integer;
    stmt_all_cnt    integer;
    -- HTML elements templates
    report_tpl CONSTANT text := '<html><head><style>{css}</style><title>Postgres profile report {samples}</title></head><body><H1>Postgres profile report {samples}</H1>'
    '<p>{pg_profile} version {pgprofile_version}</p>'
    '<p>Server name: <strong>{server_name}</strong></p>'
    '{server_description}'
    '<p>Report interval: <strong>{report_start} - {report_end}</strong></p>'
    '{report_description}{report}</body></html>';
    report_css CONSTANT text := 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 4px;} '
    'table tr td.value, table tr td.mono {font-family: Monospace;} '
    'table tr td.value {text-align: right;} '
    'table p {margin: 0.2em;}'
    'table tr.parent td:not(.hdr) {background-color: #D8E8C2;} '
    'table tr.child td {background-color: #BBDD97; border-top-style: hidden;} '
    'table.stat tr:nth-child(even), table.setlist tr:nth-child(even) {background-color: #eee;} '
    'table.stat tr:nth-child(odd), table.setlist tr:nth-child(odd) {background-color: #fff;} '
    'table tr:hover td:not(.hdr) {background-color:#d9ffcc} '
    'table th {color: black; background-color: #ffcc99;}'
    'table tr:target,td:target {border: solid; border-width: medium; border-color: limegreen;}'
    'table tr:target td:first-of-type, table td:target {font-weight: bold;}';
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
        SELECT left_bound, right_bound INTO STRICT start_id, end_id
        FROM get_sized_bounds(sserver_id, start_id, end_id);
      EXCEPTION
        WHEN OTHERS THEN
          RAISE 'Samples with sizes collected for requested interval (%) not found',
            format('%s - %s',start_id, end_id);
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

    -- Check if all samples of requested interval are available
    IF (
      SELECT count(*) != end_id - start_id + 1 FROM samples
      WHERE server_id = sserver_id AND sample_id BETWEEN start_id AND end_id
    ) THEN
      RAISE 'Not enough samples between %',
        format('%s AND %s', start_id, end_id);
    END IF;
    -- Checking sample existance, header generation
    OPEN c_sample(start_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'Start sample % does not exists', start_id;
        END IF;
        report := replace(report,'{report_start}',sample_rec.sample_time::text);
        tmp_text := '(StartID: ' || sample_rec.sample_id ||', ';
    CLOSE c_sample;

    OPEN c_sample(end_id);
        FETCH c_sample INTO sample_rec;
        IF sample_rec IS NULL THEN
            RAISE 'End sample % does not exists', end_id;
        END IF;
        report := replace(report,'{report_end}',sample_rec.sample_time::text);
        tmp_text := tmp_text || 'EndID: ' || sample_rec.sample_id ||')';
    CLOSE c_sample;
    report := replace(report,'{samples}',tmp_text);
    tmp_text := '';

    -- Populate report settings
    jreportset := jsonb_build_object(
    'htbl',jsonb_build_object(
      'reltr','class="parent"',
      'toasttr','class="child"',
      'reltdhdr','class="hdr"',
      'stattbl','class="stat"',
      'value','class="value"',
      'mono','class="mono"',
      'reltdspanhdr','rowspan="2" class="hdr"'
    ),
    'report_features',jsonb_build_object(
      'statstatements',profile_checkavail_statstatements(sserver_id, start_id, end_id),
      'planning_times',profile_checkavail_planning_times(sserver_id, start_id, end_id),
      'stmt_io_times',profile_checkavail_stmt_io_times(sserver_id, start_id, end_id),
      'statement_wal_bytes',profile_checkavail_stmt_wal_bytes(sserver_id, start_id, end_id),
      'wal_stats',profile_checkavail_walstats(sserver_id, start_id, end_id),
      'sess_stats',profile_checkavail_sessionstats(sserver_id, start_id, end_id),
      'function_stats',profile_checkavail_functions(sserver_id, start_id, end_id),
      'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start_id, end_id),
      'kcachestatements',profile_checkavail_rusage(sserver_id,start_id,end_id),
      'rusage_planstats',profile_checkavail_rusage_planstats(sserver_id,start_id,end_id)
    ),
    'report_properties',jsonb_build_object(
      'interval_duration_sec',
        (SELECT extract(epoch FROM e.sample_time - s.sample_time)
        FROM samples s JOIN samples e USING (server_id)
        WHERE e.sample_id=end_id and s.sample_id=start_id
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
    CREATE TEMPORARY TABLE top_statements AS
    SELECT * FROM top_statements(sserver_id, start_id, end_id);

    /* table size is collected in a sample when relsize field is not null
    In a report we can use relsize-based growth calculated as a sum of
    relsize increments only when sizes was collected
    in the both first and last sample, otherwise we only can use
    pg_class.relpages
    */
    CREATE TEMPORARY TABLE top_tables AS
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
    FROM top_tables(sserver_id, start_id, end_id) tt
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
          AND rel.sample_id BETWEEN start_id AND end_id
      GROUP BY rel.server_id, rel.datid, rel.relid
    ) rs USING (server_id, datid, relid);

    CREATE TEMPORARY TABLE top_indexes AS
    SELECT ti.*,
      rs.relsize_growth_avail AS relsize_growth_avail,
      CASE WHEN rs.relsize_growth_avail THEN
        ti.growth
      ELSE
        ti.relpagegrowth_bytes
      END AS best_growth
    FROM top_indexes(sserver_id, start_id, end_id) ti
    JOIN (
      SELECT server_id, datid, indexrelid,
          COALESCE(
              max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL)
              AND min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL)
          , false) AS relsize_growth_avail
      FROM sample_stat_indexes
      WHERE
          server_id = sserver_id
          AND sample_id BETWEEN start_id AND end_id
      GROUP BY server_id, datid, indexrelid
    ) rs USING (server_id, datid, indexrelid);

    CREATE TEMPORARY TABLE top_io_tables AS
    SELECT * FROM top_io_tables(sserver_id, start_id, end_id);
    CREATE TEMPORARY TABLE top_io_indexes AS
    SELECT * FROM top_io_indexes(sserver_id, start_id, end_id);
    CREATE TEMPORARY TABLE top_functions AS
    SELECT * FROM top_functions(sserver_id, start_id, end_id, false);
    CREATE TEMPORARY TABLE top_kcache_statements AS
    SELECT * FROM top_kcache_statements(sserver_id, start_id, end_id);
    ANALYZE top_statements;
    ANALYZE top_tables;
    ANALYZE top_indexes;
    ANALYZE top_io_tables;
    ANALYZE top_io_indexes;
    ANALYZE top_functions;
    ANALYZE top_kcache_statements;

    -- Reporting possible statements overflow
    tmp_report := check_stmt_cnt(sserver_id, start_id, end_id);
    IF tmp_report != '' THEN
        tmp_text := tmp_text || '<H2>Warning!</H2>';
        tmp_text := tmp_text || '<p>This interval contains sample(s) with captured statements count more than 90% of pg_stat_statements.max setting. Consider increasing this parameter.</p>';
        tmp_text := tmp_text || tmp_report;
    END IF;

    -- pg_stat_statements.track warning
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
      -- SQL texts
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
    tmp_text := tmp_text || '<li><a HREF=#ix_unused>Unused indexes</a></li>';
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

    IF jsonb_extract_path_text(jreportset, 'report_features', 'sess_stats')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=db_stat_sessions>Session statistics by database</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(dbstats_sessions_htbl(jreportset, sserver_id, start_id, end_id, topn));
    END IF;

    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H3><a NAME=st_stat>Statement statistics by database</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(statements_stats_htbl(jreportset, sserver_id, start_id, end_id, topn));
    END IF;

    tmp_text := tmp_text || '<div>';
    tmp_text := tmp_text || '<div style="display:inline-block; margin-right:2em;">'
      '<H3><a NAME=clu_stat>Cluster statistics</a></H3>';
    tmp_report := cluster_stats_reset_htbl(jreportset, sserver_id, start_id, end_id);
    IF tmp_report != '' THEN
      tmp_text := tmp_text || '<p><b>Warning!</b> Cluster statistics reset detected during report period!</p>'||tmp_report||
        '<p>Cluster statistics might be affected</p>';
    END IF;
    tmp_text := tmp_text || nodata_wrapper(cluster_stats_htbl(jreportset, sserver_id, start_id, end_id)) || '</div>';

    IF jsonb_extract_path_text(jreportset, 'report_features', 'wal_stats')::boolean THEN
      tmp_text := tmp_text || '<div style="display:inline-block"><H3><a NAME=wal_stat>WAL statistics</a></H3>';
      tmp_report := wal_stats_reset_htbl(jreportset, sserver_id, start_id, end_id);
      IF tmp_report != '' THEN
        tmp_text := tmp_text || '<p><b>Warning!</b> WAL statistics reset detected during report period!</p>'||tmp_report||
          '<p>WAL statistics might be affected</p>';
      END IF;
      tmp_text := tmp_text || nodata_wrapper(wal_stats_htbl(jreportset, sserver_id, start_id, end_id)) ||
        '</div>';
    END IF;
    tmp_text := tmp_text || '</div>';

    tmp_text := tmp_text || '<H3><a NAME=tablespace_stat>Tablespace statistics</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tablespaces_stats_htbl(jreportset, sserver_id, start_id, end_id));

    --Reporting on top queries by elapsed time
    IF jsonb_extract_path_text(jreportset, 'report_features', 'statstatements')::boolean THEN
      tmp_text := tmp_text || '<H2><a NAME=sql_stat>SQL query statistics</a></H2>';
      IF jsonb_extract_path_text(jreportset, 'report_features', 'planning_times')::boolean THEN
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
      IF jsonb_extract_path_text(jreportset, 'report_features', 'stmt_io_times')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=top_iowait>Top SQL by I/O wait time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(top_iowait_htbl(jreportset, sserver_id, start_id, end_id, topn));
      END IF;

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
      IF jsonb_extract_path_text(jreportset, 'report_features', 'statement_wal_bytes')::boolean THEN
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
      tmp_text := tmp_text || nodata_wrapper(report_queries(jreportset, sserver_id, start_id, end_id));
    END IF;

    -- Reporting Object stats
    -- Reporting scanned table
    tmp_text := tmp_text || '<H2><a NAME=schema_stat>Schema object statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=scanned_tbl>Top tables by estimated sequentially scanned volume</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_scan_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=fetch_tbl>Top tables by blocks fetched</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_fetch_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=read_tbl>Top tables by blocks read</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(tbl_top_io_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=dml_tbl>Top DML tables</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_dml_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));

    tmp_text := tmp_text || '<H3><a NAME=vac_tbl>Top tables by updated/deleted tuples</a></H3>';
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

    IF jsonb_extract_path_text(jreportset, 'report_features', 'function_stats')::boolean THEN
      tmp_text := tmp_text || '<H2><a NAME=func_stat>User function statistics</a></H2>';
      tmp_text := tmp_text || '<H3><a NAME=funcs_time_stat>Top functions by total time</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(func_top_time_htbl(jreportset, sserver_id, start_id, end_id, topn));

      tmp_text := tmp_text || '<H3><a NAME=funcs_calls_stat>Top functions by executions</a></H3>';
      tmp_text := tmp_text || nodata_wrapper(func_top_calls_htbl(jreportset, sserver_id, start_id, end_id, topn));

      IF jsonb_extract_path_text(jreportset, 'report_features', 'trigger_function_stats')::boolean THEN
        tmp_text := tmp_text || '<H3><a NAME=trg_funcs_time_stat>Top trigger functions by total time</a></H3>';
        tmp_text := tmp_text || nodata_wrapper(func_top_trg_htbl(jreportset, sserver_id, start_id, end_id, topn));
      END IF;
    END IF;

    -- Reporting vacuum related stats
    tmp_text := tmp_text || '<H2><a NAME=vacuum_stats>Vacuum-related statistics</a></H2>';
    tmp_text := tmp_text || '<H3><a NAME=top_vacuum_cnt_tbl>Top tables by vacuum operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_vacuumed_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_analyze_cnt_tbl>Top tables by analyze operations</a></H3>';
    tmp_text := tmp_text || nodata_wrapper(top_analyzed_tables_htbl(jreportset, sserver_id, start_id, end_id, topn));
    tmp_text := tmp_text || '<H3><a NAME=top_ix_vacuum_bytes_cnt_tbl>Top indexes by estimated vacuum load</a></H3>';
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

    /*
    * Dropping cache temporary tables
    * This is needed to avoid conflict with existing table if several
    * reports are collected in one session
    */
    DROP TABLE top_statements;
    DROP TABLE top_tables;
    DROP TABLE top_indexes;
    DROP TABLE top_io_tables;
    DROP TABLE top_io_indexes;
    DROP TABLE top_functions;
    DROP TABLE top_kcache_statements;

    RETURN replace(report,'{report}',tmp_text);
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_report(IN sserver_id integer, IN start_id integer, IN end_id integer,
  IN description text, IN with_growth boolean)
IS 'Statistics report generation function. Takes server_id and IDs of start and end sample (inclusive).';

DROP FUNCTION get_diffreport(integer, integer, integer, integer, integer, text, boolean);
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

DROP FUNCTION export_data(name, integer, integer, boolean);
CREATE FUNCTION export_data(IN server_name name = NULL, IN min_sample_id integer = NULL,
  IN max_sample_id integer = NULL, IN obfuscate_queries boolean = FALSE)
RETURNS TABLE(
    section_id  bigint,
    row_data    json
) SET search_path=@extschema@ AS $$
DECLARE
  section_counter   bigint = 0;
  ext_version       text = NULL;
  tables_list       json = NULL;
  sserver_id        integer = NULL;
  r_result          RECORD;
BEGIN
  /*
    Exported table will contain rows of extension tables, packed in JSON
    Each row will have a section ID, defining a table in most cases
    First sections contains metadata - extension name and version, tables list
  */
  -- Extension info
  IF (SELECT count(*) = 1 FROM pg_catalog.pg_extension WHERE extname = '{pg_profile}') THEN
    SELECT extversion INTO STRICT r_result FROM pg_catalog.pg_extension WHERE extname = '{pg_profile}';
    ext_version := r_result.extversion;
  ELSE
    ext_version := '{extension_version}';
  END IF;
  RETURN QUERY EXECUTE $q$SELECT $3, row_to_json(s)
    FROM (SELECT $1 AS extension,
              $2 AS version,
              $3 + 1 AS tab_list_section
    ) s$q$
    USING '{pg_profile}', ext_version, section_counter;
  section_counter := section_counter + 1;
  -- tables list
  EXECUTE $q$
    WITH RECURSIVE exp_tables (reloid, relname, inc_rels) AS (
      -- start with all independent tables
        SELECT rel.oid, rel.relname, array_agg(rel.oid) OVER()
          FROM pg_depend dep
            JOIN pg_extension ext ON (dep.refobjid = ext.oid)
            JOIN pg_class rel ON (rel.oid = dep.objid AND rel.relkind= 'r')
            LEFT OUTER JOIN fkdeps con ON (con.reloid = dep.objid)
          WHERE ext.extname = $1 AND rel.relname NOT LIKE ('import%') AND con.reloid IS NULL
      UNION
      -- and add all tables that have resolved dependencies by previously added tables
          SELECT con.reloid as reloid, con.relname, recurse.inc_rels||array_agg(con.reloid) OVER()
          FROM
            fkdeps con JOIN
            exp_tables recurse ON
              (array_append(recurse.inc_rels,con.reloid) @> con.reldeps AND
              NOT ARRAY[con.reloid] <@ recurse.inc_rels)
    ),
    fkdeps (reloid, relname, reldeps) AS (
      -- tables with their foreign key dependencies
      SELECT rel.oid as reloid, rel.relname, array_agg(con.confrelid), array_agg(rel.oid) OVER()
      FROM pg_depend dep
        JOIN pg_extension ext ON (dep.refobjid = ext.oid)
        JOIN pg_class rel ON (rel.oid = dep.objid AND rel.relkind= 'r')
        JOIN pg_constraint con ON (con.conrelid = dep.objid AND con.contype = 'f')
      WHERE ext.extname = $1 AND rel.relname NOT LIKE ('import%')
      GROUP BY rel.oid, rel.relname
    )
    SELECT json_agg(row_to_json(tl)) FROM
    (SELECT row_number() OVER() + $2 AS section_id, relname FROM exp_tables) tl ;
  $q$ INTO tables_list
  USING '{pg_profile}', section_counter;
  section_id := section_counter;
  row_data := tables_list;
  RETURN NEXT;
  section_counter := section_counter + 1;
  -- Server selection
  IF export_data.server_name IS NOT NULL THEN
    sserver_id := get_server_by_name(export_data.server_name);
  END IF;
  -- Tables data
  FOR r_result IN
    SELECT json_array_elements(tables_list)->>'relname' as relname
  LOOP
    -- Tables select conditions
    CASE
      WHEN r_result.relname != 'sample_settings'
        AND (r_result.relname LIKE 'sample%' OR r_result.relname LIKE 'last%') THEN
        RETURN QUERY EXECUTE format(
            $q$SELECT $1,row_to_json(dt) FROM
              (SELECT * FROM %I WHERE ($2 IS NULL OR $2 = server_id) AND
                ($3 IS NULL OR sample_id >= $3) AND
                ($4 IS NULL OR sample_id <= $4)) dt$q$,
            r_result.relname
          )
        USING
          section_counter,
          sserver_id,
          min_sample_id,
          max_sample_id;
      WHEN r_result.relname = 'bl_samples' THEN
        RETURN QUERY EXECUTE format(
            $q$
            SELECT $1,row_to_json(dt) FROM (
              SELECT *
              FROM %I b
                JOIN (
                  SELECT bl_id
                  FROM bl_samples
                    WHERE ($2 IS NULL OR $2 = server_id)
                  GROUP BY bl_id
                  HAVING
                    ($3 IS NULL OR min(sample_id) >= $3) AND
                    ($4 IS NULL OR max(sample_id) <= $4)
                ) bl_smp USING (bl_id)
              WHERE ($2 IS NULL OR $2 = server_id)
              ) dt$q$,
            r_result.relname
          )
        USING
          section_counter,
          sserver_id,
          min_sample_id,
          max_sample_id;
      WHEN r_result.relname = 'baselines' THEN
        RETURN QUERY EXECUTE format(
            $q$
            SELECT $1,row_to_json(dt) FROM (
              SELECT b.*
              FROM %I b
              JOIN bl_samples bs USING(server_id, bl_id)
                WHERE ($2 IS NULL OR $2 = server_id)
              GROUP BY b.server_id, b.bl_id, b.bl_name, b.keep_until
              HAVING
                ($3 IS NULL OR min(sample_id) >= $3) AND
                ($4 IS NULL OR max(sample_id) <= $4)
              ) dt$q$,
            r_result.relname
          )
        USING
          section_counter,
          sserver_id,
          min_sample_id,
          max_sample_id;
      WHEN r_result.relname = 'stmt_list' THEN
        RETURN QUERY EXECUTE format(
            $sql$SELECT $1,row_to_json(dt) FROM
              (SELECT rows.server_id, rows.queryid_md5,
                CASE $5
                  WHEN TRUE THEN pg_catalog.md5(rows.query)
                  ELSE rows.query
                END AS query
               FROM %I AS rows WHERE (server_id,queryid_md5) IN
                (SELECT server_id, queryid_md5 FROM sample_statements WHERE
                  ($2 IS NULL OR $2 = server_id) AND
                ($3 IS NULL OR sample_id >= $3) AND
                ($4 IS NULL OR sample_id <= $4))) dt$sql$,
            r_result.relname
          )
        USING
          section_counter,
          sserver_id,
          min_sample_id,
          max_sample_id,
          obfuscate_queries;
      ELSE
        RETURN QUERY EXECUTE format(
            $q$SELECT $1,row_to_json(dt) FROM (SELECT * FROM %I WHERE $2 IS NULL OR $2 = server_id) dt$q$,
            r_result.relname
          )
        USING section_counter, sserver_id;
    END CASE;
    section_counter := section_counter + 1;
  END LOOP;
  RETURN;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION export_data(IN server_name name, IN min_sample_id integer,
  IN max_sample_id integer, IN obfuscate_queries boolean) IS 'Export collected data as a table';
