CREATE FUNCTION collect_pg_stat_statements_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer) RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  qres              record;
  st_query          text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),',') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Check if mandatory extensions exists
    IF NOT
      (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_statements'
      )
    THEN
      RETURN;
    END IF;

    -- Dynamic statements query
    st_query := format(
      'SELECT '
        'st.userid,'
        'st.userid::regrole AS username,'
        'st.dbid,'
        'st.queryid,'
        'md5(st.query) AS queryid_md5,'
        '{statements_fields}'
        '{kcache_fields}'
      ' FROM '
      '{statements_view} st '
      '{kcache_stats} '
      'JOIN pg_catalog.pg_database db ON (db.oid=st.dbid) '
      'JOIN pg_catalog.pg_roles r ON (r.oid=st.userid) '
      'JOIN '
      '(SELECT '
        'userid,'
        'dbid,'
        'queryid,'
        '{statements_rank_calc}'
        '{kcache_rank_calc}'
      'FROM {statements_view} s '
        '{kcache_rank_join} '
      ') rank_st '
      'USING (userid, dbid, queryid)'
      ' WHERE '
        'st.queryid IS NOT NULL '
        'AND '
        'least('
          '{statements_rank_fields}'
          '{kcache_rank_fields}'
          ') <= %1$s',
      topn);

    -- pg_stat_statements versions
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      WHEN '1.3','1.4','1.5','1.6','1.7' THEN
        st_query := replace(st_query, '{statements_fields}',
          'NULL as toplevel,'
          'NULL as plans,'
          'NULL as total_plan_time,'
          'NULL as min_plan_time,'
          'NULL as max_plan_time,'
          'NULL as mean_plan_time,'
          'NULL as stddev_plan_time,'
          'st.calls,'
          'st.total_time as total_exec_time,'
          'st.min_time as min_exec_time,'
          'st.max_time as max_exec_time,'
          'st.mean_time as mean_exec_time,'
          'st.stddev_time as stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'NULL as wal_records,'
          'NULL as wal_fpi,'
          'NULL as wal_bytes,'||
          $o$regexp_replace(st.query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query$o$
        );
        st_query := replace(st_query, '{statements_rank_calc}',
            'row_number() over (ORDER BY total_time DESC) AS exec_time_rank,'
            'row_number() over (ORDER BY calls DESC) AS calls_rank,'
            'row_number() over (ORDER BY blk_read_time + blk_write_time DESC) AS io_time_rank,'
            'row_number() over (ORDER BY shared_blks_hit + shared_blks_read DESC) AS gets_rank,'
            'row_number() over (ORDER BY shared_blks_read DESC) AS read_rank,'
            'row_number() over (ORDER BY shared_blks_dirtied DESC) AS dirtied_rank,'
            'row_number() over (ORDER BY shared_blks_written DESC) AS written_rank,'
            'row_number() over (ORDER BY temp_blks_written + local_blks_written DESC) AS tempw_rank,'
            'row_number() over (ORDER BY temp_blks_read + local_blks_read DESC) AS tempr_rank '
        );
        st_query := replace(st_query, '{statements_rank_fields}',
          'exec_time_rank,'
          'calls_rank,'
          'io_time_rank,'
          'gets_rank,'
          'read_rank,'
          'tempw_rank,'
          'tempr_rank,'
          'dirtied_rank,'
          'written_rank'
        );
        st_query := replace(st_query, '{statements_view}',
          format('%1$I.pg_stat_statements',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        );
      WHEN '1.8' THEN
        st_query := replace(st_query, '{statements_fields}',
          'NULL as toplevel,'
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes,'||
          $o$regexp_replace(st.query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query$o$
        );
        st_query := replace(st_query, '{statements_rank_calc}',
            'row_number() over (ORDER BY total_plan_time + total_exec_time DESC) AS time_rank,'
            'row_number() over (ORDER BY total_plan_time DESC) AS plan_time_rank,'
            'row_number() over (ORDER BY total_exec_time DESC) AS exec_time_rank,'
            'row_number() over (ORDER BY calls DESC) AS calls_rank,'
            'row_number() over (ORDER BY blk_read_time + blk_write_time DESC) AS io_time_rank,'
            'row_number() over (ORDER BY shared_blks_hit + shared_blks_read DESC) AS gets_rank,'
            'row_number() over (ORDER BY shared_blks_read DESC) AS read_rank,'
            'row_number() over (ORDER BY shared_blks_dirtied DESC) AS dirtied_rank,'
            'row_number() over (ORDER BY shared_blks_written DESC) AS written_rank,'
            'row_number() over (ORDER BY temp_blks_written + local_blks_written DESC) AS tempw_rank,'
            'row_number() over (ORDER BY temp_blks_read + local_blks_read DESC) AS tempr_rank,'
            'row_number() over (ORDER BY wal_bytes DESC) AS wal_rank '
        );
        st_query := replace(st_query, '{statements_rank_fields}',
          'time_rank,'
          'plan_time_rank,'
          'exec_time_rank,'
          'calls_rank,'
          'io_time_rank,'
          'gets_rank,'
          'read_rank,'
          'dirtied_rank,'
          'written_rank,'
          'tempw_rank,'
          'tempr_rank,'
          'wal_rank'
        );
        st_query := replace(st_query, '{statements_view}',
          format('%1$I.pg_stat_statements',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        );
      WHEN '1.9' THEN
        st_query := replace(st_query, '{statements_fields}',
          'st.toplevel,'
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes,'||
          $o$regexp_replace(st.query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query$o$
        );
        st_query := replace(st_query, '{statements_rank_calc}',
            'row_number() over (ORDER BY total_plan_time + total_exec_time DESC) AS time_rank,'
            'row_number() over (ORDER BY total_plan_time DESC) AS plan_time_rank,'
            'row_number() over (ORDER BY total_exec_time DESC) AS exec_time_rank,'
            'row_number() over (ORDER BY calls DESC) AS calls_rank,'
            'row_number() over (ORDER BY blk_read_time + blk_write_time DESC) AS io_time_rank,'
            'row_number() over (ORDER BY shared_blks_hit + shared_blks_read DESC) AS gets_rank,'
            'row_number() over (ORDER BY shared_blks_read DESC) AS read_rank,'
            'row_number() over (ORDER BY shared_blks_dirtied DESC) AS dirtied_rank,'
            'row_number() over (ORDER BY shared_blks_written DESC) AS written_rank,'
            'row_number() over (ORDER BY temp_blks_written + local_blks_written DESC) AS tempw_rank,'
            'row_number() over (ORDER BY temp_blks_read + local_blks_read DESC) AS tempr_rank,'
            'row_number() over (ORDER BY wal_bytes DESC) AS wal_rank '
        );
        st_query := replace(st_query, '{statements_rank_fields}',
          'time_rank,'
          'plan_time_rank,'
          'exec_time_rank,'
          'calls_rank,'
          'io_time_rank,'
          'gets_rank,'
          'read_rank,'
          'dirtied_rank,'
          'written_rank,'
          'tempw_rank,'
          'tempr_rank,'
          'wal_rank'
        );
        st_query := replace(st_query, '{statements_view}',
          format('%1$I.pg_stat_statements',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        );
      ELSE
        RAISE 'Unsupported pg_stat_statements extension version.';
    END CASE; -- pg_stat_statememts versions

    CASE -- pg_stat_kcache versions
      (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extversion text)
        WHERE extname = 'pg_stat_kcache'
      )
      -- pg_stat_kcache v.2.1.0 - 2.1.3
      WHEN '2.1.0','2.1.1','2.1.2','2.1.3' THEN
        st_query := replace(st_query, '{kcache_fields}',
          ',true as kcache_avail,'
          'NULL as plan_user_time,'
          'NULL as plan_system_time,'
          'NULL as plan_minflts,'
          'NULL as plan_majflts,'
          'NULL as plan_nswaps,'
          'NULL as plan_reads,'
          'NULL as plan_writes,'
          'NULL as plan_msgsnds,'
          'NULL as plan_msgrcvs,'
          'NULL as plan_nsignals,'
          'NULL as plan_nvcsws,'
          'NULL as plan_nivcsws,'
          'kc.user_time as exec_user_time,'
          'kc.system_time as exec_system_time,'
          'kc.minflts as exec_minflts,'
          'kc.majflts as exec_majflts,'
          'kc.nswaps as exec_nswaps,'
          'kc.reads as exec_reads,'
          'kc.writes  as exec_writes,'
          'kc.msgsnds as exec_msgsnds,'
          'kc.msgrcvs as exec_msgrcvs,'
          'kc.nsignals as exec_nsignals,'
          'kc.nvcsws as exec_nvcsws,'
          'kc.nivcsws as exec_nivcsws'
        );
        st_query := replace(st_query, '{kcache_stats}',format(
          'LEFT OUTER JOIN %1$I.pg_stat_kcache() kc USING (queryid, userid, dbid) ',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          ));
        st_query := replace(st_query, '{kcache_rank_calc}',
          ', row_number() over (ORDER BY coalesce(user_time, 0.0)+coalesce(system_time, 0.0) DESC) AS cpu_time_rank,'
          'row_number() over (ORDER BY coalesce(reads, 0)+coalesce(writes, 0) DESC) AS io_rank '
        );
        st_query := replace(st_query, '{kcache_rank_join}',format(
          'LEFT OUTER JOIN %1$I.pg_stat_kcache() k USING (queryid, userid, dbid) ',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          ));
        st_query := replace(st_query, '{kcache_rank_fields}',
          ',cpu_time_rank,'
          'io_rank'
        );
      -- pg_stat_kcache v.2.2.0
      WHEN '2.2.0' THEN
        st_query := replace(st_query, '{kcache_fields}',
          ',true as kcache_avail,'
          'kc.plan_user_time as plan_user_time,'
          'kc.plan_system_time as plan_system_time,'
          'kc.plan_minflts as plan_minflts,'
          'kc.plan_majflts as plan_majflts,'
          'kc.plan_nswaps as plan_nswaps,'
          'kc.plan_reads as plan_reads,'
          'kc.plan_writes as plan_writes,'
          'kc.plan_msgsnds as plan_msgsnds,'
          'kc.plan_msgrcvs as plan_msgrcvs,'
          'kc.plan_nsignals as plan_nsignals,'
          'kc.plan_nvcsws as plan_nvcsws,'
          'kc.plan_nivcsws as plan_nivcsws,'
          'kc.exec_user_time as exec_user_time,'
          'kc.exec_system_time as exec_system_time,'
          'kc.exec_minflts as exec_minflts,'
          'kc.exec_majflts as exec_majflts,'
          'kc.exec_nswaps as exec_nswaps,'
          'kc.exec_reads as exec_reads,'
          'kc.exec_writes as exec_writes,'
          'kc.exec_msgsnds as exec_msgsnds,'
          'kc.exec_msgrcvs as exec_msgrcvs,'
          'kc.exec_nsignals as exec_nsignals,'
          'kc.exec_nvcsws as exec_nvcsws,'
          'kc.exec_nivcsws as exec_nivcsws'
        );
        st_query := replace(st_query, '{kcache_stats}',format(
          'LEFT OUTER JOIN %1$I.pg_stat_kcache() kc USING (queryid, userid, dbid) ',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          ));
        st_query := replace(st_query, '{kcache_rank_calc}',
          ', row_number() OVER (ORDER BY '
            'COALESCE(plan_user_time, 0.0) +'
            'COALESCE(plan_system_time, 0.0) '
            'DESC) AS plan_cpu_time_rank '
          ', row_number() OVER (ORDER BY '
            'COALESCE(exec_user_time, 0.0) +'
            'COALESCE(exec_system_time, 0.0) '
            'DESC) AS exec_cpu_time_rank '
          ', row_number() OVER (ORDER BY '
            'COALESCE(plan_reads, 0) + '
            'COALESCE(plan_writes, 0) '
            'DESC) AS plan_io_rank '
          ', row_number() OVER (ORDER BY '
            'COALESCE(exec_reads, 0) + '
            'COALESCE(exec_writes, 0) '
            'DESC) AS exec_io_rank '
        );
        st_query := replace(st_query, '{kcache_rank_join}',format(
          'LEFT OUTER JOIN %1$I.pg_stat_kcache() k USING (queryid, userid, dbid) ',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          ));
        st_query := replace(st_query, '{kcache_rank_fields}',
          ',plan_cpu_time_rank,'
          'exec_cpu_time_rank,'
          'plan_io_rank,'
          'exec_io_rank'
        );
      ELSE -- suitable pg_stat_kcache version not found
        st_query := replace(st_query, '{kcache_stats}','');
        st_query := replace(st_query, '{kcache_rank_calc}','');
        st_query := replace(st_query, '{kcache_rank_join}','');
        st_query := replace(st_query, '{kcache_rank_fields}','');
        st_query := replace(st_query, '{kcache_fields}',
          ',false as kcache_avail,'
          'NULL as plan_user_time,'
          'NULL as plan_system_time,'
          'NULL as plan_minflts,'
          'NULL as plan_majflts,'
          'NULL as plan_nswaps,'
          'NULL as plan_reads,'
          'NULL as plan_writes,'
          'NULL as plan_msgsnds,'
          'NULL as plan_msgrcvs,'
          'NULL as plan_nsignals,'
          'NULL as plan_nvcsws,'
          'NULL as plan_nivcsws,'
          'NULL as exec_user_time,'
          'NULL as exec_system_time,'
          'NULL as exec_minflts,'
          'NULL as exec_majflts,'
          'NULL as exec_nswaps,'
          'NULL as exec_reads,'
          'NULL as exec_writes,'
          'NULL as exec_msgsnds,'
          'NULL as exec_msgrcvs,'
          'NULL as exec_nsignals,'
          'NULL as exec_nvcsws,'
          'NULL as exec_nivcsws');
    END CASE; --pg_stat_kcache versions

    -- RAISE LOG 'stmts query: %',st_query; -- statements query debug
    -- Sample data from pg_stat_statements and pg_stat_kcache top whole cluster statements
    FOR qres IN
        SELECT
          -- pg_stat_statements fields
          sserver_id,
          s_id AS sample_id,
          dbl.userid AS userid,
          dbl.username AS username,
          dbl.datid AS datid,
          dbl.queryid AS queryid,
          dbl.queryid_md5 AS queryid_md5,
          dbl.toplevel AS toplevel,
          dbl.plans AS plans,
          dbl.total_plan_time AS total_plan_time,
          dbl.min_plan_time AS min_plan_time,
          dbl.max_plan_time AS max_plan_time,
          dbl.mean_plan_time AS mean_plan_time,
          dbl.stddev_plan_time AS stddev_plan_time,
          dbl.calls  AS calls,
          dbl.total_exec_time  AS total_exec_time,
          dbl.min_exec_time  AS min_exec_time,
          dbl.max_exec_time  AS max_exec_time,
          dbl.mean_exec_time  AS mean_exec_time,
          dbl.stddev_exec_time  AS stddev_exec_time,
          dbl.rows  AS rows,
          dbl.shared_blks_hit  AS shared_blks_hit,
          dbl.shared_blks_read  AS shared_blks_read,
          dbl.shared_blks_dirtied  AS shared_blks_dirtied,
          dbl.shared_blks_written  AS shared_blks_written,
          dbl.local_blks_hit  AS local_blks_hit,
          dbl.local_blks_read  AS local_blks_read,
          dbl.local_blks_dirtied  AS local_blks_dirtied,
          dbl.local_blks_written  AS local_blks_written,
          dbl.temp_blks_read  AS temp_blks_read,
          dbl.temp_blks_written  AS temp_blks_written,
          dbl.blk_read_time  AS blk_read_time,
          dbl.blk_write_time  AS blk_write_time,
          dbl.wal_records AS wal_records,
          dbl.wal_fpi AS wal_fpi,
          dbl.wal_bytes AS wal_bytes,
          dbl.query AS query,
          -- pg_stat_kcache fields
          dbl.kcache_avail AS kcache_avail,
          dbl.plan_user_time  AS plan_user_time,
          dbl.plan_system_time  AS plan_system_time,
          dbl.plan_minflts  AS plan_minflts,
          dbl.plan_majflts  AS plan_majflts,
          dbl.plan_nswaps  AS plan_nswaps,
          dbl.plan_reads  AS plan_reads,
          dbl.plan_writes  AS plan_writes,
          dbl.plan_msgsnds  AS plan_msgsnds,
          dbl.plan_msgrcvs  AS plan_msgrcvs,
          dbl.plan_nsignals  AS plan_nsignals,
          dbl.plan_nvcsws  AS plan_nvcsws,
          dbl.plan_nivcsws  AS plan_nivcsws,
          dbl.exec_user_time  AS exec_user_time,
          dbl.exec_system_time  AS exec_system_time,
          dbl.exec_minflts  AS exec_minflts,
          dbl.exec_majflts  AS exec_majflts,
          dbl.exec_nswaps  AS exec_nswaps,
          dbl.exec_reads  AS exec_reads,
          dbl.exec_writes  AS exec_writes,
          dbl.exec_msgsnds  AS exec_msgsnds,
          dbl.exec_msgrcvs  AS exec_msgrcvs,
          dbl.exec_nsignals  AS exec_nsignals,
          dbl.exec_nvcsws  AS exec_nvcsws,
          dbl.exec_nivcsws  AS exec_nivcsws
        FROM dblink('server_connection',st_query)
        AS dbl (
          -- pg_stat_statements fields
            userid              oid,
            username            name,
            datid               oid,
            queryid             bigint,
            queryid_md5         char(32),
            toplevel            boolean,
            plans               bigint,
            total_plan_time     double precision,
            min_plan_time       double precision,
            max_plan_time       double precision,
            mean_plan_time      double precision,
            stddev_plan_time    double precision,
            calls               bigint,
            total_exec_time     double precision,
            min_exec_time       double precision,
            max_exec_time       double precision,
            mean_exec_time      double precision,
            stddev_exec_time    double precision,
            rows                bigint,
            shared_blks_hit     bigint,
            shared_blks_read    bigint,
            shared_blks_dirtied bigint,
            shared_blks_written bigint,
            local_blks_hit      bigint,
            local_blks_read     bigint,
            local_blks_dirtied  bigint,
            local_blks_written  bigint,
            temp_blks_read      bigint,
            temp_blks_written   bigint,
            blk_read_time       double precision,
            blk_write_time      double precision,
            wal_records         bigint,
            wal_fpi             bigint,
            wal_bytes           numeric,
            query               text,
          -- pg_stat_kcache fields
            kcache_avail        boolean,
            plan_user_time      double precision, --  User CPU time used
            plan_system_time    double precision, --  System CPU time used
            plan_minflts         bigint, -- Number of page reclaims (soft page faults)
            plan_majflts         bigint, -- Number of page faults (hard page faults)
            plan_nswaps         bigint, -- Number of swaps
            plan_reads          bigint, -- Number of bytes read by the filesystem layer
            --reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
            plan_writes         bigint, -- Number of bytes written by the filesystem layer
            --plan_writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
            plan_msgsnds        bigint, -- Number of IPC messages sent
            plan_msgrcvs        bigint, -- Number of IPC messages received
            plan_nsignals       bigint, -- Number of signals received
            plan_nvcsws         bigint, -- Number of voluntary context switches
            plan_nivcsws        bigint,
            exec_user_time      double precision, --  User CPU time used
            exec_system_time    double precision, --  System CPU time used
            exec_minflts         bigint, -- Number of page reclaims (soft page faults)
            exec_majflts         bigint, -- Number of page faults (hard page faults)
            exec_nswaps         bigint, -- Number of swaps
            exec_reads          bigint, -- Number of bytes read by the filesystem layer
            --reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
            exec_writes         bigint, -- Number of bytes written by the filesystem layer
            --exec_writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
            exec_msgsnds        bigint, -- Number of IPC messages sent
            exec_msgrcvs        bigint, -- Number of IPC messages received
            exec_nsignals       bigint, -- Number of signals received
            exec_nvcsws         bigint, -- Number of voluntary context switches
            exec_nivcsws        bigint
        ) JOIN sample_stat_database sd ON (dbl.datid = sd.datid AND sd.sample_id = s_id AND sd.server_id = sserver_id)
    LOOP
        INSERT INTO stmt_list(
          server_id,
          queryid_md5,
          query
        )
        VALUES (sserver_id,qres.queryid_md5,qres.query) ON CONFLICT DO NOTHING;

        -- User names
        UPDATE roles_list SET username = qres.username
        WHERE
          (server_id, userid) =
          (sserver_id, qres.userid)
          AND username != qres.username;

        INSERT INTO roles_list (
          server_id,
          userid,
          username
        ) VALUES (
          sserver_id,
          qres.userid,
          COALESCE(qres.username, '_unknown_')
        )
        ON CONFLICT DO NOTHING;

        INSERT INTO sample_statements(
          server_id,
          sample_id,
          userid,
          datid,
          toplevel,
          queryid,
          queryid_md5,
          plans,
          total_plan_time,
          min_plan_time,
          max_plan_time,
          mean_plan_time,
          stddev_plan_time,
          calls,
          total_exec_time,
          min_exec_time,
          max_exec_time,
          mean_exec_time,
          stddev_exec_time,
          rows,
          shared_blks_hit,
          shared_blks_read,
          shared_blks_dirtied,
          shared_blks_written,
          local_blks_hit,
          local_blks_read,
          local_blks_dirtied,
          local_blks_written,
          temp_blks_read,
          temp_blks_written,
          blk_read_time,
          blk_write_time,
          wal_records,
          wal_fpi,
          wal_bytes
        )
        VALUES (
            qres.sserver_id,
            qres.sample_id,
            qres.userid,
            qres.datid,
            qres.toplevel,
            qres.queryid,
            qres.queryid_md5,
            qres.plans,
            qres.total_plan_time,
            qres.min_plan_time,
            qres.max_plan_time,
            qres.mean_plan_time,
            qres.stddev_plan_time,
            qres.calls,
            qres.total_exec_time,
            qres.min_exec_time,
            qres.max_exec_time,
            qres.mean_exec_time,
            qres.stddev_exec_time,
            qres.rows,
            qres.shared_blks_hit,
            qres.shared_blks_read,
            qres.shared_blks_dirtied,
            qres.shared_blks_written,
            qres.local_blks_hit,
            qres.local_blks_read,
            qres.local_blks_dirtied,
            qres.local_blks_written,
            qres.temp_blks_read,
            qres.temp_blks_written,
            qres.blk_read_time,
            qres.blk_write_time,
            qres.wal_records,
            qres.wal_fpi,
            qres.wal_bytes
        );
        IF qres.kcache_avail THEN
          INSERT INTO sample_kcache(
            server_id,
            sample_id,
            userid,
            datid,
            queryid,
            queryid_md5,
            plan_user_time,
            plan_system_time,
            plan_minflts,
            plan_majflts,
            plan_nswaps,
            plan_reads,
            plan_writes,
            plan_msgsnds,
            plan_msgrcvs,
            plan_nsignals,
            plan_nvcsws,
            plan_nivcsws,
            exec_user_time,
            exec_system_time,
            exec_minflts,
            exec_majflts,
            exec_nswaps,
            exec_reads,
            exec_writes,
            exec_msgsnds,
            exec_msgrcvs,
            exec_nsignals,
            exec_nvcsws,
            exec_nivcsws
          )
          VALUES (
            qres.sserver_id,
            qres.sample_id,
            qres.userid,
            qres.datid,
            qres.queryid,
            qres.queryid_md5,
            qres.plan_user_time,
            qres.plan_system_time,
            qres.plan_minflts,
            qres.plan_majflts,
            qres.plan_nswaps,
            qres.plan_reads,
            qres.plan_writes,
            qres.plan_msgsnds,
            qres.plan_msgrcvs,
            qres.plan_nsignals,
            qres.plan_nvcsws,
            qres.plan_nivcsws,
            qres.exec_user_time,
            qres.exec_system_time,
            qres.exec_minflts,
            qres.exec_majflts,
            qres.exec_nswaps,
            qres.exec_reads,
            qres.exec_writes,
            qres.exec_msgsnds,
            qres.exec_msgrcvs,
            qres.exec_nsignals,
            qres.exec_nvcsws,
            qres.exec_nivcsws
          );
        END IF;
    END LOOP;

    -- Agregated pg_stat_kcache data
    CASE (
        SELECT extversion FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extversion text)
        WHERE extname = 'pg_stat_kcache'
    )
      WHEN '2.1.0','2.1.1','2.1.2','2.1.3' THEN
        INSERT INTO sample_kcache_total(
          server_id,
          sample_id,
          datid,
          exec_user_time,
          exec_system_time,
          exec_minflts,
          exec_majflts,
          exec_nswaps,
          exec_reads,
          exec_writes,
          exec_msgsnds,
          exec_msgrcvs,
          exec_nsignals,
          exec_nvcsws,
          exec_nivcsws,
          statements
        )
        SELECT sd.server_id,sd.sample_id,dbl.*
        FROM
          dblink('server_connection',
            format('SELECT
                dbid as datid,
                sum(user_time),
                sum(system_time),
                sum(minflts),
                sum(majflts),
                sum(nswaps),
                sum(reads),
                sum(writes),
                sum(msgsnds),
                sum(msgrcvs),
                sum(nsignals),
                sum(nvcsws),
                sum(nivcsws),
                count(*)
              FROM %1$I.pg_stat_kcache()
              GROUP BY dbid',
                (
                  SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                    AS x(extname text, extnamespace text)
                  WHERE extname = 'pg_stat_kcache'
                )
            )
          ) AS dbl (
            datid               oid,
            exec_user_time           double precision,
            exec_system_time         double precision,
            exec_minflts             bigint,
            exec_majflts             bigint, -- Number of page faults (hard page faults)
            exec_nswaps              bigint, -- Number of swaps
            exec_reads               bigint, -- Number of bytes read by the filesystem layer
            exec_writes              bigint, -- Number of bytes written by the filesystem layer
            exec_msgsnds             bigint, -- Number of IPC messages sent
            exec_msgrcvs             bigint, -- Number of IPC messages received
            exec_nsignals            bigint, -- Number of signals received
            exec_nvcsws              bigint, -- Number of voluntary context switches
            exec_nivcsws             bigint,
            stmts               integer
        ) JOIN sample_stat_database sd USING (datid)
        WHERE sd.sample_id = s_id AND sd.server_id = sserver_id;

        -- Flushing pg_stat_kcache
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_kcache_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          )
        ) AS t(res char(1));
      WHEN '2.2.0' THEN
        INSERT INTO sample_kcache_total(
          server_id,
          sample_id,
          datid,
          plan_user_time,
          plan_system_time,
          plan_minflts,
          plan_majflts,
          plan_nswaps,
          plan_reads,
          plan_writes,
          plan_msgsnds,
          plan_msgrcvs,
          plan_nsignals,
          plan_nvcsws,
          plan_nivcsws,
          exec_user_time,
          exec_system_time,
          exec_minflts,
          exec_majflts,
          exec_nswaps,
          exec_reads,
          exec_writes,
          exec_msgsnds,
          exec_msgrcvs,
          exec_nsignals,
          exec_nvcsws,
          exec_nivcsws,
          statements
        )
        SELECT sd.server_id,sd.sample_id,dbl.*
        FROM
          dblink('server_connection',
            format('SELECT '
                'dbid as datid, '
                'sum(plan_user_time), '
                'sum(plan_system_time), '
                'sum(plan_minflts), '
                'sum(plan_majflts), '
                'sum(plan_nswaps), '
                'sum(plan_reads), '
                'sum(plan_writes), '
                'sum(plan_msgsnds), '
                'sum(plan_msgrcvs), '
                'sum(plan_nsignals), '
                'sum(plan_nvcsws), '
                'sum(plan_nivcsws), '
                'sum(exec_user_time), '
                'sum(exec_system_time), '
                'sum(exec_minflts), '
                'sum(exec_majflts), '
                'sum(exec_nswaps), '
                'sum(exec_reads), '
                'sum(exec_writes), '
                'sum(exec_msgsnds), '
                'sum(exec_msgrcvs), '
                'sum(exec_nsignals), '
                'sum(exec_nvcsws), '
                'sum(exec_nivcsws), '
                'count(*) '
              'FROM %1$I.pg_stat_kcache() '
              'WHERE top '
              'GROUP BY dbid',
                (
                  SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                    AS x(extname text, extnamespace text)
                  WHERE extname = 'pg_stat_kcache'
                )
            )
          ) AS dbl (
            datid               oid,
            plan_user_time      double precision,
            plan_system_time    double precision,
            plan_minflts         bigint,
            plan_majflts         bigint, -- Number of page faults (hard page faults)
            plan_nswaps         bigint, -- Number of swaps
            plan_reads          bigint, -- Number of bytes read by the filesystem layer
            plan_writes         bigint, -- Number of bytes written by the filesystem layer
            plan_msgsnds        bigint, -- Number of IPC messages sent
            plan_msgrcvs        bigint, -- Number of IPC messages received
            plan_nsignals       bigint, -- Number of signals received
            plan_nvcsws         bigint, -- Number of voluntary context switches
            plan_nivcsws        bigint,
            exec_user_time      double precision,
            exec_system_time    double precision,
            exec_minflts         bigint,
            exec_majflts         bigint, -- Number of page faults (hard page faults)
            exec_nswaps         bigint, -- Number of swaps
            exec_reads          bigint, -- Number of bytes read by the filesystem layer
            exec_writes         bigint, -- Number of bytes written by the filesystem layer
            exec_msgsnds        bigint, -- Number of IPC messages sent
            exec_msgrcvs        bigint, -- Number of IPC messages received
            exec_nsignals       bigint, -- Number of signals received
            exec_nvcsws         bigint, -- Number of voluntary context switches
            exec_nivcsws        bigint,
            stmts               integer
        ) JOIN sample_stat_database sd USING (datid)
        WHERE sd.sample_id = s_id AND sd.server_id = sserver_id;

        -- Flushing pg_stat_kcache
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_kcache_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          )
        ) AS t(res char(1));
      ELSE
        NULL;
    END CASE;

    -- Agregated statements data
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      -- pg_stat_statements v 1.3-1.7
      WHEN '1.3','1.4','1.5','1.6','1.7' THEN
        st_query := format('SELECT '
            'dbid as datid,'
            'NULL,' -- plans
            'NULL,' -- total_plan_time
            'sum(calls),'
            'sum(total_time),'
            'sum(rows),'
            'sum(shared_blks_hit),'
            'sum(shared_blks_read),'
            'sum(shared_blks_dirtied),'
            'sum(shared_blks_written),'
            'sum(local_blks_hit),'
            'sum(local_blks_read),'
            'sum(local_blks_dirtied),'
            'sum(local_blks_written),'
            'sum(temp_blks_read),'
            'sum(temp_blks_written),'
            'sum(blk_read_time),'
            'sum(blk_write_time),'
            'NULL,' -- wal_records
            'NULL,' -- wal_fpi
            'NULL,' -- wal_bytes
            'count(*) '
        'FROM %1$I.pg_stat_statements '
        'GROUP BY dbid',
          (
            SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
              AS x(extname text, extnamespace text)
            WHERE extname = 'pg_stat_statements'
          )
        );
      -- pg_stat_statements v 1.8
      WHEN '1.8' THEN
        st_query := format('SELECT '
            'dbid as datid,'
            'sum(plans),'
            'sum(total_plan_time),'
            'sum(calls),'
            'sum(total_exec_time),'
            'sum(rows),'
            'sum(shared_blks_hit),'
            'sum(shared_blks_read),'
            'sum(shared_blks_dirtied),'
            'sum(shared_blks_written),'
            'sum(local_blks_hit),'
            'sum(local_blks_read),'
            'sum(local_blks_dirtied),'
            'sum(local_blks_written),'
            'sum(temp_blks_read),'
            'sum(temp_blks_written),'
            'sum(blk_read_time),'
            'sum(blk_write_time),'
            'sum(wal_records),'
            'sum(wal_fpi),'
            'sum(wal_bytes),'
            'count(*) '
        'FROM %1$I.pg_stat_statements '
        'GROUP BY dbid',
          (
            SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
              AS x(extname text, extnamespace text)
            WHERE extname = 'pg_stat_statements'
          )
        );
      -- pg_stat_statements v 1.9
      WHEN '1.9' THEN
        st_query := format('SELECT '
            'dbid as datid,'
            'sum(plans),'
            'sum(total_plan_time),'
            'sum(calls),'
            'sum(total_exec_time),'
            'sum(rows),'
            'sum(shared_blks_hit),'
            'sum(shared_blks_read),'
            'sum(shared_blks_dirtied),'
            'sum(shared_blks_written),'
            'sum(local_blks_hit),'
            'sum(local_blks_read),'
            'sum(local_blks_dirtied),'
            'sum(local_blks_written),'
            'sum(temp_blks_read),'
            'sum(temp_blks_written),'
            'sum(blk_read_time),'
            'sum(blk_write_time),'
            'sum(wal_records),'
            'sum(wal_fpi),'
            'sum(wal_bytes),'
            'count(*) '
        'FROM %1$I.pg_stat_statements WHERE toplevel '
        'GROUP BY dbid',
          (
            SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
              AS x(extname text, extnamespace text)
            WHERE extname = 'pg_stat_statements'
          )
        );
      ELSE
        RAISE 'Unsupported pg_stat_statements version.';
    END CASE;

    INSERT INTO sample_statements_total(
      server_id,
      sample_id,
      datid,
      plans,
      total_plan_time,
      calls,
      total_exec_time,
      rows,
      shared_blks_hit,
      shared_blks_read,
      shared_blks_dirtied,
      shared_blks_written,
      local_blks_hit,
      local_blks_read,
      local_blks_dirtied,
      local_blks_written,
      temp_blks_read,
      temp_blks_written,
      blk_read_time,
      blk_write_time,
      wal_records,
      wal_fpi,
      wal_bytes,
      statements
    )
    SELECT sd.server_id,sd.sample_id,dbl.*
    FROM
    dblink('server_connection',st_query
    ) AS dbl (
        datid               oid,
        plans               bigint,
        total_plan_time     double precision,
        calls               bigint,
        total_exec_time     double precision,
        rows                bigint,
        shared_blks_hit     bigint,
        shared_blks_read    bigint,
        shared_blks_dirtied bigint,
        shared_blks_written bigint,
        local_blks_hit      bigint,
        local_blks_read     bigint,
        local_blks_dirtied  bigint,
        local_blks_written  bigint,
        temp_blks_read      bigint,
        temp_blks_written   bigint,
        blk_read_time       double precision,
        blk_write_time      double precision,
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        stmts               integer
    ) JOIN sample_stat_database sd USING (datid)
    WHERE sd.sample_id = s_id AND sd.server_id = sserver_id;

    -- Flushing statements
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      -- pg_stat_statements v 1.3-1.8
      WHEN '1.3','1.4','1.5','1.6','1.7','1.8','1.9' THEN
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_statements_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        ) AS t(res char(1));
      ELSE
        RAISE 'Unsupported pg_stat_statements version.';
    END CASE;
END;
$$ LANGUAGE plpgsql;
