CREATE FUNCTION collect_pg_stat_statements_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer) RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  qres              record;
  st_query          text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
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

    -- Save used statements extension in sample_settings
    INSERT INTO sample_settings(
      server_id,
      first_seen,
      setting_scope,
      name,
      setting,
      reset_val,
      boot_val,
      unit,
      sourcefile,
      sourceline,
      pending_restart
    )
    SELECT
      s.server_id,
      s.sample_time,
      2 as setting_scope,
      'statements_extension',
      'pg_stat_statements',
      'pg_stat_statements',
      'pg_stat_statements',
      null,
      null,
      null,
      false
    FROM samples s LEFT OUTER JOIN  v_sample_settings prm ON
      (s.server_id, s.sample_id, prm.name, prm.setting_scope) =
      (prm.server_id, prm.sample_id, 'statements_extension', 2)
    WHERE s.server_id = sserver_id AND s.sample_id = s_id AND (prm.setting IS NULL OR prm.setting != 'pg_stat_statements');

    -- Dynamic statements query
    st_query := format(
      'SELECT '
        'st.userid,'
        'st.userid::regrole AS username,'
        'st.dbid,'
        'st.queryid,'
        '{statements_fields} '
      'FROM '
        '{statements_view} st '
    );

    st_query := replace(st_query, '{statements_view}',
      format('%1$I.pg_stat_statements(false)',
        (
          SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extnamespace text)
          WHERE extname = 'pg_stat_statements'
        )
      )
    );

    -- pg_stat_statements versions
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      WHEN '1.3','1.4','1.5','1.6','1.7'
      THEN
        st_query := replace(st_query, '{statements_fields}',
          'true as toplevel,'
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
          'NULL as wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time, '
          'NULL as temp_blk_read_time, '
          'NULL as temp_blk_write_time '
        );
      WHEN '1.8'
      THEN
        st_query := replace(st_query, '{statements_fields}',
          'true as toplevel,'
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
          'st.wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time, '
          'NULL as temp_blk_read_time, '
          'NULL as temp_blk_write_time '
        );
      WHEN '1.9'
      THEN
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
          'st.wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time, '
          'NULL as temp_blk_read_time, '
          'NULL as temp_blk_write_time '
        );
      WHEN '1.10'
      THEN
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
          'st.wal_bytes, '
          'st.jit_functions, '
          'st.jit_generation_time, '
          'st.jit_inlining_count, '
          'st.jit_inlining_time, '
          'st.jit_optimization_count, '
          'st.jit_optimization_time, '
          'st.jit_emission_count, '
          'st.jit_emission_time, '
          'st.temp_blk_read_time, '
          'st.temp_blk_write_time '
        );
      ELSE
        RAISE 'Unsupported pg_stat_statements extension version.';
    END CASE; -- pg_stat_statememts versions

    -- Get statements data
    INSERT INTO last_stat_statements (
        server_id,
        sample_id,
        userid,
        username,
        datid,
        queryid,
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
        wal_bytes,
        toplevel,
        in_sample,
        jit_functions,
        jit_generation_time,
        jit_inlining_count,
        jit_inlining_time,
        jit_optimization_count,
        jit_optimization_time,
        jit_emission_count,
        jit_emission_time,
        temp_blk_read_time,
        temp_blk_write_time
      )
    SELECT
      sserver_id,
      s_id,
      dbl.userid,
      dbl.username,
      dbl.datid,
      dbl.queryid,
      dbl.plans,
      dbl.total_plan_time,
      dbl.min_plan_time,
      dbl.max_plan_time,
      dbl.mean_plan_time,
      dbl.stddev_plan_time,
      dbl.calls,
      dbl.total_exec_time,
      dbl.min_exec_time,
      dbl.max_exec_time,
      dbl.mean_exec_time,
      dbl.stddev_exec_time,
      dbl.rows,
      dbl.shared_blks_hit,
      dbl.shared_blks_read,
      dbl.shared_blks_dirtied,
      dbl.shared_blks_written,
      dbl.local_blks_hit,
      dbl.local_blks_read,
      dbl.local_blks_dirtied,
      dbl.local_blks_written,
      dbl.temp_blks_read,
      dbl.temp_blks_written,
      dbl.blk_read_time,
      dbl.blk_write_time,
      dbl.wal_records,
      dbl.wal_fpi,
      dbl.wal_bytes,
      dbl.toplevel,
      false,
      dbl.jit_functions,
      dbl.jit_generation_time,
      dbl.jit_inlining_count,
      dbl.jit_inlining_time,
      dbl.jit_optimization_count,
      dbl.jit_optimization_time,
      dbl.jit_emission_count,
      dbl.jit_emission_time,
      dbl.temp_blk_read_time,
      dbl.temp_blk_write_time
    FROM dblink('server_connection',st_query)
    AS dbl (
      -- pg_stat_statements fields
        userid              oid,
        username            name,
        datid               oid,
        queryid             bigint,
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
        jit_functions       bigint,
        jit_generation_time double precision,
        jit_inlining_count  bigint,
        jit_inlining_time   double precision,
        jit_optimization_count  bigint,
        jit_optimization_time   double precision,
        jit_emission_count  bigint,
        jit_emission_time   double precision,
        temp_blk_read_time  double precision,
        temp_blk_write_time double precision
      );
    EXECUTE format('ANALYZE last_stat_statements_srv%1$s',
      sserver_id);

    -- Rusage data collection when available
    IF
      (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_kcache'
      )
    THEN
      -- Dynamic rusage query
      st_query := format(
        'SELECT '
          'kc.userid,'
          'kc.dbid,'
          'kc.queryid,'
          '{kcache_fields} '
        'FROM '
          '{kcache_view} kc '
      );

      st_query := replace(st_query, '{kcache_view}',
        format('%1$I.pg_stat_kcache()',
          (
            SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
              AS x(extname text, extnamespace text)
            WHERE extname = 'pg_stat_kcache'
          )
        )
      );

      CASE -- pg_stat_kcache versions
        (
          SELECT extversion
          FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extversion text)
          WHERE extname = 'pg_stat_kcache'
        )
        -- pg_stat_kcache v.2.1.0 - 2.1.3
        WHEN '2.1.0','2.1.1','2.1.2','2.1.3'
        THEN
          st_query := replace(st_query, '{kcache_fields}',
            'true as toplevel,'
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
            'kc.writes as exec_writes,'
            'kc.msgsnds as exec_msgsnds,'
            'kc.msgrcvs as exec_msgrcvs,'
            'kc.nsignals as exec_nsignals,'
            'kc.nvcsws as exec_nvcsws,'
            'kc.nivcsws as exec_nivcsws '
          );
        -- pg_stat_kcache v.2.2.0, 2.2.1, 2.2.2
        WHEN '2.2.0', '2.2.1', '2.2.2', '2.2.3'
        THEN
          st_query := replace(st_query, '{kcache_fields}',
            'kc.top as toplevel,'
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
            'kc.exec_nivcsws as exec_nivcsws '
          );
        ELSE
          st_query := NULL;
      END CASE; -- pg_stat_kcache versions

      IF st_query IS NOT NULL THEN
        INSERT INTO last_stat_kcache(
          server_id,
          sample_id,
          userid,
          datid,
          toplevel,
          queryid,
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
        SELECT
          sserver_id,
          s_id,
          dbl.userid,
          dbl.datid,
          dbl.toplevel,
          dbl.queryid,
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
          userid            oid,
          datid             oid,
          queryid           bigint,
          toplevel          boolean,
          plan_user_time    double precision,
          plan_system_time  double precision,
          plan_minflts      bigint,
          plan_majflts      bigint,
          plan_nswaps       bigint,
          plan_reads        bigint,
          plan_writes       bigint,
          plan_msgsnds      bigint,
          plan_msgrcvs      bigint,
          plan_nsignals     bigint,
          plan_nvcsws       bigint,
          plan_nivcsws      bigint,
          exec_user_time    double precision,
          exec_system_time  double precision,
          exec_minflts      bigint,
          exec_majflts      bigint,
          exec_nswaps       bigint,
          exec_reads        bigint,
          exec_writes       bigint,
          exec_msgsnds      bigint,
          exec_msgrcvs      bigint,
          exec_nsignals     bigint,
          exec_nvcsws       bigint,
          exec_nivcsws      bigint
        ) JOIN last_stat_statements lss USING (userid, datid, queryid, toplevel)
        WHERE
          (lss.server_id, lss.sample_id) = (sserver_id, s_id);
        EXECUTE format('ANALYZE last_stat_kcache_srv%1$s',
          sserver_id);
      END IF; -- st_query is not null
    END IF; -- pg_stat_kcache extension is available

    PERFORM mark_pg_stat_statements(sserver_id, s_id, topn);

    -- Get queries texts
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      WHEN '1.3','1.4','1.5','1.6','1.7','1.8'
      THEN
        st_query :=
          'SELECT userid, dbid, true AS toplevel, queryid, '||
          $o$regexp_replace(query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query $o$ ||
          'FROM %1$I.pg_stat_statements(true) '
          'WHERE queryid IN (%s)';
      WHEN '1.9', '1.10'
      THEN
        st_query :=
          'SELECT userid, dbid, toplevel, queryid, '||
          $o$regexp_replace(query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query $o$ ||
          'FROM %1$I.pg_stat_statements(true) '
          'WHERE queryid IN (%s)';
      ELSE
        RAISE 'Unsupported pg_stat_statements extension version.';
    END CASE;

    -- Substitute pg_stat_statements extension schema and queries list
    st_query := format(st_query,
        (
          SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extnamespace text)
          WHERE extname = 'pg_stat_statements'
        ),
        (
          SELECT string_agg(queryid::text,',')
          FROM last_stat_statements
          WHERE
            (server_id, sample_id, in_sample) =
            (sserver_id, s_id, true)
        )
    );

    -- Now we can save statement
    FOR qres IN (
      SELECT
        userid,
        datid,
        toplevel,
        queryid,
        query
      FROM dblink('server_connection',st_query) AS
        dbl(
            userid    oid,
            datid     oid,
            toplevel  boolean,
            queryid   bigint,
            query     text
          )
        JOIN last_stat_statements lst USING (userid, datid, toplevel, queryid)
      WHERE
        (lst.server_id, lst.sample_id, lst.in_sample) =
        (sserver_id, s_id, true)
    )
    LOOP
      -- statement texts
      INSERT INTO stmt_list AS isl (
          server_id,
          last_sample_id,
          queryid_md5,
          query
        )
      VALUES (
          sserver_id,
          NULL,
          md5(COALESCE(qres.query, '')),
          qres.query
        )
      ON CONFLICT ON CONSTRAINT pk_stmt_list
      DO UPDATE SET last_sample_id = NULL
      WHERE
        isl.last_sample_id IS NOT NULL;

      -- bind queryid to queryid_md5 for this sample
      -- different text queries can have the same queryid
      -- between samples
      UPDATE last_stat_statements SET queryid_md5 = md5(COALESCE(qres.query, ''))
      WHERE (server_id, sample_id, userid, datid, toplevel, queryid) =
        (sserver_id, s_id, qres.userid, qres.datid, qres.toplevel, qres.queryid);
    END LOOP; -- over sample statements

    -- Flushing pg_stat_kcache
    CASE (
        SELECT extversion FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extversion text)
        WHERE extname = 'pg_stat_kcache'
    )
      WHEN '2.1.0','2.1.1','2.1.2','2.1.3','2.2.0','2.2.1','2.2.2','2.2.3'
      THEN
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

    -- Flushing statements
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      -- pg_stat_statements v 1.3-1.8
      WHEN '1.3','1.4','1.5','1.6','1.7','1.8','1.9','1.10'
      THEN
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

    -- Save the diffs in a sample
    PERFORM save_pg_stat_statements(sserver_id, s_id);
    -- Delete obsolete last_* data
    DELETE FROM last_stat_kcache WHERE server_id = sserver_id AND sample_id < s_id;
    DELETE FROM last_stat_statements WHERE server_id = sserver_id AND sample_id < s_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION mark_pg_stat_statements(IN sserver_id integer, IN s_id integer, IN topn integer)
RETURNS void
SET search_path=@extschema@ AS $$
  -- Mark statements to include in a sample
  UPDATE last_stat_statements ust
  SET in_sample = true
  FROM
    (SELECT
      cur.server_id,
      cur.sample_id,
      cur.userid,
      cur.datid,
      cur.queryid,
      cur.toplevel,
      cur.wal_bytes IS NOT NULL AS wal_avail,
      cur.total_plan_time IS NOT NULL AS plantime_avail,
      COALESCE(cur.blk_read_time,0) + COALESCE(cur.blk_write_time,0) > 0 AS iotime_avail,
      row_number() over (ORDER BY cur.total_plan_time + cur.total_exec_time DESC NULLS LAST) AS time_rank,
      row_number() over (ORDER BY cur.total_plan_time DESC NULLS LAST) AS plan_time_rank,
      row_number() over (ORDER BY cur.total_exec_time DESC NULLS LAST) AS exec_time_rank,
      row_number() over (ORDER BY cur.calls DESC NULLS LAST) AS calls_rank,
      row_number() over (ORDER BY cur.blk_read_time + cur.blk_write_time DESC NULLS LAST) AS io_time_rank,
      CASE WHEN COALESCE(cur.temp_blk_read_time, 0) + COALESCE(cur.temp_blk_write_time, 0) > 0 THEN
        row_number() over (ORDER BY COALESCE(cur.temp_blk_read_time, 0) + COALESCE(cur.temp_blk_write_time, 0)
          DESC NULLS LAST)
      ELSE NULL END AS io_temp_rank,
      row_number() over (ORDER BY cur.shared_blks_hit + cur.shared_blks_read DESC NULLS LAST) AS gets_rank,
      row_number() over (ORDER BY cur.shared_blks_read DESC NULLS LAST) AS read_rank,
      row_number() over (ORDER BY cur.shared_blks_dirtied DESC NULLS LAST) AS dirtied_rank,
      row_number() over (ORDER BY cur.shared_blks_written DESC NULLS LAST) AS written_rank,
      row_number() over (ORDER BY cur.temp_blks_written + cur.local_blks_written DESC NULLS LAST) AS tempw_rank,
      row_number() over (ORDER BY cur.temp_blks_read + cur.local_blks_read DESC NULLS LAST) AS tempr_rank,
      row_number() over (ORDER BY cur.wal_bytes DESC NULLS LAST) AS wal_rank
    FROM
      last_stat_statements cur
      -- In case of statements in already dropped database
      JOIN sample_stat_database db USING (server_id, sample_id, datid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    ) diff
  WHERE
    (
      (wal_avail AND wal_rank <= topn) OR
      (plantime_avail AND least(time_rank, plan_time_rank) <= topn) OR
      (iotime_avail AND io_time_rank <= topn) OR
      least(
        exec_time_rank,
        calls_rank,
        gets_rank,
        read_rank,
        dirtied_rank,
        written_rank,
        io_temp_rank,
        tempw_rank,
        tempr_rank
      ) <= topn
    )
    AND
    (ust.server_id ,ust.sample_id, ust.userid, ust.datid, ust.queryid, ust.toplevel, ust.in_sample) =
    (diff.server_id, diff.sample_id, diff.userid, diff.datid, diff.queryid, diff.toplevel, false);

  -- Mark rusage stats to include in a sample
  UPDATE last_stat_statements ust
  SET in_sample = true
  FROM
    (SELECT
      cur.server_id,
      cur.sample_id,
      cur.userid,
      cur.datid,
      cur.queryid,
      cur.toplevel,
      COALESCE(plan_user_time, 0.0) + COALESCE(plan_system_time, 0.0) > 0.0 AS plans_stats_avail,
      row_number() OVER (ORDER BY plan_user_time + plan_system_time DESC NULLS LAST) AS plan_cpu_time_rank,
      row_number() OVER (ORDER BY exec_user_time + exec_system_time DESC NULLS LAST) AS exec_cpu_time_rank,
      row_number() OVER (ORDER BY plan_reads + plan_writes DESC NULLS LAST) AS plan_io_rank,
      row_number() OVER (ORDER BY exec_reads + exec_writes DESC NULLS LAST) AS exec_io_rank
    FROM
      last_stat_kcache cur
      -- In case of statements in already dropped database
      JOIN sample_stat_database db USING (server_id, sample_id, datid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    ) diff
  WHERE
    (
      (plans_stats_avail AND least(plan_cpu_time_rank, plan_io_rank) <= topn) OR
      least(
        exec_cpu_time_rank,
        exec_io_rank
      ) <= topn
    )
    AND
    (ust.server_id, ust.sample_id, ust.userid, ust.datid, ust.queryid, ust.toplevel, ust.in_sample) =
    (diff.server_id, diff.sample_id, diff.userid, diff.datid, diff.queryid, diff.toplevel, false);
$$ LANGUAGE sql;

CREATE FUNCTION save_pg_stat_statements(IN sserver_id integer, IN s_id integer)
RETURNS void
SET search_path=@extschema@ AS $$
  -- This function performs save marked statements data in sample tables
  -- User names
  INSERT INTO roles_list AS irl (
    server_id,
    last_sample_id,
    userid,
    username
  )
  SELECT DISTINCT
    sserver_id,
    NULL::integer,
    st.userid,
    COALESCE(st.username, '_unknown_')
  FROM
    last_stat_statements st
  WHERE (st.server_id, st.sample_id, in_sample) = (sserver_id, s_id, true)
  ON CONFLICT ON CONSTRAINT pk_roles_list
  DO UPDATE SET
    (last_sample_id, username) =
    (EXCLUDED.last_sample_id, EXCLUDED.username)
  WHERE
    (irl.last_sample_id, irl.username) IS DISTINCT FROM
    (EXCLUDED.last_sample_id, EXCLUDED.username)
  ;

  -- Statement stats
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
    wal_bytes,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time,
    temp_blk_read_time,
    temp_blk_write_time
  )
  SELECT
    sserver_id,
    s_id,
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
    wal_bytes,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time,
    temp_blk_read_time,
    temp_blk_write_time
  FROM
    last_stat_statements JOIN stmt_list USING (server_id, queryid_md5)
  WHERE
    (server_id, sample_id, in_sample) = (sserver_id, s_id, true);

  /*
  * Aggregated statements stats
  */
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
    statements,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time,
    temp_blk_read_time,
    temp_blk_write_time
  )
  SELECT
    server_id,
    sample_id,
    datid,
    sum(lss.plans),
    sum(lss.total_plan_time),
    sum(lss.calls),
    sum(lss.total_exec_time),
    sum(lss.rows),
    sum(lss.shared_blks_hit),
    sum(lss.shared_blks_read),
    sum(lss.shared_blks_dirtied),
    sum(lss.shared_blks_written),
    sum(lss.local_blks_hit),
    sum(lss.local_blks_read),
    sum(lss.local_blks_dirtied),
    sum(lss.local_blks_written),
    sum(lss.temp_blks_read),
    sum(lss.temp_blks_written),
    sum(lss.blk_read_time),
    sum(lss.blk_write_time),
    sum(lss.wal_records),
    sum(lss.wal_fpi),
    sum(lss.wal_bytes),
    count(*),
    sum(lss.jit_functions),
    sum(lss.jit_generation_time),
    sum(lss.jit_inlining_count),
    sum(lss.jit_inlining_time),
    sum(lss.jit_optimization_count),
    sum(lss.jit_optimization_time),
    sum(lss.jit_emission_count),
    sum(lss.jit_emission_time),
    sum(lss.temp_blk_read_time),
    sum(lss.temp_blk_write_time)
  FROM
    last_stat_statements lss
    -- In case of already dropped database
    JOIN sample_stat_database ssd USING (server_id, sample_id, datid)
  WHERE
    (server_id, sample_id) = (sserver_id, s_id)
  GROUP BY
    server_id,
    sample_id,
    datid
  ;

  /*
  * If rusage data is available we should just save it in sample for saved
  * statements
  */
  INSERT INTO sample_kcache (
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
      exec_nivcsws,
      toplevel
  )
  SELECT
    cur.server_id,
    cur.sample_id,
    cur.userid,
    cur.datid,
    cur.queryid,
    sst.queryid_md5,
    cur.plan_user_time,
    cur.plan_system_time,
    cur.plan_minflts,
    cur.plan_majflts,
    cur.plan_nswaps,
    cur.plan_reads,
    cur.plan_writes,
    cur.plan_msgsnds,
    cur.plan_msgrcvs,
    cur.plan_nsignals,
    cur.plan_nvcsws,
    cur.plan_nivcsws,
    cur.exec_user_time,
    cur.exec_system_time,
    cur.exec_minflts,
    cur.exec_majflts,
    cur.exec_nswaps,
    cur.exec_reads,
    cur.exec_writes,
    cur.exec_msgsnds,
    cur.exec_msgrcvs,
    cur.exec_nsignals,
    cur.exec_nvcsws,
    cur.exec_nivcsws,
    cur.toplevel
  FROM
    last_stat_kcache cur JOIN last_stat_statements sst ON
      (sst.server_id, cur.server_id, sst.sample_id, sst.userid, sst.datid, sst.queryid, sst.toplevel) =
      (sserver_id, sserver_id, cur.sample_id, cur.userid, cur.datid, cur.queryid, cur.toplevel)
  WHERE
    (cur.server_id, cur.sample_id, sst.in_sample) = (sserver_id, s_id, true)
    AND sst.queryid_md5 IS NOT NULL;

  -- Aggregated pg_stat_kcache data
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
  SELECT
    cur.server_id,
    cur.sample_id,
    cur.datid,
    sum(plan_user_time),
    sum(plan_system_time),
    sum(plan_minflts),
    sum(plan_majflts),
    sum(plan_nswaps),
    sum(plan_reads),
    sum(plan_writes),
    sum(plan_msgsnds),
    sum(plan_msgrcvs),
    sum(plan_nsignals),
    sum(plan_nvcsws),
    sum(plan_nivcsws),
    sum(exec_user_time),
    sum(exec_system_time),
    sum(exec_minflts),
    sum(exec_majflts),
    sum(exec_nswaps),
    sum(exec_reads),
    sum(exec_writes),
    sum(exec_msgsnds),
    sum(exec_msgrcvs),
    sum(exec_nsignals),
    sum(exec_nvcsws),
    sum(exec_nivcsws),
    count(*)
  FROM
    last_stat_kcache cur
    -- In case of already dropped database
    JOIN sample_stat_database db USING (server_id, sample_id, datid)
  WHERE
    (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
    toplevel
  GROUP BY
    server_id,
    sample_id,
    datid
  ;
$$ LANGUAGE sql;
