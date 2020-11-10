CREATE FUNCTION collect_pg_stat_statements_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer) RETURNS void SET search_path=@extschema@,public AS $$
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
        'st.dbid,'
        'st.queryid,'
        'left(md5(db.datname || r.rolname || st.query ), 10) AS queryid_md5,'
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
          'kc.user_time as user_time,'
          'kc.system_time as system_time,'
          'kc.minflts as minflts,'
          'kc.majflts as majflts,'
          'kc.nswaps as nswaps,'
          'kc.reads as reads,'
          'kc.writes  as writes,'
          'kc.msgsnds as msgsnds,'
          'kc.msgrcvs as msgrcvs,'
          'kc.nsignals as nsignals,'
          'kc.nvcsws as nvcsws,'
          'kc.nivcsws as nivcsws'
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
          ', row_number() over (ORDER BY user_time+system_time DESC) AS cpu_time_rank,'
          'row_number() over (ORDER BY reads+writes DESC) AS io_rank '
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
      ELSE -- sutable pg_stat_kcache version not found
        st_query := replace(st_query, '{kcache_stats}','');
        st_query := replace(st_query, '{kcache_rank_calc}','');
        st_query := replace(st_query, '{kcache_rank_join}','');
        st_query := replace(st_query, '{kcache_rank_fields}','');
        st_query := replace(st_query, '{kcache_fields}',
          ',false as kcache_avail,'
          'NULL as user_time,'
          'NULL as system_time,'
          'NULL as minflts,'
          'NULL as majflts,'
          'NULL as nswaps,'
          'NULL as reads,'
          'NULL  as writes,'
          'NULL as msgsnds,'
          'NULL as msgrcvs,'
          'NULL as nsignals,'
          'NULL as nvcsws,'
          'NULL as nivcsws');
    END CASE; --pg_stat_kcache versions

    -- RAISE LOG 'stmts query: %',st_query; -- statements query debug
    -- Sample data from pg_stat_statements and pg_stat_kcache top whole cluster statements
    FOR qres IN
        SELECT
          -- pg_stat_statements fields
          sserver_id,
          s_id AS sample_id,
          dbl.userid AS userid,
          dbl.datid AS datid,
          dbl.queryid AS queryid,
          dbl.queryid_md5 AS queryid_md5,
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
          dbl.user_time  AS user_time,
          dbl.system_time  AS system_time,
          dbl.minflts  AS minflts,
          dbl.majflts  AS majflts,
          dbl.nswaps  AS nswaps,
          dbl.reads  AS reads,
          dbl.writes  AS writes,
          dbl.msgsnds  AS msgsnds,
          dbl.msgrcvs  AS msgrcvs,
          dbl.nsignals  AS nsignals,
          dbl.nvcsws  AS nvcsws,
          dbl.nivcsws  AS nivcsws
        FROM dblink('server_connection',st_query)
        AS dbl (
          -- pg_stat_statements fields
            userid              oid,
            datid               oid,
            queryid             bigint,
            queryid_md5         char(10),
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
            user_time           double precision, --  User CPU time used
            system_time         double precision, --  System CPU time used
            minflts             bigint, -- Number of page reclaims (soft page faults)
            majflts             bigint, -- Number of page faults (hard page faults)
            nswaps              bigint, -- Number of swaps
            reads               bigint, -- Number of bytes read by the filesystem layer
            --reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
            writes              bigint, -- Number of bytes written by the filesystem layer
            --writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
            msgsnds             bigint, -- Number of IPC messages sent
            msgrcvs             bigint, -- Number of IPC messages received
            nsignals            bigint, -- Number of signals received
            nvcsws              bigint, -- Number of voluntary context switches
            nivcsws             bigint
        ) JOIN sample_stat_database sd ON (dbl.datid = sd.datid AND sd.sample_id = s_id AND sd.server_id = sserver_id)
    LOOP
        INSERT INTO stmt_list(
          queryid_md5,
          query
        )
        VALUES (qres.queryid_md5,qres.query) ON CONFLICT DO NOTHING;

        INSERT INTO sample_statements(
          server_id,
          sample_id,
          userid,
          datid,
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
            user_time,
            system_time,
            minflts,
            majflts,
            nswaps,
            reads,
            writes,
            msgsnds,
            msgrcvs,
            nsignals,
            nvcsws,
            nivcsws
          )
          VALUES (
            qres.sserver_id,
            qres.sample_id,
            qres.userid,
            qres.datid,
            qres.queryid,
            qres.queryid_md5,
            qres.user_time,
            qres.system_time,
            qres.minflts,
            qres.majflts,
            qres.nswaps,
            qres.reads,
            qres.writes,
            qres.msgsnds,
            qres.msgrcvs,
            qres.nsignals,
            qres.nvcsws,
            qres.nivcsws
          );
        END IF;
    END LOOP;

    -- Aggregeted pg_stat_kcache data
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
          user_time,
          system_time,
          minflts,
          majflts,
          nswaps,
          reads,
          writes,
          msgsnds,
          msgrcvs,
          nsignals,
          nvcsws,
          nivcsws,
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
                sum(nivcsws ),
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
            user_time           double precision,
            system_time         double precision,
            minflts             bigint,
            majflts             bigint, -- Number of page faults (hard page faults)
            nswaps              bigint, -- Number of swaps
            reads               bigint, -- Number of bytes read by the filesystem layer
            writes              bigint, -- Number of bytes written by the filesystem layer
            msgsnds             bigint, -- Number of IPC messages sent
            msgrcvs             bigint, -- Number of IPC messages received
            nsignals            bigint, -- Number of signals received
            nvcsws              bigint, -- Number of voluntary context switches
            nivcsws             bigint,
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

    -- Aggregeted statements data
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
      ELSE
        RAISE 'Unsupported pg_stat_statements version. Supported versions are 1.3, 1.4, 1.5, 1.6, 1.7, 1.8';
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
      WHEN '1.3','1.4','1.5','1.6','1.7','1.8' THEN
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
        RAISE 'Unsupported pg_stat_statements version. Supported versions are 1.3 - 1.8';
    END CASE;
END;
$$ LANGUAGE plpgsql;
