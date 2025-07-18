/* ==== Export and import functions ==== */

DROP FUNCTION IF EXISTS export_data(name, integer, integer, boolean);
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
    RAISE 'Export is not supported for manual installed version';
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
            JOIN pg_class rel ON (rel.oid = dep.objid AND rel.relkind IN ('r','p'))
            LEFT OUTER JOIN fkdeps con ON (con.reloid = dep.objid)
          WHERE ext.extname = $1 AND rel.relname NOT IN
              ('import_queries', 'import_queries_version_order',
              'report', 'report_static', 'report_struct', 'last_stat_activity')
            AND NOT rel.relispartition
            AND con.reloid IS NULL
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
        JOIN pg_class rel ON (rel.oid = dep.objid AND rel.relkind IN ('r','p'))
        JOIN pg_constraint con ON (con.conrelid = dep.objid AND con.contype = 'f')
      WHERE ext.extname = $1 AND rel.relname NOT IN
        ('import_queries', 'import_queries_version_order',
        'report', 'report_static', 'report_struct', 'last_stat_activity')
        AND NOT rel.relispartition
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
                  WHEN TRUE THEN
                    encode(sha224(convert_to(rows.query, 'UTF8')), 'base64')
                    || ', Query length: '|| length(rows.query)::text
                  ELSE rows.query
                END AS query,
                last_sample_id
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
      WHEN r_result.relname = 'act_query' THEN
        RETURN QUERY EXECUTE format(
            $sql$SELECT $1,row_to_json(dt) FROM
              (SELECT rows.server_id, rows.act_query_md5,
                CASE $5
                  WHEN TRUE THEN
                    encode(sha224(convert_to(rows.act_query, 'UTF8')), 'base64')
                    || ', Query length: '|| length(rows.act_query)::text
                  ELSE rows.act_query
                END AS act_query,
                last_sample_id
               FROM %I AS rows WHERE (server_id, act_query_md5) IN
                (SELECT server_id, act_query_md5 FROM sample_act_statement WHERE
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

CREATE FUNCTION import_data(data regclass, server_name_prefix text = NULL) RETURNS bigint
SET search_path=@extschema@ AS $$
DECLARE
  import_meta     jsonb;
  tables_list     jsonb;
  servers_list    jsonb; -- import servers list
  tmp_srv_map     jsonb = '{}'; -- import server_id to local server_id mapping
  dump_versions   text[];

  new_server_id   integer = null;
  s_id            integer;
  sserver_id      integer;
  import_stage    integer;
  tot_processed   bigint = 0;
  func_processed  bigint = 0;
  c_datarows      refcursor;

  servers_sect_id   integer = NULL;
  settings_sect_id  integer = NULL;

  r_result        RECORD;
BEGIN
  -- Get import metadata
  EXECUTE format('SELECT row_data::jsonb FROM %s WHERE section_id = 0',data)
  INTO STRICT import_meta;

  -- Check dump compatibility
  IF (SELECT count(*) < 1 FROM import_queries_version_order
      WHERE (extension, version) =
        (import_meta ->> 'extension', import_meta ->> 'version'))
  THEN
    RAISE 'Unsupported extension version: %', (import_meta ->> 'extension')||' '||(import_meta ->> 'version');
  END IF;

  -- Get import tables list
  EXECUTE format('SELECT row_data::jsonb FROM %s WHERE section_id = $1',data)
  USING (import_meta ->> 'tab_list_section')::integer
  INTO STRICT tables_list;

  -- Get vital section identifiers
  FOR r_result IN (
    SELECT
      section_id,
      relname
    FROM
      jsonb_to_recordset(tables_list) as tbllist(section_id integer, relname text)
    WHERE
      relname IN ('servers', 'sample_settings')
  ) LOOP
    CASE r_result.relname
      WHEN 'servers' THEN servers_sect_id := r_result.section_id;
      WHEN 'sample_settings' THEN settings_sect_id := r_result.section_id;
    END CASE;
  END LOOP;

  -- Servers processing
  -- Get import servers list
  EXECUTE format($q$SELECT
      jsonb_agg(srvjs.row_data::jsonb)
    FROM
      %1$s srvjs
    WHERE
      srvjs.section_id = $1$q$,
    data)
  USING servers_sect_id
  INTO STRICT servers_list;

  /*
   * Performing importing to local servers matching. We need to consider several cases:
   * - creation dates and system identifiers matched - we have a match
   * - creation dates and system identifiers don't match, but names matched - conflict as we can't create a new server
   * - nothing matched - a new local server is to be created
   * By the way, we'll populate tmp_srv_map table, containing
   * a mapping between local and importing servers to use on data load.
   */
  FOR r_result IN EXECUTE format($q$SELECT
      COALESCE($3,'')||
        imp_srv.server_name       imp_server_name,
      ls.server_name              local_server_name,
      imp_srv.server_created      imp_server_created,
      ls.server_created           local_server_created,
      d.row_data->>'reset_val'    imp_system_identifier,
      ls.system_identifier        local_system_identifier,
      imp_srv.server_id           imp_server_id,
      ls.server_id                local_server_id,
      imp_srv.server_description  imp_server_description,
      imp_srv.db_exclude          imp_server_db_exclude,
      imp_srv.connstr             imp_server_connstr,
      imp_srv.max_sample_age      imp_server_max_sample_age,
      imp_srv.last_sample_id      imp_server_last_sample_id,
      imp_srv.size_smp_wnd_start  imp_size_smp_wnd_start,
      imp_srv.size_smp_wnd_dur    imp_size_smp_wnd_dur,
      imp_srv.size_smp_interval   imp_size_smp_interval,
      imp_srv.srv_settings        imp_srv_settings
    FROM
      jsonb_to_recordset($1) as
        imp_srv(
          server_id           integer,
          server_name         name,
          server_description  text,
          server_created      timestamp with time zone,
          db_exclude          name[],
          enabled             boolean,
          connstr             text,
          max_sample_age      integer,
          last_sample_id      integer,
          size_smp_wnd_start  time with time zone,
          size_smp_wnd_dur    interval hour to second,
          size_smp_interval   interval day to minute,
          srv_settings        jsonb
        )
      JOIN %s d ON
        (d.section_id = $2 AND d.row_data->>'name' = 'system_identifier'
          AND (d.row_data->>'server_id')::integer = imp_srv.server_id)
      LEFT OUTER JOIN (
        SELECT
          srv.server_id,
          srv.server_name,
          srv.server_created,
          set.reset_val as system_identifier
        FROM servers srv
          LEFT OUTER JOIN sample_settings set ON (set.server_id = srv.server_id AND set.name = 'system_identifier')
        ) ls ON
        ((imp_srv.server_created = ls.server_created AND d.row_data->>'reset_val' = ls.system_identifier)
          OR COALESCE($3,'')||imp_srv.server_name = ls.server_name)
    $q$,
    data)
  USING
    servers_list,
    settings_sect_id,
    server_name_prefix
  LOOP
    IF r_result.imp_server_created = r_result.local_server_created AND
      r_result.imp_system_identifier = r_result.local_system_identifier
    THEN
      /* use this local server when matched by server creation time and system identifier */
      tmp_srv_map := jsonb_set(
        tmp_srv_map,
        ARRAY[r_result.imp_server_id::text],
        to_jsonb(r_result.local_server_id)
      );
      /* Update local server if new last_sample_id is greatest*/
      UPDATE servers
      SET
        (
          db_exclude,
          connstr,
          max_sample_age,
          last_sample_id,
          size_smp_wnd_start,
          size_smp_wnd_dur,
          size_smp_interval,
          srv_settings
        ) = (
          r_result.imp_server_db_exclude,
          r_result.imp_server_connstr,
          r_result.imp_server_max_sample_age,
          r_result.imp_server_last_sample_id,
          r_result.imp_size_smp_wnd_start,
          r_result.imp_size_smp_wnd_dur,
          r_result.imp_size_smp_interval,
          r_result.imp_srv_settings
        )
      WHERE server_id = r_result.local_server_id
        AND last_sample_id < r_result.imp_server_last_sample_id;
    ELSIF r_result.imp_server_name = r_result.local_server_name
    THEN
      /* Names matched, but identifiers does not - we have a conflict */
      RAISE 'Local server "%" creation date or system identifier does not match imported one (try renaming local server)',
        r_result.local_server_name;
    ELSIF r_result.local_server_name IS NULL
    THEN
      /* No match at all - we are creating a new server */
      INSERT INTO servers AS srv (
        server_name,
        server_description,
        server_created,
        db_exclude,
        enabled,
        connstr,
        max_sample_age,
        last_sample_id,
        size_smp_wnd_start,
        size_smp_wnd_dur,
        size_smp_interval,
        srv_settings)
      VALUES (
        r_result.imp_server_name,
        r_result.imp_server_description,
        r_result.imp_server_created,
        r_result.imp_server_db_exclude,
        FALSE,
        r_result.imp_server_connstr,
        r_result.imp_server_max_sample_age,
        r_result.imp_server_last_sample_id,
        r_result.imp_size_smp_wnd_start,
        r_result.imp_size_smp_wnd_dur,
        r_result.imp_size_smp_interval,
        r_result.imp_srv_settings
      )
      RETURNING server_id INTO new_server_id;
      tmp_srv_map := jsonb_set(
        tmp_srv_map,
        ARRAY[r_result.imp_server_id::text],
        to_jsonb(new_server_id)
      );
      PERFORM create_server_partitions(new_server_id);
    ELSE
      /* This shouldn't ever happen */
      RAISE 'Import and local servers matching exception';
    END IF;
  END LOOP;

  /* Version history array for checking during import
  *  This array should be ordered oldest version last, thus first
  *  entry will be the current version
  */
  WITH RECURSIVE versions AS (
      SELECT
        import_meta ->> 'extension' AS extension,
        import_meta ->> 'version' AS version,
        1 AS lvl
    UNION
      SELECT vo.parent_extension, vo.parent_version, v.lvl + 1 AS lvl
      FROM versions v JOIN import_queries_version_order vo ON
        (vo.extension, vo.version) = (v.extension, v.version)
  )
  SELECT array_agg(version || ':' || extension ORDER BY lvl DESC) INTO dump_versions
  FROM versions
  WHERE extension IS NOT NULL;

  /* Import tables data */

  SET CONSTRAINTS ALL DEFERRED;
  import_stage := 0;
  WHILE import_stage < 3 LOOP
    FOR r_result IN (
      SELECT
        section_id,
        relname
      FROM
        jsonb_to_recordset(tables_list) AS tab_list(section_id integer, relname text)
    )
    LOOP
      CASE import_stage
        WHEN 0 THEN CONTINUE WHEN r_result.relname LIKE 'last_%';
        WHEN 1 THEN CONTINUE WHEN r_result.relname NOT LIKE 'last_%' OR
          r_result.relname = 'last_stat_kcache';
        WHEN 2 THEN CONTINUE WHEN r_result.relname != 'last_stat_kcache';
      END CASE;

      OPEN c_datarows FOR EXECUTE
        format('SELECT row_data FROM %s WHERE section_id = $1', data)
      USING r_result.section_id;
        RAISE NOTICE 'Started processing table %', r_result.relname;
        func_processed = 0;

        SELECT rows_processed, COALESCE(new_import_meta, import_meta)
          INTO func_processed, import_meta
        FROM import_section_data_profile(
          c_datarows,
          r_result.relname,
          tmp_srv_map,
          import_meta,
          dump_versions);
        -- try subsample import
        IF func_processed = -1 THEN
          SELECT rows_processed, COALESCE(new_import_meta, import_meta)
          INTO func_processed, import_meta
          FROM import_section_data_subsample(
            c_datarows,
            r_result.relname,
            tmp_srv_map,
            import_meta,
            dump_versions);
        END IF;

        IF func_processed = -1 THEN
          RAISE 'No import method for table %', r_result.relname;
        END IF;

        IF func_processed > 0 THEN
          RAISE NOTICE 'Analyzing table %', r_result.relname;
          EXECUTE format('ANALYZE %I', r_result.relname);
        END IF;

        RAISE NOTICE 'Finished processing %',
          format('table %s (%s rows)',r_result.relname, func_processed);
        tot_processed := tot_processed + func_processed;
        /*
        * In case of pgpro_pwr extension we should call two functions in series
        * First would be pg_profile function and if it will fail to process a table
        * the coursor is to be directed to pgpro_pwr import function
        */
      CLOSE c_datarows;
    END LOOP; -- over dump tables
    import_stage := import_stage + 1;
  END LOOP; -- over import stages

  -- Finalize import
  FOR r_result IN (SELECT * FROM jsonb_each(tmp_srv_map)) LOOP
    sserver_id := r_result.value;
    SELECT max(sample_id) INTO s_id FROM samples WHERE server_id = sserver_id;

    -- Updating dictionary tables setting last_sample_id
    UPDATE tablespaces_list utl SET last_sample_id = s_id - 1
    FROM tablespaces_list tl LEFT JOIN sample_stat_tablespaces cur
      ON (cur.server_id, cur.sample_id, cur.tablespaceid) =
        (sserver_id, s_id, tl.tablespaceid)
    WHERE
      tl.last_sample_id IS NULL AND
      (utl.server_id, utl.tablespaceid) = (sserver_id, tl.tablespaceid) AND
      tl.server_id = sserver_id AND cur.server_id IS NULL;

    UPDATE funcs_list ufl SET last_sample_id = s_id - 1
    FROM funcs_list fl LEFT JOIN sample_stat_user_functions cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.funcid) =
        (sserver_id, s_id, fl.datid, fl.funcid)
    WHERE
      fl.last_sample_id IS NULL AND
      fl.server_id = sserver_id AND cur.server_id IS NULL AND
      (ufl.server_id, ufl.datid, ufl.funcid) =
      (sserver_id, fl.datid, fl.funcid);

    UPDATE indexes_list uil SET last_sample_id = s_id - 1
    FROM indexes_list il LEFT JOIN sample_stat_indexes cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.indexrelid) =
        (sserver_id, s_id, il.datid, il.indexrelid)
    WHERE
      il.last_sample_id IS NULL AND
      il.server_id = sserver_id AND cur.server_id IS NULL AND
      (uil.server_id, uil.datid, uil.indexrelid) =
      (sserver_id, il.datid, il.indexrelid);

    UPDATE tables_list utl SET last_sample_id = s_id - 1
    FROM tables_list tl LEFT JOIN sample_stat_tables cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.relid) =
        (sserver_id, s_id, tl.datid, tl.relid)
    WHERE
      tl.last_sample_id IS NULL AND
      tl.server_id = sserver_id AND cur.server_id IS NULL AND
      (utl.server_id, utl.datid, utl.relid) =
      (sserver_id, tl.datid, tl.relid);

    UPDATE stmt_list slu SET last_sample_id = s_id - 1
    FROM sample_statements ss RIGHT JOIN stmt_list sl
      ON (ss.server_id, ss.sample_id, ss.queryid_md5) =
        (sserver_id, s_id, sl.queryid_md5)
    WHERE
      sl.server_id = sserver_id AND
      sl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (slu.server_id, slu.queryid_md5) = (sserver_id, sl.queryid_md5);

    UPDATE roles_list rlu SET last_sample_id = s_id - 1
    FROM
        sample_statements ss
      RIGHT JOIN roles_list rl
      ON (ss.server_id, ss.sample_id, ss.userid) =
        (sserver_id, s_id, rl.userid)
    WHERE
      rl.server_id = sserver_id AND
      rl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (rlu.server_id, rlu.userid) = (sserver_id, rl.userid);

    -- Cleanup last_ tables
    DELETE FROM last_stat_activity WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_activity_count WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_archiver WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_cluster WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_database WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_indexes WHERE server_id=sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_io WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_kcache WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_slru WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_statements WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_tables WHERE server_id=sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_tablespaces WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_user_functions WHERE server_id=sserver_id AND sample_id != s_id;
    DELETE FROM last_stat_wal WHERE server_id = sserver_id AND sample_id != s_id;
    DELETE FROM last_extension_versions WHERE server_id = sserver_id AND sample_id != s_id;
  END LOOP; --over import servers
  SET CONSTRAINTS ALL IMMEDIATE;
  RETURN tot_processed;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION import_data(regclass, text) IS
  'Import sample data from table, exported by export_data function';

CREATE FUNCTION import_section_data_profile(IN data refcursor, IN imp_table_name name, IN srv_map jsonb,
  IN import_meta jsonb, IN versions_array text[],
  OUT rows_processed bigint, OUT new_import_meta jsonb)
SET search_path=@extschema@ AS $$
DECLARE
  datarow          record;
  rowcnt           bigint = 0;
  row_proc         bigint = 0;
  section_meta     jsonb;
BEGIN
  new_import_meta := import_meta;
  CASE imp_table_name
    WHEN 'servers' THEN NULL; -- skip already imported servers table
    -- skip obsolete tables from old versions
    WHEN 'sample_stat_tables_failures' THEN NULL;
    WHEN 'sample_stat_indexes_failures' THEN NULL;
    WHEN 'samples' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO samples(server_id, sample_id, sample_time)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.sample_time
        FROM json_to_record(datarow.row_data) AS dr(
            server_id       integer,
            sample_id       integer,
            sample_time     timestamp(0) with time zone
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ON CONFLICT ON CONSTRAINT pk_samples DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'baselines' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO baselines(server_id, bl_id, bl_name, keep_until)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.bl_id,
          dr.bl_name,
          dr.keep_until
        FROM json_to_record(datarow.row_data) AS dr(
            server_id    integer,
            bl_id        integer,
            bl_name      character varying(25),
            keep_until   timestamp (0) with time zone
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_settings' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_settings(server_id,first_seen,setting_scope,name,setting,
          reset_val,boot_val,unit,sourcefile,sourceline,pending_restart)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.first_seen,
          dr.setting_scope,
          dr.name,
          dr.setting,
          dr.reset_val,
          dr.boot_val,
          dr.unit,
          dr.sourcefile,
          dr.sourceline,
          dr.pending_restart
        FROM json_to_record(datarow.row_data) AS dr(
            server_id        integer,
            first_seen       timestamp(0) with time zone,
            setting_scope    smallint,
            name             text,
            setting          text,
            reset_val        text,
            boot_val         text,
            unit             text,
            sourcefile       text,
            sourceline       integer,
            pending_restart  boolean
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ON CONFLICT ON CONSTRAINT pk_sample_settings DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'bl_samples' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO bl_samples(server_id, sample_id, bl_id)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.bl_id
        FROM json_to_record(datarow.row_data) AS dr(
            server_id       integer,
            sample_id       integer,
            bl_id           integer
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_cluster' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_cluster(server_id,sample_id,checkpoints_timed,
          checkpoints_req,checkpoints_done,checkpoint_write_time,checkpoint_sync_time,buffers_checkpoint,
          slru_checkpoint,buffers_clean,maxwritten_clean,buffers_backend,buffers_backend_fsync,
          buffers_alloc,stats_reset,wal_size,wal_lsn,in_recovery)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.checkpoints_timed,
          dr.checkpoints_req,
          dr.checkpoints_done,
          dr.checkpoint_write_time,
          dr.checkpoint_sync_time,
          dr.buffers_checkpoint,
          dr.slru_checkpoint,
          dr.buffers_clean,
          dr.maxwritten_clean,
          dr.buffers_backend,
          dr.buffers_backend_fsync,
          dr.buffers_alloc,
          dr.stats_reset,
          dr.wal_size,
          dr.wal_lsn,
          dr.in_recovery
        FROM json_to_record(datarow.row_data) AS dr(
            server_id              integer,
            sample_id              integer,
            checkpoints_timed      bigint,
            checkpoints_req        bigint,
            checkpoints_done       bigint,
            checkpoint_write_time  double precision,
            checkpoint_sync_time   double precision,
            buffers_checkpoint     bigint,
            slru_checkpoint        bigint,
            buffers_clean          bigint,
            maxwritten_clean       bigint,
            buffers_backend        bigint,
            buffers_backend_fsync  bigint,
            buffers_alloc          bigint,
            stats_reset            timestamp with time zone,
            wal_size               bigint,
            wal_lsn                pg_lsn,
            in_recovery            boolean
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_cluster DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_database' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_database(server_id, sample_id, datid,datname,
          xact_commit, xact_rollback, blks_read, blks_hit, tup_returned, tup_fetched,
          tup_inserted, tup_updated, tup_deleted, conflicts, temp_files, temp_bytes,
          deadlocks, checksum_failures, checksum_last_failure, blk_read_time,
          blk_write_time, stats_reset, datsize,
          datsize_delta, datistemplate, session_time, active_time,
          idle_in_transaction_time, sessions, sessions_abandoned, sessions_fatal,
          sessions_killed, parallel_workers_to_launch, parallel_workers_launched)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.datname,
          dr.xact_commit,
          dr.xact_rollback,
          dr.blks_read,
          dr.blks_hit,
          dr.tup_returned,
          dr.tup_fetched,
          dr.tup_inserted,
          dr.tup_updated,
          dr.tup_deleted,
          dr.conflicts,
          dr.temp_files,
          dr.temp_bytes,
          dr.deadlocks,
          dr.checksum_failures,
          dr.checksum_last_failure,
          dr.blk_read_time,
          dr.blk_write_time,
          dr.stats_reset,
          dr.datsize,
          dr.datsize_delta,
          dr.datistemplate,
          dr.session_time,
          dr.active_time,
          dr.idle_in_transaction_time,
          dr.sessions,
          dr.sessions_abandoned,
          dr.sessions_fatal,
          dr.sessions_killed,
          dr.parallel_workers_to_launch,
          dr.parallel_workers_launched
        FROM json_to_record(datarow.row_data) AS dr(
          server_id       integer,
          sample_id       integer,
          datid           oid,
          datname         name,
          xact_commit     bigint,
          xact_rollback   bigint,
          blks_read       bigint,
          blks_hit        bigint,
          tup_returned    bigint,
          tup_fetched     bigint,
          tup_inserted    bigint,
          tup_updated     bigint,
          tup_deleted     bigint,
          conflicts       bigint,
          temp_files      bigint,
          temp_bytes      bigint,
          deadlocks       bigint,
          blk_read_time   double precision,
          blk_write_time  double precision,
          stats_reset     timestamp with time zone,
          datsize         bigint,
          datsize_delta   bigint,
          datistemplate   boolean,
          session_time    double precision,
          active_time     double precision,
          idle_in_transaction_time  double precision,
          sessions        bigint,
          sessions_abandoned  bigint,
          sessions_fatal      bigint,
          sessions_killed     bigint,
          parallel_workers_to_launch  bigint,
          parallel_workers_launched   bigint,
          checksum_failures   bigint,
          checksum_last_failure timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_database DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_wal' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_wal(server_id,sample_id,wal_records,
          wal_fpi,wal_bytes,wal_buffers_full,wal_write,wal_sync,
          wal_write_time,wal_sync_time,stats_reset)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.wal_records,
          dr.wal_fpi,
          dr.wal_bytes,
          dr.wal_buffers_full,
          dr.wal_write,
          dr.wal_sync,
          dr.wal_write_time,
          dr.wal_sync_time,
          dr.stats_reset
        FROM json_to_record(datarow.row_data) AS dr(
            server_id           integer,
            sample_id           integer,
            wal_records         bigint,
            wal_fpi             bigint,
            wal_bytes           numeric,
            wal_buffers_full    bigint,
            wal_write           bigint,
            wal_sync            bigint,
            wal_write_time      double precision,
            wal_sync_time       double precision,
            stats_reset         timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_wal DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_archiver' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_archiver(server_id,sample_id,archived_count,last_archived_wal,
          last_archived_time,failed_count,last_failed_wal,last_failed_time,
          stats_reset)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.archived_count,
          dr.last_archived_wal,
          dr.last_archived_time,
          dr.failed_count,
          dr.last_failed_wal,
          dr.last_failed_time,
          dr.stats_reset
        FROM json_to_record(datarow.row_data) AS dr(
            server_id           integer,
            sample_id           integer,
            archived_count      bigint,
            last_archived_wal   text,
            last_archived_time  timestamp with time zone,
            failed_count        bigint,
            last_failed_wal     text,
            last_failed_time    timestamp with time zone,
            stats_reset         timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_archiver DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_io' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_io(server_id,sample_id,backend_type,object,context,reads,
          read_bytes,read_time,writes,write_bytes,write_time,writebacks,writeback_time,extends,
          extend_bytes,extend_time,op_bytes,hits,evictions,reuses,fsyncs,fsync_time,stats_reset)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.backend_type,
          dr.object,
          dr.context,
          dr.reads,
          dr.read_bytes,
          dr.read_time,
          dr.writes,
          dr.write_bytes,
          dr.write_time,
          dr.writebacks,
          dr.writeback_time,
          dr.extends,
          dr.extend_bytes,
          dr.extend_time,
          dr.op_bytes,
          dr.hits,
          dr.evictions,
          dr.reuses,
          dr.fsyncs,
          dr.fsync_time,
          dr.stats_reset
        FROM json_to_record(datarow.row_data) AS dr(
            server_id                   integer,
            sample_id                   integer,
            backend_type                text,
            object                      text,
            context                     text,
            reads                       bigint,
            read_bytes                  numeric,
            read_time                   double precision,
            writes                      bigint,
            write_bytes                 numeric,
            write_time                  double precision,
            writebacks                  bigint,
            writeback_time              double precision,
            extends                     bigint,
            extend_bytes                numeric,
            extend_time                 double precision,
            op_bytes                    bigint,
            hits                        bigint,
            evictions                   bigint,
            reuses                      bigint,
            fsyncs                      bigint,
            fsync_time                  double precision,
            stats_reset                 timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_io DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_slru' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_slru(server_id,sample_id,name,blks_zeroed,
          blks_hit,blks_read,blks_written,blks_exists,flushes,truncates,
          stats_reset)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.name,
          dr.blks_zeroed,
          dr.blks_hit,
          dr.blks_read,
          dr.blks_written,
          dr.blks_exists,
          dr.flushes,
          dr.truncates,
          dr.stats_reset
        FROM json_to_record(datarow.row_data) AS dr(
            server_id      integer,
            sample_id      integer,
            name           text,
            blks_zeroed    bigint,
            blks_hit       bigint,
            blks_read      bigint,
            blks_written   bigint,
            blks_exists    bigint,
            flushes        bigint,
            truncates      bigint,
            stats_reset    timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_slru DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'roles_list' THEN
      CASE
        WHEN array_position(versions_array, '0.3.5:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO roles_list(server_id,last_sample_id,userid,username)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              s.sample_id,
              dr.userid,
              dr.username
            FROM json_to_record(datarow.row_data) AS dr(
                server_id      integer,
                last_sample_id integer,
                userid         oid,
                username       name
              ) LEFT JOIN samples s ON (s.server_id, s.sample_id) =
                ((srv_map ->> dr.server_id::text)::integer, dr.last_sample_id)
            ON CONFLICT ON CONSTRAINT pk_roles_list DO
              UPDATE SET (last_sample_id, username) =
                (EXCLUDED.last_sample_id, EXCLUDED.username)
              WHERE (roles_list.last_sample_id, roles_list.username) IS DISTINCT FROM
                (EXCLUDED.last_sample_id, EXCLUDED.username);
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
          END LOOP; -- over data rows
        ELSE
          RAISE '%', format('[import %s]: Unsupported extension version: %s %s',
            imp_table_name, import_meta ->> 'extension', import_meta ->> 'version');
      END CASE; -- over import versions
    WHEN 'tables_list' THEN
      section_meta := '{}'::jsonb;
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        /* In case of old import (before 4.5) we should collect relid - reltoastrelid
        * binding here. We'll need it in further insert into sample_stat_tables
        */
        IF datarow.row_data #> ARRAY['reltoastrelid'] IS NOT NULL THEN
          section_meta := jsonb_set(section_meta,
            ARRAY[format('%s.%s',
              datarow.row_data #>> ARRAY['server_id'],
              datarow.row_data #>> ARRAY['relid'])
            ],
            jsonb_build_object('reltoastrelid', datarow.row_data #> ARRAY['reltoastrelid'])
          );
        END IF;
        INSERT INTO tables_list(server_id,last_sample_id,datid,relid,relkind,
          schemaname,relname)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          s.sample_id,
          dr.datid,
          dr.relid,
          dr.relkind,
          dr.schemaname,
          dr.relname
        FROM json_to_record(datarow.row_data) AS dr(
            server_id      integer,
            last_sample_id integer,
            datid          oid,
            relid          oid,
            relkind        character(1),
            schemaname     name,
            relname        name
          ) LEFT JOIN samples s ON (s.server_id, s.sample_id) =
            ((srv_map ->> dr.server_id::text)::integer, dr.last_sample_id)
          ON CONFLICT ON CONSTRAINT pk_tables_list DO
          UPDATE SET (last_sample_id, schemaname, relname) =
            (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.relname)
          WHERE (tables_list.last_sample_id, tables_list.schemaname, tables_list.relname)
            IS DISTINCT FROM
            (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.relname);
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
      -- Return collected relations mapping
      IF section_meta != '{}'::jsonb THEN
        new_import_meta := jsonb_set(new_import_meta, '{relations_map}', section_meta);
      END IF;
    WHEN 'stmt_list' THEN
      section_meta := '{}'::jsonb;
      CASE
        WHEN array_position(versions_array, '0.3.2:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO stmt_list(server_id, last_sample_id, queryid_md5, query)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              s.sample_id,
              dr.queryid_md5,
              dr.query
            FROM json_to_record(datarow.row_data) AS dr (
                server_id      integer,
                last_sample_id integer,
                queryid_md5    character(32),
                query          text
              )
              LEFT JOIN samples s ON (s.server_id, s.sample_id) =
                ((srv_map ->> dr.server_id::text)::integer, dr.last_sample_id)
            ON CONFLICT ON CONSTRAINT pk_stmt_list
              DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
        WHEN array_position(versions_array, '0.3.1:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            IF section_meta #> ARRAY[datarow.row_data->>'queryid_md5'] IS NULL THEN
              section_meta := jsonb_set(section_meta,
                ARRAY[datarow.row_data->>'queryid_md5'], '{}'::jsonb);
            END IF;
            section_meta := jsonb_set(section_meta,
              ARRAY[datarow.row_data->>'queryid_md5', datarow.row_data->>'server_id'],
              to_jsonb(left(encode(sha224(convert_to(
                datarow.row_data->>'query',
                'UTF8')
              ), 'base64'), 32))
            );
            INSERT INTO stmt_list(server_id, last_sample_id, queryid_md5, query)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              NULL,
              left(encode(sha224(convert_to(dr.query ,'UTF8')), 'base64'), 32),
              dr.query
            FROM json_to_record(datarow.row_data) AS dr (
                server_id      integer,
                query          text
              )
            ON CONFLICT ON CONSTRAINT pk_stmt_list
              DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
          -- Return queryid mapping
          new_import_meta := jsonb_set(new_import_meta, '{queryid_map}', section_meta);
        ELSE
          RAISE '%', format('[import %s]: Unsupported extension version: %s %s',
            imp_table_name, import_meta ->> 'extension', import_meta ->> 'version');
      END CASE; -- over import versions
    WHEN 'funcs_list' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO funcs_list(server_id, last_sample_id,datid,funcid,schemaname,
          funcname,funcargs)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          s.sample_id,
          dr.datid,
          dr.funcid,
          dr.schemaname,
          dr.funcname,
          dr.funcargs
        FROM json_to_record(datarow.row_data) AS dr(
            server_id      integer,
            last_sample_id integer,
            datid          oid,
            funcid         oid,
            schemaname     name,
            funcname       name,
            funcargs       text
          )
          LEFT JOIN samples s ON (s.server_id, s.sample_id) =
            ((srv_map ->> dr.server_id::text)::integer, dr.last_sample_id)
        ON CONFLICT ON CONSTRAINT pk_funcs_list DO
        UPDATE SET (last_sample_id, schemaname, funcname, funcargs) =
          (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.funcname, EXCLUDED.funcargs)
        WHERE (funcs_list.last_sample_id, funcs_list.schemaname, funcs_list.funcname,
          funcs_list.funcargs) IS DISTINCT FROM
          (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.funcname, EXCLUDED.funcargs);
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_timings' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_timings(server_id, sample_id, event, time_spent)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.event,
          dr.time_spent
        FROM json_to_record(datarow.row_data) AS dr(
            server_id      integer,
            sample_id      integer,
            event          text,
            time_spent     interval minute to second(2)
          )
         JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_timings
          DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'tablespaces_list' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO tablespaces_list(server_id, last_sample_id,
          tablespaceid, tablespacename, tablespacepath)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          s.sample_id,
          dr.tablespaceid,
          dr.tablespacename,
          dr.tablespacepath
        FROM json_to_record(datarow.row_data) AS dr(
            server_id      integer,
            last_sample_id integer,
            tablespaceid   oid,
            tablespacename name,
            tablespacepath text
          )
          LEFT JOIN samples s ON (s.server_id, s.sample_id) =
            ((srv_map ->> dr.server_id::text)::integer, dr.last_sample_id)
        ON CONFLICT ON CONSTRAINT pk_tablespace_list DO
        UPDATE SET (last_sample_id, tablespacename, tablespacepath) =
          (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath)
        WHERE (tablespaces_list.last_sample_id, tablespaces_list.tablespacename,
            tablespaces_list.tablespacepath) IS DISTINCT FROM
          (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath);
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_tablespaces' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_tablespaces(server_id, sample_id,
          tablespaceid, size, size_delta)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.tablespaceid,
          dr.size,
          dr.size_delta
        FROM json_to_record(datarow.row_data) AS dr(
            server_id     integer,
            sample_id     integer,
            tablespaceid  oid,
            size          bigint,
            size_delta    bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_tablespaces
          DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'wait_sampling_total' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO wait_sampling_total(server_id, sample_id, sample_wevnt_id,
          event_type, event, tot_waited, stmt_waited)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.sample_wevnt_id,
          dr.event_type,
          dr.event,
          dr.tot_waited,
          dr.stmt_waited
        FROM json_to_record(datarow.row_data) AS dr(
            server_id           integer,
            sample_id           integer,
            sample_wevnt_id     integer,
            event_type          text,
            event               text,
            tot_waited          bigint,
            stmt_waited         bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_weid
          DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'indexes_list' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO indexes_list(server_id, last_sample_id, datid, indexrelid, relid,
          schemaname, indexrelname)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          s.sample_id,
          dr.datid,
          dr.indexrelid,
          dr.relid,
          dr.schemaname,
          dr.indexrelname
        FROM json_to_record(datarow.row_data) AS dr(
            server_id      integer,
            last_sample_id integer,
            datid          oid,
            indexrelid     oid,
            relid          oid,
            schemaname     name,
            indexrelname   name
          )
          LEFT JOIN samples s ON (s.server_id, s.sample_id) =
            ((srv_map ->> dr.server_id::text)::integer, dr.last_sample_id)
        ON CONFLICT ON CONSTRAINT pk_indexes_list DO
        UPDATE SET (last_sample_id, schemaname, indexrelname) =
          (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.indexrelname)
        WHERE (indexes_list.last_sample_id, indexes_list.schemaname, indexes_list.indexrelname)
          IS DISTINCT FROM
          (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.indexrelname);
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_statements' THEN
      CASE -- Import version selector
        WHEN array_position(versions_array, '4.7:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO sample_statements(server_id, sample_id, userid, datid, queryid, queryid_md5,
              plans, total_plan_time, min_plan_time, max_plan_time, mean_plan_time,
              sum_plan_time_sq, calls, total_exec_time, min_exec_time, max_exec_time,
              mean_exec_time, sum_exec_time_sq, rows, shared_blks_hit, shared_blks_read,
              shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
              local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
              shared_blk_read_time, shared_blk_write_time, wal_records, wal_fpi, wal_bytes,
              wal_buffers_full, toplevel , jit_functions, jit_generation_time, jit_inlining_count,
              jit_inlining_time, jit_optimization_count, jit_optimization_time,
              jit_emission_count, jit_emission_time, temp_blk_read_time, temp_blk_write_time,
              local_blk_read_time, local_blk_write_time, jit_deform_count, jit_deform_time,
              parallel_workers_to_launch, parallel_workers_launched, stats_since, minmax_stats_since)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.userid,
              dr.datid,
              dr.queryid,
              dr.queryid_md5,
              dr.plans,
              dr.total_plan_time,
              dr.min_plan_time,
              dr.max_plan_time,
              dr.mean_plan_time,
              dr.sum_plan_time_sq,
              dr.calls,
              dr.total_exec_time,
              dr.min_exec_time,
              dr.max_exec_time,
              dr.mean_exec_time,
              dr.sum_exec_time_sq,
              dr.rows,
              dr.shared_blks_hit,
              dr.shared_blks_read,
              dr.shared_blks_dirtied,
              dr.shared_blks_written,
              dr.local_blks_hit,
              dr.local_blks_read,
              dr.local_blks_dirtied,
              dr.local_blks_written,
              dr.temp_blks_read,
              dr.temp_blks_written,
              dr.shared_blk_read_time,
              dr.shared_blk_write_time,
              dr.wal_records,
              dr.wal_fpi,
              dr.wal_bytes,
              dr.wal_buffers_full,
              COALESCE(dr.toplevel, true),
              dr.jit_functions,
              dr.jit_generation_time,
              dr.jit_inlining_count,
              dr.jit_inlining_time,
              dr.jit_optimization_count,
              dr.jit_optimization_time,
              dr.jit_emission_count,
              dr.jit_emission_time,
              dr.temp_blk_read_time,
              dr.temp_blk_write_time,
              dr.local_blk_read_time,
              dr.local_blk_write_time,
              dr.jit_deform_count,
              dr.jit_deform_time,
              dr.parallel_workers_to_launch,
              dr.parallel_workers_launched,
              dr.stats_since,
              dr.minmax_stats_since
            FROM json_to_record(datarow.row_data) AS dr(
              server_id            integer,
              sample_id            integer,
              userid               oid,
              datid                oid,
              queryid              bigint,
              queryid_md5          character(32),
              plans                bigint,
              total_plan_time      double precision,
              min_plan_time        double precision,
              max_plan_time        double precision,
              mean_plan_time       double precision,
              sum_plan_time_sq     numeric,
              calls                bigint,
              total_exec_time      double precision,
              min_exec_time        double precision,
              max_exec_time        double precision,
              mean_exec_time       double precision,
              sum_exec_time_sq     numeric,
              rows                 bigint,
              shared_blks_hit      bigint,
              shared_blks_read     bigint,
              shared_blks_dirtied  bigint,
              shared_blks_written  bigint,
              local_blks_hit       bigint,
              local_blks_read      bigint,
              local_blks_dirtied   bigint,
              local_blks_written   bigint,
              temp_blks_read       bigint,
              temp_blks_written    bigint,
              shared_blk_read_time  double precision,
              shared_blk_write_time double precision,
              wal_records          bigint,
              wal_fpi              bigint,
              wal_bytes            numeric,
              wal_buffers_full     bigint,
              toplevel             boolean,
              jit_functions        bigint,
              jit_generation_time  double precision,
              jit_inlining_count   bigint,
              jit_inlining_time    double precision,
              jit_optimization_count  bigint,
              jit_optimization_time   double precision,
              jit_emission_count   bigint,
              jit_emission_time    double precision,
              temp_blk_read_time   double precision,
              temp_blk_write_time  double precision,
              local_blk_read_time  double precision,
              local_blk_write_time double precision,
              jit_deform_count     bigint,
              jit_deform_time      double precision,
              parallel_workers_to_launch  bigint,
              parallel_workers_launched   bigint,
              stats_since          timestamp with time zone,
              minmax_stats_since   timestamp with time zone
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT ON CONSTRAINT pk_sample_statements_n DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
        WHEN array_position(versions_array, '4.0:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO sample_statements(server_id, sample_id, userid, datid, queryid, queryid_md5,
              plans, total_plan_time, min_plan_time, max_plan_time, mean_plan_time,
              sum_plan_time_sq, calls, total_exec_time, min_exec_time, max_exec_time,
              mean_exec_time, sum_exec_time_sq, rows, shared_blks_hit, shared_blks_read,
              shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
              local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
              shared_blk_read_time, shared_blk_write_time, wal_records, wal_fpi, wal_bytes,
              toplevel , jit_functions, jit_generation_time, jit_inlining_count,
              jit_inlining_time, jit_optimization_count, jit_optimization_time,
              jit_emission_count, jit_emission_time, temp_blk_read_time, temp_blk_write_time)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.userid,
              dr.datid,
              dr.queryid,
              dr.queryid_md5,
              dr.plans,
              dr.total_plan_time,
              dr.min_plan_time,
              dr.max_plan_time,
              dr.mean_plan_time,
              CASE
                WHEN dr.plans = 0 THEN 0
                WHEN dr.plans = 1 THEN
                  pow(dr.total_plan_time::numeric, 2)
                ELSE
                  pow(dr.stddev_plan_time::numeric, 2) * dr.plans +
                  pow(dr.mean_plan_time::numeric, 2) * dr.plans
              END AS sum_plan_time_sq,
              dr.calls,
              dr.total_exec_time,
              dr.min_exec_time,
              dr.max_exec_time,
              dr.mean_exec_time,
              CASE
                WHEN dr.calls = 0 THEN 0
                WHEN dr.calls = 1 THEN
                  pow(dr.total_exec_time::numeric, 2)
                ELSE
                  pow(dr.stddev_exec_time::numeric, 2) * dr.calls +
                  pow(dr.mean_exec_time::numeric, 2) * dr.calls
              END AS sum_exec_time_sq,
              dr.rows,
              dr.shared_blks_hit,
              dr.shared_blks_read,
              dr.shared_blks_dirtied,
              dr.shared_blks_written,
              dr.local_blks_hit,
              dr.local_blks_read,
              dr.local_blks_dirtied,
              dr.local_blks_written,
              dr.temp_blks_read,
              dr.temp_blks_written,
              dr.blk_read_time as shared_blk_read_time,
              dr.blk_write_time as shared_blk_write_time,
              dr.wal_records,
              dr.wal_fpi,
              dr.wal_bytes,
              COALESCE(dr.toplevel, true),
              dr.jit_functions,
              dr.jit_generation_time,
              dr.jit_inlining_count,
              dr.jit_inlining_time,
              dr.jit_optimization_count,
              dr.jit_optimization_time,
              dr.jit_emission_count,
              dr.jit_emission_time,
              dr.temp_blk_read_time,
              dr.temp_blk_write_time
            FROM json_to_record(datarow.row_data) AS dr(
              server_id            integer,
              sample_id            integer,
              userid               oid,
              datid                oid,
              queryid              bigint,
              queryid_md5          character(32),
              plans                bigint,
              total_plan_time      double precision,
              min_plan_time        double precision,
              max_plan_time        double precision,
              mean_plan_time       double precision,
              stddev_plan_time     double precision,
              calls                bigint,
              total_exec_time      double precision,
              min_exec_time        double precision,
              max_exec_time        double precision,
              mean_exec_time       double precision,
              stddev_exec_time     double precision,
              rows                 bigint,
              shared_blks_hit      bigint,
              shared_blks_read     bigint,
              shared_blks_dirtied  bigint,
              shared_blks_written  bigint,
              local_blks_hit       bigint,
              local_blks_read      bigint,
              local_blks_dirtied   bigint,
              local_blks_written   bigint,
              temp_blks_read       bigint,
              temp_blks_written    bigint,
              blk_read_time        double precision,
              blk_write_time       double precision,
              wal_records          bigint,
              wal_fpi              bigint,
              wal_bytes            numeric,
              toplevel             boolean,
              jit_functions        bigint,
              jit_generation_time  double precision,
              jit_inlining_count   bigint,
              jit_inlining_time    double precision,
              jit_optimization_count  bigint,
              jit_optimization_time   double precision,
              jit_emission_count   bigint,
              jit_emission_time    double precision,
              temp_blk_read_time   double precision,
              temp_blk_write_time  double precision
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT ON CONSTRAINT pk_sample_statements_n DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
        WHEN array_position(versions_array, '0.3.1:pg_profile') IS NOT NULL THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;

            INSERT INTO roles_list (server_id,userid,username)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.userid,
              '_unknown_'
            FROM json_to_record(datarow.row_data) AS dr(
                  server_id            integer,
                  userid               oid
              )
              JOIN
                servers s_ctl ON
                  ((srv_map ->> dr.server_id::text)::integer) =
                  (s_ctl.server_id)
            ON CONFLICT ON CONSTRAINT pk_roles_list DO
              UPDATE SET (last_sample_id, username) =
                (EXCLUDED.last_sample_id, EXCLUDED.username);

            INSERT INTO sample_statements(server_id, sample_id, userid, datid, queryid, queryid_md5,
              plans, total_plan_time, min_plan_time, max_plan_time, mean_plan_time,
              sum_plan_time_sq, calls, total_exec_time, min_exec_time, max_exec_time,
              mean_exec_time, sum_exec_time_sq, rows, shared_blks_hit, shared_blks_read,
              shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
              local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
              shared_blk_read_time, shared_blk_write_time, wal_records, wal_fpi, wal_bytes,
              toplevel , jit_functions, jit_generation_time, jit_inlining_count,
              jit_inlining_time, jit_optimization_count, jit_optimization_time,
              jit_emission_count, jit_emission_time, temp_blk_read_time, temp_blk_write_time)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.userid,
              dr.datid,
              dr.queryid,
              CASE WHEN starts_with(versions_array[1], '0.3.1:') THEN
                import_meta #>> ARRAY['queryid_map', dr.queryid_md5, dr.server_id::text]
              ELSE
                dr.queryid_md5
              END,
              dr.plans,
              dr.total_plan_time,
              dr.min_plan_time,
              dr.max_plan_time,
              dr.mean_plan_time,
              CASE
                WHEN dr.plans = 0 THEN 0
                WHEN dr.plans = 1 THEN
                  pow(dr.total_plan_time::numeric, 2)
                ELSE
                  pow(dr.stddev_plan_time::numeric, 2) * dr.plans +
                  pow(dr.mean_plan_time::numeric, 2) * dr.plans
              END AS sum_plan_time_sq,
              dr.calls,
              dr.total_exec_time,
              dr.min_exec_time,
              dr.max_exec_time,
              dr.mean_exec_time,
              CASE
                WHEN dr.calls = 0 THEN 0
                WHEN dr.calls = 1 THEN
                  pow(dr.total_exec_time::numeric, 2)
                ELSE
                  pow(dr.stddev_exec_time::numeric, 2) * dr.calls +
                  pow(dr.mean_exec_time::numeric, 2) * dr.calls
              END AS sum_exec_time_sq,
              dr.rows,
              dr.shared_blks_hit,
              dr.shared_blks_read,
              dr.shared_blks_dirtied,
              dr.shared_blks_written,
              dr.local_blks_hit,
              dr.local_blks_read,
              dr.local_blks_dirtied,
              dr.local_blks_written,
              dr.temp_blks_read,
              dr.temp_blks_written,
              dr.blk_read_time,
              dr.blk_write_time,
              dr.wal_records,
              dr.wal_fpi,
              dr.wal_bytes,
              COALESCE(dr.toplevel, true),
              dr.jit_functions,
              dr.jit_generation_time,
              dr.jit_inlining_count,
              dr.jit_inlining_time,
              dr.jit_optimization_count,
              dr.jit_optimization_time,
              dr.jit_emission_count,
              dr.jit_emission_time,
              dr.temp_blk_read_time,
              dr.temp_blk_write_time
            FROM json_to_record(datarow.row_data) AS dr(
              server_id            integer,
              sample_id            integer,
              userid               oid,
              datid                oid,
              queryid              bigint,
              queryid_md5          character(32),
              plans                bigint,
              total_plan_time      double precision,
              min_plan_time        double precision,
              max_plan_time        double precision,
              mean_plan_time       double precision,
              stddev_plan_time     double precision,
              calls                bigint,
              total_exec_time      double precision,
              min_exec_time        double precision,
              max_exec_time        double precision,
              mean_exec_time       double precision,
              stddev_exec_time     double precision,
              rows                 bigint,
              shared_blks_hit      bigint,
              shared_blks_read     bigint,
              shared_blks_dirtied  bigint,
              shared_blks_written  bigint,
              local_blks_hit       bigint,
              local_blks_read      bigint,
              local_blks_dirtied   bigint,
              local_blks_written   bigint,
              temp_blks_read       bigint,
              temp_blks_written    bigint,
              blk_read_time        double precision,
              blk_write_time       double precision,
              wal_records          bigint,
              wal_fpi              bigint,
              wal_bytes            numeric,
              toplevel             boolean,
              jit_functions        bigint,
              jit_generation_time  double precision,
              jit_inlining_count   bigint,
              jit_inlining_time    double precision,
              jit_optimization_count  bigint,
              jit_optimization_time   double precision,
              jit_emission_count   bigint,
              jit_emission_time    double precision,
              temp_blk_read_time   double precision,
              temp_blk_write_time  double precision
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT ON CONSTRAINT pk_sample_statements_n DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
        ELSE
          RAISE '%', format('[import %s]: Unsupported extension version: %s %s',
            imp_table_name, import_meta ->> 'extension', import_meta ->> 'version');
      END CASE; -- over import versions
    WHEN 'sample_statements_total' THEN
      CASE -- Import version selector
        WHEN array_position(versions_array, '4.7:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO sample_statements_total(server_id, sample_id, datid, plans, total_plan_time,
              calls, total_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
              shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied,
              local_blks_written, temp_blks_read, temp_blks_written, shared_blk_read_time,
              shared_blk_write_time, wal_records, wal_fpi, wal_bytes, wal_buffers_full, statements, jit_functions,
              jit_generation_time, jit_inlining_count, jit_inlining_time, jit_optimization_count,
              jit_optimization_time, jit_emission_count, jit_emission_time, temp_blk_read_time,
              temp_blk_write_time, mean_max_plan_time, mean_max_exec_time, mean_min_plan_time,
              mean_min_exec_time, local_blk_read_time, local_blk_write_time, jit_deform_count,
              jit_deform_time)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.datid,
              dr.plans,
              dr.total_plan_time,
              dr.calls,
              dr.total_exec_time,
              dr.rows,
              dr.shared_blks_hit,
              dr.shared_blks_read,
              dr.shared_blks_dirtied,
              dr.shared_blks_written,
              dr.local_blks_hit,
              dr.local_blks_read,
              dr.local_blks_dirtied,
              dr.local_blks_written,
              dr.temp_blks_read,
              dr.temp_blks_written,
              dr.shared_blk_read_time,
              dr.shared_blk_write_time,
              dr.wal_records,
              dr.wal_fpi,
              dr.wal_bytes,
              dr.wal_buffers_full,
              dr.statements,
              dr.jit_functions,
              dr.jit_generation_time,
              dr.jit_inlining_count,
              dr.jit_inlining_time,
              dr.jit_optimization_count,
              dr.jit_optimization_time,
              dr.jit_emission_count,
              dr.jit_emission_time,
              dr.temp_blk_read_time,
              dr.temp_blk_write_time,
              dr.mean_max_plan_time,
              dr.mean_max_exec_time,
              dr.mean_min_plan_time,
              dr.mean_min_exec_time,
              dr.local_blk_read_time,
              dr.local_blk_write_time,
              dr.jit_deform_count,
              dr.jit_deform_time
            FROM json_to_record(datarow.row_data) AS dr(
                server_id            integer,
                sample_id            integer,
                datid                oid,
                plans                bigint,
                total_plan_time      double precision,
                calls                bigint,
                total_exec_time      double precision,
                rows                 bigint,
                shared_blks_hit      bigint,
                shared_blks_read     bigint,
                shared_blks_dirtied  bigint,
                shared_blks_written  bigint,
                local_blks_hit       bigint,
                local_blks_read      bigint,
                local_blks_dirtied   bigint,
                local_blks_written   bigint,
                temp_blks_read       bigint,
                temp_blks_written    bigint,
                shared_blk_read_time  double precision,
                shared_blk_write_time double precision,
                wal_records          bigint,
                wal_fpi              bigint,
                wal_bytes            numeric,
                wal_buffers_full     bigint,
                statements           bigint,
                jit_functions        bigint,
                jit_generation_time  double precision,
                jit_inlining_count   bigint,
                jit_inlining_time    double precision,
                jit_optimization_count  bigint,
                jit_optimization_time   double precision,
                jit_emission_count   bigint,
                jit_emission_time    double precision,
                temp_blk_read_time   double precision,
                temp_blk_write_time  double precision,
                mean_max_plan_time  double precision,
                mean_max_exec_time  double precision,
                mean_min_plan_time  double precision,
                mean_min_exec_time  double precision,
                local_blk_read_time double precision,
                local_blk_write_time  double precision,
                jit_deform_count    bigint,
                jit_deform_time     double precision
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT ON CONSTRAINT pk_sample_statements_total DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
        WHEN array_position(versions_array, '0.3.1:pg_profile') IS NOT NULL THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO sample_statements_total(server_id, sample_id, datid, plans, total_plan_time,
              calls, total_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied,
              shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied,
              local_blks_written, temp_blks_read, temp_blks_written, shared_blk_read_time,
              shared_blk_write_time, wal_records, wal_fpi, wal_bytes, wal_buffers_full, statements, jit_functions,
              jit_generation_time, jit_inlining_count, jit_inlining_time, jit_optimization_count,
              jit_optimization_time, jit_emission_count, jit_emission_time, temp_blk_read_time,
              temp_blk_write_time, mean_max_plan_time, mean_max_exec_time, mean_min_plan_time,
              mean_min_exec_time)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.datid,
              dr.plans,
              dr.total_plan_time,
              dr.calls,
              dr.total_exec_time,
              dr.rows,
              dr.shared_blks_hit,
              dr.shared_blks_read,
              dr.shared_blks_dirtied,
              dr.shared_blks_written,
              dr.local_blks_hit,
              dr.local_blks_read,
              dr.local_blks_dirtied,
              dr.local_blks_written,
              dr.temp_blks_read,
              dr.temp_blks_written,
              dr.blk_read_time as shared_blk_read_time,
              dr.blk_write_time as shared_blk_write_time,
              dr.wal_records,
              dr.wal_fpi,
              dr.wal_bytes,
              dr.wal_buffers_full,
              dr.statements,
              dr.jit_functions,
              dr.jit_generation_time,
              dr.jit_inlining_count,
              dr.jit_inlining_time,
              dr.jit_optimization_count,
              dr.jit_optimization_time,
              dr.jit_emission_count,
              dr.jit_emission_time,
              dr.temp_blk_read_time,
              dr.temp_blk_write_time,
              dr.mean_max_plan_time,
              dr.mean_max_exec_time,
              dr.mean_min_plan_time,
              dr.mean_min_exec_time
            FROM json_to_record(datarow.row_data) AS dr(
                server_id            integer,
                sample_id            integer,
                datid                oid,
                plans                bigint,
                total_plan_time      double precision,
                calls                bigint,
                total_exec_time      double precision,
                rows                 bigint,
                shared_blks_hit      bigint,
                shared_blks_read     bigint,
                shared_blks_dirtied  bigint,
                shared_blks_written  bigint,
                local_blks_hit       bigint,
                local_blks_read      bigint,
                local_blks_dirtied   bigint,
                local_blks_written   bigint,
                temp_blks_read       bigint,
                temp_blks_written    bigint,
                blk_read_time        double precision,
                blk_write_time       double precision,
                wal_records          bigint,
                wal_fpi              bigint,
                wal_bytes            numeric,
                wal_buffers_full     bigint,
                statements           bigint,
                jit_functions        bigint,
                jit_generation_time  double precision,
                jit_inlining_count   bigint,
                jit_inlining_time    double precision,
                jit_optimization_count  bigint,
                jit_optimization_time   double precision,
                jit_emission_count   bigint,
                jit_emission_time    double precision,
                temp_blk_read_time   double precision,
                temp_blk_write_time  double precision,
                mean_max_plan_time  double precision,
                mean_max_exec_time  double precision,
                mean_min_plan_time  double precision,
                mean_min_exec_time  double precision
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT ON CONSTRAINT pk_sample_statements_total DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
        ELSE
          RAISE '%', format('[import %s]: Unsupported extension version: %s %s',
            imp_table_name, import_meta ->> 'extension', import_meta ->> 'version');
      END CASE; -- over import versions
    WHEN 'sample_kcache_total' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_kcache_total(server_id, sample_id, datid, plan_user_time,
          plan_system_time, plan_minflts, plan_majflts, plan_nswaps, plan_reads, plan_writes,
          plan_msgsnds, plan_msgrcvs, plan_nsignals, plan_nvcsws, plan_nivcsws,
          exec_user_time, exec_system_time, exec_minflts, exec_majflts, exec_nswaps,
          exec_reads, exec_writes, exec_msgsnds, exec_msgrcvs, exec_nsignals, exec_nvcsws,
          exec_nivcsws, statements)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.plan_user_time,
          dr.plan_system_time,
          dr.plan_minflts,
          dr.plan_majflts,
          dr.plan_nswaps,
          dr.plan_reads,
          dr.plan_writes,
          dr.plan_msgsnds,
          dr.plan_msgrcvs,
          dr.plan_nsignals,
          dr.plan_nvcsws,
          dr.plan_nivcsws,
          dr.exec_user_time,
          dr.exec_system_time,
          dr.exec_minflts,
          dr.exec_majflts,
          dr.exec_nswaps,
          dr.exec_reads,
          dr.exec_writes,
          dr.exec_msgsnds,
          dr.exec_msgrcvs,
          dr.exec_nsignals,
          dr.exec_nvcsws,
          dr.exec_nivcsws,
          dr.statements
        FROM json_to_record(datarow.row_data) AS dr(
            server_id         integer,
            sample_id         integer,
            datid             oid,
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
            exec_nivcsws      bigint,
            statements        bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_kcache_total DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_user_functions' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_user_functions(server_id, sample_id, datid, funcid,
          calls, total_time, self_time, trg_fn)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.funcid,
          dr.calls,
          dr.total_time,
          dr.self_time,
          dr.trg_fn
        FROM json_to_record(datarow.row_data) AS dr(
            server_id   integer,
            sample_id   integer,
            datid       oid,
            funcid      oid,
            calls       bigint,
            total_time  double precision,
            self_time   double precision,
            trg_fn      boolean
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_user_functions DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_user_func_total' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_user_func_total(server_id, sample_id, datid, calls,
          total_time, trg_fn)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.calls,
          dr.total_time,
          dr.trg_fn
        FROM json_to_record(datarow.row_data) AS dr(
            server_id   integer,
            sample_id   integer,
            datid       oid,
            calls       bigint,
            total_time  double precision,
            trg_fn      boolean
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_user_func_total DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_tables' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_tables(server_id,sample_id,
          datid,relid,reltoastrelid,tablespaceid,seq_scan,
          seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,n_tup_hot_upd,
          n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,last_vacuum,
          last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,autovacuum_count,
          analyze_count,autoanalyze_count,total_vacuum_time,total_autovacuum_time,
          total_analyze_time,total_autoanalyze_time,heap_blks_read,heap_blks_hit,idx_blks_read,
          idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,tidx_blks_hit,
          relsize,relsize_diff,relpages_bytes,relpages_bytes_diff,last_seq_scan,
          last_idx_scan,n_tup_newpage_upd)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.relid,
          COALESCE(
            dr.reltoastrelid,
            (import_meta #>> ARRAY[
                'relations_map',
                format('%s.%s', dr.server_id::text, dr.relid::text),
                'reltoastrelid'
              ]
            )::oid
          ) AS reltoastrelid,
          dr.tablespaceid,
          dr.seq_scan,
          dr.seq_tup_read,
          dr.idx_scan,
          dr.idx_tup_fetch,
          dr.n_tup_ins,
          dr.n_tup_upd,
          dr.n_tup_del,
          dr.n_tup_hot_upd,
          dr.n_live_tup,
          dr.n_dead_tup,
          dr.n_mod_since_analyze,
          dr.n_ins_since_vacuum,
          dr.last_vacuum,
          dr.last_autovacuum,
          dr.last_analyze,
          dr.last_autoanalyze,
          dr.vacuum_count,
          dr.autovacuum_count,
          dr.analyze_count,
          dr.autoanalyze_count,
          dr.total_vacuum_time,
          dr.total_autovacuum_time,
          dr.total_analyze_time,
          dr.total_autoanalyze_time,
          dr.heap_blks_read,
          dr.heap_blks_hit,
          dr.idx_blks_read,
          dr.idx_blks_hit,
          dr.toast_blks_read,
          dr.toast_blks_hit,
          dr.tidx_blks_read,
          dr.tidx_blks_hit,
          dr.relsize,
          dr.relsize_diff,
          dr.relpages_bytes,
          dr.relpages_bytes_diff,
          dr.last_seq_scan,
          dr.last_idx_scan,
          dr.n_tup_newpage_upd
        FROM json_to_record(datarow.row_data) AS dr(
          server_id            integer,
          sample_id            integer,
          datid                oid,
          relid                oid,
          reltoastrelid        oid,
          tablespaceid         oid,
          seq_scan             bigint,
          seq_tup_read         bigint,
          idx_scan             bigint,
          idx_tup_fetch        bigint,
          n_tup_ins            bigint,
          n_tup_upd            bigint,
          n_tup_del            bigint,
          n_tup_hot_upd        bigint,
          n_live_tup           bigint,
          n_dead_tup           bigint,
          n_mod_since_analyze  bigint,
          n_ins_since_vacuum   bigint,
          last_vacuum          timestamp with time zone,
          last_autovacuum      timestamp with time zone,
          last_analyze         timestamp with time zone,
          last_autoanalyze     timestamp with time zone,
          vacuum_count         bigint,
          autovacuum_count     bigint,
          analyze_count        bigint,
          autoanalyze_count    bigint,
          total_vacuum_time       double precision,
          total_autovacuum_time   double precision,
          total_analyze_time      double precision,
          total_autoanalyze_time  double precision,
          heap_blks_read       bigint,
          heap_blks_hit        bigint,
          idx_blks_read        bigint,
          idx_blks_hit         bigint,
          toast_blks_read      bigint,
          toast_blks_hit       bigint,
          tidx_blks_read       bigint,
          tidx_blks_hit        bigint,
          relsize              bigint,
          relsize_diff         bigint,
          relpages_bytes       bigint,
          relpages_bytes_diff  bigint,
          last_seq_scan        timestamp with time zone,
          last_idx_scan        timestamp with time zone,
          n_tup_newpage_upd    bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_tables DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
      /*
       Before v 4.5 it was possible to collect a heap statistics without toast statistics
       and it didn't cause any conflicts. Now sample_stat_tables references itself for a
       toast statistics. Setting a reltoastrelid field from old tables_list will cause a
       constraint violation. So, we should remove toasts references from samples without
       toast statistics collected.
      */
      IF array_position(versions_array, '4.5:pg_profile') IS NULL THEN
        ANALYZE sample_stat_tables;
        RAISE NOTICE 'Cleanup possible missing toast statistics from sample_stat_tables ...';
        UPDATE sample_stat_tables usst
        SET reltoastrelid = NULL
        FROM sample_stat_tables h LEFT JOIN sample_stat_tables t ON
          (t.server_id, t.sample_id, t.datid, t.relid) =
          (h.server_id, h.sample_id, h.datid, h.reltoastrelid)
        WHERE (usst.server_id, usst.sample_id, usst.datid, usst.relid) =
          (h.server_id, h.sample_id, h.datid, h.relid)
          AND t.relid IS NULL;
      END IF;
    WHEN 'sample_stat_tables_total' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_tables_total(server_id,sample_id,datid,tablespaceid,relkind,
          seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,
          n_tup_hot_upd,vacuum_count,autovacuum_count,analyze_count,autoanalyze_count,
          total_vacuum_time,total_autovacuum_time,total_analyze_time,total_autoanalyze_time,
          heap_blks_read,heap_blks_hit,idx_blks_read,idx_blks_hit,toast_blks_read,
          toast_blks_hit,tidx_blks_read,tidx_blks_hit,relsize_diff,n_tup_newpage_upd)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.tablespaceid,
          dr.relkind,
          dr.seq_scan,
          dr.seq_tup_read,
          dr.idx_scan,
          dr.idx_tup_fetch,
          dr.n_tup_ins,
          dr.n_tup_upd,
          dr.n_tup_del,
          dr.n_tup_hot_upd,
          dr.vacuum_count,
          dr.autovacuum_count,
          dr.analyze_count,
          dr.total_vacuum_time,
          dr.total_autovacuum_time,
          dr.total_analyze_time,
          dr.total_autoanalyze_time,
          dr.autoanalyze_count,
          dr.heap_blks_read,
          dr.heap_blks_hit,
          dr.idx_blks_read,
          dr.idx_blks_hit,
          dr.toast_blks_read,
          dr.toast_blks_hit,
          dr.tidx_blks_read,
          dr.tidx_blks_hit,
          dr.relsize_diff,
          dr.n_tup_newpage_upd
        FROM json_to_record(datarow.row_data) AS dr(
          server_id          integer,
          sample_id          integer,
          datid              oid,
          tablespaceid       oid,
          relkind            character(1),
          seq_scan           bigint,
          seq_tup_read       bigint,
          idx_scan           bigint,
          idx_tup_fetch      bigint,
          n_tup_ins          bigint,
          n_tup_upd          bigint,
          n_tup_del          bigint,
          n_tup_hot_upd      bigint,
          vacuum_count       bigint,
          autovacuum_count   bigint,
          analyze_count      bigint,
          autoanalyze_count  bigint,
          total_vacuum_time       double precision,
          total_autovacuum_time   double precision,
          total_analyze_time      double precision,
          total_autoanalyze_time  double precision,
          heap_blks_read     bigint,
          heap_blks_hit      bigint,
          idx_blks_read      bigint,
          idx_blks_hit       bigint,
          toast_blks_read    bigint,
          toast_blks_hit     bigint,
          tidx_blks_read     bigint,
          tidx_blks_hit      bigint,
          relsize_diff       bigint,
          n_tup_newpage_upd  bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_tables_tot DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_indexes' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_indexes(server_id,sample_id,datid,indexrelid,tablespaceid,
          idx_scan,idx_tup_read,idx_tup_fetch,idx_blks_read,idx_blks_hit,relsize,
          relsize_diff,indisunique,relpages_bytes,relpages_bytes_diff,last_idx_scan)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.indexrelid,
          dr.tablespaceid,
          dr.idx_scan,
          dr.idx_tup_read,
          dr.idx_tup_fetch,
          dr.idx_blks_read,
          dr.idx_blks_hit,
          dr.relsize,
          dr.relsize_diff,
          dr.indisunique,
          dr.relpages_bytes,
          dr.relpages_bytes_diff,
          dr.last_idx_scan
        FROM json_to_record(datarow.row_data) AS dr(
          server_id      integer,
          sample_id      integer,
          datid          oid,
          indexrelid     oid,
          tablespaceid   oid,
          idx_scan       bigint,
          idx_tup_read   bigint,
          idx_tup_fetch  bigint,
          idx_blks_read  bigint,
          idx_blks_hit   bigint,
          relsize        bigint,
          relsize_diff   bigint,
          indisunique    boolean,
          relpages_bytes bigint,
          relpages_bytes_diff bigint,
          last_idx_scan  timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_indexes DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_indexes_total' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_indexes_total (server_id,sample_id,datid,tablespaceid,idx_scan,
          idx_tup_read,idx_tup_fetch,idx_blks_read,idx_blks_hit,relsize_diff)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.tablespaceid,
          dr.idx_scan,
          dr.idx_tup_read,
          dr.idx_tup_fetch,
          dr.idx_blks_read,
          dr.idx_blks_hit,
          dr.relsize_diff
        FROM json_to_record(datarow.row_data) AS dr(
          server_id      integer,
          sample_id      integer,
          datid          oid,
          tablespaceid   oid,
          idx_scan       bigint,
          idx_tup_read   bigint,
          idx_tup_fetch  bigint,
          idx_blks_read  bigint,
          idx_blks_hit   bigint,
          relsize_diff   bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_indexes_tot DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_kcache' THEN
      CASE -- Import version selector
        WHEN array_position(versions_array, '0.3.2:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO sample_kcache(server_id,sample_id,userid,datid,queryid,queryid_md5,
              plan_user_time,plan_system_time,plan_minflts,plan_majflts,
              plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,
              plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,
              exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,
              exec_nsignals,exec_nvcsws,exec_nivcsws,toplevel,stats_since)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.userid,
              dr.datid,
              dr.queryid,
              dr.queryid_md5,
              dr.plan_user_time,
              dr.plan_system_time,
              dr.plan_minflts,
              dr.plan_majflts,
              dr.plan_nswaps,
              dr.plan_reads,
              dr.plan_writes,
              dr.plan_msgsnds,
              dr.plan_msgrcvs,
              dr.plan_nsignals,
              dr.plan_nvcsws,
              dr.plan_nivcsws,
              dr.exec_user_time,
              dr.exec_system_time,
              dr.exec_minflts,
              dr.exec_majflts,
              dr.exec_nswaps,
              dr.exec_reads,
              dr.exec_writes,
              dr.exec_msgsnds,
              dr.exec_msgrcvs,
              dr.exec_nsignals,
              dr.exec_nvcsws,
              dr.exec_nivcsws,
              coalesce(dr.toplevel, true),
              dr.stats_since
            FROM json_to_record(datarow.row_data) AS dr(
              server_id         integer,
              sample_id         integer,
              userid            oid,
              datid             oid,
              queryid           bigint,
              queryid_md5       character(32),
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
              exec_nivcsws      bigint,
              toplevel          boolean,
              stats_since       timestamp with time zone
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT ON CONSTRAINT pk_sample_kcache_n DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
          END LOOP; -- over data rows
        WHEN array_position(versions_array, '0.3.1:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO sample_kcache(server_id,sample_id,userid,datid,queryid,queryid_md5,
              plan_user_time,plan_system_time,plan_minflts,plan_majflts,
              plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,
              plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,
              exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,
              exec_nsignals,exec_nvcsws,exec_nivcsws,toplevel,stats_since)
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.userid,
              dr.datid,
              dr.queryid,
              CASE WHEN starts_with(versions_array[1], '0.3.1:') THEN
                import_meta #>> ARRAY[dr.queryid_md5, dr.server_id::text]
              ELSE
                dr.queryid_md5
              END,
              dr.plan_user_time,
              dr.plan_system_time,
              dr.plan_minflts,
              dr.plan_majflts,
              dr.plan_nswaps,
              dr.plan_reads,
              dr.plan_writes,
              dr.plan_msgsnds,
              dr.plan_msgrcvs,
              dr.plan_nsignals,
              dr.plan_nvcsws,
              dr.plan_nivcsws,
              dr.exec_user_time,
              dr.exec_system_time,
              dr.exec_minflts,
              dr.exec_majflts,
              dr.exec_nswaps,
              dr.exec_reads,
              dr.exec_writes,
              dr.exec_msgsnds,
              dr.exec_msgrcvs,
              dr.exec_nsignals,
              dr.exec_nvcsws,
              dr.exec_nivcsws,
              coalesce(dr.toplevel, true),
              dr.stats_since
            FROM json_to_record(datarow.row_data) AS dr(
              server_id         integer,
              sample_id         integer,
              userid            oid,
              datid             oid,
              queryid           bigint,
              queryid_md5       character(32),
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
              exec_nivcsws      bigint,
              toplevel          boolean,
              stats_since    timestamp with time zone
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT ON CONSTRAINT pk_sample_kcache_n DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
          END LOOP; -- over data rows
        ELSE
          RAISE '%', format('[import %s]: Unsupported extension version: %s %s',
            imp_table_name, import_meta ->> 'extension', import_meta ->> 'version');
      END CASE; -- over import versions
    WHEN 'last_stat_database' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_database (server_id,sample_id,datid,datname,xact_commit,
          xact_rollback,blks_read,blks_hit,tup_returned,tup_fetched,tup_inserted,
          tup_updated,tup_deleted,conflicts,temp_files,temp_bytes,deadlocks,
          checksum_failures,checksum_last_failure,
          blk_read_time,blk_write_time,stats_reset,datsize,datsize_delta,datistemplate,
          session_time,active_time,
          idle_in_transaction_time,sessions,sessions_abandoned,sessions_fatal,
          sessions_killed,parallel_workers_to_launch,parallel_workers_launched)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.datname,
          dr.xact_commit,
          dr.xact_rollback,
          dr.blks_read,
          dr.blks_hit,
          dr.tup_returned,
          dr.tup_fetched,
          dr.tup_inserted,
          dr.tup_updated,
          dr.tup_deleted,
          dr.conflicts,
          dr.temp_files,
          dr.temp_bytes,
          dr.deadlocks,
          dr.checksum_failures,
          dr.checksum_last_failure,
          dr.blk_read_time,
          dr.blk_write_time,
          dr.stats_reset,
          dr.datsize,
          dr.datsize_delta,
          dr.datistemplate,
          dr.session_time,
          dr.active_time,
          dr.idle_in_transaction_time,
          dr.sessions,
          dr.sessions_abandoned,
          dr.sessions_fatal,
          dr.sessions_killed,
          dr.parallel_workers_to_launch,
          dr.parallel_workers_launched
        FROM json_to_record(datarow.row_data) AS dr(
          server_id       integer,
          sample_id       integer,
          datid           oid,
          datname         name,
          xact_commit     bigint,
          xact_rollback   bigint,
          blks_read       bigint,
          blks_hit        bigint,
          tup_returned    bigint,
          tup_fetched     bigint,
          tup_inserted    bigint,
          tup_updated     bigint,
          tup_deleted     bigint,
          conflicts       bigint,
          temp_files      bigint,
          temp_bytes      bigint,
          deadlocks       bigint,
          blk_read_time   double precision,
          blk_write_time  double precision,
          stats_reset     timestamp with time zone,
          datsize         bigint,
          datsize_delta   bigint,
          datistemplate   boolean,
          session_time    double precision,
          active_time     double precision,
          idle_in_transaction_time  double precision,
          sessions        bigint,
          sessions_abandoned  bigint,
          sessions_fatal      bigint,
          sessions_killed     bigint,
          parallel_workers_to_launch  bigint,
          parallel_workers_launched   bigint,
          checksum_failures   bigint,
          checksum_last_failure timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_tablespaces' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_tablespaces (server_id,sample_id,tablespaceid,tablespacename,
          tablespacepath,size,size_delta)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.tablespaceid,
          dr.tablespacename,
          dr.tablespacepath,
          dr.size,
          dr.size_delta
        FROM json_to_record(datarow.row_data) AS dr(
          server_id       integer,
          sample_id       integer,
          tablespaceid    oid,
          tablespacename  name,
          tablespacepath  text,
          size            bigint,
          size_delta      bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_cluster' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_cluster (server_id,sample_id,checkpoints_timed,
          checkpoints_req,checkpoints_done,checkpoint_write_time,checkpoint_sync_time,
          buffers_checkpoint,slru_checkpoint,buffers_clean,maxwritten_clean,buffers_backend,
          buffers_backend_fsync,buffers_alloc,stats_reset,wal_size)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.checkpoints_timed,
          dr.checkpoints_req,
          dr.checkpoints_done,
          dr.checkpoint_write_time,
          dr.checkpoint_sync_time,
          dr.buffers_checkpoint,
          dr.slru_checkpoint,
          dr.buffers_clean,
          dr.maxwritten_clean,
          dr.buffers_backend,
          dr.buffers_backend_fsync,
          dr.buffers_alloc,
          dr.stats_reset,
          dr.wal_size
        FROM json_to_record(datarow.row_data) AS dr(
          server_id              integer,
          sample_id              integer,
          checkpoints_timed      bigint,
          checkpoints_req        bigint,
          checkpoints_done       bigint,
          checkpoint_write_time  double precision,
          checkpoint_sync_time   double precision,
          buffers_checkpoint     bigint,
          slru_checkpoint        bigint,
          buffers_clean          bigint,
          maxwritten_clean       bigint,
          buffers_backend        bigint,
          buffers_backend_fsync  bigint,
          buffers_alloc          bigint,
          stats_reset            timestamp with time zone,
          wal_size               bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_archiver' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_archiver (server_id,sample_id,archived_count,last_archived_wal,
          last_archived_time,failed_count,last_failed_wal,last_failed_time,stats_reset)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.archived_count,
          dr.last_archived_wal,
          dr.last_archived_time,
          dr.failed_count,
          dr.last_failed_wal,
          dr.last_failed_time,
          dr.stats_reset
        FROM json_to_record(datarow.row_data) AS dr(
          server_id           integer,
          sample_id           integer,
          archived_count      bigint,
          last_archived_wal   text,
          last_archived_time  timestamp with time zone,
          failed_count        bigint,
          last_failed_wal     text,
          last_failed_time    timestamp with time zone,
          stats_reset         timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_tables' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_tables (server_id,sample_id,datid,relid,schemaname,relname,
          seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,
          n_tup_hot_upd,n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,
          last_vacuum,last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,
          autovacuum_count,analyze_count,autoanalyze_count,total_vacuum_time,total_autovacuum_time,
          total_analyze_time,total_autoanalyze_time,heap_blks_read,heap_blks_hit,
          idx_blks_read,idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,
          tidx_blks_hit,relsize,relsize_diff,tablespaceid,reltoastrelid,relkind,in_sample,
          relpages_bytes,relpages_bytes_diff,last_seq_scan,last_idx_scan,n_tup_newpage_upd,
          reloptions)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.relid,
          dr.schemaname,
          dr.relname,
          dr.seq_scan,
          dr.seq_tup_read,
          dr.idx_scan,
          dr.idx_tup_fetch,
          dr.n_tup_ins,
          dr.n_tup_upd,
          dr.n_tup_del,
          dr.n_tup_hot_upd,
          dr.n_live_tup,
          dr.n_dead_tup,
          dr.n_mod_since_analyze,
          dr.n_ins_since_vacuum,
          dr.last_vacuum,
          dr.last_autovacuum,
          dr.last_analyze,
          dr.last_autoanalyze,
          dr.vacuum_count,
          dr.autovacuum_count,
          dr.analyze_count,
          dr.autoanalyze_count,
          dr.total_vacuum_time,
          dr.total_autovacuum_time,
          dr.total_analyze_time,
          dr.total_autoanalyze_time,
          dr.heap_blks_read,
          dr.heap_blks_hit,
          dr.idx_blks_read,
          dr.idx_blks_hit,
          dr.toast_blks_read,
          dr.toast_blks_hit,
          dr.tidx_blks_read,
          dr.tidx_blks_hit,
          dr.relsize,
          dr.relsize_diff,
          dr.tablespaceid,
          dr.reltoastrelid,
          dr.relkind,
          COALESCE(dr.in_sample, false),
          dr.relpages_bytes,
          dr.relpages_bytes_diff,
          dr.last_seq_scan,
          dr.last_idx_scan,
          dr.n_tup_newpage_upd,
          dr.reloptions
        FROM json_to_record(datarow.row_data) AS dr(
          server_id            integer,
          sample_id            integer,
          datid                oid,
          relid                oid,
          schemaname           name,
          relname              name,
          seq_scan             bigint,
          seq_tup_read         bigint,
          idx_scan             bigint,
          idx_tup_fetch        bigint,
          n_tup_ins            bigint,
          n_tup_upd            bigint,
          n_tup_del            bigint,
          n_tup_hot_upd        bigint,
          n_live_tup           bigint,
          n_dead_tup           bigint,
          n_mod_since_analyze  bigint,
          n_ins_since_vacuum   bigint,
          last_vacuum          timestamp with time zone,
          last_autovacuum      timestamp with time zone,
          last_analyze         timestamp with time zone,
          last_autoanalyze     timestamp with time zone,
          total_vacuum_time       double precision,
          total_autovacuum_time   double precision,
          total_analyze_time      double precision,
          total_autoanalyze_time  double precision,
          vacuum_count         bigint,
          autovacuum_count     bigint,
          analyze_count        bigint,
          autoanalyze_count    bigint,
          heap_blks_read       bigint,
          heap_blks_hit        bigint,
          idx_blks_read        bigint,
          idx_blks_hit         bigint,
          toast_blks_read      bigint,
          toast_blks_hit       bigint,
          tidx_blks_read       bigint,
          tidx_blks_hit        bigint,
          relsize              bigint,
          relsize_diff         bigint,
          tablespaceid         oid,
          reltoastrelid        oid,
          relkind              character(1),
          in_sample            boolean,
          relpages_bytes       bigint,
          relpages_bytes_diff  bigint,
          last_seq_scan        timestamp with time zone,
          last_idx_scan        timestamp with time zone,
          n_tup_newpage_upd    bigint,
          reloptions           jsonb
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_indexes' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_indexes (server_id,sample_id,datid,relid,indexrelid,
          schemaname,relname,indexrelname,idx_scan,idx_tup_read,idx_tup_fetch,
          idx_blks_read,idx_blks_hit,relsize,relsize_diff,tablespaceid,indisunique,
          in_sample,relpages_bytes,relpages_bytes_diff,last_idx_scan,reloptions)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.relid,
          dr.indexrelid,
          dr.schemaname,
          dr.relname,
          dr.indexrelname,
          dr.idx_scan,
          dr.idx_tup_read,
          dr.idx_tup_fetch,
          dr.idx_blks_read,
          dr.idx_blks_hit,
          dr.relsize,
          dr.relsize_diff,
          dr.tablespaceid,
          dr.indisunique,
          COALESCE(dr.in_sample, false),
          dr.relpages_bytes,
          dr.relpages_bytes_diff,
          dr.last_idx_scan,
          dr.reloptions
        FROM json_to_record(datarow.row_data) AS dr(
          server_id      integer,
          sample_id      integer,
          datid          oid,
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
          indisunique    boolean,
          in_sample      boolean,
          relpages_bytes bigint,
          relpages_bytes_diff bigint,
          last_idx_scan  timestamp with time zone,
          reloptions     jsonb
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_user_functions' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_user_functions (server_id,sample_id,datid,funcid,schemaname,
          funcname,funcargs,calls,total_time,self_time,trg_fn,in_sample)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.datid,
          dr.funcid,
          dr.schemaname,
          dr.funcname,
          dr.funcargs,
          dr.calls,
          dr.total_time,
          dr.self_time,
          dr.trg_fn,
          COALESCE(dr.in_sample, false)
        FROM json_to_record(datarow.row_data) AS dr(
          server_id   integer,
          sample_id   integer,
          datid       oid,
          funcid      oid,
          schemaname  name,
          funcname    name,
          funcargs    text,
          calls       bigint,
          total_time  double precision,
          self_time   double precision,
          trg_fn      boolean,
          in_sample   boolean
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_wal' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_wal (server_id,sample_id,wal_records,
          wal_fpi,wal_bytes,wal_buffers_full,wal_write,wal_sync,
          wal_write_time,wal_sync_time,stats_reset)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.wal_records,
          dr.wal_fpi,
          dr.wal_bytes,
          dr.wal_buffers_full,
          dr.wal_write,
          dr.wal_sync,
          dr.wal_write_time,
          dr.wal_sync_time,
          dr.stats_reset
        FROM json_to_record(datarow.row_data) AS dr(
          server_id           integer,
          sample_id           integer,
          wal_records         bigint,
          wal_fpi             bigint,
          wal_bytes           numeric,
          wal_buffers_full    bigint,
          wal_write           bigint,
          wal_sync            bigint,
          wal_write_time      double precision,
          wal_sync_time       double precision,
          stats_reset         timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_kcache' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_kcache (server_id,sample_id,userid,datid,toplevel,queryid,
          plan_user_time,plan_system_time,plan_minflts,plan_majflts,
          plan_nswaps,plan_reads,plan_writes,plan_msgsnds,plan_msgrcvs,plan_nsignals,
          plan_nvcsws,plan_nivcsws,exec_user_time,exec_system_time,exec_minflts,
          exec_majflts,exec_nswaps,exec_reads,exec_writes,exec_msgsnds,exec_msgrcvs,
          exec_nsignals,exec_nvcsws,exec_nivcsws,stats_since
        )
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.userid,
          dr.datid,
          dr.toplevel,
          dr.queryid,
          dr.plan_user_time,
          dr.plan_system_time,
          dr.plan_minflts,
          dr.plan_majflts,
          dr.plan_nswaps,
          dr.plan_reads,
          dr.plan_writes,
          dr.plan_msgsnds,
          dr.plan_msgrcvs,
          dr.plan_nsignals,
          dr.plan_nvcsws,
          dr.plan_nivcsws,
          dr.exec_user_time,
          dr.exec_system_time,
          dr.exec_minflts,
          dr.exec_majflts,
          dr.exec_nswaps,
          dr.exec_reads,
          dr.exec_writes,
          dr.exec_msgsnds,
          dr.exec_msgrcvs,
          dr.exec_nsignals,
          dr.exec_nvcsws,
          dr.exec_nivcsws,
          dr.stats_since
        FROM json_to_record(datarow.row_data) AS dr(
          server_id         integer,
          sample_id         integer,
          userid            oid,
          datid             oid,
          toplevel          boolean,
          queryid           bigint,
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
          exec_nivcsws      bigint,
          stats_since       timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_statements' THEN
      CASE -- Import version selector
        WHEN array_position(versions_array, '4.7:pg_profile') >= 1 THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO last_stat_statements (server_id, sample_id, userid, username, datid, queryid,
              queryid_md5, plans, total_plan_time, min_plan_time, max_plan_time, mean_plan_time,
              stddev_plan_time, calls, total_exec_time, min_exec_time, max_exec_time,
              mean_exec_time, stddev_exec_time, rows, shared_blks_hit, shared_blks_read,
              shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
              local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
              shared_blk_read_time, shared_blk_write_time, wal_records, wal_fpi, wal_bytes, wal_buffers_full,
              toplevel, in_sample, jit_functions, jit_generation_time, jit_inlining_count,
              jit_inlining_time, jit_optimization_count, jit_optimization_time,
              jit_emission_count, jit_emission_time, temp_blk_read_time, temp_blk_write_time,
              local_blk_read_time, local_blk_write_time, jit_deform_count, jit_deform_time,
              parallel_workers_to_launch, parallel_workers_launched, stats_since, minmax_stats_since
              )
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.userid,
              dr.username,
              dr.datid,
              dr.queryid,
              dr.queryid_md5,
              dr.plans,
              dr.total_plan_time,
              dr.min_plan_time,
              dr.max_plan_time,
              dr.mean_plan_time,
              dr.stddev_plan_time,
              dr.calls,
              dr.total_exec_time,
              dr.min_exec_time,
              dr.max_exec_time,
              dr.mean_exec_time,
              dr.stddev_exec_time,
              dr.rows,
              dr.shared_blks_hit,
              dr.shared_blks_read,
              dr.shared_blks_dirtied,
              dr.shared_blks_written,
              dr.local_blks_hit,
              dr.local_blks_read,
              dr.local_blks_dirtied,
              dr.local_blks_written,
              dr.temp_blks_read,
              dr.temp_blks_written,
              dr.blk_read_time,
              dr.blk_write_time,
              dr.wal_records,
              dr.wal_fpi,
              dr.wal_bytes,
              dr.wal_buffers_full,
              dr.toplevel,
              dr.in_sample,
              dr.jit_functions,
              dr.jit_generation_time,
              dr.jit_inlining_count,
              dr.jit_inlining_time,
              dr.jit_optimization_count,
              dr.jit_optimization_time,
              dr.jit_emission_count,
              dr.jit_emission_time,
              dr.temp_blk_read_time,
              dr.temp_blk_write_time,
              dr.local_blk_read_time,
              dr.local_blk_write_time,
              dr.jit_deform_count,
              dr.jit_deform_time,
              dr.parallel_workers_to_launch,
              dr.parallel_workers_launched,
              dr.stats_since,
              dr.minmax_stats_since
            FROM json_to_record(datarow.row_data) AS dr(
              server_id            integer,
              sample_id            integer,
              userid               oid,
              username             name,
              datid                oid,
              queryid              bigint,
              queryid_md5          character(32),
              plans                bigint,
              total_plan_time      double precision,
              min_plan_time        double precision,
              max_plan_time        double precision,
              mean_plan_time       double precision,
              stddev_plan_time     double precision,
              calls                bigint,
              total_exec_time      double precision,
              min_exec_time        double precision,
              max_exec_time        double precision,
              mean_exec_time       double precision,
              stddev_exec_time     double precision,
              rows                 bigint,
              shared_blks_hit      bigint,
              shared_blks_read     bigint,
              shared_blks_dirtied  bigint,
              shared_blks_written  bigint,
              local_blks_hit       bigint,
              local_blks_read      bigint,
              local_blks_dirtied   bigint,
              local_blks_written   bigint,
              temp_blks_read       bigint,
              temp_blks_written    bigint,
              blk_read_time        double precision,
              blk_write_time       double precision,
              wal_records          bigint,
              wal_fpi              bigint,
              wal_bytes            numeric,
              wal_buffers_full     bigint,
              toplevel             boolean,
              in_sample            boolean,
              jit_functions        bigint,
              jit_generation_time  double precision,
              jit_inlining_count   bigint,
              jit_inlining_time    double precision,
              jit_optimization_count  bigint,
              jit_optimization_time   double precision,
              jit_emission_count   bigint,
              jit_emission_time    double precision,
              temp_blk_read_time   double precision,
              temp_blk_write_time  double precision,
              local_blk_read_time  double precision,
              local_blk_write_time double precision,
              jit_deform_count     bigint,
              jit_deform_time      double precision,
              parallel_workers_to_launch  bigint,
              parallel_workers_launched   bigint,
              stats_since          timestamp with time zone,
              minmax_stats_since   timestamp with time zone
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
        WHEN array_position(versions_array, '0.3.1:pg_profile') IS NOT NULL THEN
          LOOP
            FETCH data INTO datarow;
            EXIT WHEN NOT FOUND;
            INSERT INTO last_stat_statements (server_id, sample_id, userid, username, datid, queryid,
              queryid_md5, plans, total_plan_time, min_plan_time, max_plan_time, mean_plan_time,
              stddev_plan_time, calls, total_exec_time, min_exec_time, max_exec_time,
              mean_exec_time, stddev_exec_time, rows, shared_blks_hit, shared_blks_read,
              shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read,
              local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written,
              shared_blk_read_time, shared_blk_write_time, wal_records, wal_fpi, wal_bytes,
              toplevel, in_sample, jit_functions, jit_generation_time, jit_inlining_count,
              jit_inlining_time, jit_optimization_count, jit_optimization_time,
              jit_emission_count, jit_emission_time, temp_blk_read_time, temp_blk_write_time
              )
            SELECT
              (srv_map ->> dr.server_id::text)::integer,
              dr.sample_id,
              dr.userid,
              dr.username,
              dr.datid,
              dr.queryid,
              dr.queryid_md5,
              dr.plans,
              dr.total_plan_time,
              dr.min_plan_time,
              dr.max_plan_time,
              dr.mean_plan_time,
              dr.stddev_plan_time,
              dr.calls,
              dr.total_exec_time,
              dr.min_exec_time,
              dr.max_exec_time,
              dr.mean_exec_time,
              dr.stddev_exec_time,
              dr.rows,
              dr.shared_blks_hit,
              dr.shared_blks_read,
              dr.shared_blks_dirtied,
              dr.shared_blks_written,
              dr.local_blks_hit,
              dr.local_blks_read,
              dr.local_blks_dirtied,
              dr.local_blks_written,
              dr.temp_blks_read,
              dr.temp_blks_written,
              dr.blk_read_time,
              dr.blk_write_time,
              dr.wal_records,
              dr.wal_fpi,
              dr.wal_bytes,
              dr.toplevel,
              dr.in_sample,
              dr.jit_functions,
              dr.jit_generation_time,
              dr.jit_inlining_count,
              dr.jit_inlining_time,
              dr.jit_optimization_count,
              dr.jit_optimization_time,
              dr.jit_emission_count,
              dr.jit_emission_time,
              dr.temp_blk_read_time,
              dr.temp_blk_write_time
            FROM json_to_record(datarow.row_data) AS dr(
              server_id            integer,
              sample_id            integer,
              userid               oid,
              username             name,
              datid                oid,
              queryid              bigint,
              queryid_md5          character(32),
              plans                bigint,
              total_plan_time      double precision,
              min_plan_time        double precision,
              max_plan_time        double precision,
              mean_plan_time       double precision,
              stddev_plan_time     double precision,
              calls                bigint,
              total_exec_time      double precision,
              min_exec_time        double precision,
              max_exec_time        double precision,
              mean_exec_time       double precision,
              stddev_exec_time     double precision,
              rows                 bigint,
              shared_blks_hit      bigint,
              shared_blks_read     bigint,
              shared_blks_dirtied  bigint,
              shared_blks_written  bigint,
              local_blks_hit       bigint,
              local_blks_read      bigint,
              local_blks_dirtied   bigint,
              local_blks_written   bigint,
              temp_blks_read       bigint,
              temp_blks_written    bigint,
              blk_read_time        double precision,
              blk_write_time       double precision,
              wal_records          bigint,
              wal_fpi              bigint,
              wal_bytes            numeric,
              toplevel             boolean,
              in_sample            boolean,
              jit_functions        bigint,
              jit_generation_time  double precision,
              jit_inlining_count   bigint,
              jit_inlining_time    double precision,
              jit_optimization_count  bigint,
              jit_optimization_time   double precision,
              jit_emission_count   bigint,
              jit_emission_time    double precision,
              temp_blk_read_time   double precision,
              temp_blk_write_time  double precision
              )
            JOIN
              samples s_ctl ON
                ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
                (s_ctl.server_id, s_ctl.sample_id)
            ON CONFLICT DO NOTHING;
            GET DIAGNOSTICS row_proc = ROW_COUNT;
            rowcnt := rowcnt + row_proc;
            IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
              RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
            END IF;
          END LOOP; -- over data rows
        ELSE
          RAISE '%', format('[import %s]: Unsupported extension version: %s %s',
            imp_table_name, import_meta ->> 'extension', import_meta ->> 'version');
      END CASE; -- over import versions
    WHEN 'last_stat_io' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_io (server_id,sample_id,backend_type,object,context,reads,
          read_bytes,read_time,writes,write_bytes,write_time,writebacks,writeback_time,extends,
          extend_bytes,extend_time,op_bytes,hits,evictions,reuses,fsyncs,fsync_time,stats_reset
          )
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.backend_type,
          dr.object,
          dr.context,
          dr.reads,
          dr.read_bytes,
          dr.read_time,
          dr.writes,
          dr.write_bytes,
          dr.write_time,
          dr.writebacks,
          dr.writeback_time,
          dr.extends,
          dr.extend_bytes,
          dr.extend_time,
          dr.op_bytes,
          dr.hits,
          dr.evictions,
          dr.reuses,
          dr.fsyncs,
          dr.fsync_time,
          dr.stats_reset
        FROM json_to_record(datarow.row_data) AS dr(
          server_id         integer,
          sample_id         integer,
          backend_type      text,
          object            text,
          context           text,
          reads             bigint,
          read_bytes        numeric,
          read_time         double precision,
          writes            bigint,
          write_bytes       numeric,
          write_time        double precision,
          writebacks        bigint,
          writeback_time    double precision,
          extends           bigint,
          extend_bytes      numeric,
          extend_time       double precision,
          op_bytes          bigint,
          hits              bigint,
          evictions         bigint,
          reuses            bigint,
          fsyncs            bigint,
          fsync_time        double precision,
          stats_reset       timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_slru' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_slru (server_id,sample_id,name,blks_zeroed,
          blks_hit,blks_read,blks_written,blks_exists,flushes,truncates,
          stats_reset
          )
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.name,
          dr.blks_zeroed,
          dr.blks_hit,
          dr.blks_read,
          dr.blks_written,
          dr.blks_exists,
          dr.flushes,
          dr.truncates,
          dr.stats_reset
        FROM json_to_record(datarow.row_data) AS dr(
          server_id      integer,
          sample_id      integer,
          name           text,
          blks_zeroed    bigint,
          blks_hit       bigint,
          blks_read      bigint,
          blks_written   bigint,
          blks_exists    bigint,
          flushes        bigint,
          truncates      bigint,
          stats_reset    timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'extension_versions' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO extension_versions(server_id,datid,extname,first_seen,last_sample_id,extversion)
        SELECT
          (srv_map ->> dr.server_id::text)::integer AS server_id,
          dr.datid,
          dr.extname,
          dr.first_seen,
          dr.last_sample_id,
          dr.extversion
        FROM json_to_record(datarow.row_data) AS dr(
            server_id        integer,
            datid            oid,
            extname          name,
            first_seen       timestamp(0) with time zone,
            last_sample_id   integer,
            extversion       text
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ON CONFLICT ON CONSTRAINT pk_extension_versions DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_extension_versions' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_extension_versions(server_id, datid, sample_id, extname, extversion)
        SELECT
          (srv_map ->> dr.server_id::text)::integer AS server_id,
          dr.datid,
          dr.sample_id,
          dr.extname,
          dr.extversion
        FROM json_to_record(datarow.row_data) AS dr(
            server_id        integer,
            datid            oid,
            sample_id        integer,
            extname          name,
            extversion       text
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ON CONFLICT ON CONSTRAINT pk_last_extension_versions DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'table_storage_parameters' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO table_storage_parameters(server_id,datid,relid,first_seen,last_sample_id,reloptions)
        SELECT
          (srv_map ->> dr.server_id::text)::integer AS server_id,
          dr.datid,
          dr.relid,
          dr.first_seen,
          dr.last_sample_id,
          dr.reloptions
        FROM json_to_record(datarow.row_data) AS dr(
            server_id        integer,
            datid            oid,
            relid            oid,
            first_seen       timestamp(0) with time zone,
            last_sample_id   integer,
            reloptions       jsonb
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ON CONFLICT ON CONSTRAINT pk_table_storage_parameters DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'index_storage_parameters' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO index_storage_parameters(server_id,datid,relid,indexrelid,first_seen,last_sample_id,reloptions)
        SELECT
          (srv_map ->> dr.server_id::text)::integer AS server_id,
          dr.datid,
          dr.relid,
          dr.indexrelid,
          dr.first_seen,
          dr.last_sample_id,
          dr.reloptions
        FROM json_to_record(datarow.row_data) AS dr(
            server_id        integer,
            datid            oid,
            relid            oid,
            indexrelid       oid,
            first_seen       timestamp(0) with time zone,
            last_sample_id   integer,
            reloptions       jsonb
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ON CONFLICT ON CONSTRAINT pk_index_storage_parameters DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    ELSE
      rows_processed := -1;
      RETURN; -- table not found
  END CASE; -- over table name
  rows_processed := rowcnt;
  RETURN;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION import_section_data_subsample(IN data refcursor, IN imp_table_name name, IN srv_map jsonb,
  IN import_meta jsonb, IN versions_array text[],
  OUT rows_processed bigint, OUT new_import_meta jsonb)
SET search_path=@extschema@ AS $$
DECLARE
  datarow          record;
  rowcnt           bigint = 0;
  row_proc         bigint = 0;
  section_meta     jsonb;
BEGIN
  new_import_meta := import_meta;
  CASE imp_table_name
    WHEN 'last_stat_activity_count' THEN NULL; -- skip table without PK
    WHEN 'server_subsample' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO server_subsample(server_id, subsample_enabled, min_query_dur,
          min_xact_dur, min_xact_age, min_idle_xact_dur)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.subsample_enabled,
          dr.min_query_dur,
          dr.min_xact_dur,
          dr.min_xact_age,
          dr.min_idle_xact_dur
        FROM json_to_record(datarow.row_data) AS dr(
            server_id           integer,
            subsample_enabled   boolean,
            min_query_dur       interval hour to second,
            min_xact_dur        interval hour to second,
            min_xact_age        bigint,
            min_idle_xact_dur   interval hour to second
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ON CONFLICT ON CONSTRAINT pk_server_subsample DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_act_backend' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_act_backend(server_id, sample_id, pid,
          backend_start, datid, datname, usesysid, usename,
          client_addr, client_hostname, client_port, backend_type,
          backend_last_ts)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.pid,
          dr.backend_start,
          dr.datid,
          dr.datname,
          dr.usesysid,
          dr.usename,
          dr.client_addr,
          dr.client_hostname,
          dr.client_port,
          dr.backend_type,
          dr.backend_last_ts
        FROM json_to_record(datarow.row_data) AS dr(
            server_id         integer,
            sample_id         integer,
            pid               integer,
            backend_start     timestamp with time zone,
            datid             oid,
            datname           name,
            usesysid          oid,
            usename           name,
            application_name  text,
            client_addr       inet,
            client_hostname   text,
            client_port       integer,
            backend_type      text,
            backend_last_ts   timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_backends DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_act_xact' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_act_xact(server_id, sample_id, pid, backend_start, xact_start,
          backend_xid, xact_last_ts)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.pid,
          dr.backend_start,
          dr.xact_start,
          dr.backend_xid,
          dr.xact_last_ts
        FROM json_to_record(datarow.row_data) AS dr(
            server_id         integer,
            sample_id         integer,
            pid               integer,
            backend_start     timestamp with time zone,
            xact_start        timestamp with time zone,
            backend_xid       text,
            xact_last_ts      timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_xact DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_act_backend_state' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_act_backend_state(server_id, sample_id,
          pid, backend_start, application_name, state_code,
          state_change, state_last_ts, xact_start, backend_xmin,
          backend_xmin_age, query_start)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.pid,
          dr.backend_start,
          dr.application_name,
          dr.state_code,
          dr.state_change,
          dr.state_last_ts,
          dr.xact_start,
          dr.backend_xmin,
          dr.backend_xmin_age,
          dr.query_start
        FROM json_to_record(datarow.row_data) AS dr(
            server_id         integer,
            sample_id         integer,
            pid               integer,
            backend_start     timestamp with time zone,
            application_name  text,
            state_code        integer,
            state_change      timestamp with time zone,
            state_last_ts     timestamp with time zone,
            xact_start        timestamp with time zone,
            backend_xmin      text,
            backend_xmin_age  bigint,
            query_start       timestamp with time zone
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_bk_state DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'act_query' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO act_query(server_id, act_query_md5, act_query, last_sample_id)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.act_query_md5,
          dr.act_query,
          s.sample_id
        FROM json_to_record(datarow.row_data) AS dr(
            server_id      integer,
            act_query_md5  char(32),
            act_query      text,
            last_sample_id integer
          )
          LEFT JOIN samples s ON (s.server_id, s.sample_id) =
            ((srv_map ->> dr.server_id::text)::integer, dr.last_sample_id)
        ON CONFLICT ON CONSTRAINT pk_act_query DO
          NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_act_statement' THEN
      IF array_position(versions_array, '4.7:pg_profile') >= 1 THEN
        /*
         V4.6 released with little bit incorrect schema, so, we will
         process it later. Here is V4.7+ import
        */
        LOOP
          FETCH data INTO datarow;
          EXIT WHEN NOT FOUND;

          INSERT INTO sample_act_statement(server_id, sample_id, pid,
            leader_pid, query_start, query_id, act_query_md5,
            stmt_last_ts, xact_start)
          SELECT
            (srv_map ->> dr.server_id::text)::integer,
            dr.sample_id,
            dr.pid,
            dr.leader_pid,
            dr.query_start,
            dr.query_id,
            dr.act_query_md5,
            dr.stmt_last_ts,
            dr.xact_start
          FROM json_to_record(datarow.row_data) AS dr(
              server_id         integer,
              sample_id         integer,
              pid               integer,
              leader_pid        integer,
              state_change      timestamp with time zone,
              query_start       timestamp with time zone,
              query_id          bigint,
              act_query_md5     char(32),
              stmt_last_ts      timestamp with time zone,
              xact_start        timestamp with time zone
            )
          JOIN
            samples s_ctl ON
              ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
              (s_ctl.server_id, s_ctl.sample_id)
          ON CONFLICT ON CONSTRAINT pk_sample_act_stmt DO NOTHING;
          GET DIAGNOSTICS row_proc = ROW_COUNT;
          rowcnt := rowcnt + row_proc;
          IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
            RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
          END IF;
        END LOOP; -- over data rows
      ELSE
        /* V4.6 import special case
          We should update sample_act_backend_state table with
          query_start field obtained from the sample_act_statement
          data row. By the way we should get xact_start from
          sample_act_backend_state and finally insert the data in
          sample_act_statement table
        */
        LOOP
          FETCH data INTO datarow;
          EXIT WHEN NOT FOUND;

          WITH u AS (
            UPDATE sample_act_backend_state usabs
              SET query_start = dr.query_start
            FROM json_to_record(datarow.row_data) AS dr(
                server_id         integer,
                sample_id         integer,
                pid               integer,
                leader_pid        integer,
                state_change      timestamp with time zone,
                query_start       timestamp with time zone,
                query_id          bigint,
                act_query_md5     char(32),
                stmt_last_ts      timestamp with time zone
              )
            WHERE
              (usabs.server_id, usabs.sample_id, usabs.pid, usabs.state_change) =
              ((srv_map ->> dr.server_id::text)::integer, dr.sample_id, dr.pid, dr.state_change)
            RETURNING usabs.server_id, usabs.sample_id, usabs.pid, dr.leader_pid,
              dr.query_start, dr.query_id, dr.act_query_md5, dr.stmt_last_ts,
              usabs.xact_start
          )
          INSERT INTO sample_act_statement(server_id, sample_id, pid,
            leader_pid, query_start, query_id, act_query_md5,
            stmt_last_ts, xact_start)
          SELECT
            u.server_id,
            u.sample_id,
            u.pid,
            u.leader_pid,
            u.query_start,
            u.query_id,
            u.act_query_md5,
            u.stmt_last_ts,
            u.xact_start
          FROM u
          ON CONFLICT ON CONSTRAINT pk_sample_act_stmt DO NOTHING;
          GET DIAGNOSTICS row_proc = ROW_COUNT;
          rowcnt := rowcnt + row_proc;
          IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
            RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
          END IF;
        END LOOP; -- over data rows
      END IF;
    WHEN 'last_stat_activity' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_activity(server_id, sample_id,
          subsample_ts, datid, datname, pid, leader_pid, usesysid,
          usename, application_name, client_addr, client_hostname,
          client_port, backend_start, xact_start, query_start,
          state_change, state, backend_xid, backend_xmin, query_id,
          query, backend_type, backend_xmin_age
        )
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.subsample_ts,
          dr.datid,
          dr.datname,
          dr.pid,
          dr.leader_pid,
          dr.usesysid,
          dr.usename,
          dr.application_name,
          dr.client_addr,
          dr.client_hostname,
          dr.client_port,
          dr.backend_start,
          dr.xact_start,
          dr.query_start,
          dr.state_change,
          dr.state,
          dr.backend_xid,
          dr.backend_xmin,
          dr.query_id,
          dr.query,
          dr.backend_type,
          dr.backend_xmin_age
        FROM json_to_record(datarow.row_data) AS dr(
            server_id         integer,
            sample_id         integer,
            subsample_ts      timestamp with time zone,
            datid             oid,
            datname           name,
            pid               integer,
            leader_pid        integer,
            usesysid          oid,
            usename           name,
            application_name  text,
            client_addr       inet,
            client_hostname   text,
            client_port       integer,
            backend_start     timestamp with time zone,
            xact_start        timestamp with time zone,
            query_start       timestamp with time zone,
            state_change      timestamp with time zone,
            state             text,
            backend_xid       text,
            backend_xmin      text,
            query_id          bigint,
            query             text,
            backend_type      text,
            backend_xmin_age  bigint
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'session_attr' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO session_attr(server_id, sess_attr_id,
          backend_type, datid, datname, usesysid, usename,
          application_name, client_addr, last_sample_id)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sess_attr_id,
          dr.backend_type,
          dr.datid,
          dr.datname,
          dr.usesysid,
          dr.usename,
          dr.application_name,
          dr.client_addr,
          dr.last_sample_id
        FROM json_to_record(datarow.row_data) AS dr(
            server_id         integer,
            sess_attr_id      integer,
            backend_type      text,
            datid             oid,
            datname           name,
            usesysid          oid,
            usename           name,
            application_name  text,
            client_addr       inet,
            last_sample_id    integer
          )
        JOIN
          servers s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer) =
            (s_ctl.server_id)
        ON CONFLICT ON CONSTRAINT pk_subsample_sa DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'sample_stat_activity_cnt' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO sample_stat_activity_cnt(server_id, sample_id,
          subsample_ts, sess_attr_id, total, active, idle, idle_t,
          idle_ta, state_null, lwlock, lock, bufferpin, activity,
          extension, client, ipc, timeout, io)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.subsample_ts,
          dr.sess_attr_id,
          dr.total,
          dr.active,
          dr.idle,
          dr.idle_t,
          dr.idle_ta,
          dr.state_null,
          dr.lwlock,
          dr.lock,
          dr.bufferpin,
          dr.activity,
          dr.extension,
          dr.client,
          dr.ipc,
          dr.timeout,
          dr.io
        FROM json_to_record(datarow.row_data) AS dr(
            server_id         integer,
            sample_id         integer,
            subsample_ts      timestamp with time zone,
            sess_attr_id      integer,
            total             integer,
            active            integer,
            idle              integer,
            idle_t            integer,
            idle_ta           integer,
            state_null        integer,
            lwlock            integer,
            lock              integer,
            bufferpin         integer,
            activity          integer,
            extension         integer,
            client            integer,
            ipc               integer,
            timeout           integer,
            io                integer
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT ON CONSTRAINT pk_sample_stat_activity_cnt DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    WHEN 'last_stat_activity_count' THEN
      LOOP
        FETCH data INTO datarow;
        EXIT WHEN NOT FOUND;
        INSERT INTO last_stat_activity_count(server_id, sample_id,
          subsample_ts, backend_type, datid, datname, usesysid,
          usename, application_name, client_addr, total, active, idle,
          idle_t, idle_ta, state_null, lwlock, lock, bufferpin,
          activity, extension, client, ipc, timeout, io)
        SELECT
          (srv_map ->> dr.server_id::text)::integer,
          dr.sample_id,
          dr.subsample_ts,
          dr.backend_type,
          dr.datid,
          dr.datname,
          dr.usesysid,
          dr.usename,
          dr.application_name,
          dr.client_addr,
          dr.total,
          dr.active,
          dr.idle,
          dr.idle_t,
          dr.idle_ta,
          dr.state_null,
          dr.lwlock,
          dr.lock,
          dr.bufferpin,
          dr.activity,
          dr.extension,
          dr.client,
          dr.ipc,
          dr.timeout,
          dr.io
        FROM json_to_record(datarow.row_data) AS dr(
            server_id         integer,
            sample_id         integer,
            subsample_ts      timestamp with time zone,
            backend_type      text,
            datid             oid,
            datname           name,
            usesysid          oid,
            usename           name,
            application_name  text,
            client_addr       inet,
            total             integer,
            active            integer,
            idle              integer,
            idle_t            integer,
            idle_ta           integer,
            state_null        integer,
            lwlock            integer,
            lock              integer,
            bufferpin         integer,
            activity          integer,
            extension         integer,
            client            integer,
            ipc               integer,
            timeout           integer,
            io                integer
          )
        JOIN
          samples s_ctl ON
            ((srv_map ->> dr.server_id::text)::integer, dr.sample_id) =
            (s_ctl.server_id, s_ctl.sample_id)
        ON CONFLICT DO NOTHING;
        GET DIAGNOSTICS row_proc = ROW_COUNT;
        rowcnt := rowcnt + row_proc;
        IF (rowcnt > 0 AND rowcnt % 1000 = 0) THEN
          RAISE NOTICE '%', format('Table %s processed: %s rows', imp_table_name, rowcnt);
        END IF;
      END LOOP; -- over data rows
    ELSE
      rows_processed := -1;
      RETURN; -- table not found
  END CASE; -- over table name
  rows_processed := rowcnt;
  RETURN;
END;
$$ LANGUAGE plpgsql;
