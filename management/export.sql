/* ==== Export and import functions ==== */

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

CREATE FUNCTION import_data(data regclass) RETURNS bigint
SET search_path=@extschema@ AS $$
DECLARE
  import_meta     jsonb;
  tables_list     jsonb;
  servers_list    jsonb; -- import servers list
  servers_map     jsonb = '[]'::jsonb; -- import server_id to local server_id mapping

  row_proc        bigint;
  rows_processed  bigint = 0;
  new_server_id   integer = null;

  r_result        RECORD;
BEGIN
  -- Get import metagata
  EXECUTE format('SELECT row_data::jsonb FROM %s WHERE section_id = 0',data)
  INTO STRICT import_meta;

  -- Check dump compatibility
  IF (SELECT count(*) < 1 FROM import_queries_version_order
      WHERE extension = import_meta ->> 'extension'
        AND version = import_meta ->> 'version')
  THEN
    RAISE 'Unsupported extension version: %', (import_meta ->> 'extension')||' '||(import_meta ->> 'version');
  END IF;

  -- Get import tables list
  EXECUTE format('SELECT row_data::jsonb FROM %s WHERE section_id = $1',data)
  USING (import_meta ->> 'tab_list_section')::integer
  INTO STRICT tables_list;
  -- Servers processing
  -- Get import servers list
  EXECUTE format($q$SELECT
      jsonb_agg(srvjs.row_data::jsonb)
    FROM
      jsonb_to_recordset($1) as tbllist(section_id integer, relname text),
      %1$s srvjs
    WHERE
      tbllist.relname = 'servers'
      AND srvjs.section_id = tbllist.section_id$q$,
    data)
  USING tables_list
  INTO STRICT servers_list;

  /*
   * Performing importing to local servers matching. We need to consider several cases:
   * - creation dates and system identifiers matched - we have a match
   * - creation dates and system identifiers don't match, but names matched - conflict as we can't create a new server
   * - nothing matched - a new local server is to be created
   * By the way, we'll populate servers_map structure, containing
   * a mapping between local and importing servers to use on data load.
   */
  FOR r_result IN EXECUTE format($q$SELECT
      imp_srv.server_name         imp_server_name,
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
      imp_srv.size_smp_interval   imp_size_smp_interval
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
          size_smp_interval   interval day to minute
        )
      JOIN jsonb_to_recordset($2) AS tbllist(section_id integer, relname text)
        ON (tbllist.relname = 'sample_settings')
      JOIN %s d ON
        (d.section_id = tbllist.section_id AND d.row_data->>'name' = 'system_identifier'
          AND (d.row_data->>'server_id')::integer = imp_srv.server_id)
      LEFT OUTER JOIN (
        SELECT
          server_id,
          server_name,
          server_created,
          reset_val as system_identifier
        FROM servers
          JOIN sample_settings USING (server_id)
        WHERE name = 'system_identifier') ls ON
        ((imp_srv.server_created = ls.server_created AND d.row_data->>'reset_val' = ls.system_identifier)
          OR imp_srv.server_name = ls.server_name)
    $q$,
    data)
  USING
    servers_list,
    tables_list
  LOOP
    IF r_result.imp_server_created = r_result.local_server_created AND
      r_result.imp_system_identifier = r_result.local_system_identifier
    THEN
      /* use this local server when matched by server creation time and system identifier */
      servers_map := jsonb_insert(servers_map,'{0}',
        jsonb_build_object('imp_srv_id',r_result.imp_server_id,'local_srv_id',r_result.local_server_id));
      /* Update local server if new last_sample_id is greatest*/
      UPDATE servers
      SET
        (
          db_exclude,
          connstr,
          max_sample_age,
          last_sample_id
        ) = (
          r_result.imp_server_db_exclude,
          r_result.imp_server_connstr,
          r_result.imp_server_max_sample_age,
          r_result.imp_server_last_sample_id
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
        size_smp_interval)
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
        r_result.imp_size_smp_interval
      )
      RETURNING server_id INTO new_server_id;
      servers_map := jsonb_insert(servers_map,'{0}',
        jsonb_build_object('imp_srv_id',r_result.imp_server_id,'local_srv_id',new_server_id));
    ELSE
      /* This shouldn't ever happen */
      RAISE 'Import and local servers matching exception';
    END IF;
  END LOOP;
  -- Load tables data
  FOR r_result IN (
    -- get most recent versions of queries for importing tables
    WITH RECURSIVE ver_order (extension,version,level) AS (
      SELECT
        extension,
        version,
        1 as level
      FROM import_queries_version_order
      WHERE extension = import_meta ->> 'extension'
        AND version = import_meta ->> 'version'
      UNION ALL
      SELECT
        vo.parent_extension,
        vo.parent_version,
        vor.level + 1 as level
      FROM import_queries_version_order vo
        JOIN ver_order vor ON
          ((vo.extension, vo.version) = (vor.extension, vor.version))
      WHERE vo.parent_version IS NOT NULL
    )
    SELECT
      q.query,
      tbllist.section_id as section_id,
      tbllist.relname
    FROM
      ver_order vo JOIN
      (SELECT min(o.level) as level,q.extension, q.relname FROM ver_order o
      JOIN import_queries q ON (o.extension, o.version) = (q.extension, q.from_version)
      GROUP BY q.extension, q.relname) as min_level ON
        (vo.extension,vo.level) = (min_level.extension,min_level.level)
      JOIN import_queries q ON
        (q.extension,q.from_version,q.relname) = (vo.extension,vo.version,min_level.relname)
      RIGHT OUTER JOIN jsonb_to_recordset(tables_list) as tbllist(section_id integer, relname text) ON
        (tbllist.relname = q.relname)
    WHERE tbllist.relname NOT IN ('servers')
  )
  LOOP
    -- Forgotten query for table check
    IF r_result.query IS NULL THEN
      RAISE 'There is no import query for relation %', r_result.relname;
    END IF;
    -- execute load query for each import relation
    EXECUTE
      format(r_result.query,
        data)
    USING
      servers_map,
      r_result.section_id;
    GET DIAGNOSTICS row_proc = ROW_COUNT;
    rows_processed := rows_processed + row_proc;
  END LOOP;

  RETURN rows_processed;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION import_data(data regclass) IS 'Import sample data from table, exported by export_data function';
