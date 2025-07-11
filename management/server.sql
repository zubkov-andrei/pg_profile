/* ========= Server functions ========= */

CREATE FUNCTION create_server(IN server name, IN server_connstr text, IN server_enabled boolean = TRUE,
IN max_sample_age integer = NULL, IN description text = NULL) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    server_exists     integer;
    sserver_id        integer;
BEGIN

    SELECT count(*) INTO server_exists FROM servers WHERE server_name=server;
    IF server_exists > 0 THEN
        RAISE 'Server already exists.';
    END IF;

    INSERT INTO servers(server_name,server_description,connstr,enabled,max_sample_age)
    VALUES (server,description,server_connstr,server_enabled,max_sample_age)
    RETURNING server_id INTO sserver_id;

    -- Subsample settings table entry
    INSERT INTO server_subsample(
      server_id,
      subsample_enabled)
    VALUES (
      sserver_id,
      null);

    /*
    * We might create server sections to avoid concurrency on tables
    */
    PERFORM create_server_partitions(sserver_id);

    RETURN sserver_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION create_server(IN server name, IN server_connstr text, IN server_enabled boolean,
IN max_sample_age integer, IN description text) IS 'Create a new server';

CREATE FUNCTION create_server_partitions(IN sserver_id integer) RETURNS integer
SET search_path=@extschema@ AS $$
DECLARE
    in_extension      boolean;
BEGIN
    -- Create last_stat_statements table partition
    EXECUTE format(
      'CREATE TABLE last_stat_statements_srv%1$s PARTITION OF last_stat_statements '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    -- PK constraint for new partition
    EXECUTE format(
      'ALTER TABLE last_stat_statements_srv%1$s '
      'ADD CONSTRAINT pk_last_stat_satements_srv%1$s PRIMARY KEY (server_id, sample_id, userid, datid, queryid, toplevel)',
      sserver_id);

    -- Create last_stat_kcache table partition
    EXECUTE format(
      'CREATE TABLE last_stat_kcache_srv%1$s PARTITION OF last_stat_kcache '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_kcache_srv%1$s '
      'ADD CONSTRAINT pk_last_stat_kcache_srv%1$s PRIMARY KEY (server_id, sample_id, datid, userid, queryid, toplevel), '
      'ADD CONSTRAINT fk_last_kcache_stmts_srv%1$s FOREIGN KEY '
        '(server_id, sample_id, datid, userid, queryid, toplevel) REFERENCES '
        'last_stat_statements_srv%1$s(server_id, sample_id, datid, userid, queryid, toplevel) '
        'ON DELETE CASCADE',
      sserver_id);

    -- Create last_stat_database table partition
    EXECUTE format(
      'CREATE TABLE last_stat_database_srv%1$s PARTITION OF last_stat_database '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_database_srv%1$s '
        'ADD CONSTRAINT pk_last_stat_database_srv%1$s PRIMARY KEY (server_id, sample_id, datid), '
        'ADD CONSTRAINT fk_last_stat_database_samples_srv%1$s '
          'FOREIGN KEY (server_id, sample_id) '
          'REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT',
        sserver_id);

    -- Create last_stat_tablespaces table partition
    EXECUTE format(
      'CREATE TABLE last_stat_tablespaces_srv%1$s PARTITION OF last_stat_tablespaces '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_tablespaces_srv%1$s '
        'ADD CONSTRAINT pk_last_stat_tablespaces_srv%1$s PRIMARY KEY (server_id, sample_id, tablespaceid), '
        'ADD CONSTRAINT fk_last_stat_tablespaces_samples_srv%1$s '
          'FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) '
          'ON DELETE RESTRICT',
        sserver_id);

    -- Create last_stat_tables table partition
    EXECUTE format(
      'CREATE TABLE last_stat_tables_srv%1$s PARTITION OF last_stat_tables '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_tables_srv%1$s '
        'ADD CONSTRAINT pk_last_stat_tables_srv%1$s '
          'PRIMARY KEY (server_id, sample_id, datid, relid), '
        'ADD CONSTRAINT fk_last_stat_tables_dat_srv%1$s '
          'FOREIGN KEY (server_id, sample_id, datid) '
          'REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT',
        sserver_id);

    -- Create last_stat_indexes table partition
    EXECUTE format(
      'CREATE TABLE last_stat_indexes_srv%1$s PARTITION OF last_stat_indexes '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_indexes_srv%1$s '
        'ADD CONSTRAINT pk_last_stat_indexes_srv%1$s '
          'PRIMARY KEY (server_id, sample_id, datid, indexrelid), '
        'ADD CONSTRAINT fk_last_stat_indexes_dat_srv%1$s '
        'FOREIGN KEY (server_id, sample_id, datid) '
          'REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT',
        sserver_id);

    -- Create last_stat_user_functions table partition
    EXECUTE format(
      'CREATE TABLE last_stat_user_functions_srv%1$s PARTITION OF last_stat_user_functions '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_user_functions_srv%1$s '
      'ADD CONSTRAINT pk_last_stat_user_functions_srv%1$s '
      'PRIMARY KEY (server_id, sample_id, datid, funcid), '
      'ADD CONSTRAINT fk_last_stat_user_functions_dat_srv%1$s '
        'FOREIGN KEY (server_id, sample_id, datid) '
        'REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT',
        sserver_id);

    -- Create last_stat_activity table partition
    EXECUTE format(
      'CREATE TABLE last_stat_activity_srv%1$s PARTITION OF last_stat_activity '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_activity_srv%1$s '
      'ADD CONSTRAINT pk_last_stat_activity_srv%1$s '
        'PRIMARY KEY (server_id, sample_id, pid, subsample_ts), '
      'ADD CONSTRAINT fk_last_stat_activity_sample_srv%1$s '
        'FOREIGN KEY (server_id, sample_id) '
        'REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT',
        sserver_id);

    -- Create last_stat_activity table partition
    EXECUTE format(
      'CREATE TABLE last_stat_activity_count_srv%1$s PARTITION OF last_stat_activity_count '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_activity_count_srv%1$s '
      'ADD CONSTRAINT fk_last_stat_activity_count_sample_srv%1$s '
        'FOREIGN KEY (server_id, sample_id) '
        'REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT',
        sserver_id);

--<extension_start>
    /*
    * Check if partition is already in our extension. This happens when function
    * is called during CREATE EXTENSION script execution
    */
    SELECT count(*) = 1 INTO in_extension
    FROM pg_depend dep
      JOIN pg_extension ext ON (dep.refobjid = ext.oid)
      JOIN pg_class rel ON (rel.oid = dep.objid AND rel.relkind= 'r')
    WHERE ext.extname='{pg_profile}'
      AND rel.relname = format('last_stat_statements_srv%1$s', sserver_id);

    IF NOT in_extension THEN
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_statements_srv%1$s',
        sserver_id);
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_kcache_srv%1$s',
        sserver_id);
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_database_srv%1$s',
        sserver_id);
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_tablespaces_srv%1$s',
        sserver_id);
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_tables_srv%1$s',
        sserver_id);
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_indexes_srv%1$s',
        sserver_id);
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_user_functions_srv%1$s',
        sserver_id);
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_activity_srv%1$s',
        sserver_id);
      EXECUTE format('ALTER EXTENSION {pg_profile} ADD TABLE last_stat_activity_count_srv%1$s',
        sserver_id);
    END IF;
--<extension_end>

    RETURN sserver_id;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION drop_server(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    del_rows    integer;
    dserver_id  integer;
BEGIN
    SELECT server_id INTO STRICT dserver_id FROM servers WHERE server_name = server;
    DELETE FROM bl_samples WHERE server_id = dserver_id;
--<extension_start>
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_kcache_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_statements_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_database_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_tables_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_indexes_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_tablespaces_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_user_functions_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_activity_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION {pg_profile} DROP TABLE last_stat_activity_count_srv%1$s',
      dserver_id);
--<extension_end>
    EXECUTE format(
      'DROP TABLE last_stat_kcache_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_statements_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_database_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_tables_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_indexes_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_tablespaces_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_user_functions_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_activity_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_activity_count_srv%1$s',
      dserver_id);
    DELETE FROM last_stat_cluster WHERE server_id = dserver_id;
    DELETE FROM last_stat_io WHERE server_id = dserver_id;
    DELETE FROM last_stat_slru WHERE server_id = dserver_id;
    DELETE FROM last_stat_wal WHERE server_id = dserver_id;
    DELETE FROM last_stat_archiver WHERE server_id = dserver_id;
    DELETE FROM sample_stat_tablespaces WHERE server_id = dserver_id;
    DELETE FROM tablespaces_list WHERE server_id = dserver_id;
    /*
     * We have several constraints that should be deferred to avoid
     * violation due to several cascade deletion branches
     */
    SET CONSTRAINTS
        fk_stat_indexes_indexes,
        fk_st_tablespaces_tablespaces,
        fk_st_tables_tables,
        fk_indexes_tables,
        fk_user_functions_functions,
        fk_stmt_list,
        fk_kcache_stmt_list,
        fk_statements_roles,
        fk_extension_versions_samples,
        fk_table_storage_parameters_samples,
        fk_table_storage_parameters_tables,
        fk_index_storage_parameters_samples,
        fk_index_storage_parameters_indexes
      DEFERRED;
    DELETE FROM samples WHERE server_id = dserver_id;
    DELETE FROM indexes_list WHERE server_id = dserver_id;
    DELETE FROM tables_list WHERE server_id = dserver_id;
    SET CONSTRAINTS
        fk_stat_indexes_indexes,
        fk_st_tablespaces_tablespaces,
        fk_st_tables_tables,
        fk_indexes_tables,
        fk_user_functions_functions,
        fk_stmt_list,
        fk_kcache_stmt_list,
        fk_statements_roles,
        fk_extension_versions_samples,
        fk_table_storage_parameters_samples,
        fk_table_storage_parameters_tables,
        fk_index_storage_parameters_samples,
        fk_index_storage_parameters_indexes
      IMMEDIATE;
    DELETE FROM servers WHERE server_id = dserver_id;
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION drop_server(IN server name) IS 'Drop a server';

CREATE FUNCTION rename_server(IN server name, IN server_new_name name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET server_name = server_new_name WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION rename_server(IN server name, IN server_new_name name) IS 'Rename existing server';

CREATE FUNCTION set_server_connstr(IN server name, IN server_connstr text) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET connstr = server_connstr WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_server_connstr(IN server name, IN server_connstr text) IS 'Update server connection string';

CREATE FUNCTION set_server_description(IN server name, IN description text) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET server_description = description WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_server_description(IN server name, IN description text) IS 'Update server description';

CREATE FUNCTION set_server_max_sample_age(IN server name, IN max_sample_age integer) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET max_sample_age = set_server_max_sample_age.max_sample_age WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_server_max_sample_age(IN server name, IN max_sample_age integer) IS 'Update server max_sample_age period';

CREATE FUNCTION enable_server(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET enabled = TRUE WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION enable_server(IN server name) IS 'Enable existing server (will be included in take_sample() call)';

CREATE FUNCTION set_server_subsampling(IN server name, IN subsample_enabled boolean,
  IN min_query_duration interval hour to second,
  IN min_xact_duration interval hour to second,
  IN min_xact_age integer,
  IN min_idle_xact_dur interval hour to second)
RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE server_subsample smp
    SET
      (subsample_enabled, min_query_dur, min_xact_dur, min_xact_age,
        min_idle_xact_dur) =
      (set_server_subsampling.subsample_enabled, set_server_subsampling.min_query_duration,
      set_server_subsampling.min_xact_duration, set_server_subsampling.min_xact_age,
      set_server_subsampling.min_idle_xact_dur)
    FROM servers s
    WHERE
      s.server_name = set_server_subsampling.server AND
      smp.server_id = s.server_id;

    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION set_server_subsampling(IN name, IN boolean,
  IN interval hour to second, IN interval hour to second, IN integer,
  IN interval hour to second)
IS 'Setup subsampling for a server';

CREATE FUNCTION set_server_setting(IN server name,
  IN setting text,
  IN value   text = NULL)
RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
    etext    text := '';
    edetail  text := '';
    econtext text := '';
    settings_template CONSTANT jsonb := '{"collect": {}}'::jsonb;
BEGIN
    IF set_server_setting.setting IN (
        'collect_pg_stat_statements',
        'collect_pg_wait_sampling',
        'collect_objects',
        'collect_vacuum_stats',
        'collect_relations',
        'collect_functions'
      )
    THEN
        BEGIN
            PERFORM set_server_setting.value::boolean;
        EXCEPTION
            WHEN OTHERS THEN
                BEGIN
                    GET STACKED DIAGNOSTICS etext = MESSAGE_TEXT,
                        edetail = PG_EXCEPTION_DETAIL,
                        econtext = PG_EXCEPTION_CONTEXT;
                    RAISE 'Value for collection conditions should be boolean: %',
                        etext
                        USING DETAIL = edetail;
                END;
        END;
        UPDATE servers SET srv_settings =
          jsonb_strip_nulls(jsonb_set(
            COALESCE(srv_settings, settings_template),
            ARRAY['collect', substr(set_server_setting.setting, 9)],
            to_jsonb(set_server_setting.value::boolean)
          ))
          WHERE server_name = server;
        GET DIAGNOSTICS upd_rows = ROW_COUNT;
        RETURN upd_rows;
    ELSE
        RAISE 'Unsupported setting';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION show_server_settings(IN server name)
RETURNS TABLE(scope text, setting text, value text)
SET search_path=@extschema@ AS $$
  SELECT
    scope.key AS scope,
    val.key::text AS setting,
    val.value::text AS value
  FROM
    servers s,
    jsonb_each(s.srv_settings) scope,
    jsonb_each(scope.value) val
  WHERE
    server_name = show_server_settings.server
$$ LANGUAGE sql;

CREATE FUNCTION disable_server(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET enabled = FALSE WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION disable_server(IN server name) IS 'Disable existing server (will be excluded from take_sample() call)';

CREATE FUNCTION set_server_db_exclude(IN server name, IN exclude_db name[]) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
BEGIN
    UPDATE servers SET db_exclude = exclude_db WHERE server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION set_server_db_exclude(IN server name, IN exclude_db name[]) IS 'Exclude databases from object stats collection. Useful in RDS.';

CREATE FUNCTION set_server_size_sampling(IN server name,
  IN window_start time with time zone = NULL,
  IN window_duration interval hour to second = NULL,
  IN sample_interval interval day to minute = NULL,
  IN collect_mode text = NULL)
RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    upd_rows integer;
    relsizes jsonb;
BEGIN
    -- We should validate collect_mode value
    IF COALESCE(LOWER(collect_mode), 'off') NOT IN ('on', 'off', 'schedule') THEN
      RAISE 'collect_mode can only be ''on'', ''off'' or ''schedule''';
    END IF;

    SELECT
      COALESCE(srv_settings #> ARRAY['relsizes'], '{}'::jsonb)
        INTO relsizes
    FROM servers
    WHERE server_name = server;

    IF (window_start IS NOT NULL) THEN
      relsizes := jsonb_set(
        relsizes,
        ARRAY['window_start'],
        to_jsonb(window_start)
      );
    END IF;
    IF (window_duration IS NOT NULL) THEN
      relsizes := jsonb_set(
        relsizes,
        ARRAY['window_duration'],
        to_jsonb(window_duration)
      );
    END IF;
    IF (sample_interval IS NOT NULL) THEN
      relsizes := jsonb_set(
        relsizes,
        ARRAY['sample_interval'],
        to_jsonb(sample_interval)
      );
    END IF;
    -- collect_mode can be set to null
    relsizes := jsonb_set(
      relsizes,
      ARRAY['collect_mode'],
      to_jsonb(LOWER(collect_mode))
    );

    UPDATE servers
    SET
      srv_settings = jsonb_set(
        COALESCE(srv_settings, '{}'::jsonb),
        ARRAY['relsizes'],
        relsizes)
    WHERE
      server_name = server;
    GET DIAGNOSTICS upd_rows = ROW_COUNT;
    RETURN upd_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION set_server_size_sampling(IN server name, IN window_start time with time zone,
  IN window_duration interval hour to second, IN sample_interval interval day to minute, IN collect_mode text)
IS 'Set relation sizes sampling settings for a server';

CREATE FUNCTION show_servers()
RETURNS TABLE(server_name name, connstr text, enabled boolean, max_sample_age integer, description text)
SET search_path=@extschema@ AS $$
DECLARE
  c_priv CURSOR FOR
    SELECT server_name, connstr, enabled, max_sample_age, server_description FROM servers;

  c_unpriv CURSOR FOR
    SELECT server_name, '<hidden>' as connstr, enabled, max_sample_age, server_description FROM servers;
BEGIN
  IF has_column_privilege('servers', 'connstr', 'SELECT') THEN
    FOR server_name, connstr, enabled, max_sample_age, description IN SELECT s.server_name, s.connstr, s.enabled, s.max_sample_age, s.server_description FROM servers s LOOP
      RETURN NEXT;
    END LOOP;
  ELSE
    FOR server_name, connstr, enabled, max_sample_age, description IN SELECT s.server_name, '<hidden>' as connstr, s.enabled, s.max_sample_age, s.server_description FROM servers s LOOP
      RETURN NEXT;
    END LOOP;
  END IF;
  RETURN;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION show_servers() IS 'Displays all servers';

CREATE FUNCTION show_servers_size_sampling()
RETURNS TABLE (
  server_name name,
  window_start time with time zone,
  window_end time with time zone,
  window_duration interval hour to second,
  sample_interval interval day to minute,
  collect_mode text
)
SET search_path=@extschema@ AS $$
  SELECT
    server_name,
    window_start,
    window_start + window_duration,
    window_duration,
    sample_interval,
    collect_mode
  FROM
    servers,
    jsonb_to_record(srv_settings #> '{relsizes}') AS (
      window_start    time with time zone,
      window_duration interval hour to second,
      sample_interval interval day to minute,
      collect_mode    text
    )
$$ LANGUAGE sql;
COMMENT ON FUNCTION show_servers_size_sampling() IS
  'Displays relation sizes sampling settings for all servers';

CREATE FUNCTION delete_samples(IN server_id integer, IN start_id integer = NULL, IN end_id integer = NULL)
RETURNS integer
SET search_path=@extschema@ AS $$
DECLARE
  smp_delcount  integer;
BEGIN
  /*
  * There could exist sample before deletion interval using
  * dictionary values having last_sample_id value in deletion
  * interval. So we need to move such last_sample_id values
  * to the past
  * We need to do so only if there is at last one sample before
  * deletion interval. Usually there won't any, because this
  * could happen only when there is a baseline in use or manual
  * deletion is performed.
  */
  IF (SELECT count(*) > 0 FROM samples s
    WHERE s.server_id = delete_samples.server_id AND sample_id < start_id) OR
    (SELECT count(*) > 0 FROM bl_samples bs
    WHERE bs.server_id = delete_samples.server_id
      AND bs.sample_id BETWEEN start_id AND end_id)
  THEN
    -- Statements list
    UPDATE stmt_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT queryid_md5, max(rf.sample_id) AS last_sample_id
      FROM
        sample_statements rf JOIN stmt_list lst USING (server_id, queryid_md5)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY queryid_md5
      ) new_lastids
    WHERE
      (uls.server_id, uls.queryid_md5) = (delete_samples.server_id, new_lastids.queryid_md5)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    UPDATE tablespaces_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT tablespaceid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_stat_tablespaces rf JOIN tablespaces_list lst
          USING (server_id, tablespaceid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY tablespaceid
      ) new_lastids
    WHERE
      (uls.server_id, uls.tablespaceid) =
      (delete_samples.server_id, new_lastids.tablespaceid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Roles
    UPDATE roles_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT userid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_statements rf JOIN roles_list lst
          USING (server_id, userid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY userid
      ) new_lastids
    WHERE
      (uls.server_id, uls.userid) =
      (delete_samples.server_id, new_lastids.userid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Indexes
    UPDATE indexes_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT indexrelid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_stat_indexes rf JOIN indexes_list lst
          USING (server_id, indexrelid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY indexrelid
      ) new_lastids
    WHERE
      (uls.server_id, uls.indexrelid) =
      (delete_samples.server_id, new_lastids.indexrelid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Tables
    UPDATE tables_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT relid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_stat_tables rf JOIN tables_list lst
          USING (server_id, relid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY relid
      ) new_lastids
    WHERE
      (uls.server_id, uls.relid) =
      (delete_samples.server_id, new_lastids.relid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Functions
    UPDATE funcs_list uls
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT funcid, max(rf.sample_id) AS last_sample_id
      FROM
        sample_stat_user_functions rf JOIN funcs_list lst
          USING (server_id, funcid)
        LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (rf.server_id, rf.sample_id) AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        rf.server_id = delete_samples.server_id
        AND lst.last_sample_id BETWEEN start_id AND end_id
        AND (rf.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY funcid
      ) new_lastids
    WHERE
      (uls.server_id, uls.funcid) =
      (delete_samples.server_id, new_lastids.funcid)
      AND uls.last_sample_id BETWEEN start_id AND end_id;

    -- Extensions history
    UPDATE extension_versions eve
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT
        ev.server_id,
        ev.datid,
        ev.extname,
        ev.first_seen,
        max(s.sample_id) as last_sample_id
      FROM extension_versions ev
      JOIN samples s ON (s.server_id = ev.server_id AND
        s.sample_time >= ev.first_seen AND s.sample_id <= ev.last_sample_id)
      LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (s.server_id, s.sample_id)
          AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        ev.server_id = delete_samples.server_id
        AND ev.last_sample_id BETWEEN start_id AND end_id
        AND (s.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY ev.server_id, ev.datid, ev.extname, ev.first_seen
      ) new_lastids
    WHERE
      (eve.server_id, eve.datid, eve.extname, eve.first_seen) =
      (delete_samples.server_id, new_lastids.datid, new_lastids.extname, new_lastids.first_seen);

    UPDATE table_storage_parameters tsp
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT
        tsp.server_id,
        tsp.datid,
        tsp.relid,
        tsp.first_seen,
        max(s.sample_id) as last_sample_id
      FROM table_storage_parameters tsp
      JOIN samples s ON (s.server_id = tsp.server_id AND
        s.sample_time >= tsp.first_seen AND s.sample_id <= tsp.last_sample_id)
      LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (s.server_id, s.sample_id)
          AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        tsp.server_id = delete_samples.server_id AND
        tsp.last_sample_id BETWEEN start_id AND end_id AND
        (s.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY tsp.server_id, tsp.datid, tsp.relid, tsp.first_seen
      ) new_lastids
    WHERE
      (tsp.server_id, tsp.datid, tsp.relid, tsp.first_seen) =
      (delete_samples.server_id, new_lastids.datid, new_lastids.relid, new_lastids.first_seen);

    UPDATE index_storage_parameters isp
    SET last_sample_id = new_lastids.last_sample_id
    FROM (
      SELECT
        isp.server_id,
        isp.datid,
        isp.relid,
        isp.indexrelid,
        isp.first_seen,
        max(s.sample_id) as last_sample_id
      FROM index_storage_parameters isp
      JOIN samples s ON (s.server_id = isp.server_id AND
        s.sample_time >= isp.first_seen AND s.sample_id <= isp.last_sample_id)
      LEFT JOIN bl_samples bl ON
          (bl.server_id, bl.sample_id) = (s.server_id, s.sample_id)
          AND bl.sample_id BETWEEN start_id AND end_id
      WHERE
        isp.server_id = delete_samples.server_id AND
        isp.last_sample_id BETWEEN start_id AND end_id AND
        (s.sample_id < start_id OR bl.sample_id IS NOT NULL)
      GROUP BY isp.server_id, isp.datid, isp.relid, isp.indexrelid, isp.first_seen
      ) new_lastids
    WHERE
      (isp.server_id, isp.datid, isp.relid, isp.indexrelid, isp.first_seen) =
      (delete_samples.server_id, new_lastids.datid, new_lastids.relid, new_lastids.indexrelid, new_lastids.first_seen);
  END IF;

  -- Delete specified samples without baseline samples
  SET CONSTRAINTS
      fk_stat_indexes_indexes,
      fk_st_tablespaces_tablespaces,
      fk_st_tables_tables,
      fk_indexes_tables,
      fk_user_functions_functions,
      fk_stmt_list,
      fk_kcache_stmt_list,
      fk_statements_roles,
      fk_extension_versions_samples,
      fk_table_storage_parameters_samples,
      fk_table_storage_parameters_tables,
      fk_index_storage_parameters_samples,
      fk_index_storage_parameters_indexes
    DEFERRED;
  DELETE FROM samples dsmp
  USING
    servers srv
    JOIN samples smp USING (server_id)
    LEFT JOIN bl_samples bls USING (server_id, sample_id)
  WHERE
    (dsmp.server_id, dsmp.sample_id) =
    (smp.server_id, smp.sample_id) AND
    smp.sample_id != srv.last_sample_id AND
    srv.server_id = delete_samples.server_id AND
    bls.sample_id IS NULL AND (
      (start_id IS NULL AND end_id IS NULL) OR
      smp.sample_id BETWEEN delete_samples.start_id AND delete_samples.end_id
    )
  ;
  GET DIAGNOSTICS smp_delcount := ROW_COUNT;
  SET CONSTRAINTS
      fk_stat_indexes_indexes,
      fk_st_tablespaces_tablespaces,
      fk_st_tables_tables,
      fk_indexes_tables,
      fk_user_functions_functions,
      fk_stmt_list,
      fk_kcache_stmt_list,
      fk_statements_roles,
      fk_extension_versions_samples,
      fk_table_storage_parameters_samples,
      fk_table_storage_parameters_tables,
      fk_index_storage_parameters_samples,
      fk_index_storage_parameters_indexes
    IMMEDIATE;

  IF smp_delcount > 0 THEN
    -- Delete obsolete values of postgres parameters
    DELETE FROM sample_settings ss
    USING (
      SELECT ss.server_id, max(first_seen) AS first_seen, setting_scope, name
      FROM sample_settings ss
      WHERE ss.server_id = delete_samples.server_id AND first_seen <=
        (SELECT min(sample_time) FROM samples s WHERE s.server_id = delete_samples.server_id)
      GROUP BY ss.server_id, setting_scope, name) AS ss_ref
    WHERE ss.server_id = ss_ref.server_id AND
      ss.setting_scope = ss_ref.setting_scope AND
      ss.name = ss_ref.name AND
      ss.first_seen < ss_ref.first_seen;
    -- Delete obsolete values of postgres parameters from previous versions of postgres on server
    DELETE FROM sample_settings ss
    WHERE ss.server_id = delete_samples.server_id AND first_seen <
      (SELECT min(first_seen) FROM sample_settings mss WHERE mss.server_id = delete_samples.server_id AND name = 'version' AND setting_scope = 2);
  END IF;

  RETURN smp_delcount;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION delete_samples(integer, integer, integer) IS
  'Manually deletes server samples for provided server identifier. By default deletes all samples';

CREATE FUNCTION delete_samples(IN server_name name, IN start_id integer = NULL, IN end_id integer = NULL)
RETURNS integer
SET search_path=@extschema@ AS $$
  SELECT delete_samples(server_id, start_id, end_id)
  FROM servers s
  WHERE s.server_name = delete_samples.server_name
$$ LANGUAGE sql;
COMMENT ON FUNCTION delete_samples(name, integer, integer) IS
  'Manually deletes server samples for provided server name. By default deletes all samples';

CREATE FUNCTION delete_samples(IN start_id integer = NULL, IN end_id integer = NULL)
RETURNS integer
SET search_path=@extschema@ AS $$
  SELECT delete_samples(server_id, start_id, end_id)
  FROM servers s
  WHERE s.server_name = 'local'
$$ LANGUAGE sql;
COMMENT ON FUNCTION delete_samples(integer, integer) IS
  'Manually deletes server samples of local server. By default deletes all samples';

CREATE FUNCTION delete_samples(IN server_name name, IN time_range tstzrange)
RETURNS integer
SET search_path=@extschema@ AS $$
  SELECT delete_samples(server_id, min(sample_id), max(sample_id))
  FROM servers srv JOIN samples smp USING (server_id)
  WHERE
    srv.server_name = delete_samples.server_name AND
    delete_samples.time_range @> smp.sample_time
  GROUP BY server_id
$$ LANGUAGE sql;
COMMENT ON FUNCTION delete_samples(name, tstzrange) IS
  'Manually deletes server samples for provided server name and time interval';

CREATE FUNCTION delete_samples(IN time_range tstzrange)
RETURNS integer
SET search_path=@extschema@ AS $$
  SELECT delete_samples('local', time_range);
$$ LANGUAGE sql;
COMMENT ON FUNCTION delete_samples(tstzrange) IS
  'Manually deletes server samples for time interval on local server';
