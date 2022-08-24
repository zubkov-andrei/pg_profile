CREATE FUNCTION get_report_context(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN description text = NULL,
  IN start2_id integer = NULL, IN end2_id integer = NULL)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
  report_context  jsonb;
  r_result    RECORD;

  qlen_limit  integer;
  topn        integer;

  start1_time text;
  end1_time   text;
  start2_time text;
  end2_time   text;
BEGIN
    ASSERT num_nulls(start1_id, end1_id) = 0, 'At least first interval bounds is necessary';

    -- Getting query length limit setting
    BEGIN
        qlen_limit := current_setting('{pg_profile}.max_query_length')::integer;
    EXCEPTION
        WHEN OTHERS THEN qlen_limit := 20000;
    END;

    -- Getting TopN setting
    BEGIN
        topn := current_setting('{pg_profile}.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Populate report settings
    -- Check if all samples of requested interval are available
    IF (
      SELECT count(*) != end1_id - start1_id + 1 FROM samples
      WHERE server_id = sserver_id AND sample_id BETWEEN start1_id AND end1_id
    ) THEN
      RAISE 'Not enough samples between %',
        format('%s AND %s', start1_id, end1_id);
    END IF;

    -- Get report times
    SELECT sample_time::text INTO STRICT start1_time FROM samples
    WHERE (server_id,sample_id) = (sserver_id,start1_id);
    SELECT sample_time::text INTO STRICT end1_time FROM samples
    WHERE (server_id,sample_id) = (sserver_id,end1_id);

    IF num_nulls(start2_id, end2_id) = 2 THEN
      report_context := jsonb_build_object(
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
        'statstatements',profile_checkavail_statstatements(sserver_id, start1_id, end1_id),
        'planning_times',profile_checkavail_planning_times(sserver_id, start1_id, end1_id),
        'wait_sampling_tot',profile_checkavail_wait_sampling_total(sserver_id, start1_id, end1_id),
        'io_times',profile_checkavail_io_times(sserver_id, start1_id, end1_id),
        'statement_wal_bytes',profile_checkavail_stmt_wal_bytes(sserver_id, start1_id, end1_id),
        'wal_stats',profile_checkavail_walstats(sserver_id, start1_id, end1_id),
        'sess_stats',profile_checkavail_sessionstats(sserver_id, start1_id, end1_id),
        'function_stats',profile_checkavail_functions(sserver_id, start1_id, end1_id),
        'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start1_id, end1_id),
        'kcachestatements',profile_checkavail_rusage(sserver_id,start1_id,end1_id),
        'rusage_planstats',profile_checkavail_rusage_planstats(sserver_id,start1_id,end1_id)
      ),
      'report_properties',jsonb_build_object(
        'interval_duration_sec',
          (SELECT extract(epoch FROM e.sample_time - s.sample_time)
          FROM samples s JOIN samples e USING (server_id)
          WHERE e.sample_id=end1_id and s.sample_id=start1_id
            AND server_id = sserver_id),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
          ), false),
        'topn', topn,
        'max_query_length', qlen_limit,
        'start1_id', start1_id,
        'end1_id', end1_id,
        'report_start1', start1_time,
        'report_end1', end1_time
        )
      );
    ELSIF num_nulls(start2_id, end2_id) = 0 THEN
      -- Get report times
      SELECT sample_time::text INTO STRICT start2_time FROM samples
      WHERE (server_id,sample_id) = (sserver_id,start2_id);
      SELECT sample_time::text INTO STRICT end2_time FROM samples
      WHERE (server_id,sample_id) = (sserver_id,end2_id);
      -- Check if all samples of requested interval are available
      IF (
        SELECT count(*) != end2_id - start2_id + 1 FROM samples
        WHERE server_id = sserver_id AND sample_id BETWEEN start2_id AND end2_id
      ) THEN
        RAISE 'Not enough samples between %',
          format('%s AND %s', start2_id, end2_id);
      END IF;
      report_context := jsonb_build_object(
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
        'title1',format('title="(%s - %s)"',start1_time, end1_time),
        'title2',format('title="(%s - %s)"',start2_time, end2_time)
        ),
      'report_features',jsonb_build_object(
        'statstatements',profile_checkavail_statstatements(sserver_id, start1_id, end1_id) OR
          profile_checkavail_statstatements(sserver_id, start2_id, end2_id),
        'planning_times',profile_checkavail_planning_times(sserver_id, start1_id, end1_id) OR
          profile_checkavail_planning_times(sserver_id, start2_id, end2_id),
        'wait_sampling_tot',profile_checkavail_wait_sampling_total(sserver_id, start1_id, end1_id) OR
          profile_checkavail_wait_sampling_total(sserver_id, start2_id, end2_id),
        'io_times',profile_checkavail_io_times(sserver_id, start1_id, end1_id) OR
          profile_checkavail_io_times(sserver_id, start2_id, end2_id),
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
        'max_query_length', qlen_limit,
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ), false),

        'topn', topn,
        'max_query_length', qlen_limit,

        'start1_id', start1_id,
        'end1_id', end1_id,
        'report_start1', start1_time,
        'report_end1', end1_time,

        'start2_id', start2_id,
        'end2_id', end2_id,
        'report_start2', start2_time,
        'report_end2', end2_time
        )
      );
    ELSE
      RAISE 'Two bounds must be specified for second interval';
    END IF;

    -- Server name and description
    SELECT server_name, server_description INTO STRICT r_result
    FROM servers WHERE server_id = sserver_id;
    report_context := jsonb_set(report_context, '{report_properties,server_name}',
      to_jsonb(r_result.server_name)
    );
    IF r_result.server_description IS NOT NULL AND r_result.server_description != ''
    THEN
      report_context := jsonb_set(report_context, '{report_properties,server_description}',
        to_jsonb(format(
          '<p>%s</p>',
          r_result.server_description
        ))
      );
    ELSE
      report_context := jsonb_set(report_context, '{report_properties,server_description}',to_jsonb(''::text));
    END IF;
    -- Report description
    IF description IS NOT NULL AND description != '' THEN
      report_context := jsonb_set(report_context, '{report_properties,description}',
        to_jsonb(format(
          '<h2>Report description</h2><p>%s</p>',
          description
        ))
      );
    ELSE
      report_context := jsonb_set(report_context, '{report_properties,description}',to_jsonb(''::text));
    END IF;
    -- Version substitution
    IF (SELECT count(*) = 1 FROM pg_catalog.pg_extension WHERE extname = '{pg_profile}') THEN
      SELECT extversion INTO STRICT r_result FROM pg_catalog.pg_extension WHERE extname = '{pg_profile}';
      report_context := jsonb_set(report_context, '{report_properties,pgprofile_version}',
        to_jsonb(r_result.extversion)
      );
--<manual_start>
    ELSE
      report_context := jsonb_set(report_context, '{report_properties,pgprofile_version}',
        to_jsonb('{extension_version}'::text)
      );
--<manual_end>
    END IF;
  RETURN report_context;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION get_report_template(IN report_context jsonb, IN report_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
  tpl         text = NULL;

  c_tpl_sbst  CURSOR (template text, type text) FOR
  SELECT DISTINCT s[1] AS type, s[2] AS item
  FROM regexp_matches(template, '{('||type||'):'||$o$(\w+)}$o$,'g') AS s;

  r_result    RECORD;
BEGIN
  SELECT static_text INTO STRICT tpl
  FROM report r JOIN report_static rs ON (rs.static_name = r.template)
  WHERE r.report_id = get_report_template.report_id;

  ASSERT tpl IS NOT NULL, 'Report template not found';
  -- Static content first
  -- Not found static placeholders silently removed
  WHILE strpos(tpl, '{static:') > 0 LOOP
    FOR r_result IN c_tpl_sbst(tpl, 'static') LOOP
      IF r_result.type = 'static' THEN
        tpl := replace(tpl, format('{%s:%s}', r_result.type, r_result.item),
          COALESCE((SELECT static_text FROM report_static WHERE static_name = r_result.item), '')
        );
      END IF;
    END LOOP; -- over static substitutions
  END LOOP; -- over static placeholders

  -- Properties substitution next
  WHILE strpos(tpl, '{properties:') > 0 LOOP
    FOR r_result IN c_tpl_sbst(tpl, 'properties') LOOP
      IF r_result.type = 'properties' THEN
        ASSERT report_context #>> ARRAY['report_properties', r_result.item] IS NOT NULL,
          'Property % not found',
          format('{%s,$%s}', r_result.type, r_result.item);
        tpl := replace(tpl, format('{%s:%s}', r_result.type, r_result.item),
          report_context #>> ARRAY['report_properties', r_result.item]
        );
      END IF;
    END LOOP; -- over properties substitutions
  END LOOP; -- over properties placeholders
  ASSERT tpl IS NOT NULL, 'Report template lost during substitution';

  RETURN tpl;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION init_report_temp_tables(IN report_context jsonb, IN sserver_id integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  start1_id   integer = (report_context #>> '{report_properties,start1_id}')::integer;
  start2_id   integer = (report_context #>> '{report_properties,start2_id}')::integer;
  end1_id     integer = (report_context #>> '{report_properties,end1_id}')::integer;
  end2_id     integer = (report_context #>> '{report_properties,end2_id}')::integer;
BEGIN
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

    ANALYZE top_statements1;
    ANALYZE top_tables1;
    ANALYZE top_indexes1;
    ANALYZE top_io_tables1;
    ANALYZE top_io_indexes1;
    ANALYZE top_functions1;
    ANALYZE top_kcache_statements1;

    IF jsonb_extract_path_text(report_context, 'report_features', 'wait_sampling_tot')::boolean THEN
      CREATE TEMPORARY TABLE wait_sampling_total_stats1 AS
      SELECT * FROM wait_sampling_total_stats(sserver_id, start1_id, end1_id);
      ANALYZE wait_sampling_total_stats1;
    END IF;

    IF num_nulls(start2_id, end2_id) = 0 THEN
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

      ANALYZE top_statements2;
      ANALYZE top_tables2;
      ANALYZE top_indexes2;
      ANALYZE top_io_tables2;
      ANALYZE top_io_indexes2;
      ANALYZE top_functions2;
      ANALYZE top_kcache_statements2;
      IF jsonb_extract_path_text(report_context, 'report_features', 'wait_sampling_tot')::boolean THEN
        CREATE TEMPORARY TABLE wait_sampling_total_stats2 AS
        SELECT * FROM wait_sampling_total_stats(sserver_id, start2_id, end2_id);
        ANALYZE wait_sampling_total_stats2;
      END IF;

    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION cleanup_report_temp_tables(IN report_context jsonb, IN sserver_id integer)
RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  start2_id   integer = (report_context #>> '{report_properties,start2_id}')::integer;
  end2_id     integer = (report_context #>> '{report_properties,end2_id}')::integer;
BEGIN
  DROP TABLE top_statements1;
  DROP TABLE top_tables1;
  DROP TABLE top_indexes1;
  DROP TABLE top_io_tables1;
  DROP TABLE top_io_indexes1;
  DROP TABLE top_functions1;
  DROP TABLE top_kcache_statements1;
  IF jsonb_extract_path_text(report_context, 'report_features', 'wait_sampling_tot')::boolean THEN
    DROP TABLE wait_sampling_total_stats1;
  END IF;
  IF num_nulls(start2_id, end2_id) = 0 THEN
    DROP TABLE top_statements2;
    DROP TABLE top_tables2;
    DROP TABLE top_indexes2;
    DROP TABLE top_io_tables2;
    DROP TABLE top_io_indexes2;
    DROP TABLE top_functions2;
    DROP TABLE top_kcache_statements2;
    IF jsonb_extract_path_text(report_context, 'report_features', 'wait_sampling_tot')::boolean THEN
      DROP TABLE wait_sampling_total_stats2;
    END IF;
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION template_populate_sections(IN report_context jsonb, IN sserver_id integer,
  IN template text, IN report_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    c_sections CURSOR(init_depth integer) FOR
    WITH RECURSIVE search_tree(report_id, sect_id, parent_sect_id,
      toc_cap, tbl_cap, feature, function_name, href, content, depth,
      sect_ord) AS
    (
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.feature,
          rs.function_name,
          rs.href,
          rs.content,
          init_depth,
          ARRAY[s_ord]
        FROM report_struct rs
        WHERE rs.report_id = template_populate_sections.report_id AND parent_sect_id IS NULL
      UNION ALL
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.feature,
          rs.function_name,
          rs.href,
          rs.content,
          st.depth + 1,
          sect_ord || s_ord
        FROM report_struct rs JOIN search_tree st ON
          (rs.report_id, rs.parent_sect_id) =
          (st.report_id, st.sect_id)
    )
    SELECT * FROM search_tree ORDER BY sect_ord;

    toc_t       text := '';
    sect_t      text := '';
    tpl         text;
    cur_depth   integer := 1;
    func_output text := NULL;
    skip_depth  integer = 10;
BEGIN
    FOR r_result IN c_sections(2) LOOP
      ASSERT r_result.depth BETWEEN 1 AND 5, 'Section depth is not in 1 - 5';

      -- Check if section feature enabled in report
      IF r_result.depth > skip_depth THEN
        CONTINUE;
      ELSE
        skip_depth := 10;
      END IF;
      IF r_result.feature IS NOT NULL AND (
          NOT jsonb_extract_path_text(report_context, 'report_features', r_result.feature)::boolean
        OR (
            left(r_result.feature, 1) = '!' AND
            jsonb_extract_path_text(report_context, 'report_features', ltrim(r_result.feature,'!'))::boolean
          )
        )
      THEN
        skip_depth := r_result.depth;
        CONTINUE;
      END IF;

      IF r_result.depth != cur_depth THEN
        IF r_result.depth > cur_depth THEN
          toc_t := toc_t || repeat('<ul>', r_result.depth - cur_depth);
        END IF;
        IF r_result.depth < cur_depth THEN
          toc_t := toc_t || repeat('</ul>', cur_depth - r_result.depth);
        END IF;
        cur_depth := r_result.depth;
      END IF;

      func_output := '';

      -- Executing function of report section if requested
      IF r_result.function_name IS NOT NULL THEN
        IF (SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = '{pg_profile}') THEN
          -- Fail when requested function doesn't exists in extension
          ASSERT (
            SELECT count(*) = 1
            FROM
              pg_catalog.pg_proc f JOIN pg_catalog.pg_depend dep
                ON (f.oid,'e') = (dep.objid, dep.deptype)
              JOIN pg_catalog.pg_extension ext
                ON (ext.oid = dep.refobjid)
            WHERE
              f.proname = r_result.function_name
              AND ext.extname = '{pg_profile}'
              AND pg_catalog.pg_get_function_result(f.oid) =
                'text'
              AND pg_catalog.pg_get_function_arguments(f.oid) =
                'report_context jsonb, sserver_id integer'
            ),
            'Report requested function % not found', r_result.function_name;
        ELSE
          -- When not installed as an extension check only the function existance
          ASSERT (
            SELECT count(*) = 1
            FROM
              pg_catalog.pg_proc f
            WHERE
              f.proname = r_result.function_name
              AND pg_catalog.pg_get_function_result(f.oid) =
                'text'
              AND pg_catalog.pg_get_function_arguments(f.oid) =
                'report_context jsonb, sserver_id integer'
            ),
            format('Report requested function %s not found', r_result.function_name);
        END IF;

        -- Set report context
        IF r_result.href IS NOT NULL THEN
          report_context := jsonb_set(report_context, '{report_properties,sect_href}',
            to_jsonb(r_result.href));
        ELSE
          report_context := report_context #- '{report_properties,sect_href}';
        END IF;
        IF r_result.tbl_cap IS NOT NULL THEN
          report_context := jsonb_set(report_context, '{report_properties,sect_tbl_cap}',
            to_jsonb(r_result.tbl_cap));
        ELSE
          report_context := report_context #- '{report_properties,sect_tbl_cap}';
        END IF;

        ASSERT report_context IS NOT NULL, 'Lost report context';
        -- Execute function for our report and get a section
        EXECUTE format('SELECT %I($1,$2)', r_result.function_name)
        INTO func_output
        USING
          report_context,
          sserver_id
        ;
      END IF; -- report section contains a function

      -- Insert an entry to table of contents
      IF r_result.toc_cap IS NOT NULL AND trim(r_result.toc_cap) != '' THEN
        IF r_result.function_name IS NULL OR
          (func_output IS NOT NULL AND func_output != '') THEN
            toc_t := toc_t || format(
              '<li><a HREF="#%s">%s</a></li>',
              COALESCE(r_result.href, r_result.function_name),
              r_result.toc_cap
            );
        END IF;
      END IF;

      -- Adding table title
      IF r_result.function_name IS NULL OR
        (func_output IS NOT NULL AND func_output != '') THEN
        tpl := COALESCE(r_result.content, '');
        -- Processing section header
        IF r_result.tbl_cap IS NOT NULL THEN
          IF strpos(tpl, '{header}') > 0 THEN
            tpl := replace(
              tpl,
              '{header}',
              format(
                '<H%1$s><a NAME="%2$s">%3$s</a></H%1$s>',
                r_result.depth,
                COALESCE(r_result.href, r_result.function_name),
                r_result.tbl_cap
              )
            );
          ELSE
            tpl := format(
              '<H%1$s><a NAME="%2$s">%3$s</a></H%1$s>',
              r_result.depth,
              COALESCE(r_result.href, r_result.function_name),
              r_result.tbl_cap
            ) || tpl;
          END IF;
        END IF;

        -- Processing function output
        IF strpos(tpl, '{func_output}') > 0 THEN
          tpl := replace(tpl,
            '{func_output}',
            COALESCE(func_output, '')
          );
        ELSE
          tpl := tpl || COALESCE(func_output, '');
        END IF;
        sect_t := sect_t || tpl;
      END IF;

    END LOOP; -- Over recursive sections query

    -- Closing TOC <ul> tags based on final depth
    toc_t := toc_t || repeat('</ul>', cur_depth);

    template := replace(template, '{report:toc}', toc_t);
    template := replace(template, '{report:sect}', sect_t);

    RETURN template;
END;
$$ LANGUAGE plpgsql;
