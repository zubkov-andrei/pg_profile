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
        'dbstats_reset', profile_checkavail_dbstats_reset(sserver_id, start1_id, end1_id),
        'stmt_cnt_range', profile_checkavail_stmt_cnt(sserver_id, start1_id, end1_id),
        'stmt_cnt_all', profile_checkavail_stmt_cnt(sserver_id, 0, 0),
        'cluster_stats_reset', profile_checkavail_cluster_stats_reset(sserver_id, start1_id, end1_id),
        'wal_stats_reset', profile_checkavail_wal_stats_reset(sserver_id, start1_id, end1_id),
        'statstatements',profile_checkavail_statstatements(sserver_id, start1_id, end1_id),
        'planning_times',profile_checkavail_planning_times(sserver_id, start1_id, end1_id),
        'wait_sampling_tot',profile_checkavail_wait_sampling_total(sserver_id, start1_id, end1_id),
        'io_times',profile_checkavail_io_times(sserver_id, start1_id, end1_id),
        'statement_wal_bytes',profile_checkavail_stmt_wal_bytes(sserver_id, start1_id, end1_id),
        'statements_top_temp', profile_checkavail_top_temp(sserver_id, start1_id, end1_id),
        'statements_temp_io_times', profile_checkavail_statements_temp_io_times(sserver_id, start1_id, end1_id),
        'wal_stats',profile_checkavail_walstats(sserver_id, start1_id, end1_id),
        'sess_stats',profile_checkavail_sessionstats(sserver_id, start1_id, end1_id),
        'function_stats',profile_checkavail_functions(sserver_id, start1_id, end1_id),
        'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start1_id, end1_id),
        'kcachestatements',profile_checkavail_rusage(sserver_id,start1_id,end1_id),
        'rusage_planstats',profile_checkavail_rusage_planstats(sserver_id,start1_id,end1_id),
        'statements_jit_stats',profile_checkavail_statements_jit_stats(sserver_id, start1_id, end1_id),
        'top_tables_dead', profile_checkavail_tbl_top_dead(sserver_id,start1_id,end1_id),
        'top_tables_mods', profile_checkavail_tbl_top_mods(sserver_id,start1_id,end1_id),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
          ), false)
        ),
      'report_properties',jsonb_build_object(
        'interval_duration_sec',
          (SELECT extract(epoch FROM e.sample_time - s.sample_time)
          FROM samples s JOIN samples e USING (server_id)
          WHERE e.sample_id=end1_id and s.sample_id=start1_id
            AND server_id = sserver_id),
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
        'dbstats_reset', profile_checkavail_dbstats_reset(sserver_id, start1_id, end1_id) OR
          profile_checkavail_dbstats_reset(sserver_id, start2_id, end2_id),
        'stmt_cnt_range', profile_checkavail_stmt_cnt(sserver_id, start1_id, end1_id) OR
          profile_checkavail_stmt_cnt(sserver_id, start2_id, end2_id),
        'stmt_cnt_all', profile_checkavail_stmt_cnt(sserver_id, 0, 0),
        'cluster_stats_reset', profile_checkavail_cluster_stats_reset(sserver_id, start1_id, end1_id) OR
          profile_checkavail_cluster_stats_reset(sserver_id, start2_id, end2_id),
        'wal_stats_reset', profile_checkavail_wal_stats_reset(sserver_id, start1_id, end1_id) OR
          profile_checkavail_wal_stats_reset(sserver_id, start2_id, end2_id),
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
        'statements_top_temp', profile_checkavail_top_temp(sserver_id, start1_id, end1_id) OR
            profile_checkavail_top_temp(sserver_id, start2_id, end2_id),
        'statements_temp_io_times', profile_checkavail_statements_temp_io_times(sserver_id, start1_id, end1_id) OR
            profile_checkavail_statements_temp_io_times(sserver_id, start2_id, end2_id),
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
          profile_checkavail_rusage_planstats(sserver_id, start2_id, end2_id),
        'statements_jit_stats',profile_checkavail_statements_jit_stats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_statements_jit_stats(sserver_id, start2_id, end2_id),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ), false)
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

CREATE FUNCTION get_report_datasets(IN report_context jsonb, IN sserver_id integer)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
  start1_id   integer = (report_context #>> '{report_properties,start1_id}')::integer;
  start2_id   integer = (report_context #>> '{report_properties,start2_id}')::integer;
  end1_id     integer = (report_context #>> '{report_properties,end1_id}')::integer;
  end2_id     integer = (report_context #>> '{report_properties,end2_id}')::integer;

  datasets    jsonb = '{}';
  dataset     jsonb;
  queries_set jsonb = '[]';
  r_result    RECORD;
BEGIN
  IF num_nulls(start1_id, end1_id) = 0 AND num_nulls(start2_id, end2_id) > 0 THEN
    -- Regular report
    -- database statistics dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM dbstats_format(sserver_id, start1_id, end1_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{dbstat}', dataset);

    IF (report_context #> '{report_features,dbstats_reset}')::boolean THEN
      -- dbstats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM dbstats_reset_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{dbstats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,statstatements}')::boolean THEN
      -- statements by database dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM statements_dbstats_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{statements_dbstats}', dataset);
    END IF;

    IF (report_context #> '{report_features,stmt_cnt_range}')::boolean THEN
      -- statements count of max for interval
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM stmt_cnt_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stmt_cnt_range}', dataset);
    END IF;

    IF (report_context #> '{report_features,stmt_cnt_all}')::boolean THEN
      -- statements count of max for all samples
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM stmt_cnt_format(sserver_id, 0, 0)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stmt_cnt_all}', dataset);
    END IF;

    -- cluster statistics dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM cluster_stats_format(sserver_id, start1_id, end1_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{cluster_stats}', dataset);

    IF (report_context #> '{report_features,cluster_stats_reset}')::boolean THEN
      -- cluster stats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM cluster_stats_reset_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{cluster_stats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,wal_stats_reset}')::boolean THEN
      -- WAL stats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wal_stats_reset_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wal_stats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,wal_stats}')::boolean THEN
      -- WAL stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wal_stats_format(sserver_id, start1_id, end1_id,
            (report_context #>> '{report_properties,interval_duration_sec}')::numeric)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wal_stats}', dataset);
    END IF;
    
    -- Tablespace stats dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM tablespace_stats_format(sserver_id, start1_id, end1_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{tablespace_stats}', dataset);

    IF (report_context #> '{report_features,wait_sampling_tot}')::boolean THEN
      -- Wait totals dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wait_sampling_total_stats_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wait_sampling_total_stats}', dataset);
      -- Wait events dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_wait_sampling_events_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wait_sampling_events}', dataset);
    END IF;
    
    IF (report_context #> '{report_features,statstatements}')::boolean THEN
      -- Statement stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_statements_format(sserver_id, start1_id, end1_id)
          WHERE least(
              ord_total_time,
              ord_plan_time,
              ord_exec_time,
              ord_calls,
              ord_io_time,
              ord_shared_blocks_fetched,
              ord_shared_blocks_read,
              ord_shared_blocks_dirt,
              ord_shared_blocks_written,
              ord_wal,
              ord_temp,
              ord_jit
            ) <= (report_context #>> '{report_properties,topn}')::numeric
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
        queries_set := queries_set || jsonb_build_object(
          'userid', r_result.userid,
          'datid', r_result.datid,
          'queryid', r_result.queryid
        );
      END LOOP;
      datasets := jsonb_set(datasets, '{top_statements}', dataset);
    END IF;

    IF (report_context #> '{report_features,kcachestatements}')::boolean THEN
      -- Statement rusage stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_rusage_statements_format(sserver_id, start1_id, end1_id)
          WHERE least(
              ord_cpu_time,
              ord_io_bytes
            ) <= (report_context #>> '{report_properties,topn}')::numeric
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
        queries_set := queries_set || jsonb_build_object(
          'userid', r_result.userid,
          'datid', r_result.datid,
          'queryid', r_result.queryid
        );
      END LOOP;
      datasets := jsonb_set(datasets, '{top_rusage_statements}', dataset);
    END IF;

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM report_queries_format(sserver_id, queries_set, start1_id, end1_id, NULL, NULL)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{queries}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_tables_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_dml,
          ord_seq_scan,
          ord_upd,
          ord_growth,
          ord_vac,
          ord_anl
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_tables}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_io_tables_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_read,
          ord_fetch
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_io_tables}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_io_indexes_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_read,
          ord_fetch
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_io_indexes}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_indexes_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_growth,
          ord_unused,
          ord_vac
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_indexes}', dataset);
    
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_functions_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_time,
          ord_calls,
          ord_trgtime
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_functions}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_tbl_last_sample_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_dead,
          ord_mod
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_tbl_last_sample}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM settings_format(sserver_id, start1_id, end1_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{settings}', dataset);
  ELSIF num_nulls(start1_id, end1_id, start2_id, end2_id) = 0 THEN
    -- Differential report
    -- database statistics dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM dbstats_format_diff(sserver_id, start1_id, end1_id,
          start2_id, end2_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{dbstat}', dataset);

    IF (report_context #> '{report_features,dbstats_reset}')::boolean THEN
      -- dbstats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM dbstats_reset_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{dbstats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,statstatements}')::boolean THEN
      -- statements by database dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM statements_dbstats_format_diff(sserver_id, start1_id, end1_id,
                 start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{statements_dbstats}', dataset);
    END IF;

    IF (report_context #> '{report_features,stmt_cnt_range}')::boolean THEN
      -- statements count of max for interval
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM stmt_cnt_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stmt_cnt_range}', dataset);
    END IF;

    IF (report_context #> '{report_features,stmt_cnt_all}')::boolean THEN
      -- statements count of max for all samples
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM stmt_cnt_format(sserver_id, 0, 0)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stmt_cnt_all}', dataset);
    END IF;

    -- cluster statistics dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM cluster_stats_format_diff(sserver_id, start1_id, end1_id,
                 start2_id, end2_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{cluster_stats}', dataset);

    IF (report_context #> '{report_features,cluster_stats_reset}')::boolean THEN
      -- cluster stats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM cluster_stats_reset_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{cluster_stats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,wal_stats_reset}')::boolean THEN
      -- WAL stats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wal_stats_reset_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wal_stats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,wal_stats}')::boolean THEN
      -- WAL stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wal_stats_format_diff(sserver_id,
            start1_id, end1_id, start2_id, end2_id,
            (report_context #>> '{report_properties,interval1_duration_sec}')::numeric,
            (report_context #>> '{report_properties,interval2_duration_sec}')::numeric)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wal_stats}', dataset);
    END IF;

    -- Tablespace stats dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM tablespace_stats_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{tablespace_stats}', dataset);
    
    IF (report_context #> '{report_features,wait_sampling_tot}')::boolean THEN
      -- Wait totals dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wait_sampling_total_stats_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wait_sampling_total_stats}', dataset);
      -- Wait events dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_wait_sampling_events_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wait_sampling_events}', dataset);
    END IF;

    IF (report_context #> '{report_features,statstatements}')::boolean THEN
      -- Statement stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_statements_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
          WHERE least(
              ord_total_time,
              ord_plan_time,
              ord_exec_time,
              ord_calls,
              ord_io_time,
              ord_shared_blocks_fetched,
              ord_shared_blocks_read,
              ord_shared_blocks_dirt,
              ord_shared_blocks_written,
              ord_wal,
              ord_temp,
              ord_jit
            ) <= (report_context #>> '{report_properties,topn}')::numeric
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
        queries_set := queries_set || jsonb_build_object(
          'userid', r_result.userid,
          'datid', r_result.datid,
          'queryid', r_result.queryid
        );
      END LOOP;
      datasets := jsonb_set(datasets, '{top_statements}', dataset);
    END IF;

    IF (report_context #> '{report_features,kcachestatements}')::boolean THEN
      -- Statement rusage stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_rusage_statements_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
          WHERE least(
              ord_cpu_time,
              ord_io_bytes
            ) <= (report_context #>> '{report_properties,topn}')::numeric
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
        queries_set := queries_set || jsonb_build_object(
          'userid', r_result.userid,
          'datid', r_result.datid,
          'queryid', r_result.queryid
        );
      END LOOP;
      datasets := jsonb_set(datasets, '{top_rusage_statements}', dataset);
    END IF;

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM report_queries_format(sserver_id, queries_set,
          start1_id, end1_id, start2_id, end2_id
        )
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{queries}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_tables_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_dml,
          ord_seq_scan,
          ord_upd,
          ord_growth,
          ord_vac,
          ord_anl
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_tables}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_io_tables_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_read,
          ord_fetch
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_io_tables}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_io_indexes_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_read,
          ord_fetch
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_io_indexes}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_indexes_format_diff(sserver_id,
          start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_growth,
          ord_vac
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_indexes}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_functions_format_diff(sserver_id,
          start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_time,
          ord_calls,
          ord_trgtime
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_functions}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM settings_format_diff(sserver_id,
          start1_id, end1_id, start2_id, end2_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{settings}', dataset);
  END IF;
  RETURN datasets;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION sections_jsonb(IN report_context jsonb, IN sserver_id integer,
  IN report_id integer)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    -- Recursive sections query with condition checking
    c_sections CURSOR(init_depth integer) FOR
    WITH RECURSIVE sections_tree(report_id, sect_id, parent_sect_id,
      toc_cap, tbl_cap, function_name, href, content, sect_struct, depth,
      path, ordering_path) AS
    (
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.function_name,
          rs.href,
          rs.content,
          rs.sect_struct,
          init_depth,
          ARRAY['sections', (row_number() OVER (ORDER BY s_ord ASC) - 1)::text] path,
          ARRAY[row_number() OVER (ORDER BY s_ord ASC)] as ordering_path
        FROM report_struct rs
        WHERE rs.report_id = sections_jsonb.report_id AND parent_sect_id IS NULL
          AND (
            rs.feature IS NULL OR
            (left(rs.feature,1) = '!' AND NOT jsonb_extract_path_text(report_context, 'report_features', rs.feature)::boolean) OR
            (left(rs.feature,1) != '!' AND jsonb_extract_path_text(report_context, 'report_features', rs.feature)::boolean)
          )
      UNION ALL
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.function_name,
          rs.href,
          rs.content,
          rs.sect_struct,
          st.depth + 1,
          st.path || ARRAY['sections', (row_number() OVER (PARTITION BY st.path ORDER BY s_ord ASC) - 1)::text] path,
          ordering_path || ARRAY[row_number() OVER (PARTITION BY st.path ORDER BY s_ord ASC)] as ordering_path
        FROM report_struct rs JOIN sections_tree st ON
          (rs.report_id, rs.parent_sect_id) =
          (st.report_id, st.sect_id)
        WHERE (
            rs.feature IS NULL OR
            (left(rs.feature,1) = '!' AND NOT jsonb_extract_path_text(report_context, 'report_features', rs.feature)::boolean) OR
            (left(rs.feature,1) != '!' AND jsonb_extract_path_text(report_context, 'report_features', rs.feature)::boolean)
          )
    )
    SELECT * FROM sections_tree ORDER BY ordering_path;

    -- Recursive columns query with condition checking
    c_columns CURSOR(header_blocks jsonb) FOR
    WITH RECURSIVE columns AS (
      SELECT
        ARRAY[blk_no::text, 'columns', (row_number() OVER (PARTITION BY blk_no) - 1)::text] as path,
        ARRAY[blk_no, row_number() OVER ()] as ordering_path,
        je #- '{condition}' #- '{columns}' as entry,
        je #> '{columns}' as columns
      FROM
        generate_series(0, jsonb_array_length(header_blocks) - 1) blk_no,
        jsonb_array_elements(header_blocks #> ARRAY[blk_no::text,'columns']) as je
      WHERE
        je #>> '{condition}' IS NULL OR trim(je #>> '{condition}') = '' OR
        (left(je #>> '{condition}',1) = '!' AND
          NOT jsonb_extract_path_text(
              report_context,
              'report_features',
              je #>> '{condition}'
            )::boolean) OR
        (left(je #>> '{condition}',1) != '!' AND
          jsonb_extract_path_text(
            report_context,
            'report_features',
            je #>> '{condition}'
          )::boolean)
      UNION ALL
      SELECT
        columns.path || ARRAY['columns',(row_number() OVER (PARTITION BY columns.path) - 1)::text] path,
        columns.ordering_path || ARRAY[row_number() OVER ()] as ordering_path,
        je #- '{condition}' #- '{columns}' as entry,
        je #> '{columns}' as columns
      FROM columns CROSS JOIN
        jsonb_array_elements(columns.columns) as je
      WHERE
        je #>> '{condition}' IS NULL OR trim(je #>> '{condition}') = '' OR
        (left(je #>> '{condition}',1) = '!' AND
          NOT jsonb_extract_path_text(
              report_context,
              'report_features',
              je #>> '{condition}'
            )::boolean) OR
        (left(je #>> '{condition}',1) != '!' AND
          jsonb_extract_path_text(
            report_context,
            'report_features',
            je #>> '{condition}'
          )::boolean)
    )
    SELECT * FROM columns
    ORDER BY ordering_path;

    c_new_queryids CURSOR(js_collected jsonb, js_new jsonb) FOR
    SELECT
      userid,
      datid,
      queryid
    FROM
      jsonb_array_elements(js_new) js_data_block,
      jsonb_to_recordset(js_data_block) AS (
        userid   bigint,
        datid    bigint,
        queryid  bigint
      )
    WHERE queryid IS NOT NULL AND datid IS NOT NULL
    EXCEPT
    SELECT
      userid,
      datid,
      queryid
    FROM
      jsonb_to_recordset(js_collected) AS (
        userid   bigint,
        datid    bigint,
        queryid  bigint
      );

    max_depth   CONSTANT integer := 5;

    js_hdr      jsonb;
    js_fhdr     jsonb;
    js_fdata    jsonb;
    js_report   jsonb;

    js_queryids jsonb = '[]'::jsonb;
BEGIN
    js_report := jsonb_build_object(
      'type', report_id,
      'properties', report_context #> '{report_properties}'
    );

    -- Prepare report_context queryid array
    report_context := jsonb_insert(
      report_context,
      '{report_properties,queryids}',
      '[]'::jsonb
    );

    <<sections>>
    FOR r_result IN c_sections(1) LOOP
      ASSERT r_result.depth BETWEEN 1 AND max_depth,
        format('Section depth is not in 1 - %s', max_depth);

      ASSERT js_report IS NOT NULL, format('Report JSON lost at start of section: %s', r_result.sect_id);
      -- Create "sections" array on the current level on first entry
      IF r_result.path[array_length(r_result.path, 1)] = '0' THEN
        js_report := jsonb_set(js_report, r_result.path[:array_length(r_result.path,1)-1],
          '[]'::jsonb
        );
      END IF;
      -- Section entry
      js_report := jsonb_insert(js_report, r_result.path, '{}'::jsonb);

      -- Set section attributes
      IF r_result.href IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'href'), to_jsonb(r_result.href));
      END IF;
      IF r_result.sect_id IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'sect_id'), to_jsonb(r_result.sect_id));
      END IF;
      IF r_result.tbl_cap IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'tbl_cap'), to_jsonb(r_result.tbl_cap));
      END IF;
      IF r_result.toc_cap IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'toc_cap'), to_jsonb(r_result.toc_cap));
      END IF;
      IF r_result.content IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'content'), to_jsonb(r_result.content));
      END IF;

      ASSERT js_report IS NOT NULL, format('Report JSON lost in attributes, section: %s', r_result.sect_id);
      -- Executing function of report section if requested
      IF r_result.function_name IS NOT NULL THEN
        IF (SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = '{pg_profile}') THEN
          -- Fail when requested function doesn't exists in extension
          IF (
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
            )
          THEN
            RAISE EXCEPTION 'Report requested function % not found', r_result.function_name
              USING HINT = 'This is a bug. Please report to {pg_profile} developers.';
          END IF;
        ELSE
          -- When not installed as an extension check only the function existance
          IF (
            SELECT count(*) = 1
            FROM
              pg_catalog.pg_proc f
            WHERE
              f.proname = r_result.function_name
              AND pg_catalog.pg_get_function_result(f.oid) =
                'text'
              AND pg_catalog.pg_get_function_arguments(f.oid) =
                'report_context jsonb, sserver_id integer'
            )
          THEN
            RAISE EXCEPTION 'Report requested function % not found', r_result.function_name
              USING HINT = 'This is a bug. Please report to {pg_profile} developers.';
          END IF;
        END IF;

        -- Set report_context
        IF r_result.href IS NOT NULL THEN
          report_context := jsonb_set(report_context, '{report_properties,href}',
            to_jsonb(r_result.href));
        END IF;

        ASSERT report_context IS NOT NULL, 'Lost report context';
        -- Execute function for a report and get a section
        EXECUTE format('SELECT section_structure, section_data FROM %I($1,$2)',
          r_result.function_name)
        INTO js_fhdr, js_fdata
        USING
          report_context,
          sserver_id
        ;

        ASSERT js_fhdr IS NOT NULL, format('Error in report function %s - header is null',
          r_result.function_name);
        ASSERT js_fdata IS NOT NULL, format('Error in report function %s - data is null',
          r_result.function_name);
        -- Skip processing if there is no data
        CONTINUE sections WHEN jsonb_array_length(js_fdata) = 0;

        -- Collect queryids from section data
        FOR r_queryid IN c_new_queryids(
          report_context #> '{report_properties,queryids}',
          js_fdata
        ) LOOP
          report_context := jsonb_insert(
            report_context,
            '{report_properties,queryids,0}',
            to_jsonb(r_queryid)
          );
        END LOOP;
        ASSERT report_context IS NOT NULL, 'Lost report context';

        IF jsonb_array_length(js_fdata) > 0 THEN
          -- Remove header fields with false conditions
          -- Prepare new empty array structure for generated blocks
          SELECT jsonb_agg('{}'::jsonb) INTO js_hdr
          FROM generate_series(1,jsonb_array_length(js_fhdr));
          -- Load a new structure
          FOR valid_columns IN c_columns(js_fhdr) LOOP
            IF valid_columns.path[array_length(valid_columns.path, 1)] = '0' THEN
              -- New columns list
              js_hdr := jsonb_set(js_hdr, valid_columns.path[:array_length(valid_columns.path,1)-1],
                '[]'::jsonb
              );
            END IF;
            -- Section entry
            js_hdr := jsonb_insert(js_hdr, valid_columns.path, valid_columns.entry);
          END LOOP; -- Over valid columns

          -- Assign headers and data to report
          FOR block_no IN 0..jsonb_array_length(js_hdr) - 1 LOOP
            js_fhdr := jsonb_set(js_fhdr, ARRAY[block_no::text,'columns'], js_hdr #> ARRAY[block_no::text,'columns']);
          END LOOP;
          js_report := jsonb_set(js_report, array_append(r_result.path, 'header'), js_fhdr);
          ASSERT js_report IS NOT NULL, format('Report JSON lost in header, section: %s', r_result.sect_id);
          js_report := jsonb_set(js_report, array_append(r_result.path, 'data'), js_fdata);
          ASSERT js_report IS NOT NULL, format('Report JSON lost in data, section: %s', r_result.sect_id);
        END IF; -- Function returned data rows
      -- report section contains a function

      ELSIF r_result.sect_struct IS NOT NULL THEN
          js_fhdr := r_result.sect_struct;
          -- Remove header fields with false conditions
          -- Prepare new empty array structure
          SELECT jsonb_agg('{}'::jsonb) INTO js_hdr
          FROM generate_series(1,jsonb_array_length(js_fhdr));
          -- Load a new structure
          FOR valid_columns IN c_columns(js_fhdr) LOOP
            IF valid_columns.path[array_length(valid_columns.path, 1)] = '0' THEN
              -- New columns list
              js_hdr := jsonb_set(js_hdr, valid_columns.path[:array_length(valid_columns.path,1)-1],
                '[]'::jsonb
              );
            END IF;
            -- Section entry
            js_hdr := jsonb_insert(js_hdr, valid_columns.path, valid_columns.entry);
          END LOOP; -- Over valid columns

          -- Assign headers and data to report
          FOR block_no IN 0..jsonb_array_length(js_hdr) - 1 LOOP
            js_fhdr := jsonb_set(js_fhdr, ARRAY[block_no::text,'columns'], js_hdr #> ARRAY[block_no::text,'columns']);
          END LOOP;
          js_report := jsonb_set(js_report, array_append(r_result.path, 'header'), js_fhdr);
          ASSERT js_report IS NOT NULL, format('Report JSON lost in header, section: %s', r_result.sect_id);
      END IF; -- sect_struct json field exists
    END LOOP; -- Over recursive sections query
    RETURN js_report;
END;
$$ LANGUAGE plpgsql;
