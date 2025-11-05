CREATE FUNCTION get_report_context(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN description text = NULL,
  IN start2_id integer = NULL, IN end2_id integer = NULL)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
  report_context  jsonb;
  r_result    RECORD;

  qlen_limit  integer;
  topn        integer;

  start1_time timestamp with time zone;
  end1_time   timestamp with time zone;
  start2_time timestamp with time zone;
  end2_time   timestamp with time zone;
  start1_time_ut numeric;
  end1_time_ut   numeric;
  start2_time_ut numeric;
  end2_time_ut   numeric;
BEGIN
    IF num_nulls(start1_id, end1_id) > 0 THEN
        RAISE 'At least first interval bounds is necessary';
    END IF;

    -- Check for zero-length intervals
    IF num_nulls(start1_id, end1_id) = 0 AND start1_id >= end1_id THEN
        RAISE 'First interval doesn''t have positive duration'
            USING HINT = 'end1_id must be greater than start1_id';
    END IF;
    IF num_nulls(start2_id, end2_id) = 0 AND start2_id >= end2_id THEN
        RAISE 'Second interval doesn''t have positive duration'
            USING HINT = 'end2_id must be greater than start2_id';
    END IF;

    -- Getting query length limit setting
    BEGIN
        qlen_limit := current_setting('{pg_profile}.max_query_length')::integer;
    EXCEPTION
        WHEN OTHERS THEN qlen_limit := 20000;
    END;

    -- Getting TopN setting
    BEGIN
        topn := least(current_setting('{pg_profile}.topn')::integer, 100);
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
    SELECT sample_time INTO STRICT start1_time FROM samples
    WHERE (server_id,sample_id) = (sserver_id,start1_id);
    SELECT EXTRACT(EPOCH FROM sample_time) INTO STRICT start1_time_ut FROM samples
    WHERE (server_id,sample_id) = (sserver_id,start1_id);
    SELECT sample_time INTO STRICT end1_time FROM samples
    WHERE (server_id,sample_id) = (sserver_id,end1_id);
    SELECT EXTRACT(EPOCH FROM sample_time) INTO STRICT end1_time_ut FROM samples
    WHERE (server_id,sample_id) = (sserver_id,end1_id);
    IF num_nulls(start2_id, end2_id) = 2 THEN
      report_context := jsonb_build_object(
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
        'table_new_page_updates', (
          SELECT COALESCE(sum(n_tup_newpage_upd), 0) > 0
          FROM sample_stat_tables_total
          WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
        ),
        'stat_io', (
          SELECT COUNT(*) > 0 FROM (
            SELECT backend_type
            FROM sample_stat_io
            WHERE server_id = sserver_id AND
              sample_id BETWEEN start1_id + 1 AND end1_id LIMIT 1
            ) c
        ),
        'stat_io_reset', (
          -- We should include both ends here to detect resets performed
          SELECT bool_or(group_reset)
          FROM (
            SELECT COUNT(DISTINCT stats_reset) > 1 AS group_reset
            FROM sample_stat_io
            WHERE server_id = sserver_id AND sample_id BETWEEN start1_id AND end1_id
            GROUP BY backend_type, object, context
          ) gr
        ),
        'stat_slru', (
          SELECT COUNT(*) > 0 FROM (
            SELECT name
            FROM sample_stat_slru
            WHERE server_id = sserver_id AND
              sample_id BETWEEN start1_id + 1 AND end1_id LIMIT 1
            ) c
        ),
        'stat_slru_reset', (
          -- We should include both ends here to detect resets performed
          SELECT bool_or(group_reset)
          FROM (
            SELECT COUNT(DISTINCT stats_reset) > 1 AS group_reset
            FROM sample_stat_slru
            WHERE server_id = sserver_id AND sample_id BETWEEN start1_id AND end1_id
            GROUP BY name
          ) gr
        ),
        'buffers_backend', (
          SELECT COUNT(buffers_backend) > 0 FROM sample_stat_cluster
          WHERE server_id = sserver_id AND
            sample_id BETWEEN start1_id + 1 AND end1_id
        ),
        'mean_mm_times', (
          SELECT COUNT(COALESCE(mean_max_exec_time, mean_min_exec_time,
              mean_max_plan_time, mean_min_plan_time)
            ) > 0
          FROM sample_statements_total
          WHERE server_id = sserver_id AND
            sample_id BETWEEN start1_id + 1 AND end1_id
        ),
        -- statements_cover should be used to control appearance of the cover field.
        'statements_coverage', (
          SELECT count(*) > 0
          FROM sample_statements
          WHERE server_id = sserver_id AND stats_since > start1_time AND
            sample_id BETWEEN start1_id + 1 AND end1_id
          ),
        'statements_jit_deform', (
          SELECT count(*) > 0
          FROM sample_statements_total
          WHERE server_id = sserver_id AND jit_deform_count > 0 AND
            sample_id BETWEEN start1_id + 1 AND end1_id
          ),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
          ), false),
        'extension_versions', (
          SELECT count(*) > 0
          FROM v_extension_versions
          WHERE server_id = sserver_id AND
            sample_id BETWEEN start1_id AND end1_id
          ),
        'extension_versions_show_date_columns', (
          SELECT count(*) > 0
          FROM v_extension_versions
          WHERE server_id = sserver_id AND
            sample_id BETWEEN start1_id AND end1_id AND
            (first_seen > start1_time OR last_sample_id < end1_id)
          ),
        'table_storage_parameters', (
          SELECT count(*) > 0
          FROM v_table_storage_parameters
          WHERE server_id = sserver_id AND
            sample_id BETWEEN start1_id AND end1_id
          ),
        'index_storage_parameters', (
          SELECT count(*) > 0
          FROM v_index_storage_parameters
          WHERE server_id = sserver_id AND
            sample_id BETWEEN start1_id AND end1_id
          ),
        'statements_workers_stats', (
          SELECT count(*) > 0
          FROM sample_statements
          WHERE server_id = sserver_id AND greatest(parallel_workers_to_launch,parallel_workers_launched) > 0 AND
            sample_id BETWEEN start1_id + 1 AND end1_id
          ),
        'wal_buffers_full', (
          SELECT count(*) > 0
          FROM sample_statements
          WHERE server_id = sserver_id AND wal_buffers_full > 0 AND
            sample_id BETWEEN start1_id + 1 AND end1_id
          ),
        'vacuum_time', (
          SELECT count(*) > 0
          FROM sample_stat_tables_total
          WHERE server_id = sserver_id AND
            total_vacuum_time + total_autovacuum_time > 0  AND
            sample_id BETWEEN start1_id + 1 AND end1_id
          ),
        'analyze_time', (
          SELECT count(*) > 0
          FROM sample_stat_tables_total
          WHERE server_id = sserver_id AND
            total_analyze_time + total_autoanalyze_time > 0  AND
            sample_id BETWEEN start1_id + 1 AND end1_id
          ),
        'db_parallel_workers', (
          SELECT count(*) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND greatest(parallel_workers_to_launch, parallel_workers_launched) > 0 AND
          sample_id BETWEEN start1_id + 1 AND end1_id
          ),
        'checkpoints_done_and_slru', (
          SELECT COUNT(*) > 0
          FROM sample_stat_cluster
          WHERE server_id = sserver_id AND
            greatest(slru_checkpoint, checkpoints_done) > 0 AND
            sample_id BETWEEN start1_id AND end1_id
          ),
        'restartpoints', (
          SELECT COUNT(*) > 0
          FROM sample_stat_cluster
          WHERE server_id = sserver_id AND
            greatest(restartpoints_timed, restartpoints_req, restartpoints_done) > 0 AND
            sample_id BETWEEN start1_id AND end1_id
        )
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
        'report_start1', start1_time::text,
        'report_end1', end1_time::text,
        'report_start1_ut', start1_time_ut,
        'report_end1_ut', end1_time_ut
        )
      );

      -- stat_activity features
      IF has_table_privilege('sample_act_backend', 'SELECT') THEN
        report_context := jsonb_set(report_context, '{report_features,act_backend}',
          (
          SELECT
            to_jsonb(COUNT(*) > 0) FROM (
              SELECT 1
              FROM sample_act_backend
              WHERE server_id = sserver_id AND (
                  sample_id BETWEEN start1_id + 1 AND end1_id
                )
              LIMIT 1
            ) c
          )
        );
        report_context := jsonb_set(report_context, '{report_features,act_ix}',
          (
          SELECT
            to_jsonb(COUNT(*) > 0) FROM (
              SELECT 1
              FROM sample_act_backend_state
              WHERE server_id = sserver_id AND
                state_code = 1 AND (
                  sample_id BETWEEN start1_id + 1 AND end1_id
                )
              LIMIT 1
            ) c
          )
        );
        report_context := jsonb_set(report_context, '{report_features,act_ixa}',
          (
          SELECT
            to_jsonb(COUNT(*) > 0) FROM (
              SELECT 1
              FROM sample_act_backend_state
              WHERE server_id = sserver_id AND
                state_code = 2 AND (
                  sample_id BETWEEN start1_id + 1 AND end1_id
                )
              LIMIT 1
            ) c
          )
        );
        report_context := jsonb_set(report_context, '{report_features,act_active}',
          (
          SELECT
            to_jsonb(COUNT(*) > 0) FROM (
              SELECT 1
              FROM sample_act_backend_state JOIN sample_act_statement
                USING (server_id, sample_id, pid, xact_start)
              WHERE server_id = sserver_id AND
                state_code = 3 AND (
                  sample_id BETWEEN start1_id + 1 AND end1_id
                )
              LIMIT 1
            ) c
          )
        );
      END IF;
    ELSIF num_nulls(start2_id, end2_id) = 0 THEN
      -- Get report times
      SELECT sample_time INTO STRICT start2_time FROM samples
      WHERE (server_id,sample_id) = (sserver_id,start2_id);
      SELECT EXTRACT(EPOCH FROM sample_time) INTO STRICT start2_time_ut FROM samples
      WHERE (server_id,sample_id) = (sserver_id,start2_id);
      SELECT sample_time INTO STRICT end2_time FROM samples
      WHERE (server_id,sample_id) = (sserver_id,end2_id);
      SELECT EXTRACT(EPOCH FROM sample_time) INTO STRICT end2_time_ut FROM samples
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
        'table_new_page_updates', (
          SELECT COALESCE(sum(n_tup_newpage_upd), 0) > 0
          FROM sample_stat_tables_total
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
        ),
        'stat_io', (
          SELECT COUNT(*) > 0 FROM (
            SELECT backend_type
            FROM sample_stat_io
            WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id + 1 AND end1_id OR
              sample_id BETWEEN start2_id + 1 AND end2_id
              ) LIMIT 1
            ) c
        ),
        'stat_io_reset', (
          -- We should include both ends here to detect resets performed
          SELECT bool_or(group_reset)
          FROM (
            SELECT COUNT(DISTINCT stats_reset) > 1 AS group_reset
            FROM sample_stat_io
            WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id AND end1_id OR
              sample_id BETWEEN start2_id AND end2_id
              )
            GROUP BY backend_type, object, context
          ) gr
        ),
        'stat_slru', (
          SELECT COUNT(*) > 0 FROM (
            SELECT name
            FROM sample_stat_slru
            WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id + 1 AND end1_id OR
              sample_id BETWEEN start2_id + 1 AND end2_id
              ) LIMIT 1
            ) c
        ),
        'stat_slru_reset', (
          -- We should include both ends here to detect resets performed
          SELECT bool_or(group_reset)
          FROM (
            SELECT COUNT(DISTINCT stats_reset) > 1 AS group_reset
            FROM sample_stat_slru
            WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id AND end1_id OR
              sample_id BETWEEN start2_id AND end2_id
            )
            GROUP BY name
          ) gr
        ),
        'buffers_backend', (
          SELECT COUNT(buffers_backend) > 0 FROM sample_stat_cluster
          WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id AND end1_id OR
              sample_id BETWEEN start2_id AND end2_id
            )
        ),
        'mean_mm_times', (
          SELECT COUNT(COALESCE(mean_max_exec_time, mean_min_exec_time,
              mean_max_plan_time, mean_min_plan_time)
            ) > 0
          FROM sample_statements_total
          WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id AND end1_id OR
              sample_id BETWEEN start2_id AND end2_id
            )
        ),
        -- statements_cover should be used to control appearance of the cover field.
        'statements_coverage', (
          SELECT count(*) > 0
          FROM sample_statements
          WHERE server_id = sserver_id AND
            ((stats_since > start1_time AND sample_id BETWEEN start1_id + 1 AND end1_id) OR
            (stats_since > start2_time AND sample_id BETWEEN start2_id + 1 AND end2_id))
          ),
        'statements_jit_deform', (
          SELECT count(*) > 0
          FROM sample_statements_total
          WHERE server_id = sserver_id AND jit_deform_count > 0 AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ), false),
        'extension_versions', (
          SELECT count(*) > 0
          FROM v_extension_versions
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id AND end1_id OR
            sample_id BETWEEN start2_id AND end2_id)
          ),
        'extension_versions_show_date_columns', (
          SELECT count(*) > 0
          FROM v_extension_versions
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id AND end1_id OR
            sample_id BETWEEN start2_id AND end2_id) AND
            (first_seen > least(start1_time, start2_time) OR last_sample_id < greatest(end1_id, end2_id))
          ),
        'table_storage_parameters', (
          SELECT count(*) > 0
          FROM v_table_storage_parameters
          WHERE server_id = sserver_id AND
            sample_id BETWEEN start1_id AND end1_id
          ),
        'index_storage_parameters', (
          SELECT count(*) > 0
          FROM v_index_storage_parameters
          WHERE server_id = sserver_id AND
            sample_id BETWEEN start1_id AND end1_id
          ),
        'statements_workers_stats', (
          SELECT count(*) > 0
          FROM sample_statements
          WHERE server_id = sserver_id AND
            greatest(parallel_workers_to_launch, parallel_workers_launched) > 0 AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ),
        'wal_buffers_full', (
          SELECT count(*) > 0
          FROM sample_statements
          WHERE server_id = sserver_id AND wal_buffers_full > 0 AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ),
        'vacuum_time', (
          SELECT count(*) > 0
          FROM sample_stat_tables_total
          WHERE server_id = sserver_id AND
            total_vacuum_time + total_autovacuum_time > 0  AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ),
        'analyze_time', (
          SELECT count(*) > 0
          FROM sample_stat_tables_total
          WHERE server_id = sserver_id AND
            total_analyze_time + total_autoanalyze_time > 0  AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ),
        'db_parallel_workers', (
          SELECT count(*) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND greatest(parallel_workers_to_launch, parallel_workers_launched) > 0 AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ),
        'checkpoints_done_and_slru', (
          SELECT COUNT(*) > 0
          FROM sample_stat_cluster
          WHERE server_id = sserver_id AND
            greatest(slru_checkpoint, checkpoints_done) > 0 AND (
              sample_id BETWEEN start1_id AND end1_id OR
              sample_id BETWEEN start2_id AND end2_id
            )
          ),
        'restartpoints', (
          SELECT COUNT(*) > 0
          FROM sample_stat_cluster
          WHERE server_id = sserver_id AND
            greatest(restartpoints_timed, restartpoints_req, restartpoints_done) > 0 AND (
              sample_id BETWEEN start1_id AND end1_id OR
              sample_id BETWEEN start2_id AND end2_id
            )
        )
      ),
      'report_properties', jsonb_build_object(
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
        'report_start1', start1_time::text,
        'report_end1', end1_time::text,
        'report_start1_ut', start1_time_ut,
        'report_end1_ut', end1_time_ut,

        'start2_id', start2_id,
        'end2_id', end2_id,
        'report_start2', start2_time::text,
        'report_end2', end2_time::text,
        'report_start2_ut', start2_time_ut,
        'report_end2_ut', end2_time_ut
        )
      );

      -- stat_activity features
      IF has_table_privilege('sample_act_backend', 'SELECT') THEN
        report_context := jsonb_set(report_context, '{report_features,act_backend}',
          (
          SELECT
            to_jsonb(COUNT(*) > 0) FROM (
              SELECT 1
              FROM sample_act_backend
              WHERE server_id = sserver_id AND (
                  sample_id BETWEEN start1_id + 1 AND end1_id OR
                  sample_id BETWEEN start2_id + 1 AND end2_id
                )
              LIMIT 1
            ) c
          )
        );
        report_context := jsonb_set(report_context, '{report_features,act_ix}',
          (
          SELECT
            to_jsonb(COUNT(*) > 0) FROM (
              SELECT 1
              FROM sample_act_backend_state
              WHERE server_id = sserver_id AND
                state_code = 1 AND (
                  sample_id BETWEEN start1_id + 1 AND end1_id OR
                  sample_id BETWEEN start2_id + 1 AND end2_id
                )
              LIMIT 1
            ) c
          )
        );
        report_context := jsonb_set(report_context, '{report_features,act_ixa}',
          (
          SELECT
            to_jsonb(COUNT(*) > 0) FROM (
              SELECT 1
              FROM sample_act_backend_state
              WHERE server_id = sserver_id AND
                state_code = 2 AND (
                  sample_id BETWEEN start1_id + 1 AND end1_id OR
                  sample_id BETWEEN start2_id + 1 AND end2_id
                )
              LIMIT 1
            ) c
          )
        );
        report_context := jsonb_set(report_context, '{report_features,act_active}',
          (
          SELECT
            to_jsonb(COUNT(*) > 0) FROM (
              SELECT 1
              FROM sample_act_backend_state
              WHERE server_id = sserver_id AND
                state_code = 3 AND (
                  sample_id BETWEEN start1_id + 1 AND end1_id OR
                  sample_id BETWEEN start2_id + 1 AND end2_id
                )
              LIMIT 1
            ) c
          )
        );
      END IF;
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
        to_jsonb(r_result.server_description)
      );
    END IF;
    -- Report description
    IF description IS NOT NULL AND description != '' THEN
      report_context := jsonb_set(report_context, '{report_properties,description}',
        to_jsonb(description)
      );
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

CREATE FUNCTION get_report_datasets(IN report_context jsonb, IN sserver_id integer, IN db_exclude name[] = NULL)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
  start1_id   integer = (report_context #>> '{report_properties,start1_id}')::integer;
  start2_id   integer = (report_context #>> '{report_properties,start2_id}')::integer;
  end1_id     integer = (report_context #>> '{report_properties,end1_id}')::integer;
  end2_id     integer = (report_context #>> '{report_properties,end2_id}')::integer;

  datasets         jsonb = '{}';
  queries_set      jsonb = '[]';
  act_queries_set  jsonb = '[]';
  r_dataset        text;
BEGIN
  -- report properties dataset
  datasets := jsonb_set(datasets, '{properties}',
    jsonb_build_array(report_context #> '{report_properties}')
  );

  IF num_nulls(start1_id, end1_id) = 0 AND num_nulls(start2_id, end2_id) > 0 THEN
    -- Regular report

    -- database statistics dataset
    SELECT coalesce(jsonb_set(datasets, '{dbstat}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM dbstats_format(sserver_id, start1_id, end1_id) dt;

    IF (report_context #>> '{report_features,dbstats_reset}')::boolean THEN
      -- dbstats reset dataset
      SELECT coalesce(jsonb_set(datasets, '{dbstats_reset}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM dbstats_reset_format(sserver_id, start1_id, end1_id) dt;
    END IF;

    IF (report_context #>> '{report_features,stat_io}')::boolean THEN
      -- stat_io dataset
      SELECT coalesce(jsonb_set(datasets, '{stat_io}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM cluster_stat_io_format(sserver_id, start1_id, end1_id) dt;
      IF (report_context #>> '{report_features,stat_io_reset}')::boolean THEN
        -- IO reset dataset
        SELECT coalesce(jsonb_set(datasets, '{stat_io_reset}', jsonb_agg(to_jsonb(dt))), datasets)
        INTO datasets
        FROM cluster_stat_io_reset_format(sserver_id, start1_id, end1_id) dt;
      END IF;
    END IF;

    IF (report_context #>> '{report_features,stat_slru}')::boolean THEN
      -- stat_slru dataset
      SELECT coalesce(jsonb_set(datasets, '{stat_slru}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM cluster_stat_slru_format(sserver_id, start1_id, end1_id) dt;
      IF (report_context #>> '{report_features,stat_slru_reset}')::boolean THEN
        -- SLRU reset dataset
        SELECT coalesce(jsonb_set(datasets, '{stat_slru_reset}', jsonb_agg(to_jsonb(dt))), datasets)
        INTO datasets
        FROM cluster_stat_slru_reset_format(sserver_id, start1_id, end1_id) dt;
      END IF;
    END IF;

    IF (report_context #>> '{report_features,statstatements}')::boolean THEN
      -- statements by database dataset
      SELECT coalesce(jsonb_set(datasets, '{statements_dbstats}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM statements_dbstats_format(sserver_id, start1_id, end1_id) dt;
    END IF;

    IF (report_context #>> '{report_features,stmt_cnt_range}')::boolean THEN
      -- statements count of max for interval
      SELECT coalesce(jsonb_set(datasets, '{stmt_cnt_range}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM stmt_cnt_format(sserver_id, start1_id, end1_id) dt;
    END IF;

    IF (report_context #>> '{report_features,stmt_cnt_all}')::boolean THEN
      -- statements count of max for all samples
      SELECT coalesce(jsonb_set(datasets, '{stmt_cnt_all}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM stmt_cnt_format(sserver_id, 0, 0) dt;
    END IF;

    -- cluster statistics dataset
    SELECT coalesce(jsonb_set(datasets, '{cluster_stats}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM cluster_stats_format(sserver_id, start1_id, end1_id) dt;

    IF (report_context #>> '{report_features,cluster_stats_reset}')::boolean THEN
      -- cluster stats reset dataset
      SELECT coalesce(jsonb_set(datasets, '{cluster_stats_reset}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM cluster_stats_reset_format(sserver_id, start1_id, end1_id) dt;
    END IF;

    IF (report_context #>> '{report_features,wal_stats_reset}')::boolean THEN
      -- WAL stats reset dataset
      SELECT coalesce(jsonb_set(datasets, '{wal_stats_reset}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM wal_stats_reset_format(sserver_id, start1_id, end1_id) dt;
    END IF;

    IF (report_context #>> '{report_features,wal_stats}')::boolean THEN
      -- WAL stats dataset
      SELECT coalesce(jsonb_set(datasets, '{wal_stats}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM wal_stats_format(sserver_id, start1_id, end1_id,
        (report_context #>> '{report_properties,interval_duration_sec}')::numeric) dt;
    END IF;

    -- Tablespace stats dataset
    SELECT coalesce(jsonb_set(datasets, '{tablespace_stats}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM tablespace_stats_format(sserver_id, start1_id, end1_id) dt;

    IF (report_context #>> '{report_features,wait_sampling_tot}')::boolean THEN
      -- Wait totals dataset
      SELECT coalesce(jsonb_set(datasets, '{wait_sampling_total_stats}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM wait_sampling_total_stats_format(sserver_id, start1_id, end1_id) dt;
      -- Wait events dataset
      SELECT coalesce(jsonb_set(datasets, '{wait_sampling_events}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM top_wait_sampling_events_format(sserver_id, start1_id, end1_id) dt;
    END IF;

    IF (report_context #>> '{report_features,statstatements}')::boolean THEN
      -- Statement stats dataset
      SELECT coalesce(jsonb_set(datasets, '{top_statements}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM top_statements_format(sserver_id, start1_id, end1_id) dt
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
          ord_jit,
          ord_wrkrs_cnt
        ) <= (report_context #>> '{report_properties,topn}')::numeric;
    END IF;

    IF (report_context #>> '{report_features,kcachestatements}')::boolean THEN
      -- Statement rusage stats dataset
      SELECT coalesce(jsonb_set(datasets, '{top_rusage_statements}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM top_rusage_statements_format(sserver_id, start1_id, end1_id) dt
      WHERE least(
          ord_cpu_time,
          ord_io_bytes
        ) <= (report_context #>> '{report_properties,topn}')::numeric;
    END IF;

    IF (report_context #>> '{report_features,act_backend}')::boolean THEN
      -- Session states and queries cartured by subsamples
      -- Database aggregated data
      SELECT coalesce(jsonb_set(datasets, '{db_activity_agg}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM stat_activity_agg_format(sserver_id, start1_id, end1_id) dt;
      -- Top states dataset
      SELECT coalesce(jsonb_set(datasets, '{act_top_states}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM stat_activity_states_format(sserver_id, start1_id, end1_id) dt
      WHERE least(
          ord_dur,
          ord_age
        ) <= (report_context #>> '{report_properties,topn}')::numeric;
    END IF;

    SELECT coalesce(jsonb_set(datasets, '{top_tables}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_tables_format(sserver_id, start1_id, end1_id) dt
    WHERE least(
        ord_dml,
        ord_seq_scan,
        ord_upd,
        ord_growth,
        ord_vac_cnt,
        ord_anl_cnt,
        ord_vac_time,
        ord_anl_time
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_io_tables}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_io_tables_format(sserver_id, start1_id, end1_id) dt
    WHERE least(
        ord_read,
        ord_fetch
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_io_indexes}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_io_indexes_format(sserver_id, start1_id, end1_id) dt
    WHERE least(
        ord_read,
        ord_fetch
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_indexes}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_indexes_format(sserver_id, start1_id, end1_id) dt
    WHERE least(
        ord_growth,
        ord_unused,
        ord_vac
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_functions}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_functions_format(sserver_id, start1_id, end1_id) dt
    WHERE least(
        ord_time,
        ord_calls,
        ord_trgtime
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_tbl_last_sample}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_tbl_last_sample_format(sserver_id, start1_id, end1_id) dt
    WHERE least(
        ord_dead,
        ord_mod
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{settings}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM settings_format(sserver_id, start1_id, end1_id) dt;

    -- Now we need to collect queries over datasets
    FOR r_dataset IN (SELECT jsonb_object_keys(datasets)) LOOP
      -- skip datasets without queries
      CONTINUE WHEN NOT
        (datasets #> ARRAY[r_dataset, '0']) ?| ARRAY['queryid'];
      SELECT queries_set || coalesce(jsonb_agg(to_jsonb(dt)),'[]'::jsonb)
      INTO queries_set
      FROM (
        SELECT
          userid,
          datid,
          queryid
        FROM
          jsonb_to_recordset(datasets #> ARRAY[r_dataset]) AS
            entry(
              userid  oid,
              datid   oid,
              dbname  name,
              queryid bigint
            )
        WHERE dbname <> ALL (db_exclude) OR db_exclude IS NULL
      ) dt;
    END LOOP; -- over datasets

    -- Collect stat_activity queries over datasets
    FOR r_dataset IN (SELECT jsonb_object_keys(datasets)) LOOP
      -- skip datasets without queries
      CONTINUE WHEN NOT
        (datasets #> ARRAY[r_dataset, '0']) ?| ARRAY['act_query_md5'];
      SELECT act_queries_set || coalesce(jsonb_agg(to_jsonb(dt)),'[]'::jsonb)
      INTO act_queries_set
      FROM (
        SELECT
          act_query_md5
        FROM
          jsonb_to_recordset(datasets #> ARRAY[r_dataset]) AS
            entry(
              act_query_md5  text
            )
      ) dt;
    END LOOP; -- over datasets

    -- Query texts dataset should be formed the last
    SELECT coalesce(jsonb_set(datasets, '{queries}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM report_queries_format(report_context, sserver_id, queries_set, start1_id, end1_id, NULL, NULL) dt;

    -- stat_activity queries list construction
    IF (report_context #>> '{report_features,act_backend}')::boolean THEN
      SELECT coalesce(jsonb_set(datasets, '{act_queries}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM report_active_queries_format(report_context, sserver_id, act_queries_set) dt;
    END IF;

    -- extension versions
    IF (report_context #>> '{report_features,extension_versions}')::boolean THEN
      SELECT coalesce(jsonb_set(datasets, '{extension_versions}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM extension_versions_format(sserver_id, start1_id, end1_id) dt;
    END IF;

    IF (report_context #>> '{report_features,table_storage_parameters}')::boolean THEN
      SELECT coalesce(jsonb_set(datasets, '{table_storage_parameters}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM table_storage_parameters_format(sserver_id, start1_id, end1_id) dt;
    END IF;

    IF (report_context #>> '{report_features,index_storage_parameters}')::boolean THEN
      SELECT coalesce(jsonb_set(datasets, '{index_storage_parameters}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM index_storage_parameters_format(sserver_id, start1_id, end1_id) dt;
    END IF;

  ELSIF num_nulls(start1_id, end1_id, start2_id, end2_id) = 0 THEN
    -- Differential report

    -- database statistics dataset
    SELECT coalesce(jsonb_set(datasets, '{dbstat}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM dbstats_format_diff(sserver_id, start1_id, end1_id,
      start2_id, end2_id) dt;

    IF (report_context #>> '{report_features,dbstats_reset}')::boolean THEN
      -- dbstats reset dataset
      SELECT coalesce(jsonb_set(datasets, '{dbstats_reset}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM dbstats_reset_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
    END IF;

    IF (report_context #>> '{report_features,stat_io}')::boolean THEN
      -- stat_io dataset
      SELECT coalesce(jsonb_set(datasets, '{stat_io}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM cluster_stat_io_format(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
      IF (report_context #>> '{report_features,stat_io_reset}')::boolean THEN
        -- SLRU reset dataset
        SELECT coalesce(jsonb_set(datasets, '{stat_io_reset}', jsonb_agg(to_jsonb(dt))), datasets)
        INTO datasets
        FROM cluster_stat_io_reset_format(sserver_id,
          start1_id, end1_id, start2_id, end2_id) dt;
      END IF;
    END IF;

    IF (report_context #>> '{report_features,stat_slru}')::boolean THEN
      -- stat_slru dataset
      SELECT coalesce(jsonb_set(datasets, '{stat_slru}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM cluster_stat_slru_format(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
      IF (report_context #>> '{report_features,stat_slru_reset}')::boolean THEN
        -- SLRU reset dataset
        SELECT coalesce(jsonb_set(datasets, '{stat_slru_reset}', jsonb_agg(to_jsonb(dt))), datasets)
        INTO datasets
        FROM cluster_stat_slru_reset_format(sserver_id,
          start1_id, end1_id, start2_id, end2_id) dt;
      END IF;
    END IF;

    IF (report_context #>> '{report_features,statstatements}')::boolean THEN
      -- statements by database dataset
      SELECT coalesce(jsonb_set(datasets, '{statements_dbstats}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM statements_dbstats_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
    END IF;

    IF (report_context #>> '{report_features,stmt_cnt_range}')::boolean THEN
      -- statements count of max for interval
      SELECT coalesce(jsonb_set(datasets, '{stmt_cnt_range}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM stmt_cnt_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
    END IF;

    IF (report_context #>> '{report_features,stmt_cnt_all}')::boolean THEN
      -- statements count of max for all samples
      SELECT coalesce(jsonb_set(datasets, '{stmt_cnt_all}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM stmt_cnt_format(sserver_id, 0, 0) dt;
    END IF;

    -- cluster statistics dataset
    SELECT coalesce(jsonb_set(datasets, '{cluster_stats}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM cluster_stats_format_diff(sserver_id, start1_id, end1_id,
      start2_id, end2_id) dt;

    IF (report_context #>> '{report_features,cluster_stats_reset}')::boolean THEN
      -- cluster stats reset dataset
      SELECT coalesce(jsonb_set(datasets, '{cluster_stats_reset}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM cluster_stats_reset_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
    END IF;

    IF (report_context #>> '{report_features,wal_stats_reset}')::boolean THEN
      -- WAL stats reset dataset
      SELECT coalesce(jsonb_set(datasets, '{wal_stats_reset}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM wal_stats_reset_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
    END IF;

    IF (report_context #>> '{report_features,wal_stats}')::boolean THEN
      -- WAL stats dataset
      SELECT coalesce(jsonb_set(datasets, '{wal_stats}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM wal_stats_format(sserver_id,
        start1_id, end1_id, start2_id, end2_id,
        (report_context #>> '{report_properties,interval1_duration_sec}')::numeric,
        (report_context #>> '{report_properties,interval2_duration_sec}')::numeric) dt;
    END IF;

    -- Tablespace stats dataset
    SELECT coalesce(jsonb_set(datasets, '{tablespace_stats}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM tablespace_stats_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)  dt;

    IF (report_context #>> '{report_features,wait_sampling_tot}')::boolean THEN
      -- Wait totals dataset
      SELECT coalesce(jsonb_set(datasets, '{wait_sampling_total_stats}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM wait_sampling_total_stats_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
      -- Wait events dataset
      SELECT coalesce(jsonb_set(datasets, '{wait_sampling_events}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM top_wait_sampling_events_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
    END IF;

    IF (report_context #>> '{report_features,statstatements}')::boolean THEN
      -- Statement stats dataset
      SELECT coalesce(jsonb_set(datasets, '{top_statements}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM top_statements_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt
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
          ord_jit,
          ord_wrkrs_cnt
        ) <= (report_context #>> '{report_properties,topn}')::numeric;
    END IF;

    IF (report_context #>> '{report_features,kcachestatements}')::boolean THEN
      -- Statement rusage stats dataset
      SELECT coalesce(jsonb_set(datasets, '{top_rusage_statements}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM top_rusage_statements_format_diff(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt
      WHERE least(
          ord_cpu_time,
          ord_io_bytes
        ) <= (report_context #>> '{report_properties,topn}')::numeric;
    END IF;

    IF (report_context #>> '{report_features,act_backend}')::boolean THEN
      -- Session states and queries cartured by subsamples
      -- Database aggregated data
      SELECT coalesce(jsonb_set(datasets, '{db_activity_agg}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM stat_activity_agg_format(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt;
      -- Top states dataset
      SELECT coalesce(jsonb_set(datasets, '{act_top_states}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM stat_activity_states_format(sserver_id, start1_id, end1_id,
        start2_id, end2_id) dt
      WHERE least(
          ord_dur,
          ord_age
        ) <= (report_context #>> '{report_properties,topn}')::numeric;
    END IF;

    SELECT coalesce(jsonb_set(datasets, '{top_tables}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_tables_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id) dt
    WHERE least(
        ord_dml,
        ord_seq_scan,
        ord_upd,
        ord_growth,
        ord_vac_cnt,
        ord_anl_cnt,
        ord_vac_time,
        ord_anl_time
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_io_tables}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_io_tables_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id) dt
    WHERE least(
        ord_read,
        ord_fetch
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_io_indexes}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_io_indexes_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id) dt
    WHERE least(
        ord_read,
        ord_fetch
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_indexes}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_indexes_format_diff(sserver_id,
      start1_id, end1_id, start2_id, end2_id) dt
    WHERE least(
        ord_growth,
        ord_vac
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{top_functions}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM top_functions_format_diff(sserver_id,
      start1_id, end1_id, start2_id, end2_id) dt
    WHERE least(
        ord_time,
        ord_calls,
        ord_trgtime
      ) <= (report_context #>> '{report_properties,topn}')::numeric;

    SELECT coalesce(jsonb_set(datasets, '{settings}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM settings_format_diff(sserver_id,
      start1_id, end1_id, start2_id, end2_id) dt;

    -- Now we need to collect queries over datasets
    FOR r_dataset IN (SELECT jsonb_object_keys(datasets)) LOOP
      -- skip datasets without queries
      CONTINUE WHEN NOT
        (datasets #> ARRAY[r_dataset, '0']) ?| ARRAY['queryid'];
      SELECT queries_set || coalesce(jsonb_agg(to_jsonb(dt)),'[]'::jsonb)
      INTO queries_set
      FROM (
        SELECT
          userid,
          datid,
          queryid
        FROM
          jsonb_to_recordset(datasets #> ARRAY[r_dataset]) AS
            entry(
              userid  oid,
              datid   oid,
              dbname  name,
              queryid bigint
            )
        WHERE dbname <> ALL (db_exclude) OR db_exclude IS NULL
      ) dt;
    END LOOP; -- over datasets

    -- Collect stat_activity queries over datasets
    FOR r_dataset IN (SELECT jsonb_object_keys(datasets)) LOOP
      -- skip datasets without queries
      CONTINUE WHEN NOT
        (datasets #> ARRAY[r_dataset, '0']) ?| ARRAY['act_query_md5'];
      SELECT act_queries_set || coalesce(jsonb_agg(to_jsonb(dt)),'[]'::jsonb)
      INTO act_queries_set
      FROM (
        SELECT
          act_query_md5
        FROM
          jsonb_to_recordset(datasets #> ARRAY[r_dataset]) AS
            entry(
              act_query_md5  text
            )
      ) dt;
    END LOOP; -- over datasets

    -- Query texts dataset should be formed the last
    SELECT coalesce(jsonb_set(datasets, '{queries}', jsonb_agg(to_jsonb(dt))), datasets)
    INTO datasets
    FROM report_queries_format(report_context, sserver_id, queries_set,
      start1_id, end1_id, start2_id, end2_id) dt;

    -- stat_activity queries list construction
    IF (report_context #>> '{report_features,act_backend}')::boolean THEN
      SELECT coalesce(jsonb_set(datasets, '{act_queries}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM report_active_queries_format(report_context, sserver_id, act_queries_set) dt;
    END IF;

    -- extension versions
    IF (report_context #>> '{report_features,extension_versions}')::boolean THEN
      SELECT coalesce(jsonb_set(datasets, '{extension_versions}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM extension_versions_format(sserver_id, start1_id, end1_id, start2_id, end2_id) dt;
    END IF;

    IF (report_context #>> '{report_features,table_storage_parameters}')::boolean THEN
      SELECT coalesce(jsonb_set(datasets, '{table_storage_parameters}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM table_storage_parameters_format(sserver_id, start1_id, end1_id, start2_id, end2_id) dt;
    END IF;

    IF (report_context #>> '{report_features,index_storage_parameters}')::boolean THEN
      SELECT coalesce(jsonb_set(datasets, '{index_storage_parameters}', jsonb_agg(to_jsonb(dt))), datasets)
      INTO datasets
      FROM index_storage_parameters_format(sserver_id, start1_id, end1_id, start2_id, end2_id) dt;
    END IF;

  END IF;

  IF db_exclude IS NOT NULL THEN
    SELECT jsonb_object_agg(dts.key, dts.value)
    INTO datasets
    FROM (
      SELECT dt.key, jsonb_agg(dt.value) AS value
      FROM (
        SELECT d.key, jsonb_array_elements(d.value) AS value
        FROM jsonb_each(datasets) d) dt
        WHERE NOT (dt.value ->> 'dbname' IS NOT NULL AND dt.value ->> 'dbname' = ANY (db_exclude))
        GROUP BY key
      ) dts;
  END IF;

  RETURN datasets;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION section_apply_conditions(IN js_hdr jsonb, IN report_context jsonb)
RETURNS jsonb
AS $$
DECLARE
  js_res          jsonb;
  traverse_path   text[];
  resulting_path  text[];
  entry_condition boolean;
BEGIN
  js_res := '[]';
  resulting_path := NULL;
  traverse_path := ARRAY['0'];
  WHILE array_length(traverse_path, 1) > 0 LOOP
    -- Calculate condition
    entry_condition := NOT js_hdr #> traverse_path ? 'condition' OR
      trim(js_hdr #> traverse_path ->> 'condition') = '' OR
        (left(js_hdr #> traverse_path ->> 'condition', 1) = '!' AND
          NOT jsonb_extract_path_text(
              report_context,
              'report_features',
              js_hdr #> traverse_path ->> 'condition'
            )::boolean) OR
        (left(js_hdr #> traverse_path ->> 'condition', 1) != '!' AND
          jsonb_extract_path_text(
            report_context,
            'report_features',
            js_hdr #> traverse_path ->> 'condition'
          )::boolean);

    IF jsonb_typeof(js_hdr #> traverse_path) = 'object' AND entry_condition
    THEN
      -- Return found entry
      ASSERT
        array_length(traverse_path, 1) - COALESCE(array_length(resulting_path, 1), 0) <= 2,
        format('Impossible path length increment during traverse at %s', traverse_path);
      IF COALESCE(array_length(resulting_path, 1), 0) < array_length(traverse_path, 1) THEN
        -- Append 0 value of next level
        -- Special case on top level
        IF resulting_path IS NULL THEN
           resulting_path := ARRAY['0'];
        ELSE
          resulting_path := array_cat(resulting_path,
            ARRAY[traverse_path[array_length(traverse_path, 1) - 1], '0']);
        END IF;
      ELSIF array_length(resulting_path, 1) > array_length(traverse_path, 1) THEN
        -- trim array
        resulting_path := resulting_path[:array_length(traverse_path, 1)];
        resulting_path[array_length(resulting_path, 1)] :=
          (resulting_path[array_length(resulting_path, 1)]::integer + 1)::text;
      ELSIF array_length(resulting_path, 1) = array_length(traverse_path, 1) THEN
        resulting_path[array_length(resulting_path, 1)] :=
          (resulting_path[array_length(resulting_path, 1)]::integer + 1)::text;
      END IF;
      IF array_length(resulting_path, 1) > 1 AND
        resulting_path[array_length(resulting_path, 1)] = '0'
      THEN
        js_res := jsonb_set(
          js_res,
          resulting_path[:array_length(resulting_path, 1) - 1],
          '[]'::jsonb
        );
      END IF;
      js_res := jsonb_set(js_res, resulting_path,
        js_hdr #> traverse_path #- '{columns}' #- '{rows}' #- '{condition}'
      );
    END IF;
    -- Search for next entry
    IF (js_hdr #> traverse_path ? 'columns' OR js_hdr #> traverse_path ? 'rows') AND
      entry_condition
    THEN
      -- Drill down if we have the way
      CASE
        WHEN js_hdr #> traverse_path ? 'columns' THEN
          traverse_path := traverse_path || ARRAY['columns','0'];
        WHEN js_hdr #> traverse_path ? 'rows' THEN
          traverse_path := traverse_path || ARRAY['rows','0'];
        ELSE
          RAISE EXCEPTION 'Missing rows or columns array';
     END CASE;
     ASSERT js_hdr #> traverse_path IS NOT NULL, 'Empty columns list';
    ELSE
      CASE jsonb_typeof(js_hdr #> traverse_path)
        WHEN 'object' THEN
          -- If we are observing an object (i.e. column or row), search next
          IF jsonb_array_length(js_hdr #> traverse_path[:array_length(traverse_path, 1) - 1]) - 1 >
            traverse_path[array_length(traverse_path, 1)]::integer
          THEN
            -- Find sibling if exists
            traverse_path := array_cat(traverse_path[:array_length(traverse_path, 1) - 1],
              ARRAY[(traverse_path[array_length(traverse_path, 1)]::integer + 1)::text]
            );
          ELSE
            -- Or exit on previous array level if there is no siblings
            traverse_path := traverse_path[:array_length(traverse_path, 1) - 1];
          END IF;
        WHEN 'array' THEN
          -- Special case - switch from processing columns to processing rows
          IF array_length(traverse_path, 1) = 2 AND
            traverse_path[array_length(traverse_path, 1)] = 'columns' AND
            js_hdr #> traverse_path[:1] ? 'rows' AND
            jsonb_typeof(js_hdr #> array_cat(traverse_path[:1], ARRAY['rows'])) = 'array' AND
            jsonb_array_length(js_hdr #> array_cat(traverse_path[:1], ARRAY['rows'])) > 0
          THEN
            traverse_path := array_cat(traverse_path[:1], ARRAY['rows', '0']);
            resulting_path := resulting_path[:1];
            CONTINUE;
          END IF;
          -- If we are observing an array, we are searching the next sibling in preevious level
          -- we should check if there are elements left in previous array
          IF jsonb_array_length(js_hdr #> traverse_path[:array_length(traverse_path, 1) - 2]) - 1 >
            traverse_path[array_length(traverse_path, 1) - 1]::integer
          THEN
            -- take the next item on previous level if exists
            traverse_path := array_cat(traverse_path[:array_length(traverse_path, 1) - 2],
              ARRAY[(traverse_path[array_length(traverse_path, 1) - 1]::integer + 1)::text]
            );
          ELSE
            -- Or go one level up if not
            traverse_path := traverse_path[:array_length(traverse_path, 1) - 2];
          END IF;
      END CASE;
    END IF;
  END LOOP;
  RETURN js_res;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION sections_jsonb(IN report_context jsonb, IN sserver_id integer,
  IN report_id integer)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    -- Recursive sections query with condition checking
    c_sections CURSOR(init_depth integer) FOR
    WITH RECURSIVE sections_tree(report_id, sect_id, parent_sect_id,
      toc_cap, tbl_cap, function_name, content, sect_struct, depth,
      path, ordering_path) AS
    (
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.function_name,
          rs.content,
          rs.sect_struct,
          init_depth,
          ARRAY['sections', (row_number() OVER (ORDER BY s_ord ASC) - 1)::text] path,
          ARRAY[row_number() OVER (ORDER BY s_ord ASC, sect_id)] as ordering_path
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
          rs.content,
          rs.sect_struct,
          st.depth + 1,
          st.path || ARRAY['sections', (row_number() OVER (PARTITION BY st.path ORDER BY s_ord ASC) - 1)::text] path,
          ordering_path || ARRAY[row_number() OVER (PARTITION BY st.path ORDER BY s_ord ASC, rs.sect_id)]
            as ordering_path
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

      js_fhdr := NULL;
      js_fdata := NULL;

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
      -- It has priority over static section structure
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
        ASSERT report_context IS NOT NULL, 'Lost report context';
        -- Execute function for a report and get a section
        EXECUTE format('SELECT section_structure, section_data FROM %I($1,$2)',
          r_result.function_name)
        INTO js_fhdr, js_fdata
        USING
          report_context,
          sserver_id
        ;

        IF js_fdata IS NOT NULL AND jsonb_array_length(js_fdata) > 0 THEN
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
        END IF; -- function returned data

        IF jsonb_array_length(js_fhdr) > 0 THEN
          js_fhdr := section_apply_conditions(js_fhdr, report_context);
        END IF; -- Function returned header
      END IF;-- report section description contains a function

      -- Static section structure is used when there is no function defined
      -- or the function didn't return header
      IF r_result.sect_struct IS NOT NULL AND (js_fhdr IS NULL OR
        jsonb_array_length(js_fhdr) = 0)
      THEN
          js_fhdr := section_apply_conditions(r_result.sect_struct, report_context);
      END IF; -- static sect_struct condition

      IF js_fdata IS NOT NULL THEN
         js_report := jsonb_set(js_report, array_append(r_result.path, 'data'), js_fdata);
         ASSERT js_report IS NOT NULL, format('Report JSON lost in data, section: %s', r_result.sect_id);
      END IF;
      IF js_fhdr IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'header'), js_fhdr);
        ASSERT js_report IS NOT NULL, format('Report JSON lost in header, section: %s', r_result.sect_id);
      END IF;
    END LOOP; -- Over recursive sections query
    RETURN js_report;
END;
$$ LANGUAGE plpgsql;
