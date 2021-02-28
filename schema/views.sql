/* ========= Views ========= */
CREATE VIEW v_sample_settings AS
  SELECT
    server_id,
    sample_id,
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
  FROM samples s
    JOIN sample_settings ss USING (server_id)
    JOIN LATERAL
      (SELECT server_id, name, max(first_seen) as first_seen
        FROM sample_settings WHERE server_id = s.server_id AND first_seen <= s.sample_time
        GROUP BY server_id, name) lst
      USING (server_id, name, first_seen)
;
COMMENT ON VIEW v_sample_settings IS 'Provides postgres settings for samples';

CREATE VIEW v_sample_timings AS
SELECT
  srv.server_name,
  smp.sample_id,
  smp.sample_time,
  tm.event as sampling_event,
  tm.time_spent
FROM
  sample_timings tm
  JOIN servers srv USING (server_id)
  JOIN samples smp USING (server_id, sample_id);
COMMENT ON VIEW v_sample_timings IS 'Sample taking time statistics with server names and sample times';

CREATE VIEW v_sample_stat_tables_interpolated AS
  SELECT
    stt.server_id,
    stt.sample_id,
    stt.datid,
    stt.relid,
    stt.seq_scan,
    stt.vacuum_count,
    stt.autovacuum_count,
    round(COALESCE(
      -- relsize if available
      stt.relsize,
      -- interpolation size if available
      interpolation.left_sample_relsize +
      extract(epoch from smp.sample_time - interpolation.left_sample_time)
        * interpolation.int_grow_per_second,
      -- extrapolation as the last hope
      extract(epoch from smp.sample_time - fst_sample.sample_time) *
      (lst_size.relsize - fst_size.relsize) /
      extract(epoch from lst_sample.sample_time - fst_sample.sample_time)
    )) as relsize,
    stt.relsize IS NULL AS relsize_approximated
  FROM
    sample_stat_tables stt
    JOIN samples smp USING (server_id, sample_id)
    /* Getting overall size-collected boundaries for all tables
    * HAVING condition ensures that we have at least two
    * samples with relation size collected
    */
    JOIN (
    SELECT
      server_id,
      datid,
      relid,
      min(sample_id) first_sample,
      max(sample_id) last_sample
    FROM sample_stat_tables
      WHERE relsize IS NOT NULL
    GROUP BY
      server_id,
      datid,
      relid
    HAVING min(sample_id) != max(sample_id)
    ) boundary_size_samples USING (server_id, datid, relid)
    -- Getting boundary relation sizes and times, needed for calculation of overall growth rate
    -- this data will be used when extrapolation is needed
    JOIN samples fst_sample ON
      (boundary_size_samples.server_id,boundary_size_samples.first_sample) =
      (fst_sample.server_id, fst_sample.sample_id)
    JOIN samples lst_sample ON
      (boundary_size_samples.server_id,boundary_size_samples.last_sample) =
      (lst_sample.server_id, lst_sample.sample_id)
    JOIN sample_stat_tables fst_size ON
      (boundary_size_samples.server_id,boundary_size_samples.first_sample,boundary_size_samples.datid,boundary_size_samples.relid) =
      (fst_size.server_id,fst_size.sample_id,fst_size.datid,fst_size.relid)
    JOIN sample_stat_tables lst_size ON
      (boundary_size_samples.server_id,boundary_size_samples.last_sample,boundary_size_samples.datid,boundary_size_samples.relid) =
      (lst_size.server_id,lst_size.sample_id,lst_size.datid,lst_size.relid)

    /* When relation size is unavailable and the sample is between
    * other samples with measured sizes available, we will use interpolation
    */
    LEFT OUTER JOIN LATERAL (
      SELECT
        l.sample_time as left_sample_time,
        l.relsize as left_sample_relsize,
        (r.relsize - l.relsize) / extract(epoch from r.sample_time - l.sample_time) as int_grow_per_second
      FROM (
        SELECT sample_time, relsize
        FROM sample_stat_tables
          JOIN samples USING (server_id, sample_id)
        WHERE (server_id, datid, relid) =
          (stt.server_id, stt.datid, stt.relid)
          AND sample_id < stt.sample_id AND relsize IS NOT NULL
        ORDER BY sample_id DESC
        LIMIT 1) l,
      (
        SELECT sample_time, relsize
        FROM sample_stat_tables
          JOIN samples USING (server_id, sample_id)
        WHERE (server_id, datid, relid) =
          (stt.server_id, stt.datid, stt.relid)
          AND sample_id > stt.sample_id AND relsize IS NOT NULL
        ORDER BY sample_id ASC
        LIMIT 1) r
    ) interpolation ON (stt.relsize IS NULL AND
        stt.sample_id BETWEEN boundary_size_samples.first_sample AND
          boundary_size_samples.last_sample)
;
COMMENT ON VIEW v_sample_stat_tables_interpolated IS 'Tables sizes interpolated for samples without sizes collected';

CREATE VIEW v_sample_stat_indexes_interpolated AS
  SELECT
    stt.server_id,
    stt.sample_id,
    stt.datid,
    il.relid,
    stt.tablespaceid,
    stt.indexrelid,
    round(COALESCE(
      -- relsize if available
      stt.relsize,
      -- interpolation size if available
      interpolation.left_sample_relsize +
      extract(epoch from smp.sample_time - interpolation.left_sample_time)
        * interpolation.int_grow_per_second,
      -- extrapolation as the last hope
      extract(epoch from smp.sample_time - fst_sample.sample_time) *
      (lst_size.relsize - fst_size.relsize) /
      extract(epoch from lst_sample.sample_time - fst_sample.sample_time)
    )) as indexrelsize,
    stt.relsize IS NULL AS indexrelsize_approximated
  FROM
    sample_stat_indexes stt
    JOIN indexes_list il USING (datid, indexrelid, server_id)
    JOIN samples smp USING (server_id, sample_id)
    /* Getting overall size-collected boundaries for all tables
    * HAVING condition ensures that we have at least two
    * samples with relation size collected
    */
    JOIN (
    SELECT
      server_id,
      datid,
      indexrelid,
      min(sample_id) first_sample,
      max(sample_id) last_sample
    FROM sample_stat_indexes
      WHERE relsize IS NOT NULL
    GROUP BY
      server_id,
      datid,
      indexrelid
    HAVING min(sample_id) != max(sample_id)
    ) boundary_size_samples USING (server_id, datid, indexrelid)
    -- Getting boundary relation sizes and times, needed for calculation of overall growth rate
    -- this data will be used when extrapolation is needed
    JOIN samples fst_sample ON
      (boundary_size_samples.server_id,boundary_size_samples.first_sample) =
      (fst_sample.server_id, fst_sample.sample_id)
    JOIN samples lst_sample ON
      (boundary_size_samples.server_id,boundary_size_samples.last_sample) =
      (lst_sample.server_id, lst_sample.sample_id)
    JOIN sample_stat_indexes fst_size ON
      (boundary_size_samples.server_id,boundary_size_samples.first_sample,boundary_size_samples.datid,boundary_size_samples.indexrelid) =
      (fst_size.server_id,fst_size.sample_id,fst_size.datid,fst_size.indexrelid)
    JOIN sample_stat_indexes lst_size ON
      (boundary_size_samples.server_id,boundary_size_samples.last_sample,boundary_size_samples.datid,boundary_size_samples.indexrelid) =
      (lst_size.server_id,lst_size.sample_id,lst_size.datid,lst_size.indexrelid)

    /* When relation size is unavailable and the sample is between
    * other samples with measured sizes available, we will use interpolation
    */
    LEFT OUTER JOIN LATERAL (
      SELECT
        l.sample_time as left_sample_time,
        l.relsize as left_sample_relsize,
        (r.relsize - l.relsize) / extract(epoch from r.sample_time - l.sample_time) as int_grow_per_second
      FROM (
        SELECT sample_time, relsize
        FROM sample_stat_indexes
          JOIN samples USING (server_id, sample_id)
        WHERE (server_id, datid, indexrelid) =
          (stt.server_id, stt.datid, stt.indexrelid)
          AND sample_id < stt.sample_id AND relsize IS NOT NULL
        ORDER BY sample_id DESC
        LIMIT 1) l,
      (
        SELECT sample_time, relsize
        FROM sample_stat_indexes
          JOIN samples USING (server_id, sample_id)
        WHERE (server_id, datid, indexrelid) =
          (stt.server_id, stt.datid, stt.indexrelid)
          AND sample_id > stt.sample_id AND relsize IS NOT NULL
        ORDER BY sample_id ASC
        LIMIT 1) r
    ) interpolation ON (stt.relsize IS NULL AND
        stt.sample_id BETWEEN boundary_size_samples.first_sample AND
          boundary_size_samples.last_sample)
;
COMMENT ON VIEW v_sample_stat_indexes_interpolated IS 'Tables sizes interpolated for samples without sizes collected';
