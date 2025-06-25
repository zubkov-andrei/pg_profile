INSERT INTO import_queries_version_order VALUES
('pg_profile','4.9','pg_profile','4.8')
;

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;

ALTER FUNCTION import_section_data_subsample SET search_path=@extschema@;

GRANT SELECT ON table_storage_parameters TO public;
GRANT SELECT ON v_table_storage_parameters TO public;
GRANT SELECT ON index_storage_parameters TO public;
GRANT SELECT ON v_index_storage_parameters TO public;

ALTER TABLE last_stat_tables
  ADD COLUMN reloptions jsonb;
ALTER TABLE last_stat_indexes
  ADD COLUMN reloptions jsonb;

ALTER TABLE servers
  ADD COLUMN srv_settings jsonb
;

UPDATE servers
SET srv_settings =
  jsonb_build_object('relsizes',
    jsonb_build_object(
      'window_start', to_jsonb(size_smp_wnd_start),
      'window_duration', to_jsonb(size_smp_wnd_dur),
      'sample_interval', to_jsonb(size_smp_interval),
      'collect_mode',
        CASE WHEN num_nulls(size_smp_wnd_start, size_smp_wnd_dur, size_smp_interval) = 0 THEN 'schedule'
        ELSE 'on'
        END
    )
  )
;
GRANT SELECT (srv_settings) ON servers TO public;

DROP VIEW v_sample_stat_indexes;
CREATE VIEW v_sample_stat_indexes AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
        tl.schemaname,
        tl.relname,
        tl.relkind,
        il.indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        indisunique,
        relpages_bytes,
        relpages_bytes_diff,
        last_idx_scan
    FROM
        sample_stat_indexes s
        JOIN indexes_list il USING (datid, indexrelid, server_id)
        JOIN tables_list tl USING (datid, relid, server_id);
COMMENT ON VIEW v_sample_stat_indexes IS 'Reconstructed stats view with table and index names and schemas';
GRANT SELECT ON v_sample_stat_indexes TO public;
