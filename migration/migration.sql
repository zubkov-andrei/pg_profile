INSERT INTO import_queries_version_order VALUES
('pg_profile','4.10','pg_profile','4.9')
;

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;


ALTER TABLE last_stat_tables
  ADD COLUMN total_vacuum_time double precision,
  ADD COLUMN total_autovacuum_time double precision,
  ADD COLUMN total_analyze_time double precision,
  ADD COLUMN total_autoanalyze_time double precision;

ALTER TABLE sample_stat_tables
  ADD COLUMN total_vacuum_time double precision,
  ADD COLUMN total_autovacuum_time double precision,
  ADD COLUMN total_analyze_time double precision,
  ADD COLUMN total_autoanalyze_time double precision;

ALTER TABLE sample_stat_tables_total
  ADD COLUMN total_vacuum_time double precision,
  ADD COLUMN total_autovacuum_time double precision,
  ADD COLUMN total_analyze_time double precision,
  ADD COLUMN total_autoanalyze_time double precision;


DROP VIEW v_sample_stat_tables;
CREATE VIEW v_sample_stat_tables AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        tablespacename,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze,
        n_ins_since_vacuum,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count,
        heap_blks_read,
        heap_blks_hit,
        idx_blks_read,
        idx_blks_hit,
        toast_blks_read,
        toast_blks_hit,
        tidx_blks_read,
        tidx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        reltoastrelid,
        relkind,
        relpages_bytes,
        relpages_bytes_diff,
        last_seq_scan,
        last_idx_scan,
        n_tup_newpage_upd,
        total_vacuum_time,
        total_autovacuum_time,
        total_analyze_time,
        total_autoanalyze_time
    FROM sample_stat_tables
      JOIN tables_list USING (server_id, datid, relid)
      JOIN tablespaces_list tl USING (server_id, tablespaceid);
COMMENT ON VIEW v_sample_stat_tables IS 'Tables stats view with table names and schemas';
GRANT SELECT ON v_sample_stat_tables TO public;

ALTER TABLE sample_stat_io
  ADD COLUMN read_bytes numeric,
  ADD COLUMN write_bytes numeric,
  ADD COLUMN extend_bytes numeric;

ALTER TABLE last_stat_io
  ADD COLUMN read_bytes numeric,
  ADD COLUMN write_bytes numeric,
  ADD COLUMN extend_bytes numeric;

ALTER TABLE sample_stat_cluster
  ADD COLUMN checkpoints_done bigint,
  ADD COLUMN slru_checkpoint bigint;

ALTER TABLE last_stat_cluster
  ADD COLUMN checkpoints_done bigint,
  ADD COLUMN slru_checkpoint bigint;

ALTER TABLE sample_stat_database
  ADD COLUMN parallel_workers_to_launch bigint,
  ADD COLUMN parallel_workers_launched bigint;

ALTER TABLE last_stat_database
  ADD COLUMN parallel_workers_to_launch bigint,
  ADD COLUMN parallel_workers_launched bigint;

DO
$$
  BEGIN
    BEGIN
      ALTER TABLE last_stat_database
        RENAME CONSTRAINT sample_stat_database_datid_not_null TO last_stat_database_datid_not_null;
      ALTER TABLE last_stat_database
        RENAME CONSTRAINT sample_stat_database_datname_not_null TO last_stat_database_datname_not_null;
      ALTER TABLE last_stat_database
        RENAME CONSTRAINT sample_stat_database_sample_id_not_null TO last_stat_database_sample_id_not_null;
      ALTER TABLE last_stat_database
        RENAME CONSTRAINT sample_stat_database_server_id_not_null TO last_stat_database_server_id_not_null;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  END;
$$;

ALTER TABLE sample_statements
  ADD COLUMN wal_buffers_full bigint,
  ADD COLUMN parallel_workers_to_launch bigint,
  ADD COLUMN parallel_workers_launched bigint;

ALTER TABLE last_stat_statements
  ADD COLUMN wal_buffers_full bigint,
  ADD COLUMN parallel_workers_to_launch bigint,
  ADD COLUMN parallel_workers_launched bigint;

ALTER TABLE sample_statements_total
  ADD COLUMN wal_buffers_full bigint;

