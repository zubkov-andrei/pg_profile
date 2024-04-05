INSERT INTO import_queries_version_order VALUES
('pg_profile','4.5','pg_profile','4.4')
;

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;

-- Move reltoastrelid to sample_stat_tables
ALTER TABLE sample_stat_tables
  ADD COLUMN reltoastrelid       oid,
  ADD CONSTRAINT fk_st_tables_toast FOREIGN KEY (server_id, sample_id, datid, reltoastrelid)
      REFERENCES sample_stat_tables(server_id, sample_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE
;

UPDATE sample_stat_tables usst
SET reltoastrelid = tl.reltoastrelid
FROM tables_list tl JOIN sample_stat_tables sst
  ON (sst.server_id, sst.datid, sst.relid) =
     (tl.server_id, tl.datid, tl.reltoastrelid)
WHERE 
  (usst.server_id, usst.sample_id, usst.datid, usst.relid) =
  (tl.server_id, sst.sample_id, tl.datid, tl.relid)
  AND coalesce(tl.reltoastrelid, 0) != 0
;

DROP VIEW v_sample_stat_tables;

ALTER TABLE tables_list
  DROP COLUMN reltoastrelid
;

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
        n_tup_newpage_upd
    FROM sample_stat_tables
      JOIN tables_list USING (server_id, datid, relid)
      JOIN tablespaces_list tl USING (server_id, tablespaceid);
COMMENT ON VIEW v_sample_stat_tables IS 'Tables stats view with table names and schemas';
GRANT SELECT ON v_sample_stat_tables TO public;
