/* ==== Indexes stats tables ==== */
CREATE TABLE indexes_list(
    server_id       integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    datid           oid NOT NULL,
    indexrelid      oid NOT NULL,
    relid           oid NOT NULL,
    schemaname      name NOT NULL,
    indexrelname    name NOT NULL,
    CONSTRAINT pk_indexes_list PRIMARY KEY (server_id, datid, indexrelid),
    CONSTRAINT fk_indexes_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid)
);
COMMENT ON TABLE indexes_list IS 'Index names and schemas, captured in samples';

CREATE TABLE sample_stat_indexes (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    indexrelid          oid,
    tablespaceid        oid NOT NULL,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize             bigint,
    relsize_diff        bigint,
    indisunique         bool,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint,
    CONSTRAINT fk_stat_indexes_indexes FOREIGN KEY (server_id, datid, indexrelid)
      REFERENCES indexes_list(server_id, datid, indexrelid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_stat_indexes_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_stat_indexes_tablespaces FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_indexes PRIMARY KEY (server_id, sample_id, datid, indexrelid)
);
COMMENT ON TABLE sample_stat_indexes IS 'Stats increments for user indexes in all databases by samples';

CREATE VIEW v_sample_stat_indexes AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
        tl.schemaname,
        tl.relname,
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
        relpages_bytes_diff
    FROM
        sample_stat_indexes s
        JOIN indexes_list il USING (datid, indexrelid, server_id)
        JOIN tables_list tl USING (datid, relid, server_id);
COMMENT ON VIEW v_sample_stat_indexes IS 'Reconstructed stats view with table and index names and schemas';

CREATE TABLE last_stat_indexes (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    indexrelid          oid,
    schemaname          name,
    relname             name,
    indexrelname        name,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize             bigint,
    relsize_diff        bigint,
    tablespaceid        oid NOT NULL,
    indisunique         bool,
    in_sample           boolean NOT NULL DEFAULT false,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint
);
ALTER TABLE last_stat_indexes ADD CONSTRAINT pk_last_stat_indexes PRIMARY KEY (server_id, sample_id, datid, relid, indexrelid);
ALTER TABLE last_stat_indexes ADD CONSTRAINT fk_last_stat_indexes_dat FOREIGN KEY (server_id, sample_id, datid)
-- Restrict deleting last data sample
  REFERENCES last_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_indexes IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_indexes_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    tablespaceid        oid,
    idx_scan            bigint,
    idx_tup_read        bigint,
    idx_tup_fetch       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    relsize_diff         bigint,
    CONSTRAINT fk_stat_indexes_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_stat_tablespaces_tot_dat FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_indexes_tot PRIMARY KEY (server_id, sample_id, datid, tablespaceid)
);
COMMENT ON TABLE sample_stat_indexes_total IS 'Total stats for indexes in all databases by samples';
