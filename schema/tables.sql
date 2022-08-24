/* ==== Tables stats history ==== */
CREATE TABLE tables_list(
    server_id           integer REFERENCES servers(server_id) ON DELETE CASCADE,
    datid               oid,
    relid               oid,
    relkind             char(1) NOT NULL,
    reltoastrelid       oid,
    schemaname          name NOT NULL,
    relname             name NOT NULL,
    last_sample_id      integer,
    CONSTRAINT pk_tables_list PRIMARY KEY (server_id, datid, relid),
    CONSTRAINT fk_tables_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_toast_table FOREIGN KEY (server_id, datid, reltoastrelid)
      REFERENCES tables_list (server_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT uk_toast_table UNIQUE (server_id, datid, reltoastrelid)
);
CREATE INDEX ix_tables_list_samples ON tables_list(server_id, last_sample_id);
COMMENT ON TABLE tables_list IS 'Table names and schemas, captured in samples';

CREATE TABLE sample_stat_tables (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    tablespaceid        oid NOT NULL,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_live_tup          bigint,
    n_dead_tup          bigint,
    n_mod_since_analyze bigint,
    n_ins_since_vacuum  bigint,
    last_vacuum         timestamp with time zone,
    last_autovacuum     timestamp with time zone,
    last_analyze        timestamp with time zone,
    last_autoanalyze    timestamp with time zone,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    heap_blks_read      bigint,
    heap_blks_hit       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    toast_blks_read     bigint,
    toast_blks_hit      bigint,
    tidx_blks_read      bigint,
    tidx_blks_hit       bigint,
    relsize             bigint,
    relsize_diff        bigint,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint,
    CONSTRAINT pk_sample_stat_tables PRIMARY KEY (server_id, sample_id, datid, relid),
    CONSTRAINT fk_st_tables_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tables_tablespace FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tables_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid)
      ON DELETE NO ACTION ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX is_sample_stat_tables_ts ON sample_stat_tables(server_id, sample_id, tablespaceid);
CREATE INDEX ix_sample_stat_tables_rel ON sample_stat_tables(server_id, datid, relid);

COMMENT ON TABLE sample_stat_tables IS 'Stats increments for user tables in all databases by samples';

CREATE VIEW v_sample_stat_tables AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
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
        relpages_bytes_diff
    FROM sample_stat_tables JOIN tables_list USING (server_id, datid, relid);
COMMENT ON VIEW v_sample_stat_tables IS 'Tables stats view with table names and schemas';

CREATE TABLE last_stat_tables(
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    schemaname          name,
    relname             name,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_live_tup          bigint,
    n_dead_tup          bigint,
    n_mod_since_analyze bigint,
    n_ins_since_vacuum  bigint,
    last_vacuum         timestamp with time zone,
    last_autovacuum     timestamp with time zone,
    last_analyze        timestamp with time zone,
    last_autoanalyze    timestamp with time zone,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    heap_blks_read      bigint,
    heap_blks_hit       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    toast_blks_read     bigint,
    toast_blks_hit      bigint,
    tidx_blks_read      bigint,
    tidx_blks_hit       bigint,
    relsize             bigint,
    relsize_diff        bigint,
    tablespaceid        oid,
    reltoastrelid       oid,
    relkind             char(1),
    in_sample           boolean NOT NULL DEFAULT false,
    relpages_bytes      bigint,
    relpages_bytes_diff bigint
);
ALTER TABLE last_stat_tables ADD CONSTRAINT pk_last_stat_tables
  PRIMARY KEY (server_id, sample_id, datid, relid);
ALTER TABLE last_stat_tables ADD CONSTRAINT fk_last_stat_tables_dat
  FOREIGN KEY (server_id, sample_id, datid)
  REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_tables IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_tables_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    tablespaceid        oid,
    relkind             char(1) NOT NULL,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    heap_blks_read      bigint,
    heap_blks_hit       bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint,
    toast_blks_read     bigint,
    toast_blks_hit      bigint,
    tidx_blks_read      bigint,
    tidx_blks_hit       bigint,
    relsize_diff        bigint,
    CONSTRAINT pk_sample_stat_tables_tot PRIMARY KEY (server_id, sample_id, datid, relkind, tablespaceid),
    CONSTRAINT fk_st_tables_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_st_tablespaces_tot_dat FOREIGN KEY (server_id, sample_id, tablespaceid)
      REFERENCES sample_stat_tablespaces(server_id, sample_id, tablespaceid) ON DELETE CASCADE
);
CREATE INDEX ix_sample_stat_tables_total_ts ON sample_stat_tables_total(server_id, sample_id, tablespaceid);

COMMENT ON TABLE sample_stat_tables_total IS 'Total stats for all tables in all databases by samples';
