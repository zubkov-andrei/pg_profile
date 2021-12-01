/* ==== Database stats history tables === */

CREATE TABLE sample_stat_database
(
    server_id           integer,
    sample_id           integer,
    datid               oid,
    datname             name NOT NULL,
    xact_commit         bigint,
    xact_rollback       bigint,
    blks_read           bigint,
    blks_hit            bigint,
    tup_returned        bigint,
    tup_fetched         bigint,
    tup_inserted        bigint,
    tup_updated         bigint,
    tup_deleted         bigint,
    conflicts           bigint,
    temp_files          bigint,
    temp_bytes          bigint,
    deadlocks           bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    stats_reset         timestamp with time zone,
    datsize             bigint,
    datsize_delta       bigint,
    datistemplate       boolean,
    session_time        double precision,
    active_time         double precision,
    idle_in_transaction_time  double precision,
    sessions            bigint,
    sessions_abandoned  bigint,
    sessions_fatal      bigint,
    sessions_killed     bigint,
    CONSTRAINT fk_statdb_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT pk_sample_stat_database PRIMARY KEY (server_id,sample_id,datid)
);
COMMENT ON TABLE sample_stat_database IS 'Sample database statistics table (fields from pg_stat_database)';

CREATE TABLE last_stat_database AS SELECT * FROM sample_stat_database WHERE 0=1;
ALTER TABLE last_stat_database  ADD CONSTRAINT pk_last_stat_database PRIMARY KEY (server_id, sample_id, datid);
ALTER TABLE last_stat_database ADD CONSTRAINT fk_last_stat_database_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_database IS 'Last sample data for calculating diffs in next sample';
