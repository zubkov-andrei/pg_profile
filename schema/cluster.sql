/* ==== Clusterwide stats history tables ==== */

CREATE TABLE sample_stat_cluster
(
    server_id                  integer,
    sample_id                  integer,
    checkpoints_timed          bigint,
    checkpoints_req            bigint,
    checkpoint_write_time      double precision,
    checkpoint_sync_time       double precision,
    buffers_checkpoint         bigint,
    buffers_clean              bigint,
    maxwritten_clean           bigint,
    buffers_backend            bigint,
    buffers_backend_fsync      bigint,
    buffers_alloc              bigint,
    stats_reset                timestamp with time zone, --bgwriter_stats_reset actually
    wal_size                   bigint,
    wal_lsn                    pg_lsn,
    in_recovery                boolean,
    restartpoints_timed        bigint,
    restartpoints_req          bigint,
    restartpoints_done         bigint,
    checkpoint_stats_reset     timestamp with time zone,
    checkpoints_done           bigint,
    slru_checkpoint            bigint,
    CONSTRAINT fk_statcluster_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pk_sample_stat_cluster PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_cluster IS 'Sample cluster statistics table (fields from pg_stat_bgwriter, etc.)';

CREATE TABLE last_stat_cluster(LIKE sample_stat_cluster);
ALTER TABLE last_stat_cluster ADD CONSTRAINT pk_last_stat_cluster_samples
  PRIMARY KEY (server_id, sample_id);
ALTER TABLE last_stat_cluster ADD CONSTRAINT fk_last_stat_cluster_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT
    DEFERRABLE INITIALLY IMMEDIATE;
COMMENT ON TABLE last_stat_cluster IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_wal
(
    server_id           integer,
    sample_id           integer,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    wal_buffers_full    bigint,
    wal_write           bigint,
    wal_sync            bigint,
    wal_write_time      double precision,
    wal_sync_time       double precision,
    stats_reset         timestamp with time zone,
    CONSTRAINT fk_statwal_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pk_sample_stat_wal PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_wal IS 'Sample WAL statistics table';

CREATE TABLE last_stat_wal AS SELECT * FROM sample_stat_wal WHERE false;
ALTER TABLE last_stat_wal ADD CONSTRAINT pk_last_stat_wal_samples
  PRIMARY KEY (server_id, sample_id);
ALTER TABLE last_stat_wal ADD CONSTRAINT fk_last_stat_wal_samples
  FOREIGN KEY (server_id, sample_id)
  REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT
    DEFERRABLE INITIALLY IMMEDIATE;
COMMENT ON TABLE last_stat_wal IS 'Last WAL sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_archiver
(
    server_id                   integer,
    sample_id                   integer,
    archived_count              bigint,
    last_archived_wal           text,
    last_archived_time          timestamp with time zone,
    failed_count                bigint,
    last_failed_wal             text,
    last_failed_time            timestamp with time zone,
    stats_reset                 timestamp with time zone,
    CONSTRAINT fk_sample_stat_archiver_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pk_sample_stat_archiver PRIMARY KEY (server_id, sample_id)
);
COMMENT ON TABLE sample_stat_archiver IS 'Sample archiver statistics table (fields from pg_stat_archiver)';

CREATE TABLE last_stat_archiver AS SELECT * FROM sample_stat_archiver WHERE 0=1;
ALTER TABLE last_stat_archiver ADD CONSTRAINT pk_last_stat_archiver_samples
  PRIMARY KEY (server_id, sample_id);
ALTER TABLE last_stat_archiver ADD CONSTRAINT fk_last_stat_archiver_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT
  DEFERRABLE INITIALLY IMMEDIATE;
COMMENT ON TABLE last_stat_archiver IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_io
(
    server_id                   integer,
    sample_id                   integer,
    backend_type                text,
    object                      text,
    context                     text,
    reads                       bigint,
    read_time                   double precision,
    writes                      bigint,
    write_time                  double precision,
    writebacks                  bigint,
    writeback_time              double precision,
    extends                     bigint,
    extend_time                 double precision,
    op_bytes                    bigint,
    hits                        bigint,
    evictions                   bigint,
    reuses                      bigint,
    fsyncs                      bigint,
    fsync_time                  double precision,
    stats_reset                 timestamp with time zone,
    read_bytes                  numeric,
    write_bytes                 numeric,
    extend_bytes                numeric,
    CONSTRAINT pk_sample_stat_io PRIMARY KEY (server_id, sample_id, backend_type, object, context),
    CONSTRAINT fk_sample_stat_io_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
COMMENT ON TABLE sample_stat_io IS 'Sample IO statistics table (fields from pg_stat_io)';

CREATE TABLE last_stat_io (LIKE sample_stat_io);
ALTER TABLE last_stat_io ADD CONSTRAINT pk_last_stat_io_samples
  PRIMARY KEY (server_id, sample_id, backend_type, object, context);
ALTER TABLE last_stat_io ADD CONSTRAINT fk_last_stat_io_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT
  DEFERRABLE INITIALLY IMMEDIATE;
COMMENT ON TABLE last_stat_io IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_slru
(
    server_id     integer,
    sample_id     integer,
    name          text,
    blks_zeroed   bigint,
    blks_hit      bigint,
    blks_read     bigint,
    blks_written  bigint,
    blks_exists   bigint,
    flushes       bigint,
    truncates     bigint,
    stats_reset   timestamp with time zone,
    CONSTRAINT pk_sample_stat_slru PRIMARY KEY (server_id, sample_id, name),
    CONSTRAINT fk_sample_stat_slru_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
COMMENT ON TABLE sample_stat_slru IS 'Sample SLRU statistics table (fields from pg_stat_slru)';

CREATE TABLE last_stat_slru (LIKE sample_stat_slru);
ALTER TABLE last_stat_slru ADD CONSTRAINT pk_last_stat_slru_samples
  PRIMARY KEY (server_id, sample_id, name);
ALTER TABLE last_stat_slru ADD CONSTRAINT fk_last_stat_slru_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT
  DEFERRABLE INITIALLY IMMEDIATE;
COMMENT ON TABLE last_stat_slru IS 'Last sample data for calculating diffs in next sample';
