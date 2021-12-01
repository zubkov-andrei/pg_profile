/* === Statements history tables ==== */
CREATE TABLE stmt_list(
    server_id      integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    queryid_md5    char(32),
    query          text,
    CONSTRAINT pk_stmt_list PRIMARY KEY (server_id, queryid_md5)
);
COMMENT ON TABLE stmt_list IS 'Statements, captured in samples';

CREATE TABLE sample_statements (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    datid               oid,
    queryid             bigint,
    queryid_md5         char(32),
    plans               bigint,
    total_plan_time     double precision,
    min_plan_time       double precision,
    max_plan_time       double precision,
    mean_plan_time      double precision,
    stddev_plan_time    double precision,
    calls               bigint,
    total_exec_time     double precision,
    min_exec_time       double precision,
    max_exec_time       double precision,
    mean_exec_time      double precision,
    stddev_exec_time    double precision,
    rows                bigint,
    shared_blks_hit     bigint,
    shared_blks_read    bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit      bigint,
    local_blks_read     bigint,
    local_blks_dirtied  bigint,
    local_blks_written  bigint,
    temp_blks_read      bigint,
    temp_blks_written   bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    toplevel            boolean,
    CONSTRAINT pk_sample_statements_n PRIMARY KEY (server_id,sample_id,datid,userid,queryid),
    CONSTRAINT fk_stmt_list FOREIGN KEY (server_id,queryid_md5)
      REFERENCES stmt_list (server_id,queryid_md5) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_statments_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE,
    CONSTRAINT fk_statements_roles FOREIGN KEY (server_id, userid)
      REFERENCES roles_list (server_id, userid)
);
CREATE INDEX ix_sample_stmts_qid ON sample_statements (queryid_md5);
COMMENT ON TABLE sample_statements IS 'Sample statement statistics table (fields from pg_stat_statements)';

CREATE TABLE sample_statements_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    plans               bigint,
    total_plan_time     double precision,
    calls               bigint,
    total_exec_time     double precision,
    rows                bigint,
    shared_blks_hit     bigint,
    shared_blks_read    bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit      bigint,
    local_blks_read     bigint,
    local_blks_dirtied  bigint,
    local_blks_written  bigint,
    temp_blks_read      bigint,
    temp_blks_written   bigint,
    blk_read_time       double precision,
    blk_write_time      double precision,
    wal_records         bigint,
    wal_fpi             bigint,
    wal_bytes           numeric,
    statements          bigint,
    CONSTRAINT pk_sample_statements_total PRIMARY KEY (server_id, sample_id, datid),
    CONSTRAINT fk_statments_t_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_statements_total IS 'Aggregated stats for sample, based on pg_stat_statements';
