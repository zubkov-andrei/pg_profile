/* ==== rusage statements history tables ==== */
CREATE TABLE sample_kcache (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    datid               oid,
    queryid             bigint,
    queryid_md5         char(32),
    plan_user_time      double precision, --  User CPU time used
    plan_system_time    double precision, --  System CPU time used
    plan_minflts         bigint, -- Number of page reclaims (soft page faults)
    plan_majflts         bigint, -- Number of page faults (hard page faults)
    plan_nswaps         bigint, -- Number of swaps
    plan_reads          bigint, -- Number of bytes read by the filesystem layer
    plan_writes         bigint, -- Number of bytes written by the filesystem layer
    plan_msgsnds        bigint, -- Number of IPC messages sent
    plan_msgrcvs        bigint, -- Number of IPC messages received
    plan_nsignals       bigint, -- Number of signals received
    plan_nvcsws         bigint, -- Number of voluntary context switches
    plan_nivcsws        bigint,
    exec_user_time      double precision, --  User CPU time used
    exec_system_time    double precision, --  System CPU time used
    exec_minflts         bigint, -- Number of page reclaims (soft page faults)
    exec_majflts         bigint, -- Number of page faults (hard page faults)
    exec_nswaps         bigint, -- Number of swaps
    exec_reads          bigint, -- Number of bytes read by the filesystem layer
    exec_writes         bigint, -- Number of bytes written by the filesystem layer
    exec_msgsnds        bigint, -- Number of IPC messages sent
    exec_msgrcvs        bigint, -- Number of IPC messages received
    exec_nsignals       bigint, -- Number of signals received
    exec_nvcsws         bigint, -- Number of voluntary context switches
    exec_nivcsws        bigint,
    toplevel            boolean,
    stats_since         timestamp with time zone,
    CONSTRAINT pk_sample_kcache_n PRIMARY KEY (server_id, sample_id, datid, userid, queryid, toplevel),
    CONSTRAINT fk_kcache_stmt_list FOREIGN KEY (server_id,queryid_md5)
      REFERENCES stmt_list (server_id,queryid_md5)
      ON DELETE NO ACTION ON UPDATE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_kcache_st FOREIGN KEY (server_id, sample_id, datid, userid, queryid, toplevel)
      REFERENCES sample_statements(server_id, sample_id, datid, userid, queryid, toplevel) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX ix_sample_kcache_sl ON sample_kcache(server_id,queryid_md5);

COMMENT ON TABLE sample_kcache IS 'Sample sample_kcache statistics table (fields from pg_stat_kcache)';

CREATE TABLE last_stat_kcache (
    server_id           integer,
    sample_id           integer,
    userid              oid,
    datid               oid,
    toplevel            boolean DEFAULT true,
    queryid             bigint,
    plan_user_time      double precision, --  User CPU time used
    plan_system_time    double precision, --  System CPU time used
    plan_minflts         bigint, -- Number of page reclaims (soft page faults)
    plan_majflts         bigint, -- Number of page faults (hard page faults)
    plan_nswaps         bigint, -- Number of swaps
    plan_reads          bigint, -- Number of bytes read by the filesystem layer
    plan_writes         bigint, -- Number of bytes written by the filesystem layer
    plan_msgsnds        bigint, -- Number of IPC messages sent
    plan_msgrcvs        bigint, -- Number of IPC messages received
    plan_nsignals       bigint, -- Number of signals received
    plan_nvcsws         bigint, -- Number of voluntary context switches
    plan_nivcsws        bigint,
    exec_user_time      double precision, --  User CPU time used
    exec_system_time    double precision, --  System CPU time used
    exec_minflts         bigint, -- Number of page reclaims (soft page faults)
    exec_majflts         bigint, -- Number of page faults (hard page faults)
    exec_nswaps         bigint, -- Number of swaps
    exec_reads          bigint, -- Number of bytes read by the filesystem layer
    exec_writes         bigint, -- Number of bytes written by the filesystem layer
    exec_msgsnds        bigint, -- Number of IPC messages sent
    exec_msgrcvs        bigint, -- Number of IPC messages received
    exec_nsignals       bigint, -- Number of signals received
    exec_nvcsws         bigint, -- Number of voluntary context switches
    exec_nivcsws        bigint,
    stats_since         timestamp with time zone
)
PARTITION BY LIST (server_id);

CREATE TABLE sample_kcache_total (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    plan_user_time           double precision, --  User CPU time used
    plan_system_time         double precision, --  System CPU time used
    plan_minflts              bigint, -- Number of page reclaims (soft page faults)
    plan_majflts              bigint, -- Number of page faults (hard page faults)
    plan_nswaps              bigint, -- Number of swaps
    plan_reads               bigint, -- Number of bytes read by the filesystem layer
    --plan_reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
    plan_writes              bigint, -- Number of bytes written by the filesystem layer
    --plan_writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
    plan_msgsnds             bigint, -- Number of IPC messages sent
    plan_msgrcvs             bigint, -- Number of IPC messages received
    plan_nsignals            bigint, -- Number of signals received
    plan_nvcsws              bigint, -- Number of voluntary context switches
    plan_nivcsws             bigint,
    exec_user_time           double precision, --  User CPU time used
    exec_system_time         double precision, --  System CPU time used
    exec_minflts              bigint, -- Number of page reclaims (soft page faults)
    exec_majflts              bigint, -- Number of page faults (hard page faults)
    exec_nswaps              bigint, -- Number of swaps
    exec_reads               bigint, -- Number of bytes read by the filesystem layer
    --exec_reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
    exec_writes              bigint, -- Number of bytes written by the filesystem layer
    --exec_writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
    exec_msgsnds             bigint, -- Number of IPC messages sent
    exec_msgrcvs             bigint, -- Number of IPC messages received
    exec_nsignals            bigint, -- Number of signals received
    exec_nvcsws              bigint, -- Number of voluntary context switches
    exec_nivcsws             bigint,
    statements               bigint NOT NULL,
    CONSTRAINT pk_sample_kcache_total PRIMARY KEY (server_id, sample_id, datid),
    CONSTRAINT fk_kcache_t_st FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database(server_id, sample_id, datid) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
COMMENT ON TABLE sample_kcache_total IS 'Aggregated stats for kcache, based on pg_stat_kcache';
