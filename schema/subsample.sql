/* Subsample objects storage */

CREATE TABLE server_subsample (
    server_id           integer,
    subsample_enabled   boolean DEFAULT true NOT NULL,
    min_query_dur       interval hour to second,
    min_xact_dur        interval hour to second,
    min_xact_age        bigint,
    min_idle_xact_dur   interval hour to second,
    CONSTRAINT pk_server_subsample PRIMARY KEY (server_id),
    CONSTRAINT fk_server_subsample_server FOREIGN KEY (server_id)
      REFERENCES servers(server_id) ON DELETE CASCADE
);
COMMENT ON TABLE server_subsample IS 'Server subsampling settings';

CREATE TABLE sample_act_backend (
    server_id         integer,
    sample_id         integer,
    pid               integer,
    backend_start     timestamp with time zone,
    datid             oid,
    datname           name,
    usesysid          oid,
    usename           name,
    client_addr       inet,
    client_hostname   text,
    client_port       integer,
    backend_type      text,
    backend_last_ts   timestamp with time zone,
    CONSTRAINT pk_sample_backends PRIMARY KEY (server_id, sample_id, pid, backend_start),
    CONSTRAINT fk_backends_sample FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);

CREATE TABLE sample_act_xact (
    server_id         integer,
    sample_id         integer,
    pid               integer,
    backend_start     timestamp with time zone,
    xact_start        timestamp with time zone,
    backend_xid       xid,
    xact_last_ts      timestamp with time zone,
    CONSTRAINT pk_sample_xact PRIMARY KEY (server_id, sample_id, pid, xact_start),
    CONSTRAINT fk_xact_act_backend FOREIGN KEY (server_id, sample_id, pid, backend_start)
      REFERENCES sample_act_backend(server_id, sample_id, pid, backend_start)
      ON DELETE CASCADE
);

CREATE TABLE sample_act_backend_state (
    server_id         integer,
    sample_id         integer,
    pid               integer,
    backend_start     timestamp with time zone,
    application_name  text,
    state_code        integer, -- 1 idle in xact, 2 idle in xact aborted, 3 active
    state_change      timestamp with time zone,
    state_last_ts     timestamp with time zone,
    xact_start        timestamp with time zone,
    backend_xmin      xid,
    backend_xmin_age  bigint,
    CONSTRAINT pk_sample_bk_state PRIMARY KEY (server_id, sample_id, pid, state_change),
    CONSTRAINT fk_bk_state_bk FOREIGN KEY (server_id, sample_id, pid, backend_start)
      REFERENCES sample_act_backend (server_id, sample_id, pid, backend_start)
      ON DELETE CASCADE,
    CONSTRAINT fk_bk_state_xact FOREIGN KEY (server_id, sample_id, pid, xact_start)
      REFERENCES sample_act_xact (server_id, sample_id, pid, xact_start)
      ON DELETE CASCADE
);

CREATE TABLE act_query(
    server_id      integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    act_query_md5  char(32),
    act_query      text,
    last_sample_id integer,
    CONSTRAINT pk_act_query PRIMARY KEY (server_id, act_query_md5),
    CONSTRAINT fk_act_query_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX ix_act_query_smp ON act_query(server_id, last_sample_id);
COMMENT ON TABLE act_query IS 'Statements, captured in subsamples from pg_stat_activity';

CREATE TABLE sample_act_statement (
    server_id         integer,
    sample_id         integer,
    pid               integer,
    leader_pid        integer,
    state_change      timestamp with time zone,
    query_start       timestamp with time zone,
    query_id          bigint,
    act_query_md5     char(32),
    stmt_last_ts      timestamp with time zone,
    CONSTRAINT pk_sample_act_stmt PRIMARY KEY (server_id, sample_id, pid, query_start),
    CONSTRAINT fk_act_stmt_bk_state FOREIGN KEY (server_id, sample_id, pid, state_change)
      REFERENCES sample_act_backend_state (server_id, sample_id, pid, state_change)
      ON DELETE CASCADE,
    CONSTRAINT fk_act_stmt_query FOREIGN KEY (server_id, act_query_md5)
      REFERENCES act_query(server_id, act_query_md5)
      ON DELETE CASCADE
);

CREATE TABLE last_stat_activity (
    server_id         integer,
    sample_id         integer,
    subsample_ts      timestamp with time zone,
    datid             oid,
    datname           name,
    pid               integer,
    leader_pid        integer,
    usesysid          oid,
    usename           name,
    application_name  text,
    client_addr       inet,
    client_hostname   text,
    client_port       integer,
    backend_start     timestamp with time zone,
    xact_start        timestamp with time zone,
    query_start       timestamp with time zone,
    state_change      timestamp with time zone,
    state             text,
    backend_xid       xid,
    backend_xmin      xid,
    query_id          bigint,
    query             text,
    backend_type      text,
    backend_xmin_age  bigint
)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_activity IS 'Subsample table for interesting stat_activity entries';

CREATE TABLE session_attr(
    server_id         integer,
    sess_attr_id      serial,
    backend_type      text,
    datid             oid,
    datname           name,
    usesysid          oid,
    usename           name,
    application_name  text,
    client_addr       inet,
    last_sample_id    integer,
    CONSTRAINT pk_subsample_sa PRIMARY KEY (server_id, sess_attr_id),
    CONSTRAINT uk_subsample_sa UNIQUE (server_id, backend_type, datid,
      datname, usesysid, usename, application_name, client_addr),
    CONSTRAINT fk_session_attr_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
--ALTER SEQUENCE session_attr_sess_attr_id CYCLE;

CREATE TABLE sample_stat_activity_cnt(
    server_id         integer,
    sample_id         integer,
    subsample_ts      timestamp with time zone,

    sess_attr_id      integer,

    total             integer,
    active            integer,
    idle              integer,
    idle_t            integer,
    idle_ta           integer,
    state_null        integer,
    lwlock            integer,
    lock              integer,
    bufferpin         integer,
    activity          integer,
    extension         integer,
    client            integer,
    ipc               integer,
    timeout           integer,
    io                integer,

    CONSTRAINT pk_sample_stat_activity_cnt PRIMARY KEY (
      server_id, sample_id, subsample_ts, sess_attr_id
    ),
    CONSTRAINT subsample_attrs FOREIGN KEY (server_id, sess_attr_id)
      REFERENCES session_attr(server_id, sess_attr_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);

CREATE TABLE last_stat_activity_count(
    server_id         integer,
    sample_id         integer,
    subsample_ts      timestamp with time zone,
    backend_type      text,
    datid             oid,
    datname           name,
    usesysid          oid,
    usename           name,
    application_name  text,
    client_addr       inet,
    total             integer,
    active            integer,
    idle              integer,
    idle_t            integer,
    idle_ta           integer,
    state_null        integer,
    lwlock            integer,
    lock              integer,
    bufferpin         integer,
    activity          integer,
    extension         integer,
    client            integer,
    ipc               integer,
    timeout           integer,
    io                integer
)
PARTITION BY LIST (server_id);

GRANT SELECT ON sample_act_backend TO pg_read_all_stats;
GRANT SELECT ON sample_act_xact TO pg_read_all_stats;
GRANT SELECT ON sample_act_backend_state TO pg_read_all_stats;
GRANT SELECT ON act_query TO pg_read_all_stats;
GRANT SELECT ON sample_act_statement TO pg_read_all_stats;
GRANT SELECT ON sample_stat_activity_cnt TO pg_read_all_stats;
GRANT SELECT ON last_stat_activity_count TO pg_read_all_stats;
GRANT SELECT ON session_attr TO pg_read_all_stats;
