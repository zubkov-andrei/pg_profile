/* ==== Function stats history ==== */

CREATE TABLE funcs_list(
    server_id       integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    datid           oid,
    funcid          oid,
    schemaname      name NOT NULL,
    funcname        name NOT NULL,
    funcargs        text NOT NULL,
    last_sample_id  integer,
    CONSTRAINT pk_funcs_list PRIMARY KEY (server_id, datid, funcid),
    CONSTRAINT fk_funcs_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX ix_funcs_list_samples ON funcs_list (server_id, last_sample_id);
COMMENT ON TABLE funcs_list IS 'Function names and schemas, captured in samples';

CREATE TABLE sample_stat_user_functions (
    server_id   integer,
    sample_id   integer,
    datid       oid,
    funcid      oid,
    calls       bigint,
    total_time  double precision,
    self_time   double precision,
    trg_fn      boolean,
    CONSTRAINT fk_user_functions_functions FOREIGN KEY (server_id, datid, funcid)
      REFERENCES funcs_list (server_id, datid, funcid)
      ON DELETE NO ACTION
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_user_functions_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database (server_id, sample_id, datid) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pk_sample_stat_user_functions PRIMARY KEY (server_id, sample_id, datid, funcid)
);
CREATE INDEX ix_sample_stat_user_functions_fl ON sample_stat_user_functions(server_id, datid, funcid);

COMMENT ON TABLE sample_stat_user_functions IS 'Stats increments for user functions in all databases by samples';

CREATE VIEW v_sample_stat_user_functions AS
    SELECT
        server_id,
        sample_id,
        datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        calls,
        total_time,
        self_time,
        trg_fn
    FROM sample_stat_user_functions JOIN funcs_list USING (server_id, datid, funcid);
COMMENT ON VIEW v_sample_stat_user_functions IS 'Reconstructed stats view with function names and schemas';

CREATE TABLE last_stat_user_functions (LIKE v_sample_stat_user_functions, in_sample boolean NOT NULL DEFAULT false)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_user_functions IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_user_func_total (
    server_id   integer,
    sample_id   integer,
    datid       oid,
    calls       bigint,
    total_time  double precision,
    trg_fn      boolean,
    CONSTRAINT fk_user_func_tot_dat FOREIGN KEY (server_id, sample_id, datid)
      REFERENCES sample_stat_database (server_id, sample_id, datid) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pk_sample_stat_user_func_total PRIMARY KEY (server_id, sample_id, datid, trg_fn)
);
COMMENT ON TABLE sample_stat_user_func_total IS 'Total stats for user functions in all databases by samples';
