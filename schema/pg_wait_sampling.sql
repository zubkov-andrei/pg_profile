CREATE TABLE wait_sampling_total(
    server_id           integer,
    sample_id           integer,
    sample_wevnt_id     integer,
    event_type          text NOT NULL,
    event               text NOT NULL,
    tot_waited          bigint NOT NULL,
    stmt_waited         bigint,
    CONSTRAINT pk_sample_weid PRIMARY KEY (server_id, sample_id, sample_wevnt_id),
    CONSTRAINT uk_sample_we UNIQUE (server_id, sample_id, event_type, event),
    CONSTRAINT fk_wait_sampling_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
