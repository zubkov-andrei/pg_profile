/* ========= Authentication Statistics (pg_auth_mon) ========= */

-- Table to store authentication statistics deltas per sample
CREATE TABLE sample_stat_auth (
    server_id           integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE
                        DEFERRABLE INITIALLY IMMEDIATE,
    sample_id           integer NOT NULL,
    rolname             name,
    successful_logins   bigint,
    failed_logins       bigint,
    hba_conflicts       bigint,
    CONSTRAINT uq_sample_stat_auth UNIQUE NULLS NOT DISTINCT (server_id, sample_id, rolname),
    CONSTRAINT fk_sample_stat_auth_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);

COMMENT ON TABLE sample_stat_auth IS 'Authentication statistics deltas from pg_auth_mon extension';
COMMENT ON COLUMN sample_stat_auth.rolname IS 'Role name. NULL indicates invalid/non-existent usernames';
COMMENT ON COLUMN sample_stat_auth.successful_logins IS 'Number of successful authentication attempts since last sample';
COMMENT ON COLUMN sample_stat_auth.failed_logins IS 'Number of failed authentication attempts since last sample';
COMMENT ON COLUMN sample_stat_auth.hba_conflicts IS 'Number of HBA conflicts since last sample';

CREATE INDEX ix_sample_stat_auth ON sample_stat_auth(server_id, sample_id);
CREATE INDEX ix_sample_stat_auth_role ON sample_stat_auth(server_id, sample_id, rolname);

-- Table to store last cumulative values for delta calculation
CREATE TABLE last_stat_auth (
    server_id                   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE
                                DEFERRABLE INITIALLY IMMEDIATE,
    sample_id                   integer NOT NULL,
    rolname                     name NOT NULL,
    successful_attempts         bigint,
    other_auth_failures         bigint,
    total_hba_conflicts         bigint,
    CONSTRAINT pk_last_stat_auth PRIMARY KEY (server_id, rolname)
);

COMMENT ON TABLE last_stat_auth IS 'Last cumulative authentication statistics for delta calculation';

CREATE INDEX ix_last_stat_auth ON last_stat_auth(server_id, sample_id);

-- Aggregated authentication statistics per sample (all users combined)
CREATE TABLE sample_stat_auth_total (
    server_id           integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE
                        DEFERRABLE INITIALLY IMMEDIATE,
    sample_id           integer NOT NULL,
    total_successful    bigint,
    total_failed        bigint,
    total_hba_conflicts bigint,
    unique_users        integer,
    CONSTRAINT pk_sample_stat_auth_total PRIMARY KEY (server_id, sample_id),
    CONSTRAINT fk_sample_stat_auth_total_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);

COMMENT ON TABLE sample_stat_auth_total IS 'Aggregated authentication statistics across all users';
COMMENT ON COLUMN sample_stat_auth_total.unique_users IS 'Number of distinct users with authentication attempts';

CREATE INDEX ix_sample_stat_auth_total ON sample_stat_auth_total(server_id, sample_id);
