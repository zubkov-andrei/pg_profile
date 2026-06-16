/* ========= Core tables ========= */

/* This table has custom processing in export_data() function - review
of export_data() should be performed after any fiels change */

CREATE TABLE servers (
    server_id           SERIAL PRIMARY KEY,
    server_name         name UNIQUE NOT NULL,
    server_description  text,
    server_created      timestamp with time zone DEFAULT now(),
    db_exclude          name[] DEFAULT NULL,
    enabled             boolean DEFAULT TRUE,
    connstr             text,
    max_sample_age      integer NULL,
    last_sample_id      integer DEFAULT 0 NOT NULL,
    size_smp_wnd_start  time with time zone,
    size_smp_wnd_dur    interval hour to second,
    size_smp_interval   interval day to minute,
    srv_settings        jsonb,
    server_hostname     text,
    server_ip           text,
    server_port         integer
);
COMMENT ON TABLE servers IS 'Monitored servers (Postgres clusters) list';
COMMENT ON COLUMN servers.server_hostname IS 'Physical server hostname from hostname() extension. NULL when hostname extension is not installed.';
COMMENT ON COLUMN servers.server_ip IS 'Server listen address from pg_settings. May be a comma-separated list (e.g. ''*'' or ''0.0.0.0'').';
COMMENT ON COLUMN servers.server_port IS 'Server port number from pg_settings.';

CREATE INDEX ix_servers_hostname ON servers(server_hostname) WHERE server_hostname IS NOT NULL;
CREATE INDEX ix_servers_ip ON servers(server_ip) WHERE server_ip IS NOT NULL;

CREATE TABLE samples (
    server_id integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    sample_id integer NOT NULL,
    sample_time timestamp (0) with time zone,
    server_hostname text,
    server_ip text,
    server_port integer,
    CONSTRAINT pk_samples PRIMARY KEY (server_id, sample_id)
);

CREATE INDEX ix_sample_time ON samples(server_id, sample_time);
CREATE INDEX ix_samples_hostname ON samples(server_id, server_hostname) WHERE server_hostname IS NOT NULL;
CREATE INDEX ix_samples_ip ON samples(server_id, server_ip) WHERE server_ip IS NOT NULL;
COMMENT ON TABLE samples IS 'Sample times list';
COMMENT ON COLUMN samples.server_hostname IS 'Server hostname at time of sample collection. NULL when hostname extension is not installed.';
COMMENT ON COLUMN samples.server_ip IS 'Server listen address at time of sample collection from pg_settings.';
COMMENT ON COLUMN samples.server_port IS 'Server port number at time of sample collection from pg_settings.';

CREATE TABLE baselines (
    server_id   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
    bl_id       SERIAL,
    bl_name     varchar (25) NOT NULL,
    keep_until  timestamp (0) with time zone,
    CONSTRAINT pk_baselines PRIMARY KEY (server_id, bl_id),
    CONSTRAINT uk_baselines UNIQUE (server_id,bl_name) DEFERRABLE INITIALLY IMMEDIATE
);
COMMENT ON TABLE baselines IS 'Baselines list';

CREATE TABLE bl_samples (
    server_id   integer NOT NULL,
    sample_id   integer NOT NULL,
    bl_id       integer NOT NULL,
    CONSTRAINT fk_bl_samples_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_bl_samples_baselines FOREIGN KEY (server_id, bl_id)
      REFERENCES baselines(server_id, bl_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pk_bl_samples PRIMARY KEY (server_id, bl_id, sample_id)
);
CREATE INDEX ix_bl_samples_blid ON bl_samples(bl_id);
CREATE INDEX ix_bl_samples_sample ON bl_samples(server_id, sample_id);
COMMENT ON TABLE bl_samples IS 'Samples in baselines';
