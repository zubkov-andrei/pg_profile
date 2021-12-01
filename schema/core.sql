/* ========= Core tables ========= */

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
    size_smp_interval   interval day to minute
);
COMMENT ON TABLE servers IS 'Monitored servers (Postgres clusters) list';

INSERT INTO servers (server_name,enabled,connstr) VALUES ('local',true,'dbname='||current_database()||' port='||current_setting('port'));

CREATE TABLE samples (
    server_id integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    sample_id integer NOT NULL,
    sample_time timestamp (0) with time zone,
    CONSTRAINT pk_samples PRIMARY KEY (server_id, sample_id)
);

CREATE INDEX ix_sample_time ON samples(server_id, sample_time);
COMMENT ON TABLE samples IS 'Sample times list';

CREATE TABLE baselines (
    server_id   integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE,
    bl_id       SERIAL,
    bl_name     varchar (25) NOT NULL,
    keep_until  timestamp (0) with time zone,
    CONSTRAINT pk_baselines PRIMARY KEY (server_id, bl_id),
    CONSTRAINT uk_baselines UNIQUE (server_id,bl_name)
);
COMMENT ON TABLE baselines IS 'Baselines list';

CREATE TABLE bl_samples (
    server_id   integer NOT NULL,
    sample_id   integer NOT NULL,
    bl_id       integer NOT NULL,
    CONSTRAINT fk_bl_samples_samples FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT,
    CONSTRAINT fk_bl_samples_baselines FOREIGN KEY (server_id, bl_id) REFERENCES baselines(server_id, bl_id) ON DELETE CASCADE,
    CONSTRAINT pk_bl_samples PRIMARY KEY (server_id, bl_id, sample_id)
);
CREATE INDEX ix_bl_samples_blid ON bl_samples(bl_id);
COMMENT ON TABLE bl_samples IS 'Samples in baselines';
