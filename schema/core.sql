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
    size_smp_interval   interval day to minute,
    server_hostname     text,
    server_ip           inet,
    server_port         integer,
    cpu_cores           integer,
    cpu_model           text,
    cpu_mhz             numeric,
    cpu_sockets         integer,
    memory_total_mb     bigint,
    load_avg_1min       numeric,
    load_avg_5min       numeric,
    load_avg_15min      numeric,
    cpu_user_pct        numeric,
    cpu_system_pct      numeric,
    cpu_idle_pct        numeric
);
COMMENT ON TABLE servers IS 'Monitored servers (Postgres clusters) list';
COMMENT ON COLUMN servers.server_hostname IS 'Physical server hostname from hostname() extension. NULL when hostname extension is not installed or when connected via Unix socket.';
COMMENT ON COLUMN servers.server_ip IS 'Server IP address from inet_server_addr() function. NULL for Unix socket connections or when no network listener is available.';
COMMENT ON COLUMN servers.server_port IS 'Server port number from inet_server_port() function. NULL for Unix socket connections.';
COMMENT ON COLUMN servers.cpu_cores IS 'Number of logical CPU cores from system detection (Linux only)';
COMMENT ON COLUMN servers.cpu_model IS 'CPU model name from /proc/cpuinfo (Linux only)';
COMMENT ON COLUMN servers.cpu_mhz IS 'CPU frequency in MHz from /proc/cpuinfo (Linux only)';
COMMENT ON COLUMN servers.cpu_sockets IS 'Number of physical CPU sockets from system detection (Linux only)';
COMMENT ON COLUMN servers.memory_total_mb IS 'Total system memory in MB from /proc/meminfo (Linux only)';
COMMENT ON COLUMN servers.load_avg_1min IS 'System load average over 1 minute from /proc/loadavg (Linux only)';
COMMENT ON COLUMN servers.load_avg_5min IS 'System load average over 5 minutes from /proc/loadavg (Linux only)';
COMMENT ON COLUMN servers.load_avg_15min IS 'System load average over 15 minutes from /proc/loadavg (Linux only)';
COMMENT ON COLUMN servers.cpu_user_pct IS 'CPU user percentage from /proc/stat (Linux only)';
COMMENT ON COLUMN servers.cpu_system_pct IS 'CPU system percentage from /proc/stat (Linux only)';
COMMENT ON COLUMN servers.cpu_idle_pct IS 'CPU idle percentage from /proc/stat (Linux only)';

CREATE INDEX ix_servers_hostname ON servers(server_hostname) WHERE server_hostname IS NOT NULL;
CREATE INDEX ix_servers_ip ON servers(server_ip) WHERE server_ip IS NOT NULL;
CREATE INDEX ix_servers_cpu_model ON servers(cpu_model) WHERE cpu_model IS NOT NULL;

CREATE TABLE samples (
    server_id integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    sample_id integer NOT NULL,
    sample_time timestamp (0) with time zone,
    server_hostname text,
    server_ip inet,
    server_port integer,
    cpu_cores integer,
    cpu_model text,
    cpu_mhz numeric,
    cpu_sockets integer,
    memory_total_mb bigint,
    load_avg_1min numeric,
    load_avg_5min numeric,
    load_avg_15min numeric,
    cpu_user_pct numeric,
    cpu_system_pct numeric,
    cpu_idle_pct numeric,
    CONSTRAINT pk_samples PRIMARY KEY (server_id, sample_id)
);

CREATE INDEX ix_sample_time ON samples(server_id, sample_time);
CREATE INDEX ix_samples_hostname ON samples(server_id, server_hostname) WHERE server_hostname IS NOT NULL;
CREATE INDEX ix_samples_ip ON samples(server_id, server_ip) WHERE server_ip IS NOT NULL;
CREATE INDEX ix_samples_cpu_model ON samples(server_id, cpu_model) WHERE cpu_model IS NOT NULL;
COMMENT ON TABLE samples IS 'Sample times list';
COMMENT ON COLUMN samples.server_hostname IS 'Server hostname at time of sample collection. NULL when hostname extension is not installed or when connected via Unix socket.';
COMMENT ON COLUMN samples.server_ip IS 'Server IP address at time of sample collection. NULL for Unix socket connections or when no network listener is available.';
COMMENT ON COLUMN samples.server_port IS 'Server port number at time of sample collection. NULL for Unix socket connections.';
COMMENT ON COLUMN samples.cpu_cores IS 'Number of logical CPU cores at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.cpu_model IS 'CPU model name at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.cpu_mhz IS 'CPU frequency in MHz at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.cpu_sockets IS 'Number of physical CPU sockets at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.memory_total_mb IS 'Total system memory in MB at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.load_avg_1min IS 'System load average over 1 minute at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.load_avg_5min IS 'System load average over 5 minutes at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.load_avg_15min IS 'System load average over 15 minutes at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.cpu_user_pct IS 'CPU user percentage at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.cpu_system_pct IS 'CPU system percentage at time of sample collection (Linux only)';
COMMENT ON COLUMN samples.cpu_idle_pct IS 'CPU idle percentage at time of sample collection (Linux only)';

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
