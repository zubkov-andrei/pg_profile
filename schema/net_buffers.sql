/* ========= Network Buffer Statistics (Linux only) ========= */

CREATE TABLE sample_stat_net_buffers (
    server_id           integer NOT NULL REFERENCES servers(server_id) ON DELETE CASCADE
                        DEFERRABLE INITIALLY IMMEDIATE,
    sample_id           integer NOT NULL,
    rmem_default        bigint,
    rmem_max            bigint,
    wmem_default        bigint,
    wmem_max            bigint,
    optmem_max          bigint,
    CONSTRAINT pk_sample_stat_net_buffers PRIMARY KEY (server_id, sample_id),
    CONSTRAINT fk_sample_stat_net_buffers_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);

COMMENT ON TABLE sample_stat_net_buffers IS 'Linux network buffer configuration from sysctl (net.core.*mem)';
COMMENT ON COLUMN sample_stat_net_buffers.rmem_default IS 'Default socket receive buffer size (bytes) - net.core.rmem_default';
COMMENT ON COLUMN sample_stat_net_buffers.rmem_max IS 'Maximum socket receive buffer size (bytes) - net.core.rmem_max';
COMMENT ON COLUMN sample_stat_net_buffers.wmem_default IS 'Default socket send buffer size (bytes) - net.core.wmem_default';
COMMENT ON COLUMN sample_stat_net_buffers.wmem_max IS 'Maximum socket send buffer size (bytes) - net.core.wmem_max';
COMMENT ON COLUMN sample_stat_net_buffers.optmem_max IS 'Maximum ancillary buffer size (bytes) - net.core.optmem_max';

CREATE INDEX ix_sample_stat_net_buffers ON sample_stat_net_buffers(server_id, sample_id);
