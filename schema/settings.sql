/* ==== Settings history table ==== */
CREATE TABLE sample_settings (
    server_id          integer,
    first_seen         timestamp (0) with time zone,
    setting_scope      smallint, -- Scope of setting. Currently may be 1 for pg_settings and 2 for other adm functions (like version)
    name               text,
    setting            text,
    reset_val          text,
    boot_val           text,
    unit               text,
    sourcefile          text,
    sourceline         integer,
    pending_restart    boolean,
    CONSTRAINT pk_sample_settings PRIMARY KEY (server_id, setting_scope, name, first_seen),
    CONSTRAINT fk_sample_settings_servers FOREIGN KEY (server_id)
      REFERENCES servers(server_id) ON DELETE CASCADE
);
-- Unique index on system_identifier to ensure there is no versions
-- as they are affecting export/import functionality
CREATE UNIQUE INDEX uk_sample_settings_sysid ON
  sample_settings (server_id,name) WHERE name='system_identifier';

COMMENT ON TABLE sample_settings IS 'pg_settings values changes detected at time of sample';

CREATE VIEW v_sample_settings AS
  SELECT
    server_id,
    sample_id,
    first_seen,
    setting_scope,
    name,
    setting,
    reset_val,
    boot_val,
    unit,
    sourcefile,
    sourceline,
    pending_restart
  FROM samples s
    JOIN sample_settings ss USING (server_id)
    JOIN LATERAL
      (SELECT server_id, name, max(first_seen) as first_seen
        FROM sample_settings WHERE server_id = s.server_id AND first_seen <= s.sample_time
        GROUP BY server_id, name) lst
      USING (server_id, name, first_seen)
;
COMMENT ON VIEW v_sample_settings IS 'Provides postgres settings for samples';
