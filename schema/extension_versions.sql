/* ==== Extension versions history table ==== */
CREATE TABLE extension_versions (
    server_id       integer,
		datid			   		oid,
    first_seen      timestamp (0) with time zone,
    last_sample_id	integer,
    extname         name,
    extversion      text,
    CONSTRAINT pk_extension_versions PRIMARY KEY (server_id, datid, extname, first_seen),
    CONSTRAINT fk_extension_versions_servers FOREIGN KEY (server_id)
      REFERENCES servers (server_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
		CONSTRAINT fk_extension_versions_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX ix_extension_versions_last_sample_id ON extension_versions(server_id, last_sample_id);
COMMENT ON TABLE extension_versions IS 'pg_extension values changes detected at time of sample';

CREATE TABLE last_extension_versions (
    server_id		integer,
		datid				oid,
		sample_id		integer,
    extname     name,
    extversion	text,
    CONSTRAINT pk_last_extension_versions PRIMARY KEY (server_id, sample_id, datid, extname)
);
COMMENT ON TABLE last_extension_versions IS 'Last sample data of pg_extension for calculating diffs in next sample';

CREATE VIEW v_extension_versions AS
	SELECT
  	ev.server_id,
    ev.datid,
    ev.extname,
    ev.first_seen,
    ev.extversion,
    ev.last_sample_id,
    s.sample_id,
    s.sample_time
	FROM extension_versions ev
	JOIN samples s ON
		s.server_id = ev.server_id AND
		s.sample_time >= ev.first_seen AND
		(s.sample_id <= ev.last_sample_id OR ev.last_sample_id IS NULL)
;
COMMENT ON VIEW v_extension_versions IS 'Provides postgres extensions for samples';
