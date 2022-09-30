/* ==== Tablespaces stats history ==== */
CREATE TABLE tablespaces_list(
    server_id           integer REFERENCES servers(server_id) ON DELETE CASCADE,
    tablespaceid        oid,
    tablespacename      name NOT NULL,
    tablespacepath      text NOT NULL, -- cannot be changed without changing oid
    last_sample_id      integer,
    CONSTRAINT pk_tablespace_list PRIMARY KEY (server_id, tablespaceid),
    CONSTRAINT fk_tablespaces_list_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
CREATE INDEX ix_tablespaces_list_smp ON tablespaces_list(server_id, last_sample_id);
COMMENT ON TABLE tablespaces_list IS 'Tablespaces, captured in samples';

CREATE TABLE sample_stat_tablespaces
(
    server_id           integer,
    sample_id           integer,
    tablespaceid        oid,
    size                bigint NOT NULL,
    size_delta          bigint NOT NULL,
    CONSTRAINT fk_stattbs_samples FOREIGN KEY (server_id, sample_id)
        REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_st_tablespaces_tablespaces FOREIGN KEY (server_id, tablespaceid)
        REFERENCES tablespaces_list(server_id, tablespaceid)
        ON DELETE NO ACTION ON UPDATE CASCADE
        DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT pk_sample_stat_tablespaces PRIMARY KEY (server_id, sample_id, tablespaceid)
);
CREATE INDEX ix_sample_stat_tablespaces_ts ON sample_stat_tablespaces(server_id, tablespaceid);

COMMENT ON TABLE sample_stat_tablespaces IS 'Sample tablespaces statistics (fields from pg_tablespace)';

CREATE VIEW v_sample_stat_tablespaces AS
    SELECT
        server_id,
        sample_id,
        tablespaceid,
        tablespacename,
        tablespacepath,
        size,
        size_delta
    FROM sample_stat_tablespaces JOIN tablespaces_list USING (server_id, tablespaceid);
COMMENT ON VIEW v_sample_stat_tablespaces IS 'Tablespaces stats view with tablespace names';

CREATE TABLE last_stat_tablespaces (LIKE v_sample_stat_tablespaces)
PARTITION BY LIST (server_id);
COMMENT ON TABLE last_stat_tablespaces IS 'Last sample data for calculating diffs in next sample';
