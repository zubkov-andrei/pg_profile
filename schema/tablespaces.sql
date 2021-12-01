/* ==== Tablespaces stats history ==== */
CREATE TABLE tablespaces_list(
    server_id           integer REFERENCES servers(server_id) ON DELETE CASCADE,
    tablespaceid        oid,
    tablespacename      name NOT NULL,
    tablespacepath      text NOT NULL, -- cannot be changed without changing oid
    CONSTRAINT pk_tablespace_list PRIMARY KEY (server_id, tablespaceid)
);
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
        REFERENCES tablespaces_list(server_id, tablespaceid) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT pk_sample_stat_tablespaces PRIMARY KEY (server_id,sample_id,tablespaceid)
);
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

CREATE TABLE last_stat_tablespaces AS SELECT * FROM v_sample_stat_tablespaces WHERE 0=1;
ALTER TABLE last_stat_tablespaces ADD CONSTRAINT pk_last_stat_tablespaces PRIMARY KEY (server_id, sample_id, tablespaceid);
ALTER TABLE last_stat_tablespaces ADD CONSTRAINT fk_last_stat_tablespaces_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_tablespaces IS 'Last sample data for calculating diffs in next sample';
