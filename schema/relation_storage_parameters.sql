CREATE TABLE table_storage_parameters (
    server_id       integer,
    datid           oid,
    relid           oid,
    first_seen      timestamp (0) with time zone,
    last_sample_id  integer,
    reloptions      jsonb,
    CONSTRAINT pk_table_storage_parameters PRIMARY KEY (server_id, datid, relid, first_seen),
    CONSTRAINT fk_table_storage_parameters_servers FOREIGN KEY (server_id)
      REFERENCES servers (server_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_table_storage_parameters_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_table_storage_parameters_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid)
      ON DELETE CASCADE ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX ix_table_storage_parameters_lsi ON table_storage_parameters(server_id, last_sample_id);
COMMENT ON TABLE table_storage_parameters IS 'Table storage parameters (pg_class.reloptions) changes detected at time of sample';

CREATE VIEW v_table_storage_parameters AS
  SELECT
    tsp.server_id,
    tsp.datid,
    tsp.relid,
    tsp.first_seen,
    tsp.last_sample_id,
    s.sample_id,
    s.sample_time,
    tsp.reloptions
  FROM table_storage_parameters tsp
  JOIN samples s ON
    s.server_id = tsp.server_id AND
    s.sample_time >= tsp.first_seen AND
    (s.sample_id <= tsp.last_sample_id OR tsp.last_sample_id IS NULL)
;
COMMENT ON VIEW v_table_storage_parameters IS 'Provides table storage parameters for samples';

CREATE TABLE index_storage_parameters (
    server_id       integer,
    datid           oid,
    relid           oid,
    indexrelid      oid,
    first_seen      timestamp (0) with time zone,
    last_sample_id  integer,
    reloptions      jsonb,
    CONSTRAINT pk_index_storage_parameters PRIMARY KEY (server_id, datid, relid, indexrelid, first_seen),
    CONSTRAINT fk_index_storage_parameters_servers FOREIGN KEY (server_id)
      REFERENCES servers (server_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_index_storage_parameters_samples FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
      DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_index_storage_parameters_indexes FOREIGN KEY (server_id, datid, indexrelid)
      REFERENCES indexes_list(server_id, datid, indexrelid)
      ON DELETE CASCADE ON UPDATE RESTRICT
      DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX ix_index_storage_parameters_lsi ON index_storage_parameters(server_id, last_sample_id);
CREATE INDEX ix_index_storage_parameters_idx ON index_storage_parameters(server_id, datid, indexrelid);
COMMENT ON TABLE index_storage_parameters IS 'Index storage parameters (pg_class.reloptions) changes detected at time of sample';

CREATE VIEW v_index_storage_parameters AS
  SELECT
    tsp.server_id,
    tsp.datid,
    tsp.relid,
    tsp.indexrelid,
    tsp.first_seen,
    tsp.last_sample_id,
    s.sample_id,
    s.sample_time,
    tsp.reloptions
  FROM index_storage_parameters tsp
  JOIN samples s ON
    s.server_id = tsp.server_id AND
    s.sample_time >= tsp.first_seen AND
    (s.sample_id <= tsp.last_sample_id OR tsp.last_sample_id IS NULL)
;
COMMENT ON VIEW v_index_storage_parameters IS 'Provides index storage parameters for samples';
