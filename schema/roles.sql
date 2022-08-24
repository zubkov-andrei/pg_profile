CREATE TABLE roles_list(
    server_id       integer REFERENCES servers(server_id) ON DELETE CASCADE,
    userid          oid,
    username        name NOT NULL,
    last_sample_id  integer,
    CONSTRAINT pk_roles_list PRIMARY KEY (server_id, userid),
    CONSTRAINT fk_roles_list_smp FOREIGN KEY (server_id, last_sample_id)
      REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
);
CREATE INDEX ix_roles_list_smp ON roles_list(server_id, last_sample_id);

COMMENT ON TABLE roles_list IS 'Roles, captured in samples';
