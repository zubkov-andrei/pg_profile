CREATE TABLE roles_list(
    server_id       integer REFERENCES servers(server_id) ON DELETE CASCADE,
    userid          oid,
    username        name NOT NULL,
    CONSTRAINT pk_roles_list PRIMARY KEY (server_id, userid)
);
COMMENT ON TABLE roles_list IS 'Roles, captured in samples';
