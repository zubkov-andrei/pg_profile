/* === Data tables used in dump import process ==== */
CREATE TABLE import_queries_version_order (
  extension         text,
  version           text,
  parent_extension  text,
  parent_version    text,
  CONSTRAINT pk_import_queries_version_order PRIMARY KEY (extension, version),
  CONSTRAINT fk_import_queries_version_order FOREIGN KEY (parent_extension, parent_version)
    REFERENCES import_queries_version_order (extension,version)
);
COMMENT ON TABLE import_queries_version_order IS 'Version history used in import process';

CREATE TABLE import_queries (
  extension       text,
  from_version    text,
  exec_order      integer,
  relname         text,
  query           text NOT NULL,
  CONSTRAINT pk_import_queries PRIMARY KEY (extension, from_version, exec_order, relname),
  CONSTRAINT fk_import_queries_version FOREIGN KEY (extension, from_version)
    REFERENCES import_queries_version_order (extension,version)
);
COMMENT ON TABLE import_queries IS 'Queries, used in import process';
