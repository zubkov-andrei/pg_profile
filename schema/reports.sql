CREATE TABLE report_static (
  static_name     text,
  static_text     text,
  CONSTRAINT pk_report_headers PRIMARY KEY (static_name)
);

CREATE TABLE report (
  report_id           integer,
  report_name         text,
  report_description  text,
  template            text,
  CONSTRAINT pk_report PRIMARY KEY (report_id),
  CONSTRAINT fk_report_template FOREIGN KEY (template)
    REFERENCES report_static(static_name)
    ON UPDATE CASCADE
);

CREATE TABLE report_struct (
  report_id       integer,
  sect_id         text,
  parent_sect_id  text,
  s_ord           integer,
  toc_cap         text,
  tbl_cap         text,
  feature         text,
  function_name   text,
  href            text,
  content         jsonb DEFAULT NULL,
  sect_struct     jsonb,
  CONSTRAINT pk_report_struct PRIMARY KEY (report_id, sect_id),
  CONSTRAINT fk_report_struct_report FOREIGN KEY (report_id)
    REFERENCES report(report_id) ON UPDATE CASCADE,
  CONSTRAINT fk_report_struct_tree FOREIGN KEY (report_id, parent_sect_id)
    REFERENCES report_struct(report_id, sect_id) ON UPDATE CASCADE
);
CREATE INDEX ix_fk_report_struct_tree ON report_struct(report_id, parent_sect_id);
