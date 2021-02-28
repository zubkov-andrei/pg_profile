ALTER TABLE servers
  ADD COLUMN server_description  text,
  ADD COLUMN server_created      timestamp with time zone DEFAULT now(),
  ADD COLUMN size_smp_wnd_start  time with time zone,
  ADD COLUMN size_smp_wnd_dur    interval hour to second,
  ADD COLUMN size_smp_interval   interval day to minute
;

UPDATE servers srv
SET server_created = smp.server_created
FROM
  (SELECT server_id, min(sample_time) AS server_created
  FROM servers JOIN samples USING (server_id)
  GROUP BY server_id) smp
WHERE srv.server_id = smp.server_id;

DO $$
DECLARE
  r_result  record;
BEGIN
FOR r_result IN SELECT n.nspname AS namespace, r.relname AS relname, c.conname AS conname
  FROM
    pg_class r
    JOIN pg_constraint c ON (r.oid = c.conrelid)
    JOIN pg_namespace n ON (r.relnamespace = n.oid)
  WHERE r.relname='sample_statements' AND c.contype = 'f' AND n.nspname = '@extschema@' AND c.confrelid = 'stmt_list'::regclass
LOOP
  EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
    r_result.namespace,
    r_result.relname,
    r_result.conname);
END LOOP;

FOR r_result IN SELECT n.nspname AS namespace, r.relname AS relname, c.conname AS conname
  FROM
    pg_class r
    JOIN pg_constraint c ON (r.oid = c.conrelid)
    JOIN pg_namespace n ON (r.relnamespace = n.oid)
  WHERE r.relname='stmt_list' AND c.contype = 'p' AND n.nspname = '@extschema@'
LOOP
  EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I',
    r_result.namespace,
    r_result.relname,
    r_result.conname);
END LOOP;
END
$$;

ALTER TABLE stmt_list
  ADD COLUMN server_id integer REFERENCES servers(server_id) ON DELETE CASCADE;

UPDATE stmt_list sl
SET server_id = ss.server_id
FROM (SELECT min(server_id) server_id, queryid_md5 FROM sample_statements GROUP BY queryid_md5) ss
WHERE sl.queryid_md5 = ss.queryid_md5;

ALTER TABLE stmt_list
  ADD CONSTRAINT pk_stmt_list PRIMARY KEY (server_id,queryid_md5);

INSERT INTO stmt_list (server_id,queryid_md5,query)
SELECT ss.server_id,queryid_md5,query
FROM stmt_list sl JOIN sample_statements ss USING (queryid_md5)
ON CONFLICT DO NOTHING;

ALTER TABLE sample_stat_database
  ADD COLUMN datistemplate boolean;
ALTER TABLE last_stat_database
  ADD COLUMN datistemplate boolean;

UPDATE sample_stat_database SET datistemplate = datname LIKE 'template_';

ALTER TABLE sample_statements
  ADD CONSTRAINT fk_stmt_list FOREIGN KEY (server_id,queryid_md5)
    REFERENCES stmt_list (server_id,queryid_md5) ON DELETE RESTRICT ON UPDATE CASCADE;

DROP VIEW v_sample_statements;

ALTER TABLE sample_kcache DROP CONSTRAINT IF EXISTS fk_kcache_sample;
DROP INDEX ix_sample_kcache_qid;

ALTER TABLE sample_kcache
  RENAME COLUMN user_time TO exec_user_time;
ALTER TABLE sample_kcache
  RENAME COLUMN system_time TO exec_system_time;
ALTER TABLE sample_kcache
  RENAME COLUMN minflts TO exec_minflts;
ALTER TABLE sample_kcache
  RENAME COLUMN majflts TO exec_majflts;
ALTER TABLE sample_kcache
  RENAME COLUMN nswaps TO exec_nswaps;
ALTER TABLE sample_kcache
  RENAME COLUMN reads TO exec_reads;
ALTER TABLE sample_kcache
  RENAME COLUMN writes TO exec_writes;
ALTER TABLE sample_kcache
  RENAME COLUMN msgsnds TO exec_msgsnds;
ALTER TABLE sample_kcache
  RENAME COLUMN msgrcvs TO exec_msgrcvs;
ALTER TABLE sample_kcache
  RENAME COLUMN nsignals TO exec_nsignals;
ALTER TABLE sample_kcache
  RENAME COLUMN nvcsws TO exec_nvcsws;
ALTER TABLE sample_kcache
  RENAME COLUMN nivcsws TO exec_nivcsws;

ALTER TABLE sample_kcache
  ADD COLUMN plan_user_time      double precision,
  ADD COLUMN plan_system_time    double precision,
  ADD COLUMN plan_minflts        bigint,
  ADD COLUMN plan_majflts        bigint,
  ADD COLUMN plan_nswaps         bigint,
  ADD COLUMN plan_reads          bigint,
  ADD COLUMN plan_writes         bigint,
  ADD COLUMN plan_msgsnds        bigint,
  ADD COLUMN plan_msgrcvs        bigint,
  ADD COLUMN plan_nsignals       bigint,
  ADD COLUMN plan_nvcsws         bigint,
  ADD COLUMN plan_nivcsws        bigint;

DROP VIEW v_sample_kcache;

ALTER TABLE sample_kcache_total
  RENAME COLUMN user_time TO exec_user_time;
ALTER TABLE sample_kcache_total
  RENAME COLUMN system_time TO exec_system_time;
ALTER TABLE sample_kcache_total
  RENAME COLUMN minflts TO exec_minflts;
ALTER TABLE sample_kcache_total
  RENAME COLUMN majflts TO exec_majflts;
ALTER TABLE sample_kcache_total
  RENAME COLUMN nswaps TO exec_nswaps;
ALTER TABLE sample_kcache_total
  RENAME COLUMN reads TO exec_reads;
ALTER TABLE sample_kcache_total
  RENAME COLUMN writes TO exec_writes;
ALTER TABLE sample_kcache_total
  RENAME COLUMN msgsnds TO exec_msgsnds;
ALTER TABLE sample_kcache_total
  RENAME COLUMN msgrcvs TO exec_msgrcvs;
ALTER TABLE sample_kcache_total
  RENAME COLUMN nsignals TO exec_nsignals;
ALTER TABLE sample_kcache_total
  RENAME COLUMN nvcsws TO exec_nvcsws;
ALTER TABLE sample_kcache_total
  RENAME COLUMN nivcsws TO exec_nivcsws;

ALTER TABLE sample_kcache_total
  ADD COLUMN plan_user_time      double precision,
  ADD COLUMN plan_system_time    double precision,
  ADD COLUMN plan_minflts         bigint,
  ADD COLUMN plan_majflts         bigint,
  ADD COLUMN plan_nswaps         bigint,
  ADD COLUMN plan_reads          bigint,
  ADD COLUMN plan_writes         bigint,
  ADD COLUMN plan_msgsnds        bigint,
  ADD COLUMN plan_msgrcvs        bigint,
  ADD COLUMN plan_nsignals       bigint,
  ADD COLUMN plan_nvcsws         bigint,
  ADD COLUMN plan_nivcsws        bigint;

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

CREATE TABLE sample_stat_tables_failures (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    relid               oid,
    size_failed         boolean,
    toastsize_failed    boolean,
    CONSTRAINT pk_sample_stat_tables_failures PRIMARY KEY (server_id, sample_id, datid, relid),
    CONSTRAINT fk_sample_tables_failures_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_sample_tables_failures_tables FOREIGN KEY (server_id, datid, relid)
      REFERENCES tables_list(server_id, datid, relid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_tables_failures IS 'For each sample lists tables with stats failed to collect';

CREATE TABLE sample_stat_indexes_failures (
    server_id           integer,
    sample_id           integer,
    datid               oid,
    indexrelid          oid,
    size_failed         boolean,
    CONSTRAINT pk_sample_stat_indexes_failures PRIMARY KEY (server_id, sample_id, datid, indexrelid),
    CONSTRAINT fk_sample_indexes_failures_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE,
    CONSTRAINT fk_sample_indexes_failures_indexes FOREIGN KEY (server_id, datid, indexrelid)
      REFERENCES indexes_list(server_id, datid, indexrelid) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_indexes_failures IS 'For each sample lists tables with stats failed to collect';

CREATE VIEW v_sample_stat_tables_interpolated AS
  SELECT
    stt.server_id,
    stt.sample_id,
    stt.datid,
    stt.relid,
    stt.seq_scan,
    stt.vacuum_count,
    stt.autovacuum_count,
    round(COALESCE(
      -- relsize if available
      stt.relsize,
      -- interpolation size if available
      interpolation.left_sample_relsize +
      extract(epoch from smp.sample_time - interpolation.left_sample_time)
        * interpolation.int_grow_per_second,
      -- extrapolation as the last hope
      extract(epoch from smp.sample_time - fst_sample.sample_time) *
      (lst_size.relsize - fst_size.relsize) /
      extract(epoch from lst_sample.sample_time - fst_sample.sample_time)
    )) as relsize,
    stt.relsize IS NULL AS relsize_approximated
  FROM
    sample_stat_tables stt
    JOIN samples smp USING (server_id, sample_id)
    /* Getting overall size-collected boundaries for all tables
    * HAVING condition ensures that we have at least two
    * samples with relation size collected
    */
    JOIN (
    SELECT
      server_id,
      datid,
      relid,
      min(sample_id) first_sample,
      max(sample_id) last_sample
    FROM sample_stat_tables
      WHERE relsize IS NOT NULL
    GROUP BY
      server_id,
      datid,
      relid
    HAVING min(sample_id) != max(sample_id)
    ) boundary_size_samples USING (server_id, datid, relid)
    -- Getting boundary relation sizes and times, needed for calculation of overall growth rate
    -- this data will be used when extrapolation is needed
    JOIN samples fst_sample ON
      (boundary_size_samples.server_id,boundary_size_samples.first_sample) =
      (fst_sample.server_id, fst_sample.sample_id)
    JOIN samples lst_sample ON
      (boundary_size_samples.server_id,boundary_size_samples.last_sample) =
      (lst_sample.server_id, lst_sample.sample_id)
    JOIN sample_stat_tables fst_size ON
      (boundary_size_samples.server_id,boundary_size_samples.first_sample,boundary_size_samples.datid,boundary_size_samples.relid) =
      (fst_size.server_id,fst_size.sample_id,fst_size.datid,fst_size.relid)
    JOIN sample_stat_tables lst_size ON
      (boundary_size_samples.server_id,boundary_size_samples.last_sample,boundary_size_samples.datid,boundary_size_samples.relid) =
      (lst_size.server_id,lst_size.sample_id,lst_size.datid,lst_size.relid)

    /* When relation size is unavailable and the sample is between
    * other samples with measured sizes available, we will use interpolation
    */
    LEFT OUTER JOIN LATERAL (
      SELECT
        l.sample_time as left_sample_time,
        l.relsize as left_sample_relsize,
        (r.relsize - l.relsize) / extract(epoch from r.sample_time - l.sample_time) as int_grow_per_second
      FROM (
        SELECT sample_time, relsize
        FROM sample_stat_tables
          JOIN samples USING (server_id, sample_id)
        WHERE (server_id, datid, relid) =
          (stt.server_id, stt.datid, stt.relid)
          AND sample_id < stt.sample_id AND relsize IS NOT NULL
        ORDER BY sample_id DESC
        LIMIT 1) l,
      (
        SELECT sample_time, relsize
        FROM sample_stat_tables
          JOIN samples USING (server_id, sample_id)
        WHERE (server_id, datid, relid) =
          (stt.server_id, stt.datid, stt.relid)
          AND sample_id > stt.sample_id AND relsize IS NOT NULL
        ORDER BY sample_id ASC
        LIMIT 1) r
    ) interpolation ON (stt.relsize IS NULL AND
        stt.sample_id BETWEEN boundary_size_samples.first_sample AND
          boundary_size_samples.last_sample)
;
COMMENT ON VIEW v_sample_stat_tables_interpolated IS 'Tables sizes interpolated for samples without sizes collected';

CREATE VIEW v_sample_stat_indexes_interpolated AS
  SELECT
    stt.server_id,
    stt.sample_id,
    stt.datid,
    il.relid,
    stt.tablespaceid,
    stt.indexrelid,
    round(COALESCE(
      -- relsize if available
      stt.relsize,
      -- interpolation size if available
      interpolation.left_sample_relsize +
      extract(epoch from smp.sample_time - interpolation.left_sample_time)
        * interpolation.int_grow_per_second,
      -- extrapolation as the last hope
      extract(epoch from smp.sample_time - fst_sample.sample_time) *
      (lst_size.relsize - fst_size.relsize) /
      extract(epoch from lst_sample.sample_time - fst_sample.sample_time)
    )) as indexrelsize,
    stt.relsize IS NULL AS indexrelsize_approximated
  FROM
    sample_stat_indexes stt
    JOIN indexes_list il USING (datid, indexrelid, server_id)
    JOIN samples smp USING (server_id, sample_id)
    /* Getting overall size-collected boundaries for all tables
    * HAVING condition ensures that we have at least two
    * samples with relation size collected
    */
    JOIN (
    SELECT
      server_id,
      datid,
      indexrelid,
      min(sample_id) first_sample,
      max(sample_id) last_sample
    FROM sample_stat_indexes
      WHERE relsize IS NOT NULL
    GROUP BY
      server_id,
      datid,
      indexrelid
    HAVING min(sample_id) != max(sample_id)
    ) boundary_size_samples USING (server_id, datid, indexrelid)
    -- Getting boundary relation sizes and times, needed for calculation of overall growth rate
    -- this data will be used when extrapolation is needed
    JOIN samples fst_sample ON
      (boundary_size_samples.server_id,boundary_size_samples.first_sample) =
      (fst_sample.server_id, fst_sample.sample_id)
    JOIN samples lst_sample ON
      (boundary_size_samples.server_id,boundary_size_samples.last_sample) =
      (lst_sample.server_id, lst_sample.sample_id)
    JOIN sample_stat_indexes fst_size ON
      (boundary_size_samples.server_id,boundary_size_samples.first_sample,boundary_size_samples.datid,boundary_size_samples.indexrelid) =
      (fst_size.server_id,fst_size.sample_id,fst_size.datid,fst_size.indexrelid)
    JOIN sample_stat_indexes lst_size ON
      (boundary_size_samples.server_id,boundary_size_samples.last_sample,boundary_size_samples.datid,boundary_size_samples.indexrelid) =
      (lst_size.server_id,lst_size.sample_id,lst_size.datid,lst_size.indexrelid)

    /* When relation size is unavailable and the sample is between
    * other samples with measured sizes available, we will use interpolation
    */
    LEFT OUTER JOIN LATERAL (
      SELECT
        l.sample_time as left_sample_time,
        l.relsize as left_sample_relsize,
        (r.relsize - l.relsize) / extract(epoch from r.sample_time - l.sample_time) as int_grow_per_second
      FROM (
        SELECT sample_time, relsize
        FROM sample_stat_indexes
          JOIN samples USING (server_id, sample_id)
        WHERE (server_id, datid, indexrelid) =
          (stt.server_id, stt.datid, stt.indexrelid)
          AND sample_id < stt.sample_id AND relsize IS NOT NULL
        ORDER BY sample_id DESC
        LIMIT 1) l,
      (
        SELECT sample_time, relsize
        FROM sample_stat_indexes
          JOIN samples USING (server_id, sample_id)
        WHERE (server_id, datid, indexrelid) =
          (stt.server_id, stt.datid, stt.indexrelid)
          AND sample_id > stt.sample_id AND relsize IS NOT NULL
        ORDER BY sample_id ASC
        LIMIT 1) r
    ) interpolation ON (stt.relsize IS NULL AND
        stt.sample_id BETWEEN boundary_size_samples.first_sample AND
          boundary_size_samples.last_sample)
;
COMMENT ON VIEW v_sample_stat_indexes_interpolated IS 'Tables sizes interpolated for samples without sizes collected';
