SET track_functions TO 'all';
SET client_min_messages = WARNING;
/* === Initialize some structures === */
DROP TABLE IF EXISTS profile.grow_table;
CREATE TABLE profile.grow_table (
  id          SERIAL PRIMARY KEY,
  short_str   varchar(50),
  long_str    text
);

CREATE INDEX IF NOT EXISTS ix_grow_table ON profile.grow_table(short_str);

CREATE TABLE profile.test_rel_storage_params1
(
  id  INTEGER,
  val NUMERIC
) WITH (
    autovacuum_vacuum_threshold = 50,
    fillfactor = 100,
    autovacuum_enabled = TRUE
);
CREATE INDEX ix_test_rel_storage_params_id1 ON profile.test_rel_storage_params1 (id) WITH (fillfactor = 100);
CREATE INDEX ix_test_rel_storage_params_val1 ON profile.test_rel_storage_params1 (val) WITH (fillfactor = 90);

CREATE TABLE profile.test_rel_storage_params2
(
  id  INTEGER,
  val NUMERIC
) WITH (
    vacuum_index_cleanup = true,
    vacuum_truncate = true,
    autovacuum_vacuum_scale_factor = 0.2
);
CREATE INDEX ix_test_rel_storage_params_id2 ON profile.test_rel_storage_params2 (id) WITH (fillfactor = 80);
CREATE INDEX ix_test_rel_storage_params_val2 ON profile.test_rel_storage_params2 (val) WITH (fillfactor = 70);

CREATE OR REPLACE FUNCTION profile.dummy_func() RETURNS VOID AS $$
BEGIN
  PERFORM pg_sleep(0.5);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION profile.grow_table_trg_f() RETURNS trigger AS
$$
BEGIN
  PERFORM pg_sleep(0.1);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER grow_table_trg
BEFORE INSERT OR UPDATE ON profile.grow_table FOR EACH ROW
EXECUTE PROCEDURE profile.grow_table_trg_f();

/* Testing sample creation */
-- (sample 1)
SELECT server,result FROM profile.take_sample();
/* Perform some load */
INSERT INTO profile.grow_table (short_str,long_str)
SELECT array_to_string(array
  (select
  substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
  trunc(random() * 62)::integer + 1, 1)
  FROM   generate_series(1, 40)), ''
) as arr1,
array_to_string(array
  (select
  substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
  trunc(random() * 62)::integer + 1, 1)
  FROM   generate_series(1, 7900)), ''
)
FROM generate_series(1,20);
INSERT INTO profile.test_rel_storage_params1(id, val)
SELECT gs.id, gs.id FROM generate_series(1,100) as gs(id);
INSERT INTO profile.test_rel_storage_params2(id, val)
SELECT gs.id, gs.id FROM generate_series(1,100) as gs(id);
SELECT * FROM profile.dummy_func();
/* Taking next sample */
-- (sample 2)
SELECT pg_sleep(1);
SELECT server,result FROM profile.take_sample();

/* Check collected data */
SELECT
  n_tup_ins,
  n_live_tup
FROM profile.sample_stat_tables st
  JOIN profile.tables_list tl USING (server_id,datid,relid)
WHERE
  tl.relname = 'grow_table' AND tl.schemaname = 'profile'
  AND st.sample_id = 2;

SELECT
  stt.n_tup_ins,
  stt.n_live_tup
FROM
  profile.sample_stat_tables stt
  JOIN profile.sample_stat_tables stm ON
    (stm.server_id, stm.sample_id, stm.datid, stm.reltoastrelid) =
    (stt.server_id, stt.sample_id, stt.datid, stt.relid)
  JOIN profile.tables_list tl ON
    (stm.server_id, stm.datid, stm.relid) =
    (tl.server_id, tl.datid, tl.relid)
WHERE
  tl.relname = 'grow_table' AND tl.schemaname = 'profile'
  AND stm.sample_id = 2;

SELECT
  calls,
  total_time > 0 tt,
  self_time > 0 st,
  trg_fn
FROM
  profile.sample_stat_user_functions f
  JOIN profile.funcs_list fl USING (server_id,datid,funcid)
WHERE
  schemaname = 'profile' AND funcname IN ('grow_table_trg_f', 'dummy_func')
  AND sample_id = 2
ORDER BY funcname;

/* Testing report */
CREATE FUNCTION profile.get_ids(IN headers jsonb)
RETURNS jsonb AS $$
DECLARE
	col jsonb;
	item text;
	ids jsonb := '{}';
BEGIN
	FOR col IN SELECT * FROM jsonb_array_elements(headers)
	LOOP
		IF col #>> '{id}' IS NOT NULL
			THEN
			IF position('[' in col #>> '{id}') > 0 AND position(']' in col #>> '{id}') > 0
				THEN
				FOREACH item IN ARRAY string_to_array(regexp_replace(col #>> '{id}', '[\[",\]]', '', 'g'), ' ')
				LOOP
					ids := ids || jsonb_build_object(item, true);
				END LOOP;
				ELSE ids := ids || jsonb_build_object(col #>> '{id}', true);
			END IF;
		END IF;
		IF col #>> '{columns}' IS NOT NULL
			THEN ids := ids ||  profile.get_ids((col #>> '{columns}')::jsonb);
		END IF;
	END LOOP;
	RETURN ids;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION profile.get_sources(IN headers jsonb)
RETURNS jsonb AS $$
DECLARE
	header jsonb;
	sources jsonb := '{}';
BEGIN
	FOR header IN SELECT * FROM jsonb_array_elements(headers)
	LOOP
		IF header #>> '{source}' IS NOT NULL
            THEN sources := sources || jsonb_build_object(
                header #>> '{source}', jsonb_build_object(
                	'ids', profile.get_ids((header #>> '{columns}')::jsonb),
                	'ordering', header #>> '{ordering}'
                )
            );
		END IF;
	END LOOP;
	RETURN sources;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION profile.get_report_sections(IN sections jsonb, IN report_context jsonb)
RETURNS jsonb AS $$
DECLARE
	elem jsonb;
	the_feature text;
	result jsonb := '{}';
BEGIN
	FOR elem IN SELECT * FROM jsonb_array_elements(sections)
	LOOP
		/* Сhecking if a section can be in the report */
		the_feature := feature FROM profile.report_struct WHERE (sect_id, report_id) = (elem #>> '{sect_id}',1);
		IF NOT (the_feature IS NULL OR (report_context #>> ARRAY['report_features', the_feature])::boolean)
			THEN RAISE EXCEPTION 'Section % should NOT be in report', elem #>> '{sect_id}';
		END IF;

		/* Recursive collection of sections identificators */
		result := result || jsonb_build_object(
			elem #>> '{sect_id}',
			jsonb_build_object(
				'exists', true,
				'headers', CASE WHEN (elem #>> '{header}') IS NOT NULL THEN profile.get_sources((elem #>> '{header}')::jsonb) END
			));
		IF elem #>> '{sections}' IS NOT NULL
			THEN result := result || profile.get_report_sections((elem #>> '{sections}')::jsonb, report_context::jsonb);
		END IF;
	END LOOP;
	RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION profile.check_dataset_queries(IN report_datasets jsonb)
RETURNS boolean AS $$
DECLARE
	elem jsonb;
	plan jsonb;
	hexid_collection jsonb := '{}';
	failed boolean := false;
BEGIN
	/** Collecting hexqueryids and hexplanids presented in queries dataset */
	FOR elem IN SELECT * FROM jsonb_array_elements((report_datasets #>> '{queries}')::jsonb)
	LOOP
		hexid_collection := jsonb_set(hexid_collection, ARRAY[elem #>> '{hexqueryid}'], to_jsonb('exists'::text), true);

		FOR plan IN SELECT * FROM jsonb_array_elements((elem #>> '{plans}')::jsonb)
		LOOP
			hexid_collection := jsonb_set(hexid_collection, ARRAY[plan #>> '{hexplanid}'], to_jsonb('exists'::text), true);
		END LOOP;
	END LOOP;
	/** Checking if hexqueryid or hexplanid presented in queries dataset */
	FOR elem IN SELECT * FROM jsonb_array_elements((report_datasets #>> '{top_statements}')::jsonb)
	LOOP
		IF elem #>> '{hexqueryid}' IS NOT NULL AND hexid_collection #>> ARRAY[elem #>> '{hexqueryid}'] IS NULL
			THEN RAISE WARNING 'Hexqueryid <%> not presented in dataset queries', elem #>> '{hexqueryid}';
            failed := true;
		END IF;
		IF elem #>> '{hexplanid}' IS NOT NULL AND hexid_collection #>> ARRAY[elem #>> '{hexplanid}'] IS NULL
			THEN RAISE WARNING 'Hexplanid % not presented in dataset queries', elem #>> '{hexplanid}';
		    failed := true;
		END IF;

	END LOOP;

	RETURN failed;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
	elem record;
	dsname text;
	item text;
	rep_id int := 1;
	server_id int := 1;
	start_id int := 1;
	end_id int := 2;
	report_context jsonb := profile.get_report_context(server_id,start_id,end_id);
	report_sections jsonb := profile.sections_jsonb(report_context,server_id,rep_id)::jsonb;
	report_datasets jsonb := profile.get_report_datasets(report_context,server_id)::jsonb;
    queries boolean := profile.check_dataset_queries(report_datasets);
	report_structure jsonb := profile.get_report_sections((report_sections #>> '{sections}')::jsonb, report_context::jsonb);
	c_recursive CURSOR FOR
		WITH RECURSIVE sel AS (
			SELECT sect_id, parent_sect_id, feature
			FROM profile.report_struct
			WHERE report_id = rep_id AND parent_sect_id IS NULL AND (feature IS NULL OR (report_context #>> ARRAY['report_features', feature::text])::boolean)

			UNION

			SELECT r.sect_id, r.parent_sect_id, r.feature
			FROM profile.report_struct AS r
			INNER JOIN sel s ON s.sect_id = r.parent_sect_id
			WHERE r.report_id = rep_id AND (r.feature IS NULL OR (report_context #>> ARRAY['report_features', r.feature::text])::boolean)
		)
		SELECT * FROM sel;
BEGIN
    IF queries
        THEN RAISE EXCEPTION 'Failed validation';
    END IF;

	FOR elem IN c_recursive
	LOOP
		IF report_structure #>> ARRAY[elem.sect_id::text, 'exists'] IS NULL
			THEN RAISE EXCEPTION 'Section % schould be in report', elem.sect_id::text;
		END IF;
		IF report_structure #>> ARRAY[elem.sect_id::text, 'headers'] IS NOT NULL
			THEN
			FOR dsname IN SELECT key FROM jsonb_each_text((report_structure #>> ARRAY[elem.sect_id::text, 'headers'])::jsonb)
			LOOP
				IF report_datasets #>> ARRAY[dsname] IS NULL
				    THEN RAISE EXCEPTION 'ERROR: Mentioned dataset <%> does not exists', dsname;
				END IF;
				IF jsonb_array_length((report_datasets #>> ARRAY[dsname])::jsonb) = 0
				    THEN RAISE EXCEPTION 'ERROR: Mentioned dataset <%> does not have any objects', dsname;
				END IF;
				FOR item IN SELECT key FROM jsonb_each_text((report_structure #>> ARRAY[elem.sect_id::text, 'headers', dsname, 'ids'])::jsonb)
				LOOP
					IF NOT (report_datasets #>> ARRAY[dsname,'0'])::jsonb ? item
					    THEN RAISE EXCEPTION 'ERROR: Mentioned id <%> does not exist in dataset %', item, dsname;
				    END IF;
				END LOOP;
			END LOOP;
		END IF;
	END LOOP;
END $$;

/* Testing diffreport */
-- (sample 3)
SELECT server,result FROM profile.take_sample();
-- SELECT count(1) FROM profile.get_diffreport_json(1,1,2,2,3);
/* Test server system identifier changing */
BEGIN;
UPDATE profile.sample_settings
SET reset_val = reset_val::bigint + 1
WHERE name = 'system_identifier';
-- (sample 4)
SELECT server,result != 'OK' FROM profile.take_sample();
ROLLBACK;
