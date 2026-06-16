/* == Testing hostname/network identity feature == */
/* This test validates the server network identity feature
   when the hostname extension is available.
   NOTE: This runs after samples_and_reports which already took samples,
   so we use existing sample data rather than taking new samples. */

SET client_min_messages = WARNING;

/* === Verify schema columns exist === */
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'profile'
  AND table_name = 'servers'
  AND column_name IN ('server_hostname', 'server_ip', 'server_port')
ORDER BY column_name;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'profile'
  AND table_name = 'samples'
  AND column_name IN ('server_hostname', 'server_ip', 'server_port')
ORDER BY column_name;

/* === Verify functions exist === */
SELECT proname, pronargs
FROM pg_proc
WHERE pronamespace = 'profile'::regnamespace
  AND proname = 'collect_system_info'
ORDER BY proname;

/* === Test that show_servers includes network columns === */
SELECT
  server_hostname IS NOT NULL AS has_hostname,
  server_ip IS NOT NULL AS has_ip,
  server_port IS NOT NULL AS has_port
FROM profile.show_servers()
WHERE server_name = 'local';

/* === Verify servers table was populated by earlier samples === */
SELECT
  server_hostname IS NOT NULL AS has_hostname,
  server_ip IS NOT NULL AS has_ip,
  server_port IS NOT NULL AS has_port
FROM profile.servers
WHERE server_name = 'local';

/* === Verify samples table was populated === */
SELECT
  server_hostname IS NOT NULL AS has_hostname,
  server_ip IS NOT NULL AS has_ip,
  server_port IS NOT NULL AS has_port
FROM profile.samples
WHERE server_id = 1
ORDER BY sample_id DESC
LIMIT 1;

/* === Verify server_hostname feature flag is set === */
SELECT
  (profile.get_report_context(1, 1, 2) #>> '{report_features,server_hostname}')::boolean
    AS hostname_feature_enabled;

/* === Test export includes new columns === */
CREATE TABLE profile.hostname_export AS SELECT * FROM profile.export_data();

SELECT count(*) > 0 AS export_has_data
FROM profile.hostname_export;

SELECT
  (row_data::json->>'server_hostname') IS NOT NULL AS export_has_hostname,
  (row_data::json->>'server_ip') IS NOT NULL AS export_has_ip,
  (row_data::json->>'server_port') IS NOT NULL AS export_has_port
FROM profile.hostname_export
WHERE section_id = 1
  AND (row_data::json->>'server_hostname') IS NOT NULL
LIMIT 1;

DROP TABLE profile.hostname_export;
