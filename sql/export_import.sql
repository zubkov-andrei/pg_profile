SET client_min_messages = WARNING;
/* === Create regular export table === */
CREATE TABLE profile.export AS SELECT * FROM profile.export_data();
/* === Create obfuscated export table === */
CREATE TABLE profile.blind_export AS SELECT * FROM profile.export_data(NULL,NULL,NULL,TRUE);
BEGIN;
/* === rename local server === */
SELECT profile.rename_server('local','src_local');
/* === check matching by creation date and system identifier === */
SELECT profile.import_data('profile.export') > 0;
/* === change src_local server creation time so it wont match === */
UPDATE profile.servers
SET
  server_created = server_created - '1 minutes'::interval
WHERE server_name = 'src_local';
/* === perform load === */
SELECT profile.import_data('profile.export') > 0;
/* === Integral check - reports must match === */
\a
\t on
WITH res AS (
  SELECT
    profile.get_report('local',1,4) AS imported,
    replace(
        replace(
        profile.get_report('src_local',1,4),'"server_name": "src_local"',
        '"server_name": "local"'),
        '<p>Server name: <strong>src_local</strong>',
        '<p>Server name: <strong>local</strong>'
    ) AS exported
)
SELECT
  CASE
    WHEN
      md5(imported) !=
      md5(exported)
    THEN
      format(E'<imported_start>\n%s\n<imported_end>\n<exported_start>\n%s\n<exported_end>',
        imported,
        exported
      )
    ELSE
      'ok'
  END as match
FROM res;
\a
\t off
/* === perform obfuscated load === */
SELECT profile.drop_server('local');
SELECT profile.import_data('profile.blind_export') > 0;
/* === check that there is no matching queries === */
SELECT
  count(*)
FROM profile.servers s_src
  CROSS JOIN profile.servers s_blind
  JOIN profile.stmt_list q_src ON
    (q_src.server_id = s_src.server_id)
  JOIN profile.stmt_list q_blind ON
    (q_src.queryid_md5 = q_blind.queryid_md5 AND q_blind.server_id = s_blind.server_id)
WHERE
  s_src.server_name = 'src_local' AND s_blind.server_name = 'local'
  AND q_src.query = q_blind.query;
ROLLBACK;
/* === drop export tables === */
DROP TABLE profile.export;
DROP TABLE profile.blind_export;
