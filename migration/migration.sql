INSERT INTO import_queries_version_order VALUES
('pg_profile','4.8','pg_profile','4.7')
;

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;

GRANT SELECT ON extension_versions TO public;
GRANT SELECT ON v_extension_versions TO public;
