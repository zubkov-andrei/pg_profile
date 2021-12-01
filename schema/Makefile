schema_files = \
	schema/core.sql \
	schema/cluster.sql \
	schema/tablespaces.sql \
	schema/roles.sql \
	schema/db.sql \
	schema/tables.sql \
	schema/indexes.sql \
	schema/statements.sql \
	schema/rusage.sql \
	schema/funcs.sql \
	schema/import.sql \
	schema/settings.sql \
	schema/smpl_timing.sql

EXTRA_CLEAN += \
	schema/schema.sql

schema/schema.sql: $(schema_files)
	cat $(schema_files) \
	> schema/schema.sql
