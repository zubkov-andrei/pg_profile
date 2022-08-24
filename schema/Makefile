schema_files = \
	core.sql \
	cluster.sql \
	tablespaces.sql \
	roles.sql \
	db.sql \
	tables.sql \
	indexes.sql \
	statements.sql \
	pg_wait_sampling.sql \
	rusage.sql \
	funcs.sql \
	import.sql \
	settings.sql \
	smpl_timing.sql \
	reports.sql

schema.sql: $(schema_files)
	cat $(schema_files) \
	> schema.sql
