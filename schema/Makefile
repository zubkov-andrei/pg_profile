schema_files = \
	core.sql \
	subsample.sql \
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
	reports.sql \
	extension_versions.sql \
	relation_storage_parameters.sql

schema.sql: $(schema_files)
	cat $(schema_files) \
	> schema.sql
