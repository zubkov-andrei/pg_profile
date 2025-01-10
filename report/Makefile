report_files = \
	functions/clusterstat.sql \
	functions/stat_io.sql \
	functions/stat_slru.sql \
	functions/dbstat.sql \
	functions/dead_mods_ix_unused.sql \
	functions/functionstat.sql \
	functions/indexstat.sql \
	functions/kcachestat_checks.sql \
	functions/kcachestat.sql \
	functions/settings.sql \
	functions/statements_checks.sql \
	functions/statementstat_dbagg.sql \
	functions/statementstat.sql \
	functions/pg_wait_sampling.sql \
	functions/tablespacestat.sql \
	functions/tablestat.sql \
	functions/top_io_stat.sql \
	functions/walstat.sql \
	functions/subsample.sql \
	functions/extensions.sql \
	section.sql \
	report.sql \
	reportdiff.sql

report_build.sql: $(report_files)
	cat $(report_files) \
	> report_build.sql
