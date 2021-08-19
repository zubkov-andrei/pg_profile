MIGRATION = \
	$(EXTENSION)--0.3.3--$(PGPROFILE_VERSION).sql

#MIGRATION_FULL =

$(EXTENSION)--0.2.1--$(PGPROFILE_VERSION).sql: migration/migration--0.2--0.3.sql $(data) migration/drop_functions.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/migration--0.2--0.3.sql \
	$(data) \
	migration/drop_functions.sql \
	$(functions) \
	> $(EXTENSION)--0.2.1--$(PGPROFILE_VERSION).sql

$(EXTENSION)--0.3.1--0.3.2.sql: migration/migration--0.3.1--0.3.2.sql migration/drop_functions.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/migration--0.3.1--0.3.2.sql \
	migration/drop_functions.sql \
	$(functions) \
	> $(EXTENSION)--0.3.1--0.3.2.sql

$(EXTENSION)--0.3.2--0.3.3.sql: report/functions/tablestat.sql
	sed -n \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	-e "1i INSERT INTO import_queries_version_order VALUES ('pg_profile','0.3.3','pg_profile','0.3.2');" \
	-e '1i DROP FUNCTION top_scan_tables_diff_htbl;' \
	-e '/^CREATE FUNCTION top_scan_tables_diff_htbl.*$$/,/^\$$\$$ LANGUAGE plpgsql;$$/p' \
	report/functions/tablestat.sql \
	> $(EXTENSION)--0.3.2--0.3.3.sql

$(EXTENSION)--0.3.3--0.3.4.sql: migration/migration--0.3.3--0.3.4.sql migration/drop_functions.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/migration--0.3.3--0.3.4.sql \
	migration/drop_functions.sql \
	$(functions) \
	> $(EXTENSION)--0.3.3--0.3.4.sql
