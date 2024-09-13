MIGRATION = \
	$(EXTENSION)--4.6--$(PGPROFILE_VERSION).sql

$(EXTENSION)--4.6--4.7.sql: migration/func_drop.sql migration/func_create.sed \
	migration/migration.sql data/report_templates.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/func_drop.sql \
	> $(EXTENSION)--4.6--4.7.sql;
	sed -n \
	$(sed_extension) \
	-f migration/func_create.sed \
	$(functions) \
	>> $(EXTENSION)--4.6--4.7.sql;
	sed \
	$(sed_extension) \
	migration/migration.sql  data/report_templates.sql \
	>> $(EXTENSION)--4.6--4.7.sql;
