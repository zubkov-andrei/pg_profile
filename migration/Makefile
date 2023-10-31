MIGRATION = \
	$(EXTENSION)--4.3--$(PGPROFILE_VERSION).sql

$(EXTENSION)--4.3--4.4.sql: migration/func_drop--4.3--4.4.sql migration/func_create--4.3--4.4.sed \
	migration/migration--4.3--4.4.sql data/report_templates.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/func_drop--4.3--4.4.sql \
	> $(EXTENSION)--4.3--4.4.sql;
	sed -n \
	$(sed_extension) \
	-f migration/func_create--4.3--4.4.sed \
	$(functions) \
	>> $(EXTENSION)--4.3--4.4.sql;
	sed \
	$(sed_extension) \
	migration/migration--4.3--4.4.sql data/report_templates.sql \
	>> $(EXTENSION)--4.3--4.4.sql;
