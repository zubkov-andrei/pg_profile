MIGRATION = \
	$(EXTENSION)--4.2--$(PGPROFILE_VERSION).sql

$(EXTENSION)--4.2--4.3.sql: migration/func_drop--4.2--4.3.sql migration/func_create--4.2--4.3.sed \
	migration/migration--4.2--4.3.sql data/report_templates.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/func_drop--4.2--4.3.sql \
	> $(EXTENSION)--4.2--4.3.sql;
	sed -n \
	$(sed_extension) \
	-f migration/func_create--4.2--4.3.sed \
	$(functions) \
	>> $(EXTENSION)--4.2--4.3.sql;
	sed \
	$(sed_extension) \
	migration/migration--4.2--4.3.sql data/report_templates.sql privileges/pg_profile.sql \
	>> $(EXTENSION)--4.2--4.3.sql;
