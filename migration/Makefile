MIGRATION = \
	$(EXTENSION)--4.1--$(PGPROFILE_VERSION).sql

$(EXTENSION)--4.1--4.2.sql: migration/func_drop--4.1--4.2.sql migration/func_create--4.1--4.2.sed \
	migration/migration--4.1--4.2.sql data/report_templates.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/func_drop--4.1--4.2.sql \
	> $(EXTENSION)--4.1--4.2.sql;
	sed -n \
	$(sed_extension) \
	-f migration/func_create--4.1--4.2.sed \
	$(functions) \
	>> $(EXTENSION)--4.1--4.2.sql;
	sed \
	$(sed_extension) \
	migration/migration--4.1--4.2.sql data/report_templates.sql \
	>> $(EXTENSION)--4.1--4.2.sql;
