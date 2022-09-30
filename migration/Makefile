MIGRATION = \
	$(EXTENSION)--4.0--$(PGPROFILE_VERSION).sql

$(EXTENSION)--4.0--4.1.sql: migration/func_drop--4.0--4.1.sql migration/func_create--4.0--4.1.sed \
	migration/migration--4.0--4.1.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/func_drop--4.0--4.1.sql \
	> $(EXTENSION)--4.0--4.1.sql;
	sed -n \
	$(sed_extension) \
	-f migration/func_create--4.0--4.1.sed \
	$(functions) \
	>> $(EXTENSION)--4.0--4.1.sql;
	cat migration/migration--4.0--4.1.sql \
	>> $(EXTENSION)--4.0--4.1.sql;
