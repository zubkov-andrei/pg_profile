MIGRATION = \
	$(EXTENSION)--0.3.6--$(PGPROFILE_VERSION).sql

$(EXTENSION)--0.3.6--3.8.sql: migration/func_drop--0.3.6--3.8.sql migration/func_create--0.3.6--3.8.sed \
	migration/migration--0.3.6--3.7.sql migration/migration--3.7--3.8.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/migration--0.3.6--3.7.sql \
	migration/migration--3.7--3.8.sql \
	migration/func_drop--0.3.6--3.8.sql \
	> $(EXTENSION)--0.3.6--3.8.sql;
	sed -n \
	$(sed_extension) \
	-f migration/func_create--0.3.6--3.8.sed \
	$(functions) \
	>> $(EXTENSION)--0.3.6--3.8.sql

$(EXTENSION)--0.3.6--4.0.sql: migration/func_drop--0.3.6--3.8.sql migration/func_create--0.3.6--3.8.sed \
  migration/func_drop--3.8--3.9.sql migration/func_create--3.8--3.9.sed \
  migration/func_drop--3.9--4.0.sql migration/func_create--3.9--4.0.sed \
	migration/migration--0.3.6--3.7.sql migration/migration--3.7--3.8.sql \
	migration/migration--3.8--3.9.sql migration/migration--3.9--4.0.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	$(sed_extension) \
	migration/migration--0.3.6--3.7.sql \
	migration/migration--3.7--3.8.sql \
	migration/migration--3.8--3.9.sql \
	migration/migration--3.9--4.0.sql \
	migration/func_drop--0.3.6--3.8.sql \
	migration/func_drop--3.8--3.9.sql \
  migration/func_drop--3.9--4.0.sql \
	> $(EXTENSION)--0.3.6--4.0.sql;
	sed -n \
	$(sed_extension) \
	-f migration/func_create--0.3.6--3.8.sed \
	-f migration/func_create--3.8--3.9.sed \
	-f migration/func_create--3.9--4.0.sed \
	$(functions) \
	>> $(EXTENSION)--0.3.6--4.0.sql
