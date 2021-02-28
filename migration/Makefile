MIGRATION = \
	$(EXTENSION)--0.2.1--$(PGPROFILE_VERSION).sql

#MIGRATION_FULL =

$(EXTENSION)--0.2.1--$(PGPROFILE_VERSION).sql: migration/migration--0.2--0.3.sql $(data) migration/drop_functions.sql $(functions)
	sed \
	-e '1i \\\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \\quit' \
	-e "s/{pg_profile}/$(EXTENSION)/" \
	migration/migration--0.2--0.3.sql \
	$(data) \
	migration/drop_functions.sql \
	$(functions) \
	> $(EXTENSION)--0.2.1--$(PGPROFILE_VERSION).sql
