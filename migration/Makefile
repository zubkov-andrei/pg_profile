MIGRATION = \
	$(EXTENSION)--0.1.4--$(PGPROFILE_VERSION).sql

$(EXTENSION)--0.1.4--$(PGPROFILE_VERSION).sql: migration/migration--0.1--0.2.sql migration/drop_functions.sql $(functions)
	echo '\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \quit' > $(EXTENSION)--0.1.4--$(PGPROFILE_VERSION).sql
	cat migration/migration--0.1--0.2.sql migration/drop_functions.sql $(functions) \
	| sed -e "s/{pg_profile}/$(EXTENSION)/" >> $(EXTENSION)--0.1.4--$(PGPROFILE_VERSION).sql
