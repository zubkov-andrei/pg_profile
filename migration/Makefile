$(EXTENSION)--0.1.2--$(PGPROFILE_VERSION).sql: migration/drop_functions.sql $(functions)
	echo '\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \quit' > $(EXTENSION)--0.1.2--$(PGPROFILE_VERSION).sql
	cat migration/drop_functions.sql $(functions) \
	| sed -e "s/{pg_profile}/$(EXTENSION)/" >> $(EXTENSION)--0.1.2--$(PGPROFILE_VERSION).sql

$(EXTENSION)--0.1.3--$(PGPROFILE_VERSION).sql: migration/drop_functions.sql $(functions)
	echo '\echo Use "ALTER EXTENSION $(EXTENSION) UPDATE" to load this file. \quit' > $(EXTENSION)--0.1.3--$(PGPROFILE_VERSION).sql
	cat migration/drop_functions.sql $(functions) \
	| sed -e "s/{pg_profile}/$(EXTENSION)/" >> $(EXTENSION)--0.1.3--$(PGPROFILE_VERSION).sql
