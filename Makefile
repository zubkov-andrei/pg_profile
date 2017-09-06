EXTENSION = pg_profile
DATA = pg_profile--0.0.1.sql pg_profile--0.0.2.sql pg_profile--0.0.1--0.0.2.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
