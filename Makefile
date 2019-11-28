EXTENSION = pg_profile
DATA = pg_profile--0.0.6--0.0.7.sql pg_profile--0.0.7.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
