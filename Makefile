PGPROFILE_VERSION = 0.1.1
EXTENSION = pg_profile
DATA_built = pg_profile--$(PGPROFILE_VERSION).sql pg_profile.control

REGRESS = pg_profile

PG_CONFIG = /usr/local/pgsql/bin/pg_config

ifdef USE_PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/$(EXTENSION)
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

schema = schema.sql internal.sql
adm_funcs = baseline.sql node.sql
snapshot = snapshot.sql
report = dbstat.sql statementstat_dbagg.sql clusterstat.sql tablespacestat.sql tablestat.sql indexstat.sql \
	dead_mods_ix_unused.sql top_io_stat.sql functionstat.sql settings.sql statements_checks.sql \
	statementstat.sql report.sql reportdiff.sql

script = $(schema) $(adm_funcs) $(snapshot) $(report)

sqlfile:
	cat $(script) | sed -e 's/SET search_path=@extschema@,public //' \
	> pg_profile--$(PGPROFILE_VERSION).sql

pg_profile.control: pg_profile.control.tpl
	sed -e 's/{version}/$(PGPROFILE_VERSION)/' pg_profile.control.tpl > pg_profile.control

pg_profile--$(PGPROFILE_VERSION).sql: $(script)
	echo '\echo Use "CREATE EXTENSION pg_profile" to load this file. \quit' > pg_profile--$(PGPROFILE_VERSION).sql
	cat $(script) >> pg_profile--$(PGPROFILE_VERSION).sql
