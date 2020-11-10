PGPROFILE_VERSION = 0.2.1
EXTENSION = pg_profile

include migration/Makefile

DATA_built = $(EXTENSION)--$(PGPROFILE_VERSION).sql $(EXTENSION).control $(MIGRATION)

REGRESS = \
	pg_profile \
	pg_profile_kcache

PG_CONFIG ?= pg_config

ifdef USE_PGXS
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/$(EXTENSION)
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

schema = schema.sql
common = internal.sql
adm_funcs = baseline.sql \
	server.sql
sample = \
	sample_pg_stat_statements.sql \
	sample.sql \
	compat.sql
report = dbstat.sql \
	statementstat_dbagg.sql \
	clusterstat.sql kcachestat.sql \
	tablespacestat.sql tablestat.sql \
	indexstat.sql \
	kcachestat_checks.sql \
	dead_mods_ix_unused.sql \
	top_io_stat.sql \
	functionstat.sql \
	statements_checks.sql \
	settings.sql \
	statementstat.sql \
	report.sql \
	reportdiff.sql
functions = $(common) $(adm_funcs) $(sample) $(report)
script = $(schema) $(functions)

sqlfile: $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql

$(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql: $(script)
	cat $(script) | sed -e 's/SET search_path=@extschema@,public //' \
	-e "s/{pg_profile}/$(EXTENSION)/" \
	> $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql

$(EXTENSION).control: control.tpl
	sed -e 's/{version}/$(PGPROFILE_VERSION)/' control.tpl > $(EXTENSION).control

$(EXTENSION)--$(PGPROFILE_VERSION).sql: $(script)
	echo '\echo Use "CREATE EXTENSION $(EXTENSION)" to load this file. \quit' > $(EXTENSION)--$(PGPROFILE_VERSION).sql
	cat $(script) | sed -e "s/{pg_profile}/$(EXTENSION)/" >> $(EXTENSION)--$(PGPROFILE_VERSION).sql

tarpkg: $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql $(DATA_built)
	tar czf $(EXTENSION)--$(PGPROFILE_VERSION).tar.gz $(DATA_built)
	tar czf $(EXTENSION)--$(PGPROFILE_VERSION)_manual.tar.gz $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql
