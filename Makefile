PGPROFILE_VERSION = 0.3.3
EXTENSION = pg_profile

include migration/Makefile

TAR_pkg = $(EXTENSION)--$(PGPROFILE_VERSION).tar.gz $(EXTENSION)--$(PGPROFILE_VERSION)_manual.tar.gz

DATA_built = $(EXTENSION)--$(PGPROFILE_VERSION).sql $(EXTENSION).control $(MIGRATION)

EXTRA_CLEAN = $(TAR_pkg) $(MIGRATION_FULL)

REGRESS = \
	create_extension \
	server_management \
	samples_and_reports \
	export_import \
	retention_and_baselines \
	drop_extension

# pg_stat_kcache tests
REGRESS += \
	kcache_create_extension \
	server_management \
	samples_and_reports \
	kcache_stat_avail \
	export_import \
	retention_and_baselines \
	kcache_drop_extension

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

schema = schema/schema.sql \
	schema/views.sql
data = data/import_queries.sql
common = management/internal.sql
adm_funcs = management/baseline.sql \
	management/server.sql
export_funcs = \
	management/export.sql
sample = \
	sample/sample_pg_stat_statements.sql \
	sample/sample.sql \
	sample/compat.sql
report = report/functions/*.sql \
	report/report.sql \
	report/reportdiff.sql

# Extension script contents
functions = $(common) $(adm_funcs) $(export_funcs) $(sample) $(report)
script = $(schema) $(data) $(functions)

# Manual script contents
functions_man = $(common) $(adm_funcs) $(sample) $(report)
script_man = $(schema) $(functions_man)

sqlfile: $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql

$(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql: $(script)
	sed -e 's/SET search_path=@extschema@ //' \
	-e "s/{pg_profile}/$(EXTENSION)/" \
	-e "s/{extension_version}/$(PGPROFILE_VERSION)/" \
	$(script_man) \
	> $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql

$(EXTENSION).control: control.tpl
	sed -e 's/{version}/$(PGPROFILE_VERSION)/' control.tpl > $(EXTENSION).control

$(EXTENSION)--$(PGPROFILE_VERSION).sql: $(script)
	sed \
	-e '1i \\\echo Use "CREATE EXTENSION $(EXTENSION)" to load this file. \\quit' \
	-e "s/{pg_profile}/$(EXTENSION)/" \
	-e "s/{extension_version}/$(PGPROFILE_VERSION)/" \
	$(script) \
	> $(EXTENSION)--$(PGPROFILE_VERSION).sql

$(EXTENSION)--$(PGPROFILE_VERSION)_manual.tar.gz: sqlfile
	tar czf $(EXTENSION)--$(PGPROFILE_VERSION)_manual.tar.gz $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql

$(EXTENSION)--$(PGPROFILE_VERSION).tar.gz: $(DATA_built)
	tar czf $(EXTENSION)--$(PGPROFILE_VERSION).tar.gz $(DATA_built)

tarpkg: $(TAR_pkg)

oldmigration: $(EXTENSION)--$(PGPROFILE_VERSION).sql $(EXTENSION).control $(MIGRATION_FULL)
