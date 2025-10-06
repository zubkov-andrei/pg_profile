PGPROFILE_VERSION = 4.11
EXTENSION = pg_profile

TAR_pkg = $(EXTENSION)--$(PGPROFILE_VERSION).tar.gz $(EXTENSION)--$(PGPROFILE_VERSION)_manual.tar.gz

default: all

include migration/Makefile

DATA_built = $(EXTENSION)--$(PGPROFILE_VERSION).sql $(EXTENSION).control $(MIGRATION)

EXTRA_CLEAN = $(TAR_pkg) $(MIGRATION) $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql $(schema) \
	$(report) data/report_templates.sql

REGRESS = \
	create_extension \
	server_management \
	samples_and_reports \
	sizes_collection \
	export_import \
	retention_and_baselines \
	drop_extension

# pg_stat_kcache tests
ifdef USE_KCACHE
REGRESS += \
	kcache_create_extension \
	server_management \
	samples_and_reports \
	sizes_collection \
	kcache_stat_avail \
	export_import \
	retention_and_baselines \
	kcache_drop_extension
endif

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

schema = schema/schema.sql

data = data/import_queries.sql \
	data/report_templates.sql
common = management/internal.sql
adm_funcs = management/baseline.sql \
	management/server.sql \
	management/local_server.sql
export_funcs = \
	management/export.sql
sample = \
	sample/sample_pg_stat_statements.sql \
	sample/pg_wait_sampling.sql \
	sample/collect_obj_stats.sql \
	sample/collect_subsamples.sql \
	sample/get_sized_bounds.sql \
	sample/init_sample.sql \
	sample/sample_dbobj_delta.sql \
	sample/show_samples.0.sql \
	sample/show_samples.1.sql \
	sample/take_sample.0.sql \
	sample/take_sample.1.sql \
	sample/take_sample.2.sql \
	sample/take_sample_subset.sql \
	sample/take_subsample.0.sql \
	sample/take_subsample.1.sql \
	sample/take_subsample.2.sql \
	sample/take_subsample_subset.sql \
	sample/compat.sql

report = report/report_build.sql

grants = \
	privileges/pg_profile.sql

# Extension script contents
functions = $(common) $(adm_funcs) $(export_funcs) $(sample) $(report)
script = $(schema) $(data) $(functions) $(grants)

# Manual script contents
functions_man = $(common) $(adm_funcs) $(sample) $(report)
script_man = $(schema) $(functions_man) $(grants) data/report_templates.sql

# Common sed replacement script
sed_extension = -e 's/{pg_profile}/$(EXTENSION)/; s/{extension_version}/$(PGPROFILE_VERSION)/; /--<manual_start>/,/--<manual_end>/d; /--<extension_end>/d; /--<extension_start>/d'
sed_manual = -e 's/{pg_profile}/$(EXTENSION)/; s/{extension_version}/$(PGPROFILE_VERSION)/; /--<extension_start>/,/--<extension_end>/d; /--<manual_start>/d; /--<manual_end>/d'

schema/schema.sql:
	${MAKE} -C schema

data/report_templates.sql:
	${MAKE} -C data

report/report_build.sql:
	${MAKE} -C report

sqlfile: $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql

$(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql: $(script)
	sed -e 's/SET search_path=@extschema@//' \
	$(sed_manual) \
	$(script_man) \
	-e '1i \\\set ON_ERROR_STOP on' \
	> $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql

$(EXTENSION).control: control.tpl
	sed -e 's/{version}/$(PGPROFILE_VERSION)/' control.tpl > $(EXTENSION).control

$(EXTENSION)--$(PGPROFILE_VERSION).sql: $(script)
	sed \
	-e '1i \\\echo Use "CREATE EXTENSION $(EXTENSION)" to load this file. \\quit' \
	$(sed_extension) \
	$(script) \
	> $(EXTENSION)--$(PGPROFILE_VERSION).sql

$(EXTENSION)--$(PGPROFILE_VERSION)_manual.tar.gz: sqlfile
	tar czf $(EXTENSION)--$(PGPROFILE_VERSION)_manual.tar.gz $(EXTENSION)--$(PGPROFILE_VERSION)_manual.sql

$(EXTENSION)--$(PGPROFILE_VERSION).tar.gz: $(DATA_built)
	tar czf $(EXTENSION)--$(PGPROFILE_VERSION).tar.gz $(DATA_built)

tarpkg: $(TAR_pkg)
