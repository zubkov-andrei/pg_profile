data_files = \
	report_static.sql \
	report.sql \
	report_struct.sql

includes = \
	static/js/utilities.js \
	static/js/chart.js \
	static/js/generate.js \
	static/js/highlight.js \
	static/js/menu.js \
	static/js/preview.js \
	static/js/copier.js \
	static/js/main.js \
	static/css/style.css

report_templates.sql: $(data_files) $(includes)
	sed -f filter.sed \
	static/js/utilities.js \
	static/js/chart.js \
	static/js/generate.js \
	static/js/highlight.js \
	static/js/menu.js \
	static/js/preview.js \
	static/js/copier.js \
	static/js/main.js \
	> static/js/script.js && \
	sed -f script.sed $(data_files) \
	> report_templates.sql; \
  rm static/js/script.js
