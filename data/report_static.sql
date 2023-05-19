/* === report_static table data === */
INSERT INTO report_static(static_name, static_text)
VALUES
('css', $css$
{style.css}
{static:css_post}
$css$
),
('version',
  '<p>{pg_profile} version {properties:pgprofile_version}</p>'),
(
 'script_js', $js$
{script.js}
$js$
),
('report',
  '<html lang="en"><head>'
  '<style>{static:css}</style>'
  '<script>const data={dynamic:data1}</script>'
  '<title>Postgres profile report ({properties:start1_id} -'
  ' {properties:end1_id})</title></head><body>'
  '<H1>Postgres profile report ({properties:start1_id} -'
  '{properties:end1_id})</H1>'
  '{static:version}'
  '<p>Server name: <strong>{properties:server_name}</strong></p>'
  '{properties:server_description}'
  '<p>Report interval: <strong>{properties:report_start1} -'
  ' {properties:report_end1}</strong></p>'
  '{properties:description}'
  '<h2>Report sections</h2>'
  '<ul id="content"></ul>'
  '<div id="container"></div>'
  '<script>{static:script_js}</script>'
  '</body></html>'),
('diffreport',
  '<html lang="en"><head>'
  '<style>{static:css}</style>'
  '<script>const data={dynamic:data1}</script>'
  '<title>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</title></head><body>'
  '<H1>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</H1>'
  '{static:version}'
  '<p>Server name: <strong>{properties:server_name}</strong></p>'
  '{properties:server_description}'
  '<p>First interval (1): <strong>{properties:report_start1} -'
  ' {properties:report_end1}</strong></p>'
  '<p>Second interval (2): <strong>{properties:report_start2} -'
  ' {properties:report_end2}</strong></p>'
  '{properties:description}'
  '<h2>Report sections</h2>'
  '<ul id="content"></ul>'
  '<div id="container"></div>'
  '<script>{static:script_js}</script>'
  '</body></html>')
;
