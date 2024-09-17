/* === report_static table data === */
INSERT INTO report_static(static_name, static_text)
VALUES
('css', $css$
{style.css}
{static:css_post}
$css$
),
(
 'script_js', $js$
{script.js}
$js$
),
('report',
  '<!DOCTYPE html>'
  '<html lang="en"><head>'
  '<style>{static:css}</style>'
  '<script>const data={dynamic:data1}</script>'
  '<title>Postgres profile report ({properties:start1_id} -'
  ' {properties:end1_id})</title></head><body>'
  '<div id="container">'
  '<H1>Postgres profile report</H1>'
  '</div>'
  '<script>{static:script_js}</script>'
  '</body></html>'),
('diffreport',
  '<!DOCTYPE html>'
  '<html lang="en"><head>'
  '<style>{static:css}</style>'
  '<script>const data={dynamic:data1}</script>'
  '<title>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</title></head><body>'
  '<div id="container">'
  '<H1>Postgres profile differential report</H1>'
  '</div>'
  '<script>{static:script_js}</script>'
  '</body></html>')
;
