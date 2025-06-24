/* === report_static table data === */
INSERT INTO report_static(static_name, static_text)
VALUES
('css', $css$
{style.css}
{static:css_post}
$css$
),
('logo', $logo$
{logo.svg}
$logo$
),
('logo_mini', $logo_mini$
{logo_mini.svg}
$logo_mini$
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
  '<script>const logoFile=`{static:logo}`</script>'
  '<script>const logoMiniFile=`{static:logo_mini}`</script>'
  '<script>const data={dynamic:data1}</script>'
  '<title>Postgres profile report ({properties:start1_id} -'
  ' {properties:end1_id})</title></head><body>'
  '<div id="container">'
  '</div>'
  '<script>{static:script_js}</script>'
  '</body></html>'),
('diffreport',
  '<!DOCTYPE html>'
  '<html lang="en"><head>'
  '<style>{static:css}</style>'
  '<script>const logoFile=`{static:logo}`</script>'
  '<script>const logoMiniFile=`{static:logo_mini}`</script>'
  '<script>const data={dynamic:data1}</script>'
  '<title>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</title></head><body>'
  '<div id="container">'
  '</div>'
  '<script>{static:script_js}</script>'
  '</body></html>')
;
