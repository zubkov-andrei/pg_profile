/*===== Settings reporting functions =====*/
CREATE FUNCTION settings_and_changes(IN sserver_id integer, IN start_id integer, IN end_id integer)
  RETURNS TABLE(
    first_seen          timestamp(0) with time zone,
    setting_scope       smallint,
    name                text,
    setting             text,
    reset_val           text,
    boot_val            text,
    unit                text,
    sourcefile          text,
    sourceline          integer,
    pending_restart     boolean,
    changed             boolean,
    default_val         boolean
  )
SET search_path=@extschema@ AS $$
  SELECT
    first_seen,
    setting_scope,
    name,
    setting,
    reset_val,
    boot_val,
    unit,
    sourcefile,
    sourceline,
    pending_restart,
    false,
    boot_val IS NOT DISTINCT FROM reset_val
  FROM v_sample_settings
  WHERE (server_id, sample_id) = (sserver_id, start_id)
  UNION ALL
  SELECT
    first_seen,
    setting_scope,
    name,
    setting,
    reset_val,
    boot_val,
    unit,
    sourcefile,
    sourceline,
    pending_restart,
    true,
    boot_val IS NOT DISTINCT FROM reset_val
  FROM sample_settings s
    JOIN samples s_start ON (s_start.server_id = s.server_id AND s_start.sample_id = start_id)
    JOIN samples s_end ON (s_end.server_id = s.server_id AND s_end.sample_id = end_id)
  WHERE s.server_id = sserver_id AND s.first_seen > s_start.sample_time AND s.first_seen <= s_end.sample_time
$$ LANGUAGE sql;

CREATE FUNCTION settings_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE (
    klass       text,
    name        text,
    reset_val   text,
    unit        text,
    source      text,
    notes       text,
    default_val boolean,
    defined_val boolean,
    h_ord       integer -- report header ordering
  )
SET search_path=@extschema@ AS $$
  SELECT
    CASE WHEN changed THEN 'new' ELSE 'init' END AS klass,
    name,
    reset_val,
    unit,
    concat_ws(':', sourcefile, sourceline) AS source,
    concat_ws(', ',
      CASE WHEN changed THEN first_seen ELSE NULL END,
      CASE WHEN pending_restart THEN 'Pending restart' ELSE NULL END
    ) AS notes,
    default_val,
    NOT default_val,
    CASE
      WHEN name = 'version' THEN 10
      WHEN (name, setting_scope) = ('pgpro_version', 1) THEN 21
      WHEN (name, setting_scope) = ('pgpro_edition', 1) THEN 22
      WHEN (name, setting_scope) = ('pgpro_build', 1) THEN 23
      ELSE NULL
    END AS h_ord
  FROM
    settings_and_changes(sserver_id, start_id, end_id)
  ORDER BY
    name,setting_scope,first_seen,pending_restart ASC NULLS FIRST
$$ LANGUAGE sql;

CREATE FUNCTION settings_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE (
    klass       text,
    name        text,
    reset_val   text,
    unit        text,
    source      text,
    notes       text,
    default_val boolean,
    defined_val boolean,
    h_ord       integer -- report header ordering
  )
SET search_path=@extschema@ AS $$
  SELECT
    concat_ws('_',
      CASE WHEN changed THEN 'new' ELSE 'init' END,
      CASE WHEN s1.name IS NULL THEN 'i2'
           WHEN s2.name IS NULL THEN 'i1'
           ELSE NULL
      END
    ) AS klass,
    name,
    reset_val,
    COALESCE(s1.unit,s2.unit) as unit,
    concat_ws(':',
      COALESCE(s1.sourcefile,s2.sourcefile),
      COALESCE(s1.sourceline,s2.sourceline)
    ) AS source,
    concat_ws(', ',
      CASE WHEN changed THEN first_seen ELSE NULL END,
      CASE WHEN pending_restart THEN 'Pending restart' ELSE NULL END
    ) AS notes,
    default_val,
    NOT default_val,
    CASE
      WHEN name = 'version' THEN 10
      WHEN (name, setting_scope) = ('pgpro_version', 1) THEN 21
      WHEN (name, setting_scope) = ('pgpro_edition', 1) THEN 22
      WHEN (name, setting_scope) = ('pgpro_build', 1) THEN 23
      ELSE NULL
    END AS h_ord
  FROM
    settings_and_changes(sserver_id, start1_id, end1_id) s1
    FULL OUTER JOIN
    settings_and_changes(sserver_id, start2_id, end2_id) s2
    USING(first_seen, setting_scope, name, setting, reset_val, pending_restart, changed, default_val)
  ORDER BY
    name,setting_scope,first_seen,pending_restart ASC NULLS FIRST
$$ LANGUAGE sql;
