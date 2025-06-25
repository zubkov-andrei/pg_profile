/*===== Extensions reporting functions =====*/
CREATE FUNCTION extension_versions_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer = NULL, IN end2_id integer = NULL)
RETURNS TABLE (
    dbname      name,
    extname     name,
    extversion  text,
    first_seen  text,
    last_seen   text,
    ord_ext     integer -- report header ordering
  )
SET search_path=@extschema@ AS $$
  SELECT
    evd.dbname,
    evd.extname,
    evd.extversion,
    CASE
      WHEN s_first.sample_id IS NOT NULL AND
        s_first.sample_id > least(start1_id, start2_id) OR
        evd.last_sample_id IS NOT NULL AND
        evd.last_sample_id < greatest(end1_id, end2_id)
      THEN evd.first_seen::text
      ELSE NULL
    END as first_seen,
    CASE
      WHEN s_first.sample_id IS NOT NULL AND
        s_first.sample_id > least(start1_id, start2_id) OR
        evd.last_sample_id IS NOT NULL AND
        evd.last_sample_id < greatest(end1_id, end2_id)
      THEN s_last.sample_time::text
      ELSE NULL
    END as last_seen,
    row_number() over (order by evd.extname, evd.dbname, evd.first_seen)::integer as ord_ext
  FROM (
    SELECT DISTINCT
      ssd.datname as dbname,
      ev.extname,
      ev.extversion,
      ev.first_seen,
      ev.last_sample_id
    FROM v_extension_versions ev
    -- Database name
    JOIN sample_stat_database ssd USING (server_id, datid, sample_id)
    WHERE
      ev.server_id = sserver_id AND
      (ev.sample_id BETWEEN start1_id AND end1_id OR
      ev.sample_id BETWEEN start2_id AND end2_id)
    ) evd
    LEFT JOIN samples s_first ON (s_first.server_id, s_first.sample_time) = (sserver_id, evd.first_seen)
    LEFT JOIN samples s_last ON (s_last.server_id, s_last.sample_id) = (sserver_id, evd.last_sample_id)
$$ LANGUAGE sql;
