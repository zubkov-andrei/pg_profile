/* ===== pg_stat_statements checks ===== */
CREATE FUNCTION profile_checkavail_stmt_cnt(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS
$$
    -- Check if statistics were reset
  SELECT COUNT(*) > 0
  FROM samples s
  JOIN (
      SELECT sample_id,sum(statements) stmt_cnt
      FROM sample_statements_total
      WHERE server_id = sserver_id AND
        ((start_id,end_id) = (0,0) OR
        sample_id BETWEEN start_id + 1 AND end_id)
      GROUP BY sample_id
  ) sample_stmt_cnt USING(sample_id)
  JOIN lateral (
      SELECT server_id,name,setting
      FROM sample_settings
      WHERE
        (server_id, name, setting_scope) = (s.server_id, 'statements_extension', 2)
        AND first_seen <= s.sample_time
      ORDER BY first_seen DESC LIMIT 1
  ) ext	USING (server_id)
  JOIN lateral (
      SELECT server_id,name,setting
      FROM sample_settings
      WHERE
        (server_id, name, setting_scope) = (s.server_id, ext.setting||'.max', 1)
        AND first_seen <= s.sample_time
      ORDER BY first_seen DESC LIMIT 1
  ) max	USING (server_id)
  WHERE
    s.server_id = sserver_id AND
    ((start_id,end_id) = (0,0) OR
      s.sample_id BETWEEN start_id + 1 AND end_id) AND
    stmt_cnt >= 0.9*cast(max.setting AS integer);
$$ LANGUAGE sql;

CREATE FUNCTION stmt_cnt(IN sserver_id integer, IN start_id integer = 0,
  IN end_id integer = 0)
RETURNS TABLE(
  sample_id     integer,
  sample_time   timestamp with time zone,
  stmt_cnt      integer,
  max_cnt       text,
  extension     text
)
SET search_path=@extschema@
AS $$
    SELECT
      s.sample_id,
      s.sample_time,
      stmt_cnt,
      max.setting::text AS max_cnt,
      ext.setting AS extension
    FROM samples s
        JOIN (
            SELECT
              sample_id,
              sum(statements)::integer AS stmt_cnt
            FROM sample_statements_total
            WHERE server_id = sserver_id AND
              ((start_id,end_id) = (0,0) OR
              sample_id BETWEEN start_id + 1 AND end_id)
            GROUP BY sample_id
        ) sample_stmt_cnt USING(sample_id)
        JOIN lateral
    (SELECT server_id,name,setting
    FROM sample_settings
    WHERE
      (server_id, name, setting_scope) = (s.server_id, 'statements_extension', 2)
      AND first_seen <= s.sample_time
    ORDER BY first_seen DESC LIMIT 1) ext
  USING (server_id)
  JOIN lateral
    (SELECT server_id,name,setting
    FROM sample_settings
    WHERE
      (server_id, name, setting_scope) = (s.server_id, ext.setting||'.max', 1)
      AND first_seen <= s.sample_time
    ORDER BY first_seen DESC LIMIT 1) max
  USING (server_id)
    WHERE
      s.server_id = sserver_id AND
      ((start_id,end_id) = (0,0) OR
        s.sample_id BETWEEN start_id + 1 AND end_id) AND
      stmt_cnt >= 0.9*cast(max.setting AS integer);
$$ LANGUAGE sql;

CREATE FUNCTION stmt_cnt_format(IN sserver_id integer, IN start_id integer = 0,
  IN end_id integer = 0)
RETURNS TABLE(
  sample_id     integer,
  sample_time   text,
  stmt_cnt      integer,
  max_cnt       text,
  extension	text
)
SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    sample_time::text,
    stmt_cnt,
    max_cnt,
    extension
  FROM
    stmt_cnt(sserver_id, start_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION stmt_cnt_format_diff(IN sserver_id integer,
  IN start1_id integer = 0, IN end1_id integer = 0,
  IN start2_id integer = 0, IN end2_id integer = 0)
RETURNS TABLE(
  interval_num  integer,
  sample_id     integer,
  sample_time   text,
  stmt_cnt      integer,
  max_cnt       text,
  extension	text
)
SET search_path=@extschema@ AS $$
  SELECT
    1 AS interval_num,
    sample_id,
    sample_time::text,
    stmt_cnt,
    max_cnt,
    extension
  FROM
    stmt_cnt(sserver_id, start1_id, end1_id)
  UNION ALL
  SELECT
    2 AS interval_num,
    sample_id,
    sample_time::text,
    stmt_cnt,
    max_cnt,
    extension
  FROM
    stmt_cnt(sserver_id, start2_id, end2_id)
$$ LANGUAGE sql;
