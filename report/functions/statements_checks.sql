/* ===== pg_stat_statements checks ===== */
CREATE FUNCTION profile_checkavail_stmt_cnt(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS
$$
    -- Check if statistics were reset
    SELECT COUNT(*) > 0 FROM samples
        JOIN (
            SELECT sample_id,sum(statements) stmt_cnt
            FROM sample_statements_total
            WHERE server_id = sserver_id AND
              ((start_id,end_id) = (0,0) OR
              sample_id BETWEEN start_id + 1 AND end_id)
            GROUP BY sample_id
        ) sample_stmt_cnt USING(sample_id)
        JOIN v_sample_settings prm USING (server_id,sample_id)
    WHERE server_id = sserver_id AND prm.name='pg_stat_statements.max' AND
      stmt_cnt >= 0.9*cast(prm.setting AS integer);
$$ LANGUAGE sql;

CREATE FUNCTION stmt_cnt(IN sserver_id integer, IN start_id integer = 0,
  IN end_id integer = 0)
RETURNS TABLE(
  sample_id     integer,
  sample_time   timestamp with time zone,
  stmt_cnt      integer,
  max_cnt       text
)
--SET search_path=@extschema@
AS $$
    SELECT sample_id,sample_time,stmt_cnt,prm.setting AS max_cnt
    FROM samples
        JOIN (
            SELECT sample_id,sum(statements) stmt_cnt
            FROM sample_statements_total
            WHERE server_id = sserver_id
              AND ((start_id, end_id) = (0,0) OR sample_id BETWEEN start_id + 1 AND end_id)
            GROUP BY sample_id
        ) sample_stmt_cnt USING(sample_id)
        JOIN v_sample_settings prm USING (server_id, sample_id)
    WHERE server_id = sserver_id AND prm.name='pg_stat_statements.max' AND
      stmt_cnt >= 0.9*cast(prm.setting AS integer)
$$ LANGUAGE sql;

CREATE FUNCTION stmt_cnt_format(IN sserver_id integer, IN start_id integer = 0,
  IN end_id integer = 0)
RETURNS TABLE(
  sample_id     integer,
  sample_time   text,
  stmt_cnt      integer,
  max_cnt       text
)
SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    sample_time::text,
    stmt_cnt,
    max_cnt
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
  max_cnt       text
)
SET search_path=@extschema@ AS $$
  SELECT
    1 AS interval_num,
    sample_id,
    sample_time::text,
    stmt_cnt,
    max_cnt
  FROM
    stmt_cnt(sserver_id, start1_id, end1_id)
  UNION ALL
  SELECT
    2 AS interval_num,
    sample_id,
    sample_time::text,
    stmt_cnt,
    max_cnt
  FROM
    stmt_cnt(sserver_id, start2_id, end2_id)
$$ LANGUAGE sql;
