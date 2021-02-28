/* ========= kcache stats functions ========= */

CREATE FUNCTION profile_checkavail_rusage(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
  SELECT
    count(*) = end_id - start_id
  FROM
    (SELECT
      sum(exec_user_time) > 0 as exec
    FROM sample_kcache_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY server_id, sample_id) exec_time_samples
  WHERE exec_time_samples.exec
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_rusage_planstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
  SELECT
    count(*) = end_id - start_id
  FROM
    (SELECT
      sum(plan_user_time) > 0 as plan
    FROM sample_kcache_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY server_id, sample_id) plan_time_samples
  WHERE plan_time_samples.plan
$$ LANGUAGE sql;
