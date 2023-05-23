/* pg_wait_sampling reporting functions */
CREATE FUNCTION profile_checkavail_wait_sampling_total(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there is table sizes collected in both bounds
  SELECT
    count(*) > 0
  FROM wait_sampling_total
  WHERE
    server_id = sserver_id
    AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION wait_sampling_total_stats(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    event_type      text,
    event           text,
    tot_waited      numeric,
    stmt_waited     numeric
)
SET search_path=@extschema@ AS $$
    SELECT
        st.event_type,
        st.event,
        sum(st.tot_waited)::numeric / 1000 AS tot_waited,
        sum(st.stmt_waited)::numeric / 1000 AS stmt_waited
    FROM wait_sampling_total st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.event_type, st.event;
$$ LANGUAGE sql;

CREATE FUNCTION wait_sampling_total_stats_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
  event_type        text,
  event_type_order  bigint,
  tot_waited        numeric,
  tot_waited_pct    numeric,
  stmt_waited       numeric,
  stmt_waited_pct   numeric
)
SET search_path=@extschema@ AS $$
    WITH tot AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats(sserver_id, start_id, end_id))
    SELECT
        COALESCE(event_type, 'Total'),
        row_number() OVER (ORDER BY event_type NULLS LAST) as event_type_order,
        round(sum(st.tot_waited), 2) as tot_waited,
        round(sum(st.tot_waited) * 100 / NULLIF(min(tot.tot_waited),0), 2) as tot_waited_pct,
        round(sum(st.stmt_waited), 2) as stmt_waited,
        round(sum(st.stmt_waited) * 100 / NULLIF(min(tot.stmt_waited),0), 2) as stmt_waited_pct
    FROM wait_sampling_total_stats(sserver_id, start_id, end_id) st CROSS JOIN tot
    GROUP BY ROLLUP(event_type)
$$ LANGUAGE sql;

CREATE FUNCTION wait_sampling_total_stats_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  event_type        text,
  event_type_order  bigint,
  tot_waited1       numeric,
  tot_waited_pct1   numeric,
  stmt_waited1      numeric,
  stmt_waited_pct1  numeric,
  tot_waited2       numeric,
  tot_waited_pct2   numeric,
  stmt_waited2      numeric,
  stmt_waited_pct2  numeric
)
SET search_path=@extschema@ AS $$
    WITH tot1 AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats(sserver_id, start1_id, end1_id)),
    tot2 AS (
      SELECT sum(tot_waited) AS tot_waited, sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats(sserver_id, start2_id, end2_id))
    SELECT
        COALESCE(event_type, 'Total'),
        row_number() OVER (ORDER BY event_type NULLS LAST) as event_type_order,
        round(sum(st1.tot_waited), 2) as tot_waited1,
        round(sum(st1.tot_waited) * 100 / NULLIF(min(tot1.tot_waited),0), 2) as tot_waited_pct1,
        round(sum(st1.stmt_waited), 2) as stmt_waited1,
        round(sum(st1.stmt_waited) * 100 / NULLIF(min(tot1.stmt_waited),0), 2) as stmt_waited_pct1,
        round(sum(st2.tot_waited), 2) as tot_waited2,
        round(sum(st2.tot_waited) * 100 / NULLIF(min(tot2.tot_waited),0), 2) as tot_waited_pct2,
        round(sum(st2.stmt_waited), 2) as stmt_waited2,
        round(sum(st2.stmt_waited) * 100 / NULLIF(min(tot2.stmt_waited),0), 2) as stmt_waited_pct2
    FROM (wait_sampling_total_stats(sserver_id, start1_id, end1_id) st1 CROSS JOIN tot1)
      FULL JOIN
        (wait_sampling_total_stats(sserver_id, start2_id, end2_id) st2 CROSS JOIN tot2)
      USING (event_type, event)
    GROUP BY ROLLUP(event_type)
$$ LANGUAGE sql;

CREATE FUNCTION top_wait_sampling_events_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
  event_type        text,
  event             text,
  total_filter      boolean,
  stmt_filter       boolean,
  tot_waited        numeric,
  tot_waited_pct    numeric,
  stmt_waited       numeric,
  stmt_waited_pct   numeric
)
SET search_path=@extschema@ AS $$
    WITH tot AS (
      SELECT
        sum(tot_waited) AS tot_waited,
        sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats(sserver_id, start_id, end_id))
    SELECT
        event_type,
        event,
        COALESCE(st.tot_waited > 0, false) AS total_filter,
        COALESCE(st.stmt_waited > 0, false) AS stmt_filter,
        round(st.tot_waited, 2) AS tot_waited,
        round(st.tot_waited * 100 / NULLIF(tot.tot_waited,0),2) AS tot_waited_pct,
        round(st.stmt_waited, 2) AS stmt_waited,
        round(st.stmt_waited * 100 / NULLIF(tot.stmt_waited,0),2) AS stmt_waited_pct
    FROM wait_sampling_total_stats(sserver_id, start_id, end_id) st CROSS JOIN tot
$$ LANGUAGE sql;

CREATE FUNCTION top_wait_sampling_events_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  event_type        text,
  event             text,
  total_filter      boolean,
  stmt_filter       boolean,
  tot_ord           bigint,
  stmt_ord          bigint,
  tot_waited1       numeric,
  tot_waited_pct1   numeric,
  tot_waited2       numeric,
  tot_waited_pct2   numeric,
  stmt_waited1      numeric,
  stmt_waited_pct1  numeric,
  stmt_waited2      numeric,
  stmt_waited_pct2  numeric
)
SET search_path=@extschema@ AS $$
    WITH tot1 AS (
      SELECT
        sum(tot_waited) AS tot_waited,
        sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats(sserver_id, start1_id, end1_id)),
    tot2 AS (
      SELECT
        sum(tot_waited) AS tot_waited,
        sum(stmt_waited) AS stmt_waited
      FROM wait_sampling_total_stats(sserver_id, start2_id, end2_id))
    SELECT
        event_type,
        event,
        COALESCE(st1.tot_waited, 0) + COALESCE(st2.tot_waited, 0) > 0 AS total_filter,
        COALESCE(st1.stmt_waited, 0) + COALESCE(st2.stmt_waited, 0) > 0 AS stmt_filter,
        row_number() OVER (ORDER BY
           COALESCE(st1.tot_waited, 0) + COALESCE(st2.tot_waited, 0) DESC,
           event_type, event) AS tot_ord,
        row_number() OVER (ORDER BY
           COALESCE(st1.stmt_waited, 0) + COALESCE(st2.stmt_waited, 0) DESC,
           event_type, event) AS stmt_ord,
        round(st1.tot_waited, 2) AS tot_waited1,
        round(st1.tot_waited * 100 / NULLIF(tot1.tot_waited,0),2) AS tot_waited_pct1,
        round(st2.tot_waited, 2) AS tot_waited2,
        round(st2.tot_waited * 100 / NULLIF(tot2.tot_waited,0),2) AS tot_waited_pct2,
        round(st1.stmt_waited, 2) AS stmt_waited1,
        round(st1.stmt_waited * 100 / NULLIF(tot1.stmt_waited,0),2) AS stmt_waited_pct1,
        round(st2.stmt_waited, 2) AS stmt_waited2,
        round(st2.stmt_waited * 100 / NULLIF(tot2.stmt_waited,0),2) AS stmt_waited_pct2
    FROM (wait_sampling_total_stats(sserver_id, start1_id, end1_id) st1 CROSS JOIN tot1)
      FULL JOIN
    (wait_sampling_total_stats(sserver_id, start2_id, end2_id) st2 CROSS JOIN tot2)
      USING (event_type, event)
$$ LANGUAGE sql;
