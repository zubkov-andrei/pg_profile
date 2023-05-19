/* ===== Function stats functions ===== */
CREATE FUNCTION profile_checkavail_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have function calls collected for report interval
  SELECT COALESCE(sum(calls), 0) > 0
  FROM sample_stat_user_func_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_trg_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if we have trigger function calls collected for report interval
  SELECT COALESCE(sum(calls), 0) > 0
  FROM sample_stat_user_func_total sn
  WHERE sn.server_id = sserver_id AND sn.sample_id BETWEEN start_id + 1 AND end_id
    AND sn.trg_fn
$$ LANGUAGE sql;
/* ===== Function stats functions ===== */

CREATE FUNCTION top_functions(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid       oid,
    funcid      oid,
    dbname      name,
    schemaname  name,
    funcname    name,
    funcargs    text,
    trg_fn      boolean,
    calls       bigint,
    total_time  double precision,
    self_time   double precision,
    m_time      double precision,
    m_stime     double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        st.funcid,
        sample_db.datname AS dbname,
        st.schemaname,
        st.funcname,
        st.funcargs,
        st.trg_fn,
        sum(st.calls)::bigint AS calls,
        sum(st.total_time)/1000 AS total_time,
        sum(st.self_time)/1000 AS self_time,
        sum(st.total_time)/NULLIF(sum(st.calls),0)/1000 AS m_time,
        sum(st.self_time)/NULLIF(sum(st.calls),0)/1000 AS m_stime
    FROM v_sample_stat_user_functions st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
    WHERE
      st.server_id = sserver_id
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY
      st.datid,
      st.funcid,
      sample_db.datname,
      st.schemaname,
      st.funcname,
      st.funcargs,
      st.trg_fn
$$ LANGUAGE sql;

CREATE FUNCTION top_functions_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid       oid,
    funcid      oid,
    dbname      name,
    schemaname  name,
    funcname    name,
    funcargs    text,
    calls       bigint,
    total_time  numeric,
    self_time   numeric,
    m_time      numeric,
    m_stime     numeric,

    ord_time    integer,
    ord_calls   integer,
    ord_trgtime integer
  )
SET search_path=@extschema@ AS $$
  SELECT
    datid,
    funcid,
    dbname,
    schemaname,
    funcname,
    funcargs,
    NULLIF(calls, 0) AS calls,
    round(CAST(NULLIF(total_time, 0.0) AS numeric), 2) AS total_time,
    round(CAST(NULLIF(self_time, 0.0) AS numeric), 2) AS self_time,
    round(CAST(NULLIF(m_time, 0.0) AS numeric), 2) AS m_time,
    round(CAST(NULLIF(m_stime, 0.0) AS numeric), 2) AS m_stime,

    CASE WHEN
      total_time > 0 AND NOT trg_fn
    THEN
      row_number() OVER (ORDER BY
        total_time
        DESC NULLS LAST,
        datid, funcid)::integer
    ELSE NULL END AS ord_time,

    CASE WHEN
      calls > 0 AND NOT trg_fn
    THEN
      row_number() OVER (ORDER BY
        calls
        DESC NULLS LAST,
        datid, funcid)::integer
    ELSE NULL END AS ord_calls,

    CASE WHEN
      total_time > 0 AND trg_fn
    THEN
      row_number() OVER (ORDER BY
        total_time
        DESC NULLS LAST,
        datid, funcid)::integer
    ELSE NULL END AS ord_trgtime
  FROM
    top_functions(sserver_id, start_id, end_id)
$$ LANGUAGE sql;

CREATE FUNCTION top_functions_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid       oid,
    funcid      oid,
    dbname      name,
    schemaname  name,
    funcname    name,
    funcargs    text,

    calls1      bigint,
    total_time1 numeric,
    self_time1  numeric,
    m_time1     numeric,
    m_stime1    numeric,

    calls2      bigint,
    total_time2 numeric,
    self_time2  numeric,
    m_time2     numeric,
    m_stime2    numeric,

    ord_time    integer,
    ord_calls   integer,
    ord_trgtime integer
  )
SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(f1.datid, f2.datid),
    COALESCE(f1.funcid, f2.funcid),
    COALESCE(f1.dbname, f2.dbname),
    COALESCE(f1.schemaname, f2.schemaname),
    COALESCE(f1.funcname, f2.funcname),
    COALESCE(f1.funcargs, f2.funcargs),

    NULLIF(f1.calls, 0) AS calls1,
    round(CAST(NULLIF(f1.total_time, 0.0) AS numeric), 2) AS total_time1,
    round(CAST(NULLIF(f1.self_time, 0.0) AS numeric), 2) AS self_time1,
    round(CAST(NULLIF(f1.m_time, 0.0) AS numeric), 2) AS m_time1,
    round(CAST(NULLIF(f1.m_stime, 0.0) AS numeric), 2) AS m_stime1,

    NULLIF(f2.calls, 0) AS calls2,
    round(CAST(NULLIF(f2.total_time, 0.0) AS numeric), 2) AS total_time2,
    round(CAST(NULLIF(f2.self_time, 0.0) AS numeric), 2) AS self_time2,
    round(CAST(NULLIF(f2.m_time, 0.0) AS numeric), 2) AS m_time2,
    round(CAST(NULLIF(f2.m_stime, 0.0) AS numeric), 2) AS m_stime2,

    CASE WHEN
      COALESCE(f1.total_time, 0) + COALESCE(f2.total_time, 0) > 0
      AND NOT COALESCE(f1.trg_fn, f2.trg_fn, false)
    THEN
      row_number() OVER (ORDER BY
        COALESCE(f1.total_time, 0) + COALESCE(f2.total_time, 0)
        DESC NULLS LAST,
        COALESCE(f1.datid, f2.datid),
        COALESCE(f1.funcid, f2.funcid))::integer
    ELSE NULL END AS ord_time,

    CASE WHEN
      COALESCE(f1.calls, 0) + COALESCE(f2.calls, 0) > 0
      AND NOT COALESCE(f1.trg_fn, f2.trg_fn, false)
    THEN
      row_number() OVER (ORDER BY
        COALESCE(f1.calls, 0) + COALESCE(f2.calls, 0)
        DESC NULLS LAST,
        COALESCE(f1.datid, f2.datid),
        COALESCE(f1.funcid, f2.funcid))::integer
    ELSE NULL END AS ord_calls,

    CASE WHEN
      COALESCE(f1.total_time, 0) + COALESCE(f2.total_time, 0) > 0
      AND COALESCE(f1.trg_fn, f2.trg_fn, false)
    THEN
      row_number() OVER (ORDER BY
        COALESCE(f1.total_time, 0) + COALESCE(f2.total_time, 0)
        DESC NULLS LAST,
        COALESCE(f1.datid, f2.datid),
        COALESCE(f1.funcid, f2.funcid))::integer
    ELSE NULL END AS ord_trgtime
  FROM
    top_functions(sserver_id, start1_id, end1_id) f1
    FULL OUTER JOIN
    top_functions(sserver_id, start2_id, end2_id) f2
    USING (datid, funcid)
$$ LANGUAGE sql;
