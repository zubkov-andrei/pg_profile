CREATE FUNCTION profile_checkavail_tbl_top_dead(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS
$$
    SELECT
        COUNT(*) > 0
    FROM v_sample_stat_tables st
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
    WHERE st.server_id=sserver_id AND NOT sample_db.datistemplate AND sample_id = end_id
        -- Min 5 MB in size
        AND COALESCE(st.relsize,st.relpages_bytes) > 5 * 1024^2
        AND st.n_dead_tup > 0;
$$ LANGUAGE sql;

CREATE FUNCTION profile_checkavail_tbl_top_mods(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS
$$
    SELECT
        COUNT(*) > 0
    FROM v_sample_stat_tables st
        -- Database name and existance condition
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
    WHERE st.server_id = sserver_id AND NOT sample_db.datistemplate AND sample_id = end_id
        AND st.relkind IN ('r','m')
        -- Min 5 MB in size
        AND COALESCE(st.relsize,st.relpages_bytes) > 5 * 1024^2
        AND n_mod_since_analyze > 0
        AND n_live_tup + n_dead_tup > 0;
$$ LANGUAGE sql;

CREATE FUNCTION top_tbl_last_sample_format(IN sserver_id integer, IN start_id integer, end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    n_live_tup          bigint,
    dead_pct            numeric,
    last_autovacuum     text,
    n_dead_tup          bigint,
    n_mod_since_analyze bigint,
    mods_pct            numeric,
    last_autoanalyze    text,
    relsize_pretty      text,

    ord_dead            integer,
    ord_mod             integer
  )
SET search_path=@extschema@ AS $$
  SELECT
    datid,
    relid,
    sample_db.datname AS dbname,
    tablespacename,
    schemaname,
    relname,

    n_live_tup,
    n_dead_tup::numeric * 100 / NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) AS dead_pct,
    last_autovacuum::text,
    n_dead_tup,
    n_mod_since_analyze,
    n_mod_since_analyze::numeric * 100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0) AS mods_pct,
    last_autoanalyze::text,
    COALESCE(
      pg_size_pretty(relsize),
      '['||pg_size_pretty(relpages_bytes)||']'
    ) AS relsize_pretty,

    CASE WHEN
      n_dead_tup > 0
    THEN
      row_number() OVER (ORDER BY
        n_dead_tup*100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0)
        DESC NULLS LAST,
        datid,relid)::integer
    ELSE NULL END AS ord_dead,

    CASE WHEN
      n_mod_since_analyze > 0
    THEN
      row_number() OVER (ORDER BY
        n_mod_since_analyze*100/NULLIF(COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0), 0)
        DESC NULLS LAST,
        datid,relid)::integer
    ELSE NULL END AS ord_mod
  FROM
    v_sample_stat_tables st
    -- Database name
    JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
  WHERE
    (server_id, sample_id, datistemplate) = (sserver_id, end_id, false)
    AND COALESCE(st.relsize,st.relpages_bytes) > 5 * 1024^2
    AND COALESCE(n_live_tup, 0) + COALESCE(n_dead_tup, 0) > 0
$$ LANGUAGE sql;
