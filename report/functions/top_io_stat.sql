/* ===== Top IO objects ===== */

CREATE FUNCTION top_io_tables(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid                       oid,
    relid                       oid,
    dbname                      name,
    tablespacename              name,
    schemaname                  name,
    relname                     name,
    heap_blks_read              bigint,
    heap_blks_hit               bigint,
    idx_blks_read               bigint,
    idx_blks_hit                bigint,
    toast_blks_read             bigint,
    toast_blks_hit              bigint,
    tidx_blks_read              bigint,
    tidx_blks_hit               bigint,
    seq_scan                    bigint,
    idx_scan                    bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        tablespaces_list.tablespacename,
        st.schemaname,
        st.relname,
        COALESCE(sum(st.heap_blks_read), 0)::bigint AS heap_blks_read,
        COALESCE(sum(st.heap_blks_hit), 0)::bigint AS heap_blks_hit,
        COALESCE(sum(st.idx_blks_read), 0)::bigint AS idx_blks_read,
        COALESCE(sum(st.idx_blks_hit), 0)::bigint AS idx_blks_hit,
        COALESCE(sum(st.toast_blks_read), 0)::bigint AS toast_blks_read,
        COALESCE(sum(st.toast_blks_hit), 0)::bigint AS toast_blks_hit,
        COALESCE(sum(st.tidx_blks_read), 0)::bigint AS tidx_blks_read,
        COALESCE(sum(st.tidx_blks_hit), 0)::bigint AS tidx_blks_hit,
        COALESCE(sum(st.seq_scan), 0)::bigint AS seq_scan,
        COALESCE(sum(st.idx_scan), 0)::bigint AS idx_scan
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
        JOIN tablespaces_list USING(server_id,tablespaceid)
    WHERE st.server_id = sserver_id
      AND st.relkind IN ('r','m','t')
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.datid,st.relid,sample_db.datname,tablespaces_list.tablespacename, st.schemaname,st.relname
$$ LANGUAGE sql;

CREATE FUNCTION top_io_tables_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE (
    datid                       oid,
    relid                       oid,
    dbname                      name,
    tablespacename              name,
    schemaname                  name,
    relname                     name,

    heap_blks_read              bigint,
    heap_blks_read_pct          numeric,
    heap_blks_fetch             bigint,
    heap_blks_proc_pct          numeric,
    idx_blks_read               bigint,
    idx_blks_read_pct           numeric,
    idx_blks_fetch              bigint,
    idx_blks_fetch_pct          numeric,
    toast_blks_read             bigint,
    toast_blks_read_pct         numeric,
    toast_blks_fetch            bigint,
    toast_blks_fetch_pct        numeric,
    tidx_blks_read              bigint,
    tidx_blks_read_pct          numeric,
    tidx_blks_fetch             bigint,
    tidx_blks_fetch_pct         numeric,
    seq_scan                    bigint,
    idx_scan                    bigint,
    hit_pct                     numeric,

    ord_read                    integer,
    ord_fetch                   integer
) SET search_path=@extschema@ AS $$
  SELECT
    datid,
    relid,
    dbname,
    tablespacename,
    schemaname,
    relname,

    NULLIF(st.heap_blks_read, 0) AS heap_blks_read,
    round(NULLIF(st.heap_blks_read, 0)::numeric * 100 /
      NULLIF(tot.blks_read, 0), 2) AS heap_blks_read_pct,

    NULLIF(st.heap_blks_read + st.heap_blks_hit, 0) AS heap_blks_fetch,
    round(NULLIF(st.heap_blks_read + st.heap_blks_hit, 0)::numeric * 100 /
      NULLIF(tot.blks_read + tot.blks_hit, 0), 2) AS heap_blks_proc_pct,

    NULLIF(st.idx_blks_read, 0) AS idx_blks_read,
    round(NULLIF(st.idx_blks_read, 0)::numeric * 100 /
      NULLIF(tot.blks_read, 0), 2) AS idx_blks_read_pct,

    NULLIF(st.idx_blks_read + st.idx_blks_hit, 0) AS idx_blks_fetch,
    round(NULLIF(st.idx_blks_read + st.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot.blks_read + tot.blks_hit, 0), 2) AS idx_blks_fetch_pct,

    NULLIF(st.toast_blks_read, 0) AS toast_blks_read,
    round(NULLIF(st.toast_blks_read, 0)::numeric * 100 /
      NULLIF(tot.blks_read, 0), 2) AS toast_blks_read_pct,

    NULLIF(st.toast_blks_read + st.toast_blks_hit, 0) AS toast_blks_fetch,
    round(NULLIF(st.toast_blks_read + st.toast_blks_hit, 0)::numeric * 100 /
      NULLIF(tot.blks_read + tot.blks_hit, 0), 2) AS toast_blks_fetch_pct,

    NULLIF(st.tidx_blks_read, 0) AS tidx_blks_read,
    round(NULLIF(st.tidx_blks_read, 0)::numeric * 100 /
      NULLIF(tot.blks_read, 0), 2) AS tidx_blks_read_pct,

    NULLIF(st.tidx_blks_read + st.tidx_blks_hit, 0) AS tidx_blks_fetch,
    round(NULLIF(st.tidx_blks_read + st.tidx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot.blks_read + tot.blks_hit, 0), 2) AS tidx_blks_fetch_pct,

    NULLIF(st.seq_scan, 0) AS seq_scan,
    NULLIF(st.idx_scan, 0) AS idx_scan,

    round(
        100.0 - (st.heap_blks_read + st.idx_blks_read +
          st.toast_blks_read + st.tidx_blks_read)::numeric * 100.0 /
        NULLIF(st.heap_blks_read + st.idx_blks_read +
          st.toast_blks_read + st.tidx_blks_read +
          st.heap_blks_hit + st.idx_blks_hit +
          st.toast_blks_hit + st.tidx_blks_hit, 0), 2) AS hit_pct,

    CASE WHEN
      st.heap_blks_read + st.idx_blks_read + st.toast_blks_read + st.tidx_blks_read > 0
    THEN
      row_number() OVER (ORDER BY
        st.heap_blks_read + st.idx_blks_read + st.toast_blks_read + st.tidx_blks_read
      DESC NULLS LAST,
      st.datid,
      st.relid)::integer
    ELSE NULL END AS ord_read,

    CASE WHEN
      st.heap_blks_read + st.idx_blks_read +
      st.toast_blks_read + st.tidx_blks_read +
      st.heap_blks_hit + st.idx_blks_hit +
      st.toast_blks_hit + st.tidx_blks_hit > 0
    THEN
      row_number() OVER (ORDER BY
        st.heap_blks_read + st.idx_blks_read +
        st.toast_blks_read + st.tidx_blks_read +
        st.heap_blks_hit + st.idx_blks_hit +
        st.toast_blks_hit + st.tidx_blks_hit
      DESC NULLS LAST,
      st.datid,
      st.relid)::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_tables(sserver_id, start_id, end_id) st
    CROSS JOIN total_dbstats(sserver_id, start_id, end_id) tot
$$ LANGUAGE sql;

CREATE FUNCTION top_io_tables_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE (
    datid                       oid,
    relid                       oid,
    dbname                      name,
    tablespacename              name,
    schemaname                  name,
    relname                     name,

    heap_blks_read1             bigint,
    heap_blks_read_pct1         numeric,
    heap_blks_fetch1            bigint,
    heap_blks_proc_pct1         numeric,
    idx_blks_read1              bigint,
    idx_blks_read_pct1          numeric,
    idx_blks_fetch1             bigint,
    idx_blks_fetch_pct1         numeric,
    toast_blks_read1            bigint,
    toast_blks_read_pct1        numeric,
    toast_blks_fetch1           bigint,
    toast_blks_fetch_pct1       numeric,
    tidx_blks_read1             bigint,
    tidx_blks_read_pct1         numeric,
    tidx_blks_fetch1            bigint,
    tidx_blks_fetch_pct1        numeric,
    seq_scan1                   bigint,
    idx_scan1                   bigint,
    hit_pct1                    numeric,

    heap_blks_read2             bigint,
    heap_blks_read_pct2         numeric,
    heap_blks_fetch2            bigint,
    heap_blks_proc_pct2         numeric,
    idx_blks_read2              bigint,
    idx_blks_read_pct2          numeric,
    idx_blks_fetch2             bigint,
    idx_blks_fetch_pct2         numeric,
    toast_blks_read2            bigint,
    toast_blks_read_pct2        numeric,
    toast_blks_fetch2           bigint,
    toast_blks_fetch_pct2       numeric,
    tidx_blks_read2             bigint,
    tidx_blks_read_pct2         numeric,
    tidx_blks_fetch2            bigint,
    tidx_blks_fetch_pct2        numeric,
    seq_scan2                   bigint,
    idx_scan2                   bigint,
    hit_pct2                    numeric,

    ord_read                    integer,
    ord_fetch                   integer
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(st1.datid, st2.datid) AS datid,
    COALESCE(st1.relid, st2.relid) AS relid,
    COALESCE(st1.dbname, st2.dbname) AS dbname,
    COALESCE(st1.tablespacename, st2.tablespacename) AS tablespacename,
    COALESCE(st1.schemaname, st2.schemaname) AS schemaname,
    COALESCE(st1.relname, st2.relname) AS relname,

    NULLIF(st1.heap_blks_read, 0) AS heap_blks_read1,
    round(NULLIF(st1.heap_blks_read, 0)::numeric * 100 /
      NULLIF(tot1.blks_read, 0), 2) AS heap_blks_read_pct1,

    NULLIF(st1.heap_blks_read + st1.heap_blks_hit, 0) AS heap_blks_fetch1,
    round(NULLIF(st1.heap_blks_read + st1.heap_blks_hit, 0)::numeric * 100 /
      NULLIF(tot1.blks_read + tot1.blks_hit, 0), 2) AS heap_blks_proc_pct1,

    NULLIF(st1.idx_blks_read, 0) AS idx_blks_read1,
    round(NULLIF(st1.idx_blks_read, 0)::numeric * 100 /
      NULLIF(tot1.blks_read, 0), 2) AS idx_blks_read_pct1,

    NULLIF(st1.idx_blks_read + st1.idx_blks_hit, 0) AS idx_blks_fetch1,
    round(NULLIF(st1.idx_blks_read + st1.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot1.blks_read + tot1.blks_hit, 0), 2) AS idx_blks_fetch_pct1,

    NULLIF(st1.toast_blks_read, 0) AS toast_blks_read1,
    round(NULLIF(st1.toast_blks_read, 0)::numeric * 100 /
      NULLIF(tot1.blks_read, 0), 2) AS toast_blks_read_pct1,

    NULLIF(st1.toast_blks_read + st1.toast_blks_hit, 0) AS toast_blks_fetch1,
    round(NULLIF(st1.toast_blks_read + st1.toast_blks_hit, 0)::numeric * 100 /
      NULLIF(tot1.blks_read + tot1.blks_hit, 0), 2) AS toast_blks_fetch_pct1,

    NULLIF(st1.tidx_blks_read, 0) AS tidx_blks_read1,
    round(NULLIF(st1.tidx_blks_read, 0)::numeric * 100 /
      NULLIF(tot1.blks_read, 0), 2) AS tidx_blks_read_pct1,

    NULLIF(st1.tidx_blks_read + st1.tidx_blks_hit, 0) AS tidx_blks_fetch1,
    round(NULLIF(st1.tidx_blks_read + st1.tidx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot1.blks_read + tot1.blks_hit, 0), 2) AS tidx_blks_fetch_pct1,

    NULLIF(st1.seq_scan, 0) AS seq_scan1,
    NULLIF(st1.idx_scan, 0) AS idx_scan1,

    round(
        100.0 - (st1.heap_blks_read + st1.idx_blks_read +
          st1.toast_blks_read + st1.tidx_blks_read)::numeric * 100.0 /
        NULLIF(st1.heap_blks_read + st1.idx_blks_read +
          st1.toast_blks_read + st1.tidx_blks_read +
          st1.heap_blks_hit + st1.idx_blks_hit +
          st1.toast_blks_hit + st1.tidx_blks_hit, 0), 2) AS hit_pct1,

    NULLIF(st2.heap_blks_read, 0) AS heap_blks_read2,
    round(NULLIF(st2.heap_blks_read, 0)::numeric * 100 /
      NULLIF(tot2.blks_read, 0), 2) AS heap_blks_read_pct2,

    NULLIF(st2.heap_blks_read + st2.heap_blks_hit, 0) AS heap_blks_fetch2,
    round(NULLIF(st2.heap_blks_read + st2.heap_blks_hit, 0)::numeric * 100 /
      NULLIF(tot2.blks_read + tot2.blks_hit, 0), 2) AS heap_blks_proc_pct2,

    NULLIF(st2.idx_blks_read, 0) AS idx_blks_read2,
    round(NULLIF(st2.idx_blks_read, 0)::numeric * 100 /
      NULLIF(tot2.blks_read, 0), 2) AS idx_blks_read_pct2,

    NULLIF(st2.idx_blks_read + st2.idx_blks_hit, 0) AS idx_blks_fetch2,
    round(NULLIF(st2.idx_blks_read + st2.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot2.blks_read + tot2.blks_hit, 0), 2) AS idx_blks_fetch_pct2,

    NULLIF(st2.toast_blks_read, 0) AS toast_blks_read2,
    round(NULLIF(st2.toast_blks_read, 0)::numeric * 100 /
      NULLIF(tot2.blks_read, 0), 2) AS toast_blks_read_pct2,

    NULLIF(st2.toast_blks_read + st2.toast_blks_hit, 0) AS toast_blks_fetch2,
    round(NULLIF(st2.toast_blks_read + st2.toast_blks_hit, 0)::numeric * 100 /
      NULLIF(tot2.blks_read + tot2.blks_hit, 0), 2) AS toast_blks_fetch_pct2,

    NULLIF(st2.tidx_blks_read, 0) AS tidx_blks_read2,
    round(NULLIF(st2.tidx_blks_read, 0)::numeric * 100 /
      NULLIF(tot2.blks_read, 0), 2) AS tidx_blks_read_pct2,

    NULLIF(st2.tidx_blks_read + st2.tidx_blks_hit, 0) AS tidx_blks_fetch2,
    round(NULLIF(st2.tidx_blks_read + st2.tidx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot2.blks_read + tot2.blks_hit, 0), 2) AS tidx_blks_fetch_pct2,

    NULLIF(st2.seq_scan, 0) AS seq_scan2,
    NULLIF(st2.idx_scan, 0) AS idx_scan2,

    round(
        100.0 - (st2.heap_blks_read + st2.idx_blks_read +
          st2.toast_blks_read + st2.tidx_blks_read)::numeric * 100.0 /
        NULLIF(st2.heap_blks_read + st2.idx_blks_read +
          st2.toast_blks_read + st2.tidx_blks_read +
          st2.heap_blks_hit + st2.idx_blks_hit +
          st2.toast_blks_hit + st2.tidx_blks_hit, 0), 2) AS hit_pct2,

    CASE WHEN
      st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read +
      st1.tidx_blks_read + st2.heap_blks_read + st2.idx_blks_read +
      st2.toast_blks_read + st2.tidx_blks_read > 0
    THEN
      row_number() OVER (ORDER BY
        st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read +
        st1.tidx_blks_read + st2.heap_blks_read + st2.idx_blks_read +
        st2.toast_blks_read + st2.tidx_blks_read
      DESC NULLS LAST,
      COALESCE(st1.datid, st2.datid),
      COALESCE(st1.relid, st2.relid))::integer
    ELSE NULL END AS ord_read,

    CASE WHEN
      st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read +
      st1.tidx_blks_read + st1.heap_blks_hit + st1.idx_blks_hit +
      st1.toast_blks_hit + st1.tidx_blks_hit + st2.heap_blks_read +
      st2.idx_blks_read + st2.toast_blks_read + st2.tidx_blks_read +
      st2.heap_blks_hit + st2.idx_blks_hit + st2.toast_blks_hit +
      st2.tidx_blks_hit > 0
    THEN
      row_number() OVER (ORDER BY
        st1.heap_blks_read + st1.idx_blks_read + st1.toast_blks_read +
        st1.tidx_blks_read + st1.heap_blks_hit + st1.idx_blks_hit +
        st1.toast_blks_hit + st1.tidx_blks_hit + st2.heap_blks_read +
        st2.idx_blks_read + st2.toast_blks_read + st2.tidx_blks_read +
        st2.heap_blks_hit + st2.idx_blks_hit + st2.toast_blks_hit +
        st2.tidx_blks_hit
      DESC NULLS LAST,
      COALESCE(st1.datid, st2.datid),
      COALESCE(st1.relid, st2.relid))::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_tables(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN
    top_io_tables(sserver_id, start2_id, end2_id) st2
    USING (datid, relid)
    CROSS JOIN total_dbstats(sserver_id, start1_id, end1_id) tot1
    CROSS JOIN total_dbstats(sserver_id, start2_id, end2_id) tot2
$$ LANGUAGE sql;

CREATE FUNCTION top_io_indexes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelid          oid,
    indexrelname        name,
    idx_scan            bigint,
    idx_blks_read       bigint,
    idx_blks_hit        bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        tablespaces_list.tablespacename,
        COALESCE(mtl.schemaname, st.schemaname)::name AS schemaname,
        COALESCE(mtl.relname || '(TOAST)',st.relname)::name AS relname,
        st.indexrelid,
        st.indexrelname,
        COALESCE(sum(st.idx_scan), 0)::bigint AS idx_scan,
        COALESCE(sum(st.idx_blks_read), 0)::bigint AS idx_blks_read,
        COALESCE(sum(st.idx_blks_hit), 0)::bigint AS idx_blks_hit
    FROM v_sample_stat_indexes st
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
        JOIN tablespaces_list ON  (st.server_id=tablespaces_list.server_id AND st.tablespaceid=tablespaces_list.tablespaceid)
        -- join main table for indexes on toast
        LEFT OUTER JOIN sample_stat_tables mtbl ON
          (mtbl.server_id, mtbl.sample_id, mtbl.datid, mtbl.reltoastrelid) =
          (st.server_id, st.sample_id, st.datid, st.relid)
        LEFT OUTER JOIN tables_list mtl ON
          (mtl.server_id, mtl.datid, mtl.relid) =
          (mtbl.server_id, mtbl.datid, mtbl.relid)
    WHERE st.server_id = sserver_id AND NOT sample_db.datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,sample_db.datname,
      COALESCE(mtl.schemaname,st.schemaname),
      COALESCE(mtl.relname||'(TOAST)',st.relname),
      st.schemaname,
      st.relname,
      tablespaces_list.tablespacename,
      st.indexrelid,
      st.indexrelname
$$ LANGUAGE sql;

CREATE FUNCTION top_io_indexes_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    indexrelid          oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelname        name,
    
    idx_scan            bigint,
    idx_blks_read       bigint,
    idx_blks_read_pct   numeric,
    idx_blks_hit_pct    numeric,
    idx_blks_fetch      bigint,
    idx_blks_fetch_pct  numeric,

    ord_read            integer,
    ord_fetch           integer
) SET search_path=@extschema@ AS $$
  SELECT
    datid,
    relid,
    indexrelid,
    dbname,
    tablespacename,
    schemaname,
    relname,
    indexrelname,

    NULLIF(st.idx_scan, 0) as idx_scan,
    NULLIF(st.idx_blks_read, 0) as idx_blks_read,
    round(NULLIF(st.idx_blks_read, 0)::numeric * 100 /
      NULLIF(tot.blks_read, 0), 2) AS idx_blks_read_pct,
    round(NULLIF(st.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(st.idx_blks_read + st.idx_blks_hit, 0), 2) AS idx_blks_hit_pct,
    NULLIF(st.idx_blks_read + st.idx_blks_hit, 0) as idx_blks_fetch,
    round(NULLIF(st.idx_blks_read + st.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot.blks_read + tot.blks_hit, 0), 2) AS idx_blks_fetch_pct,

    CASE WHEN
      COALESCE(st.idx_blks_read, 0) > 0
    THEN
      row_number() OVER (ORDER BY idx_blks_read DESC NULLS LAST,
      st.datid,
      st.indexrelid)::integer
    ELSE NULL END AS ord_read,
    
    CASE WHEN
      st.idx_blks_read + st.idx_blks_hit > 0
    THEN
      row_number() OVER (ORDER BY
        st.idx_blks_read + st.idx_blks_hit
      DESC NULLS LAST,
      st.datid,
      st.indexrelid)::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_indexes(sserver_id, start_id, end_id) st
    CROSS JOIN total_dbstats(sserver_id, start_id, end_id) tot
$$ LANGUAGE sql;


CREATE FUNCTION top_io_indexes_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    indexrelid          oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelname        name,
    
    idx_scan1           bigint,
    idx_blks_read1      bigint,
    idx_blks_read_pct1  numeric,
    idx_blks_hit_pct1   numeric,
    idx_blks_fetch1     bigint,
    idx_blks_fetch_pct1 numeric,

    idx_scan2           bigint,
    idx_blks_read2      bigint,
    idx_blks_read_pct2  numeric,
    idx_blks_hit_pct2   numeric,
    idx_blks_fetch2     bigint,
    idx_blks_fetch_pct2 numeric,

    ord_read            integer,
    ord_fetch           integer
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(st1.datid, st2.datid) AS datid,
    COALESCE(st1.relid, st2.relid) AS relid,
    COALESCE(st1.indexrelid, st2.indexrelid) AS indexrelid,
    COALESCE(st1.dbname, st2.dbname) AS dbname,
    COALESCE(st1.tablespacename, st2.tablespacename) AS tablespacename,
    COALESCE(st1.schemaname, st2.schemaname) AS schemaname,
    COALESCE(st1.relname, st2.relname) AS relname,
    COALESCE(st1.indexrelname, st2.indexrelname) AS indexrelname,

    NULLIF(st1.idx_scan, 0) as idx_scan1,
    NULLIF(st1.idx_blks_read, 0) as idx_blks_read1,
    round(NULLIF(st1.idx_blks_read, 0)::numeric * 100 /
      NULLIF(tot1.blks_read, 0), 2) AS idx_blks_read_pct1,
    round(NULLIF(st1.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(st1.idx_blks_read + st1.idx_blks_hit, 0), 2) AS idx_blks_hit_pct1,
    NULLIF(st1.idx_blks_read + st1.idx_blks_hit, 0) as idx_blks_fetch1,
    round(NULLIF(st1.idx_blks_read + st1.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot1.blks_read + tot1.blks_hit, 0), 2) AS idx_blks_fetch_pct1,

    NULLIF(st2.idx_scan, 0) as idx_scan2,
    NULLIF(st2.idx_blks_read, 0) as idx_blks_read2,
    round(NULLIF(st2.idx_blks_read, 0)::numeric * 100 /
      NULLIF(tot2.blks_read, 0), 2) AS idx_blks_read_pct2,
    round(NULLIF(st2.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(st2.idx_blks_read + st2.idx_blks_hit, 0), 2) AS idx_blks_hit_pct2,
    NULLIF(st2.idx_blks_read + st2.idx_blks_hit, 0) as idx_blks_fetch2,
    round(NULLIF(st2.idx_blks_read + st2.idx_blks_hit, 0)::numeric * 100 /
      NULLIF(tot2.blks_read + tot2.blks_hit, 0), 2) AS idx_blks_fetch_pct2,

    CASE WHEN
      st1.idx_blks_read + st2.idx_blks_read > 0
    THEN
      row_number() OVER (ORDER BY
        st1.idx_blks_read + st2.idx_blks_read
      DESC NULLS LAST,
      COALESCE(st1.datid, st2.datid),
      COALESCE(st1.indexrelid, st2.indexrelid))::integer
    ELSE NULL END AS ord_read,
    
    CASE WHEN
      st1.idx_blks_read + st2.idx_blks_read + st1.idx_blks_hit +
      st2.idx_blks_hit > 0
    THEN
      row_number() OVER (ORDER BY
        st1.idx_blks_read + st2.idx_blks_read + st1.idx_blks_hit +
        st2.idx_blks_hit
      DESC NULLS LAST,
      COALESCE(st1.datid, st2.datid),
      COALESCE(st1.indexrelid, st2.indexrelid))::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_indexes(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN
    top_io_indexes(sserver_id, start2_id, end2_id) st2
    USING (datid, relid, indexrelid)
    CROSS JOIN total_dbstats(sserver_id, start1_id, end1_id) tot1
    CROSS JOIN total_dbstats(sserver_id, start2_id, end2_id) tot2
$$ LANGUAGE sql;
