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
    heap_blks_read_pct          numeric,
    heap_blks_fetch             bigint,
    heap_blks_proc_pct          numeric,
    idx_blks_read               bigint,
    idx_blks_read_pct           numeric,
    idx_blks_fetch              bigint,
    idx_blks_fetch_pct           numeric,
    toast_blks_read             bigint,
    toast_blks_read_pct         numeric,
    toast_blks_fetch            bigint,
    toast_blks_fetch_pct        numeric,
    tidx_blks_read              bigint,
    tidx_blks_read_pct          numeric,
    tidx_blks_fetch             bigint,
    tidx_blks_fetch_pct         numeric,
    seq_scan                    bigint,
    idx_scan                    bigint
) SET search_path=@extschema@ AS $$
    WITH total AS (SELECT
      COALESCE(sum(heap_blks_read), 0) + COALESCE(sum(idx_blks_read), 0) AS total_blks_read,
      COALESCE(sum(heap_blks_read), 0) + COALESCE(sum(idx_blks_read), 0) +
      COALESCE(sum(heap_blks_hit), 0) + COALESCE(sum(idx_blks_hit), 0) AS total_blks_fetch
    FROM sample_stat_tables_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    )
    SELECT
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        tablespaces_list.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.heap_blks_read)::bigint AS heap_blks_read,
        sum(st.heap_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS heap_blks_read_pct,
        COALESCE(sum(st.heap_blks_read), 0)::bigint + COALESCE(sum(st.heap_blks_hit), 0)::bigint AS heap_blks_fetch,
        (COALESCE(sum(st.heap_blks_read), 0) + COALESCE(sum(st.heap_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS heap_blks_proc_pct,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.idx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS idx_blks_read_pct,
        COALESCE(sum(st.idx_blks_read), 0)::bigint + COALESCE(sum(st.idx_blks_hit), 0)::bigint AS idx_blks_fetch,
        (COALESCE(sum(st.idx_blks_read), 0) + COALESCE(sum(st.idx_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS idx_blks_fetch_pct,
        sum(st.toast_blks_read)::bigint AS toast_blks_read,
        sum(st.toast_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS toast_blks_read_pct,
        COALESCE(sum(st.toast_blks_read), 0)::bigint + COALESCE(sum(st.toast_blks_hit), 0)::bigint AS toast_blks_fetch,
        (COALESCE(sum(st.toast_blks_read), 0) + COALESCE(sum(st.toast_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS toast_blks_fetch_pct,
        sum(st.tidx_blks_read)::bigint AS tidx_blks_read,
        sum(st.tidx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS tidx_blks_read_pct,
        COALESCE(sum(st.tidx_blks_read), 0)::bigint + COALESCE(sum(st.tidx_blks_hit), 0)::bigint AS tidx_blks_fetch,
        (COALESCE(sum(st.tidx_blks_read), 0) + COALESCE(sum(st.tidx_blks_hit), 0)) * 100 / NULLIF(min(total.total_blks_fetch), 0) AS tidx_blks_fetch_pct,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.idx_scan)::bigint AS idx_scan
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
        JOIN tablespaces_list USING(server_id,tablespaceid)
        CROSS JOIN total
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

    NULLIF(heap_blks_read, 0) AS heap_blks_read,
    round(NULLIF(heap_blks_read_pct, 0.0), 2) AS heap_blks_read_pct,
    NULLIF(heap_blks_fetch, 0) AS heap_blks_fetch,
    round(NULLIF(heap_blks_proc_pct, 0.0), 2) AS heap_blks_proc_pct,
    NULLIF(idx_blks_read, 0) AS idx_blks_read,
    round(NULLIF(idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct,
    NULLIF(idx_blks_fetch, 0) AS idx_blks_fetch,
    round(NULLIF(idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct,
    NULLIF(toast_blks_read, 0) AS toast_blks_read,
    round(NULLIF(toast_blks_read_pct, 0.0), 2) AS toast_blks_read_pct,
    NULLIF(toast_blks_fetch, 0) AS toast_blks_fetch,
    round(NULLIF(toast_blks_fetch_pct, 0.0), 2) AS toast_blks_fetch_pct,
    NULLIF(tidx_blks_read, 0) AS tidx_blks_read,
    round(NULLIF(tidx_blks_read_pct, 0.0), 2) AS tidx_blks_read_pct,
    NULLIF(tidx_blks_fetch, 0) AS tidx_blks_fetch,
    round(NULLIF(tidx_blks_fetch_pct, 0.0), 2) AS tidx_blks_fetch_pct,
    NULLIF(seq_scan, 0) AS seq_scan,
    NULLIF(idx_scan, 0) AS idx_scan,
    round(
        100.0 - (COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) +
        COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0)) * 100.0 /
        NULLIF(heap_blks_fetch + idx_blks_fetch + toast_blks_fetch + tidx_blks_fetch, 0),2
    ) AS hit_pct,

    CASE WHEN
      COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0)
      DESC NULLS LAST,
      datid,
      relid)::integer
    ELSE NULL END AS ord_read,

    CASE WHEN
      COALESCE(heap_blks_fetch, 0) + COALESCE(idx_blks_fetch, 0) + COALESCE(toast_blks_fetch, 0) + COALESCE(tidx_blks_fetch, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(heap_blks_fetch, 0) + COALESCE(idx_blks_fetch, 0) + COALESCE(toast_blks_fetch, 0) + COALESCE(tidx_blks_fetch, 0)
      DESC NULLS LAST,
      datid,
      relid)::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_tables(sserver_id, start_id, end_id)
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
    COALESCE(rel1.datid, rel2.datid) AS datid,
    COALESCE(rel1.relid, rel2.relid) AS relid,
    COALESCE(rel1.dbname, rel2.dbname) AS dbname,
    COALESCE(rel1.tablespacename, rel2.tablespacename) AS tablespacename,
    COALESCE(rel1.schemaname, rel2.schemaname) AS schemaname,
    COALESCE(rel1.relname, rel2.relname) AS relname,

    NULLIF(rel1.heap_blks_read, 0) AS heap_blks_read1,
    round(NULLIF(rel1.heap_blks_read_pct, 0.0), 2) AS heap_blks_read_pct1,
    NULLIF(rel1.heap_blks_fetch, 0) AS heap_blks_fetch1,
    round(NULLIF(rel1.heap_blks_proc_pct, 0.0), 2) AS heap_blks_proc_pct1,
    NULLIF(rel1.idx_blks_read, 0) AS idx_blks_read1,
    round(NULLIF(rel1.idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct1,
    NULLIF(rel1.idx_blks_fetch, 0) AS idx_blks_fetch1,
    round(NULLIF(rel1.idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct1,
    NULLIF(rel1.toast_blks_read, 0) AS toast_blks_read1,
    round(NULLIF(rel1.toast_blks_read_pct, 0.0), 2) AS toast_blks_read_pct1,
    NULLIF(rel1.toast_blks_fetch, 0) AS toast_blks_fetch1,
    round(NULLIF(rel1.toast_blks_fetch_pct, 0.0), 2) AS toast_blks_fetch_pct1,
    NULLIF(rel1.tidx_blks_read, 0) AS tidx_blks_read1,
    round(NULLIF(rel1.tidx_blks_read_pct, 0.0), 2) AS tidx_blks_read_pct1,
    NULLIF(rel1.tidx_blks_fetch, 0) AS tidx_blks_fetch1,
    round(NULLIF(rel1.tidx_blks_fetch_pct, 0.0), 2) AS tidx_blks_fetch_pct1,
    NULLIF(rel1.seq_scan, 0) AS seq_scan1,
    NULLIF(rel1.idx_scan, 0) AS idx_scan1,
    round(
        100.0 - (COALESCE(rel1.heap_blks_read, 0) + COALESCE(rel1.idx_blks_read, 0) +
        COALESCE(rel1.toast_blks_read, 0) + COALESCE(rel1.tidx_blks_read, 0)) * 100.0 /
        NULLIF(rel1.heap_blks_fetch + rel1.idx_blks_fetch + rel1.toast_blks_fetch + rel1.tidx_blks_fetch, 0),2
    ) AS hit_pct1,

    NULLIF(rel2.heap_blks_read, 0) AS heap_blks_read2,
    round(NULLIF(rel2.heap_blks_read_pct, 0.0), 2) AS heap_blks_read_pct2,
    NULLIF(rel2.heap_blks_fetch, 0) AS heap_blks_fetch2,
    round(NULLIF(rel2.heap_blks_proc_pct, 0.0), 2) AS heap_blks_proc_pct2,
    NULLIF(rel2.idx_blks_read, 0) AS idx_blks_read2,
    round(NULLIF(rel2.idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct2,
    NULLIF(rel2.idx_blks_fetch, 0) AS idx_blks_fetch2,
    round(NULLIF(rel2.idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct2,
    NULLIF(rel2.toast_blks_read, 0) AS toast_blks_read2,
    round(NULLIF(rel2.toast_blks_read_pct, 0.0), 2) AS toast_blks_read_pct2,
    NULLIF(rel2.toast_blks_fetch, 0) AS toast_blks_fetch2,
    round(NULLIF(rel2.toast_blks_fetch_pct, 0.0), 2) AS toast_blks_fetch_pct2,
    NULLIF(rel2.tidx_blks_read, 0) AS tidx_blks_read2,
    round(NULLIF(rel2.tidx_blks_read_pct, 0.0), 2) AS tidx_blks_read_pct2,
    NULLIF(rel2.tidx_blks_fetch, 0) AS tidx_blks_fetch2,
    round(NULLIF(rel2.tidx_blks_fetch_pct, 0.0), 2) AS tidx_blks_fetch_pct2,
    NULLIF(rel2.seq_scan, 0) AS seq_scan2,
    NULLIF(rel2.idx_scan, 0) AS idx_scan2,
    round(
        100.0 - (COALESCE(rel2.heap_blks_read, 0) + COALESCE(rel2.idx_blks_read, 0) +
        COALESCE(rel2.toast_blks_read, 0) + COALESCE(rel2.tidx_blks_read, 0)) * 100.0 /
        NULLIF(rel2.heap_blks_fetch + rel2.idx_blks_fetch + rel2.toast_blks_fetch + rel2.tidx_blks_fetch, 0),2
    ) AS hit_pct2,

    CASE WHEN
      COALESCE(rel1.heap_blks_read, 0) + COALESCE(rel1.idx_blks_read, 0) + COALESCE(rel1.toast_blks_read, 0) + COALESCE(rel1.tidx_blks_read, 0) +
      COALESCE(rel2.heap_blks_read, 0) + COALESCE(rel2.idx_blks_read, 0) + COALESCE(rel2.toast_blks_read, 0) + COALESCE(rel2.tidx_blks_read, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.heap_blks_read, 0) + COALESCE(rel1.idx_blks_read, 0) + COALESCE(rel1.toast_blks_read, 0) + COALESCE(rel1.tidx_blks_read, 0) +
        COALESCE(rel2.heap_blks_read, 0) + COALESCE(rel2.idx_blks_read, 0) + COALESCE(rel2.toast_blks_read, 0) + COALESCE(rel2.tidx_blks_read, 0)
      DESC NULLS LAST,
      COALESCE(rel1.datid, rel2.datid),
      COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_read,

    CASE WHEN
      COALESCE(rel1.heap_blks_fetch, 0) + COALESCE(rel1.idx_blks_fetch, 0) + COALESCE(rel1.toast_blks_fetch, 0) + COALESCE(rel1.tidx_blks_fetch, 0) +
      COALESCE(rel2.heap_blks_fetch, 0) + COALESCE(rel2.idx_blks_fetch, 0) + COALESCE(rel2.toast_blks_fetch, 0) + COALESCE(rel2.tidx_blks_fetch, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.heap_blks_fetch, 0) + COALESCE(rel1.idx_blks_fetch, 0) + COALESCE(rel1.toast_blks_fetch, 0) + COALESCE(rel1.tidx_blks_fetch, 0) +
        COALESCE(rel2.heap_blks_fetch, 0) + COALESCE(rel2.idx_blks_fetch, 0) + COALESCE(rel2.toast_blks_fetch, 0) + COALESCE(rel2.tidx_blks_fetch, 0)
      DESC NULLS LAST,
      COALESCE(rel1.datid, rel2.datid),
      COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_tables(sserver_id, start1_id, end1_id) rel1
    FULL OUTER JOIN
    top_io_tables(sserver_id, start2_id, end2_id) rel2
    USING (datid, relid)
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
    idx_blks_read_pct   numeric,
    idx_blks_hit_pct    numeric,
    idx_blks_fetch      bigint,
    idx_blks_fetch_pct  numeric
) SET search_path=@extschema@ AS $$
    WITH total AS (SELECT
      COALESCE(sum(heap_blks_read)) + COALESCE(sum(idx_blks_read)) AS total_blks_read,
      COALESCE(sum(heap_blks_read)) + COALESCE(sum(idx_blks_read)) +
      COALESCE(sum(heap_blks_hit)) + COALESCE(sum(idx_blks_hit)) AS total_blks_fetch
    FROM sample_stat_tables_total
    WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id
    )
    SELECT
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name AS relname,
        st.indexrelid,
        st.indexrelname,
        sum(st.idx_scan)::bigint AS idx_scan,
        sum(st.idx_blks_read)::bigint AS idx_blks_read,
        sum(st.idx_blks_read) * 100 / NULLIF(min(total.total_blks_read), 0) AS idx_blks_read_pct,
        sum(st.idx_blks_hit) * 100 / NULLIF(COALESCE(sum(st.idx_blks_hit), 0) + COALESCE(sum(st.idx_blks_read), 0), 0) AS idx_blks_hit_pct,
        COALESCE(sum(st.idx_blks_read), 0)::bigint + COALESCE(sum(st.idx_blks_hit), 0)::bigint AS idx_blks_fetch,
        (COALESCE(sum(st.idx_blks_read), 0) + COALESCE(sum(st.idx_blks_hit), 0)) * 100 / NULLIF(min(total_blks_fetch), 0) AS idx_blks_fetch_pct
    FROM v_sample_stat_indexes st
        -- Database name
        JOIN sample_stat_database sample_db
        ON (st.server_id=sample_db.server_id AND st.sample_id=sample_db.sample_id AND st.datid=sample_db.datid)
        JOIN tablespaces_list ON  (st.server_id=tablespaces_list.server_id AND st.tablespaceid=tablespaces_list.tablespaceid)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON (st.server_id = mtbl.server_id AND st.datid = mtbl.datid AND st.relid = mtbl.reltoastrelid)
        CROSS JOIN total
    WHERE st.server_id = sserver_id AND NOT sample_db.datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,sample_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname), COALESCE(mtbl.relname||'(TOAST)',st.relname),
      st.schemaname,st.relname,tablespaces_list.tablespacename, st.indexrelid,st.indexrelname
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

    NULLIF(idx_scan, 0) as idx_scan,
    NULLIF(idx_blks_read, 0) as idx_blks_read,
    round(NULLIF(idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct,
    round(NULLIF(idx_blks_hit_pct, 0.0), 2) AS idx_blks_hit_pct,
    NULLIF(idx_blks_fetch, 0) as idx_blks_fetch,
    round(NULLIF(idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct,

    CASE WHEN
      COALESCE(idx_blks_read, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(idx_blks_read, 0)
      DESC NULLS LAST,
      datid,
      indexrelid)::integer
    ELSE NULL END AS ord_read,
    
    CASE WHEN
      COALESCE(idx_blks_fetch, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(idx_blks_fetch, 0)
      DESC NULLS LAST,
      datid,
      indexrelid)::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_indexes(sserver_id, start_id, end_id)
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
    COALESCE(rel1.datid, rel2.datid) AS datid,
    COALESCE(rel1.relid, rel2.relid) AS relid,
    COALESCE(rel1.indexrelid, rel2.indexrelid) AS indexrelid,
    COALESCE(rel1.dbname, rel2.dbname) AS dbname,
    COALESCE(rel1.tablespacename, rel2.tablespacename) AS tablespacename,
    COALESCE(rel1.schemaname, rel2.schemaname) AS schemaname,
    COALESCE(rel1.relname, rel2.relname) AS relname,
    COALESCE(rel1.indexrelname, rel2.indexrelname) AS indexrelname,

    NULLIF(rel1.idx_scan, 0) as idx_scan1,
    NULLIF(rel1.idx_blks_read, 0) as idx_blks_read1,
    round(NULLIF(rel1.idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct1,
    round(NULLIF(rel1.idx_blks_hit_pct, 0.0), 2) AS idx_blks_hit_pct1,
    NULLIF(rel1.idx_blks_fetch, 0) as idx_blks_fetch1,
    round(NULLIF(rel1.idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct1,

    NULLIF(rel2.idx_scan, 0) as idx_scan2,
    NULLIF(rel2.idx_blks_read, 0) as idx_blks_read2,
    round(NULLIF(rel2.idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct2,
    round(NULLIF(rel2.idx_blks_hit_pct, 0.0), 2) AS idx_blks_hit_pct2,
    NULLIF(rel2.idx_blks_fetch, 0) as idx_blks_fetch2,
    round(NULLIF(rel2.idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct2,

    CASE WHEN
      COALESCE(rel1.idx_blks_read, 0) + COALESCE(rel2.idx_blks_read, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.idx_blks_read, 0) + COALESCE(rel2.idx_blks_read, 0)
      DESC NULLS LAST,
      COALESCE(rel1.datid, rel2.datid),
      COALESCE(rel1.indexrelid, rel2.indexrelid))::integer
    ELSE NULL END AS ord_read,
    
    CASE WHEN
      COALESCE(rel1.idx_blks_fetch, 0) + COALESCE(rel2.idx_blks_fetch, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.idx_blks_fetch, 0) + COALESCE(rel2.idx_blks_fetch, 0)
      DESC NULLS LAST,
      COALESCE(rel1.datid, rel2.datid),
      COALESCE(rel1.indexrelid, rel2.indexrelid))::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_indexes(sserver_id, start1_id, end1_id) rel1
    FULL OUTER JOIN
    top_io_indexes(sserver_id, start2_id, end2_id) rel2
    USING (datid, relid, indexrelid)
$$ LANGUAGE sql;
