/* ===== Tables stats functions ===== */

CREATE FUNCTION top_tables(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    reltoastrelid       oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_tup_newpage_upd   bigint,
    np_upd_pct          numeric,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    growth              bigint,
    relpagegrowth_bytes bigint,
    seqscan_bytes_relsize bigint,
    seqscan_bytes_relpages bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        st.relid,
        st.reltoastrelid,
        sample_db.datname AS dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.seq_tup_read)::bigint AS seq_tup_read,
        sum(st.idx_scan)::bigint AS idx_scan,
        sum(st.idx_tup_fetch)::bigint AS idx_tup_fetch,
        sum(st.n_tup_ins)::bigint AS n_tup_ins,
        sum(st.n_tup_upd)::bigint AS n_tup_upd,
        sum(st.n_tup_del)::bigint AS n_tup_del,
        sum(st.n_tup_hot_upd)::bigint AS n_tup_hot_upd,
        sum(st.n_tup_newpage_upd)::bigint AS n_tup_newpage_upd,
        sum(st.n_tup_newpage_upd)::numeric * 100 /
          NULLIF(sum(st.n_tup_upd)::numeric, 0) AS np_upd_pct,
        sum(st.vacuum_count)::bigint AS vacuum_count,
        sum(st.autovacuum_count)::bigint AS autovacuum_count,
        sum(st.analyze_count)::bigint AS analyze_count,
        sum(st.autoanalyze_count)::bigint AS autoanalyze_count,
        sum(st.relsize_diff)::bigint AS growth,
        sum(st.relpages_bytes_diff)::bigint AS relpagegrowth_bytes,
        CASE WHEN bool_and(COALESCE(st.seq_scan, 0) = 0 OR st.relsize IS NOT NULL) THEN
          sum(st.seq_scan * st.relsize)::bigint
        ELSE NULL
        END AS seqscan_bytes_relsize,
        sum(st.seq_scan * st.relpages_bytes)::bigint AS seqscan_bytes_relpages
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
    WHERE st.server_id = sserver_id AND st.relkind IN ('r','m')
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,st.reltoastrelid,sample_db.datname,st.tablespacename,st.schemaname,st.relname
$$ LANGUAGE sql;

CREATE FUNCTION top_toasts(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_tup_newpage_upd   bigint,
    np_upd_pct          numeric,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    growth              bigint,
    relpagegrowth_bytes bigint,
    seqscan_bytes_relsize bigint,
    seqscan_bytes_relpages bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.seq_tup_read)::bigint AS seq_tup_read,
        sum(st.idx_scan)::bigint AS idx_scan,
        sum(st.idx_tup_fetch)::bigint AS idx_tup_fetch,
        sum(st.n_tup_ins)::bigint AS n_tup_ins,
        sum(st.n_tup_upd)::bigint AS n_tup_upd,
        sum(st.n_tup_del)::bigint AS n_tup_del,
        sum(st.n_tup_hot_upd)::bigint AS n_tup_hot_upd,
        sum(st.n_tup_newpage_upd)::bigint AS n_tup_newpage_upd,
        sum(st.n_tup_newpage_upd)::numeric * 100 /
          NULLIF(sum(st.n_tup_upd)::numeric, 0) AS np_upd_pct,
        sum(st.vacuum_count)::bigint AS vacuum_count,
        sum(st.autovacuum_count)::bigint AS autovacuum_count,
        sum(st.analyze_count)::bigint AS analyze_count,
        sum(st.autoanalyze_count)::bigint AS autoanalyze_count,
        sum(st.relsize_diff)::bigint AS growth,
        sum(st.relpages_bytes_diff)::bigint AS relpagegrowth_bytes,
        CASE WHEN bool_and(COALESCE(st.seq_scan, 0) = 0 OR st.relsize IS NOT NULL) THEN
          sum(st.seq_scan * st.relsize)::bigint
        ELSE NULL
        END AS seqscan_bytes_relsize,
        sum(st.seq_scan * st.relpages_bytes)::bigint AS seqscan_bytes_relpages
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
    WHERE st.server_id = sserver_id AND st.relkind = 't'
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,sample_db.datname,st.tablespacename,st.schemaname,st.relname
$$ LANGUAGE sql;

CREATE FUNCTION top_tables_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid                     oid,
    relid                     oid,
    reltoastrelid             oid,
    dbname                    name,
    tablespacename            name,
    schemaname                name,
    relname                   name,
    toastrelname              text,

    seq_scan                  bigint,
    seq_tup_read              bigint,
    idx_scan                  bigint,
    idx_tup_fetch             bigint,
    n_tup_ins                 bigint,
    n_tup_upd                 bigint,
    n_tup_del                 bigint,
    n_tup_hot_upd             bigint,
    n_tup_newpage_upd         bigint,
    np_upd_pct                numeric,
    vacuum_count              bigint,
    autovacuum_count          bigint,
    analyze_count             bigint,
    autoanalyze_count         bigint,

    toastseq_scan             bigint,
    toastseq_tup_read         bigint,
    toastidx_scan             bigint,
    toastidx_tup_fetch        bigint,
    toastn_tup_ins            bigint,
    toastn_tup_upd            bigint,
    toastn_tup_del            bigint,
    toastn_tup_hot_upd        bigint,
    toastn_tup_newpage_upd    bigint,
    toastnp_upd_pct           numeric,
    toastvacuum_count         bigint,
    toastautovacuum_count     bigint,
    toastanalyze_count        bigint,
    toastautoanalyze_count    bigint,

    growth_pretty             text,
    toastgrowth_pretty        text,
    seqscan_bytes_pretty      text,
    t_seqscan_bytes_pretty    text,
    relsize_pretty            text,
    t_relsize_pretty          text,

    ord_dml                   integer,
    ord_seq_scan              integer,
    ord_upd                   integer,
    ord_upd_np                integer,
    ord_growth                integer,
    ord_vac                   integer,
    ord_anl                   integer
  )
SET search_path=@extschema@ AS $$
  WITH rsa AS (
      SELECT
        rs.datid,
        rs.relid,
        rs.growth_avail,
        sst.relsize,
        sst.relpages_bytes
      FROM
        (SELECT
          datid,
          relid,
          max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
          min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
          CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
            max(sample_id) FILTER (WHERE relsize IS NOT NULL)
          ELSE
            max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
          END AS sid
        FROM
          sample_stat_tables
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start_id + 1 AND end_id
        GROUP BY server_id, datid, relid) AS rs
        JOIN sample_stat_tables sst ON
          (sst.server_id, sst.sample_id, sst.datid, sst.relid) =
          (sserver_id, rs.sid, rs.datid, rs.relid)
    )
  SELECT
    rel.datid,
    rel.relid,
    rel.reltoastrelid,
    rel.dbname,
    rel.tablespacename,
    rel.schemaname,
    rel.relname,
    CASE WHEN COALESCE(rel.reltoastrelid, 0) > 0
        THEN rel.relname || '(TOAST)' ELSE NULL
    END as toastrelname,
    
    NULLIF(rel.seq_scan, 0) AS seq_scan,
    NULLIF(rel.seq_tup_read, 0) AS seq_tup_read,
    NULLIF(rel.idx_scan, 0) AS idx_scan,
    NULLIF(rel.idx_tup_fetch, 0) AS idx_tup_fetch,
    NULLIF(rel.n_tup_ins, 0) AS n_tup_ins,
    NULLIF(rel.n_tup_upd, 0) AS n_tup_upd,
    NULLIF(rel.n_tup_del, 0) AS n_tup_del,
    NULLIF(rel.n_tup_hot_upd, 0) AS n_tup_hot_upd,
    NULLIF(rel.n_tup_newpage_upd, 0) AS n_tup_newpage_upd,
    ROUND(NULLIF(rel.np_upd_pct, 0), 1) AS np_upd_pct,
    NULLIF(rel.vacuum_count, 0) AS vacuum_count,
    NULLIF(rel.autovacuum_count, 0) AS autovacuum_count,
    NULLIF(rel.analyze_count, 0) AS analyze_count,
    NULLIF(rel.autoanalyze_count, 0) AS autoanalyze_count,
    
    NULLIF(toast.seq_scan, 0) AS toastseq_scan,
    NULLIF(toast.seq_tup_read, 0) AS toastseq_tup_read,
    NULLIF(toast.idx_scan, 0) AS toastidx_scan,
    NULLIF(toast.idx_tup_fetch, 0) AS toastidx_tup_fetch,
    NULLIF(toast.n_tup_ins, 0) AS toastn_tup_ins,
    NULLIF(toast.n_tup_upd, 0) AS toastn_tup_upd,
    NULLIF(toast.n_tup_del, 0) AS toastn_tup_del,
    NULLIF(toast.n_tup_hot_upd, 0) AS toastn_tup_hot_upd,
    NULLIF(toast.n_tup_newpage_upd, 0) AS toastn_tup_newpage_upd,
    ROUND(NULLIF(toast.np_upd_pct, 0), 1) AS toastnp_upd_pct,
    NULLIF(toast.vacuum_count, 0) AS toastvacuum_count,
    NULLIF(toast.autovacuum_count, 0) AS toastautovacuum_count,
    NULLIF(toast.analyze_count, 0) AS toastanalyze_count,
    NULLIF(toast.autoanalyze_count, 0) AS toastautoanalyze_count,
    
    CASE WHEN relrs.growth_avail THEN
      pg_size_pretty(NULLIF(rel.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(rel.relpagegrowth_bytes, 0))||']'
    END AS growth_pretty,

    CASE WHEN toastrs.growth_avail THEN
      pg_size_pretty(NULLIF(toast.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(toast.relpagegrowth_bytes, 0))||']'
    END AS toastgrowth_pretty,

    COALESCE(
      pg_size_pretty(NULLIF(rel.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(rel.seqscan_bytes_relpages, 0))||']'
    ) AS seqscan_bytes_pretty,

    COALESCE(
      pg_size_pretty(NULLIF(toast.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(toast.seqscan_bytes_relpages, 0))||']'
    ) AS t_seqscan_bytes_pretty,

    COALESCE(
      pg_size_pretty(NULLIF(relrs.relsize, 0)),
      '['||pg_size_pretty(NULLIF(relrs.relpages_bytes, 0))||']'
    ) AS relsize_pretty,

    COALESCE(
      pg_size_pretty(NULLIF(toastrs.relsize, 0)),
      '['||pg_size_pretty(NULLIF(toastrs.relpages_bytes, 0))||']'
    ) AS t_relsize_pretty,

    CASE WHEN
      COALESCE(rel.n_tup_ins, 0) + COALESCE(rel.n_tup_upd, 0) +
      COALESCE(rel.n_tup_del, 0) + COALESCE(toast.n_tup_ins, 0) +
      COALESCE(toast.n_tup_upd, 0) + COALESCE(toast.n_tup_del, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.n_tup_ins, 0) + COALESCE(rel.n_tup_upd, 0) +
        COALESCE(rel.n_tup_del, 0) + COALESCE(toast.n_tup_ins, 0) +
        COALESCE(toast.n_tup_upd, 0) + COALESCE(toast.n_tup_del, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_dml,

    CASE WHEN
      COALESCE(rel.seqscan_bytes_relsize, rel.seqscan_bytes_relpages, 0) +
      COALESCE(toast.seqscan_bytes_relsize, toast.seqscan_bytes_relpages, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.seqscan_bytes_relsize, rel.seqscan_bytes_relpages, 0) +
        COALESCE(toast.seqscan_bytes_relsize, toast.seqscan_bytes_relpages, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_seq_scan,

    CASE WHEN
      COALESCE(rel.n_tup_upd, 0) + COALESCE(rel.n_tup_del, 0) +
      COALESCE(toast.n_tup_upd, 0) + COALESCE(toast.n_tup_del, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.n_tup_upd, 0) + COALESCE(rel.n_tup_del, 0) +
        COALESCE(toast.n_tup_upd, 0) + COALESCE(toast.n_tup_del, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_upd,

    CASE WHEN
      COALESCE(rel.n_tup_newpage_upd, 0) + COALESCE(toast.n_tup_newpage_upd, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.n_tup_newpage_upd, 0) + COALESCE(toast.n_tup_newpage_upd, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_upd_np,

    CASE WHEN
      ((relrs.growth_avail AND rel.growth > 0) OR rel.relpagegrowth_bytes > 0) OR
      ((toastrs.growth_avail AND toast.growth > 0) OR toast.relpagegrowth_bytes > 0)
    THEN
      row_number() OVER (ORDER BY
        CASE WHEN relrs.growth_avail THEN rel.growth ELSE rel.relpagegrowth_bytes END +
        COALESCE(CASE WHEN toastrs.growth_avail THEN toast.growth ELSE toast.relpagegrowth_bytes END, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_growth,

    CASE WHEN
      COALESCE(rel.vacuum_count, 0) + COALESCE(rel.autovacuum_count, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.vacuum_count, 0) + COALESCE(rel.autovacuum_count, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_vac,

    CASE WHEN
      COALESCE(rel.analyze_count, 0) + COALESCE(rel.autoanalyze_count, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.analyze_count, 0) + COALESCE(rel.autoanalyze_count, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_anl
  FROM (
      top_tables(sserver_id, start_id, end_id) AS rel
      JOIN rsa AS relrs USING (datid, relid)
    )
    LEFT OUTER JOIN (
      top_toasts(sserver_id, start_id, end_id) AS toast
      JOIN rsa AS toastrs USING (datid, relid)
    ) ON (rel.datid, rel.reltoastrelid) = (toast.datid, toast.relid)
$$ LANGUAGE sql;

CREATE FUNCTION top_tables_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid                     oid,
    relid                     oid,
    reltoastrelid             oid,
    dbname                    name,
    tablespacename            name,
    schemaname                name,
    relname                   name,
    toastrelname              text,

    seq_scan1                 bigint,
    seq_tup_read1             bigint,
    idx_scan1                 bigint,
    idx_tup_fetch1            bigint,
    n_tup_ins1                bigint,
    n_tup_upd1                bigint,
    n_tup_del1                bigint,
    n_tup_hot_upd1            bigint,
    n_tup_newpage_upd1        bigint,
    np_upd_pct1               numeric,
    vacuum_count1             bigint,
    autovacuum_count1         bigint,
    analyze_count1            bigint,
    autoanalyze_count1        bigint,

    toastseq_scan1            bigint,
    toastseq_tup_read1        bigint,
    toastidx_scan1            bigint,
    toastidx_tup_fetch1       bigint,
    toastn_tup_ins1           bigint,
    toastn_tup_upd1           bigint,
    toastn_tup_del1           bigint,
    toastn_tup_hot_upd1       bigint,
    toastn_tup_newpage_upd1   bigint,
    toastnp_upd_pct1          numeric,
    toastvacuum_count1        bigint,
    toastautovacuum_count1    bigint,
    toastanalyze_count1       bigint,
    toastautoanalyze_count1   bigint,

    growth_pretty1            text,
    toastgrowth_pretty1       text,
    seqscan_bytes_pretty1     text,
    t_seqscan_bytes_pretty1   text,
    relsize_pretty1           text,
    t_relsize_pretty1         text,

    seq_scan2                 bigint,
    seq_tup_read2             bigint,
    idx_scan2                 bigint,
    idx_tup_fetch2            bigint,
    n_tup_ins2                bigint,
    n_tup_upd2                bigint,
    n_tup_del2                bigint,
    n_tup_hot_upd2            bigint,
    n_tup_newpage_upd2        bigint,
    np_upd_pct2               numeric,
    vacuum_count2             bigint,
    autovacuum_count2         bigint,
    analyze_count2            bigint,
    autoanalyze_count2        bigint,

    toastseq_scan2            bigint,
    toastseq_tup_read2        bigint,
    toastidx_scan2            bigint,
    toastidx_tup_fetch2       bigint,
    toastn_tup_ins2           bigint,
    toastn_tup_upd2           bigint,
    toastn_tup_del2           bigint,
    toastn_tup_hot_upd2       bigint,
    toastn_tup_newpage_upd2   bigint,
    toastnp_upd_pct2          numeric,
    toastvacuum_count2        bigint,
    toastautovacuum_count2    bigint,
    toastanalyze_count2       bigint,
    toastautoanalyze_count2   bigint,

    growth_pretty2            text,
    toastgrowth_pretty2       text,
    seqscan_bytes_pretty2     text,
    t_seqscan_bytes_pretty2   text,
    relsize_pretty2           text,
    t_relsize_pretty2         text,

    ord_dml                   integer,
    ord_seq_scan              integer,
    ord_upd                   integer,
    ord_upd_np                integer,
    ord_growth                integer,
    ord_vac                   integer,
    ord_anl                   integer
  )
SET search_path=@extschema@ AS $$
  WITH rsa1 AS (
      SELECT
        rs.datid,
        rs.relid,
        rs.growth_avail,
        sst.relsize,
        sst.relpages_bytes
      FROM
        (SELECT
          datid,
          relid,
          max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
          min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
          CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
            max(sample_id) FILTER (WHERE relsize IS NOT NULL)
          ELSE
            max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
          END AS sid
        FROM
          sample_stat_tables
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start1_id + 1 AND end1_id
        GROUP BY server_id, datid, relid) AS rs
        JOIN sample_stat_tables sst ON
          (sst.server_id, sst.sample_id, sst.datid, sst.relid) =
          (sserver_id, rs.sid, rs.datid, rs.relid)
    ),
    rsa2 AS (
      SELECT
        rs.datid,
        rs.relid,
        rs.growth_avail,
        sst.relsize,
        sst.relpages_bytes
      FROM
        (SELECT
          datid,
          relid,
          max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
          min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
          CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
            max(sample_id) FILTER (WHERE relsize IS NOT NULL)
          ELSE
            max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
          END AS sid
        FROM
          sample_stat_tables
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start2_id + 1 AND end2_id
        GROUP BY server_id, datid, relid) AS rs
        JOIN sample_stat_tables sst ON
          (sst.server_id, sst.sample_id, sst.datid, sst.relid) =
          (sserver_id, rs.sid, rs.datid, rs.relid)
    )
  SELECT
    COALESCE(rel1.datid, rel2.datid) AS datid,
    COALESCE(rel1.relid, rel2.relid) AS relid,
    COALESCE(rel1.reltoastrelid, rel2.reltoastrelid) as reltoastrelid,
    COALESCE(rel1.dbname, rel2.dbname) AS dbname,
    COALESCE(rel1.tablespacename, rel2.tablespacename) AS tablespacename,
    COALESCE(rel1.schemaname, rel2.schemaname) AS schemaname,
    COALESCE(rel1.relname, rel2.relname) AS relname,
    CASE WHEN COALESCE(rel1.reltoastrelid, rel2.reltoastrelid, 0) > 0
        THEN COALESCE(rel1.relname, rel2.relname) || '(TOAST)' ELSE NULL
    END as toastrelname,
    
    NULLIF(rel1.seq_scan, 0) AS seq_scan1,
    NULLIF(rel1.seq_tup_read, 0) AS seq_tup_read1,
    NULLIF(rel1.idx_scan, 0) AS idx_scan1,
    NULLIF(rel1.idx_tup_fetch, 0) AS idx_tup_fetch1,
    NULLIF(rel1.n_tup_ins, 0) AS n_tup_ins1,
    NULLIF(rel1.n_tup_upd, 0) AS n_tup_upd1,
    NULLIF(rel1.n_tup_del, 0) AS n_tup_del1,
    NULLIF(rel1.n_tup_hot_upd, 0) AS n_tup_hot_upd1,
    NULLIF(rel1.n_tup_newpage_upd, 0) AS n_tup_newpage_upd1,
    ROUND(NULLIF(rel1.np_upd_pct, 0), 1) AS np_upd_pct1,
    NULLIF(rel1.vacuum_count, 0) AS vacuum_count1,
    NULLIF(rel1.autovacuum_count, 0) AS autovacuum_count1,
    NULLIF(rel1.analyze_count, 0) AS analyze_count1,
    NULLIF(rel1.autoanalyze_count, 0) AS autoanalyze_count1,
    
    NULLIF(toast1.seq_scan, 0) AS toastseq_scan1,
    NULLIF(toast1.seq_tup_read, 0) AS toastseq_tup_read1,
    NULLIF(toast1.idx_scan, 0) AS toastidx_scan1,
    NULLIF(toast1.idx_tup_fetch, 0) AS toastidx_tup_fetch1,
    NULLIF(toast1.n_tup_ins, 0) AS toastn_tup_ins1,
    NULLIF(toast1.n_tup_upd, 0) AS toastn_tup_upd1,
    NULLIF(toast1.n_tup_del, 0) AS toastn_tup_del1,
    NULLIF(toast1.n_tup_hot_upd, 0) AS toastn_tup_hot_upd1,
    NULLIF(toast1.n_tup_newpage_upd, 0) AS toastn_tup_newpage_upd1,
    ROUND(NULLIF(toast1.np_upd_pct, 0), 1) AS toastnp_upd_pct1,
    NULLIF(toast1.vacuum_count, 0) AS toastvacuum_count1,
    NULLIF(toast1.autovacuum_count, 0) AS toastautovacuum_count1,
    NULLIF(toast1.analyze_count, 0) AS toastanalyze_count1,
    NULLIF(toast1.autoanalyze_count, 0) AS toastautoanalyze_count1,
    
    CASE WHEN relrs1.growth_avail THEN
      pg_size_pretty(NULLIF(rel1.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(rel1.relpagegrowth_bytes, 0))||']'
    END AS growth_pretty1,

    CASE WHEN toastrs1.growth_avail THEN
      pg_size_pretty(NULLIF(toast1.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(toast1.relpagegrowth_bytes, 0))||']'
    END AS toastgrowth_pretty1,

    COALESCE(
      pg_size_pretty(NULLIF(rel1.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(rel1.seqscan_bytes_relpages, 0))||']'
    ) AS seqscan_bytes_pretty1,

    COALESCE(
      pg_size_pretty(NULLIF(toast1.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(toast1.seqscan_bytes_relpages, 0))||']'
    ) AS t_seqscan_bytes_pretty1,

    COALESCE(
      pg_size_pretty(NULLIF(relrs1.relsize, 0)),
      '['||pg_size_pretty(NULLIF(relrs1.relpages_bytes, 0))||']'
    ) AS relsize_pretty1,

    COALESCE(
      pg_size_pretty(NULLIF(toastrs1.relsize, 0)),
      '['||pg_size_pretty(NULLIF(toastrs1.relpages_bytes, 0))||']'
    ) AS t_relsize_pretty1,

    NULLIF(rel2.seq_scan, 0) AS seq_scan2,
    NULLIF(rel2.seq_tup_read, 0) AS seq_tup_read2,
    NULLIF(rel2.idx_scan, 0) AS idx_scan2,
    NULLIF(rel2.idx_tup_fetch, 0) AS idx_tup_fetch2,
    NULLIF(rel2.n_tup_ins, 0) AS n_tup_ins2,
    NULLIF(rel2.n_tup_upd, 0) AS n_tup_upd2,
    NULLIF(rel2.n_tup_del, 0) AS n_tup_del2,
    NULLIF(rel2.n_tup_hot_upd, 0) AS n_tup_hot_upd2,
    NULLIF(rel2.n_tup_newpage_upd, 0) AS n_tup_newpage_upd2,
    ROUND(NULLIF(rel2.np_upd_pct, 0), 1) AS np_upd_pct2,
    NULLIF(rel2.vacuum_count, 0) AS vacuum_count2,
    NULLIF(rel2.autovacuum_count, 0) AS autovacuum_count2,
    NULLIF(rel2.analyze_count, 0) AS analyze_count2,
    NULLIF(rel2.autoanalyze_count, 0) AS autoanalyze_count2,
    
    NULLIF(toast2.seq_scan, 0) AS toastseq_scan2,
    NULLIF(toast2.seq_tup_read, 0) AS toastseq_tup_read2,
    NULLIF(toast2.idx_scan, 0) AS toastidx_scan2,
    NULLIF(toast2.idx_tup_fetch, 0) AS toastidx_tup_fetch2,
    NULLIF(toast2.n_tup_ins, 0) AS toastn_tup_ins2,
    NULLIF(toast2.n_tup_upd, 0) AS toastn_tup_upd2,
    NULLIF(toast2.n_tup_del, 0) AS toastn_tup_del2,
    NULLIF(toast2.n_tup_hot_upd, 0) AS toastn_tup_hot_upd2,
    NULLIF(toast2.n_tup_newpage_upd, 0) AS toastn_tup_newpage_upd2,
    ROUND(NULLIF(toast2.np_upd_pct, 0), 1) AS toastnp_upd_pct2,
    NULLIF(toast2.vacuum_count, 0) AS toastvacuum_count2,
    NULLIF(toast2.autovacuum_count, 0) AS toastautovacuum_count2,
    NULLIF(toast2.analyze_count, 0) AS toastanalyze_count2,
    NULLIF(toast2.autoanalyze_count, 0) AS toastautoanalyze_count2,
    
    CASE WHEN relrs2.growth_avail THEN
      pg_size_pretty(NULLIF(rel2.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(rel2.relpagegrowth_bytes, 0))||']'
    END AS growth_pretty2,

    CASE WHEN toastrs2.growth_avail THEN
      pg_size_pretty(NULLIF(toast2.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(toast2.relpagegrowth_bytes, 0))||']'
    END AS toastgrowth_pretty2,

    COALESCE(
      pg_size_pretty(NULLIF(rel2.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(rel2.seqscan_bytes_relpages, 0))||']'
    ) AS seqscan_bytes_pretty2,

    COALESCE(
      pg_size_pretty(NULLIF(toast2.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(toast2.seqscan_bytes_relpages, 0))||']'
    ) AS t_seqscan_bytes_pretty2,

    COALESCE(
      pg_size_pretty(NULLIF(relrs2.relsize, 0)),
      '['||pg_size_pretty(NULLIF(relrs2.relpages_bytes, 0))||']'
    ) AS relsize_pretty2,

    COALESCE(
      pg_size_pretty(NULLIF(toastrs2.relsize, 0)),
      '['||pg_size_pretty(NULLIF(toastrs2.relpages_bytes, 0))||']'
    ) AS t_relsize_pretty2,

    CASE WHEN
      COALESCE(rel1.n_tup_ins, 0) + COALESCE(rel1.n_tup_upd, 0) +
      COALESCE(rel1.n_tup_del, 0) + COALESCE(toast1.n_tup_ins, 0) +
      COALESCE(toast1.n_tup_upd, 0) + COALESCE(toast1.n_tup_del, 0) +
      COALESCE(rel2.n_tup_ins, 0) + COALESCE(rel2.n_tup_upd, 0) +
      COALESCE(rel2.n_tup_del, 0) + COALESCE(toast2.n_tup_ins, 0) +
      COALESCE(toast2.n_tup_upd, 0) + COALESCE(toast2.n_tup_del, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.n_tup_ins, 0) + COALESCE(rel1.n_tup_upd, 0) +
        COALESCE(rel1.n_tup_del, 0) + COALESCE(toast1.n_tup_ins, 0) +
        COALESCE(toast1.n_tup_upd, 0) + COALESCE(toast1.n_tup_del, 0) +
        COALESCE(rel2.n_tup_ins, 0) + COALESCE(rel2.n_tup_upd, 0) +
        COALESCE(rel2.n_tup_del, 0) + COALESCE(toast2.n_tup_ins, 0) +
        COALESCE(toast2.n_tup_upd, 0) + COALESCE(toast2.n_tup_del, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_dml,

    CASE WHEN
      COALESCE(rel1.seqscan_bytes_relsize, rel1.seqscan_bytes_relpages, 0) +
      COALESCE(toast1.seqscan_bytes_relsize, toast1.seqscan_bytes_relpages, 0) +
      COALESCE(rel2.seqscan_bytes_relsize, rel2.seqscan_bytes_relpages, 0) +
      COALESCE(toast2.seqscan_bytes_relsize, toast2.seqscan_bytes_relpages, 0)> 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.seqscan_bytes_relsize, rel1.seqscan_bytes_relpages, 0) +
        COALESCE(toast1.seqscan_bytes_relsize, toast1.seqscan_bytes_relpages, 0) +
        COALESCE(rel2.seqscan_bytes_relsize, rel2.seqscan_bytes_relpages, 0) +
        COALESCE(toast2.seqscan_bytes_relsize, toast2.seqscan_bytes_relpages, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_seq_scan,

    CASE WHEN
      COALESCE(rel1.n_tup_upd, 0) + COALESCE(rel1.n_tup_del, 0) +
      COALESCE(toast1.n_tup_upd, 0) + COALESCE(toast1.n_tup_del, 0) +
      COALESCE(rel2.n_tup_upd, 0) + COALESCE(rel2.n_tup_del, 0) +
      COALESCE(toast2.n_tup_upd, 0) + COALESCE(toast2.n_tup_del, 0)> 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.n_tup_upd, 0) + COALESCE(rel1.n_tup_del, 0) +
        COALESCE(toast1.n_tup_upd, 0) + COALESCE(toast1.n_tup_del, 0) +
        COALESCE(rel2.n_tup_upd, 0) + COALESCE(rel2.n_tup_del, 0) +
        COALESCE(toast2.n_tup_upd, 0) + COALESCE(toast2.n_tup_del, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_upd,

    CASE WHEN
      COALESCE(rel1.n_tup_newpage_upd, 0) + COALESCE(toast1.n_tup_newpage_upd, 0) +
      COALESCE(rel2.n_tup_newpage_upd, 0) + COALESCE(toast2.n_tup_newpage_upd, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.n_tup_newpage_upd, 0) + COALESCE(toast1.n_tup_newpage_upd, 0) +
        COALESCE(rel2.n_tup_newpage_upd, 0) + COALESCE(toast2.n_tup_newpage_upd, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_upd_np,

    CASE WHEN
      ((relrs1.growth_avail AND rel1.growth > 0) OR rel1.relpagegrowth_bytes > 0) OR
      ((toastrs1.growth_avail AND toast1.growth > 0) OR toast1.relpagegrowth_bytes > 0) OR
      ((relrs2.growth_avail AND rel2.growth > 0) OR rel2.relpagegrowth_bytes > 0) OR
      ((toastrs2.growth_avail AND toast2.growth > 0) OR toast2.relpagegrowth_bytes > 0)
    THEN
      row_number() OVER (ORDER BY
        CASE WHEN relrs1.growth_avail THEN rel1.growth ELSE rel1.relpagegrowth_bytes END +
        CASE WHEN toastrs1.growth_avail THEN toast1.growth ELSE toast1.relpagegrowth_bytes END +
        CASE WHEN relrs2.growth_avail THEN rel2.growth ELSE rel2.relpagegrowth_bytes END +
        CASE WHEN toastrs2.growth_avail THEN toast2.growth ELSE toast2.relpagegrowth_bytes END
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_growth,

    CASE WHEN
      COALESCE(rel1.vacuum_count, 0) + COALESCE(rel1.autovacuum_count, 0) +
      COALESCE(rel2.vacuum_count, 0) + COALESCE(rel2.autovacuum_count, 0)> 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.vacuum_count, 0) + COALESCE(rel1.autovacuum_count, 0) +
        COALESCE(rel2.vacuum_count, 0) + COALESCE(rel2.autovacuum_count, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_vac,

    CASE WHEN
      COALESCE(rel1.analyze_count, 0) + COALESCE(rel1.autoanalyze_count, 0) +
      COALESCE(rel2.analyze_count, 0) + COALESCE(rel2.autoanalyze_count, 0)> 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.analyze_count, 0) + COALESCE(rel1.autoanalyze_count, 0) +
        COALESCE(rel2.analyze_count, 0) + COALESCE(rel2.autoanalyze_count, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_anl
  FROM (
    -- Interval 1
      (
        top_tables(sserver_id, start1_id, end1_id) AS rel1
        JOIN rsa1 AS relrs1 USING (datid, relid)
      )
      LEFT OUTER JOIN (
        top_toasts(sserver_id, start1_id, end1_id) AS toast1
        JOIN rsa1 AS toastrs1 USING (datid, relid)
      ) ON (rel1.datid, rel1.reltoastrelid) = (toast1.datid, toast1.relid)
    ) FULL OUTER JOIN (
    -- Interval 2
      (
        top_tables(sserver_id, start2_id, end2_id) AS rel2
        JOIN rsa2 AS relrs2 USING (datid, relid)
      )
      LEFT OUTER JOIN (
        top_toasts(sserver_id, start2_id, end2_id) AS toast2
        JOIN rsa2 AS toastrs2 USING (datid, relid)
      ) ON (rel2.datid, rel2.reltoastrelid) = (toast2.datid, toast2.relid)
    ) ON (rel1.datid, rel1.relid) = (rel2.datid, rel2.relid)
$$ LANGUAGE sql;
