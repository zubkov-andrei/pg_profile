/* ===== Indexes stats functions ===== */

CREATE FUNCTION top_indexes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    indexrelid          oid,
    indisunique         boolean,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             text,
    indexrelname        text,
    idx_scan            bigint,
    growth              bigint,
    tbl_n_tup_ins       bigint,
    tbl_n_tup_upd       bigint,
    tbl_n_tup_del       bigint,
    tbl_n_tup_hot_upd   bigint,
    relpagegrowth_bytes bigint,
    idx_blks_read       bigint,
    idx_blks_fetch      bigint,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    vacuum_bytes_relsize  bigint,
    vacuum_bytes_relpages bigint,
    avg_indexrelsize    bigint,
    avg_relsize         bigint,
    avg_indexrelpages_bytes bigint,
    avg_relpages_bytes  bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        st.relid,
        st.indexrelid,
        st.indisunique,
        sample_db.datname,
        tablespaces_list.tablespacename,
        COALESCE(mtl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtl.relname||'(TOAST)',st.relname)::text as relname,
        st.indexrelname::text,
        sum(st.idx_scan)::bigint as idx_scan,
        sum(st.relsize_diff)::bigint as growth,
        sum(tbl.n_tup_ins)::bigint as tbl_n_tup_ins,
        sum(tbl.n_tup_upd)::bigint as tbl_n_tup_upd,
        sum(tbl.n_tup_del)::bigint as tbl_n_tup_del,
        sum(tbl.n_tup_hot_upd)::bigint as tbl_n_tup_hot_upd,
        sum(st.relpages_bytes_diff)::bigint as relpagegrowth_bytes,
        sum(st.idx_blks_read)::bigint as idx_blks_read,
        sum(st.idx_blks_hit)::bigint + sum(st.idx_blks_read)::bigint as idx_blks_fetch,
        sum(tbl.vacuum_count)::bigint as vacuum_count,
        sum(tbl.autovacuum_count)::bigint as autovacuum_count,
        
        CASE WHEN bool_and(
            COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0) = 0 OR
            st.relsize IS NOT NULL
          ) THEN
          sum((COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0)) * st.relsize)::bigint
        ELSE NULL
        END AS vacuum_bytes_relsize,

        sum(
          (COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0)) *
          st.relpages_bytes
        )::bigint AS vacuum_bytes_relpages,

        CASE WHEN bool_and(
            COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0) = 0 OR
            st.relsize IS NOT NULL
          ) THEN
          round(
            avg(st.relsize) FILTER
              (WHERE COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0) > 0)
          )::bigint
        ELSE NULL
        END AS avg_indexrelsize,
        
        CASE WHEN bool_and(
            COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0) = 0 OR
            tbl.relsize IS NOT NULL
          ) THEN
          round(
            avg(tbl.relsize) FILTER
              (WHERE COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0) > 0)
          )::bigint
        ELSE NULL
        END AS avg_relsize,

        round(
          avg(st.relpages_bytes) FILTER
            (WHERE COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0) > 0)
        )::bigint AS avg_indexrelpages_bytes,

        round(
          avg(tbl.relpages_bytes) FILTER
            (WHERE COALESCE(tbl.vacuum_count, 0) + COALESCE(tbl.autovacuum_count, 0) > 0)
        )::bigint AS avg_relpages_bytes

    FROM v_sample_stat_indexes st JOIN sample_stat_tables tbl USING (server_id, sample_id, datid, relid)
        -- Database name
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
        JOIN tablespaces_list ON (st.server_id, st.tablespaceid) = (tablespaces_list.server_id, tablespaces_list.tablespaceid)
        -- join main table for indexes on toast
        LEFT OUTER JOIN sample_stat_tables mtbl ON
          (mtbl.server_id, mtbl.sample_id, mtbl.datid, mtbl.reltoastrelid) =
          (st.server_id, st.sample_id, st.datid, st.relid)
        LEFT OUTER JOIN tables_list mtl ON
          (mtl.server_id, mtl.datid, mtl.relid) =
          (mtbl.server_id, mtbl.datid, mtbl.relid)
    WHERE st.server_id=sserver_id AND NOT sample_db.datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.datid,st.relid,st.indexrelid,st.indisunique,sample_db.datname,
      COALESCE(mtl.schemaname,st.schemaname),COALESCE(mtl.relname||'(TOAST)',st.relname), tablespaces_list.tablespacename,st.indexrelname
$$ LANGUAGE sql;

CREATE FUNCTION top_indexes_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    indexrelid          oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             text,
    indexrelname        text,

    idx_scan            bigint,
    tbl_n_tup_ins       bigint,
    tbl_n_tup_upd       bigint,
    tbl_n_tup_del       bigint,
    tbl_n_tup_hot_upd   bigint,
    idx_blks_read       bigint,
    idx_blks_fetch      bigint,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    
    growth_pretty       text,
    indexrelsize_pretty text,
    vacuum_bytes_pretty text,
    avg_indexrelsize_pretty text,
    avg_relsize_pretty  text,

    ord_growth          integer,
    ord_unused          integer,
    ord_vac             integer
  )
SET search_path=@extschema@ AS $$
    WITH rsa AS (
        SELECT
          rs.datid,
          rs.indexrelid,
          rs.growth_avail,
          sst.relsize,
          sst.relpages_bytes
        FROM
          (SELECT
            datid,
            indexrelid,
            max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
            min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
            CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
              max(sample_id) FILTER (WHERE relsize IS NOT NULL)
            ELSE
              max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
            END AS sid
          FROM
            sample_stat_indexes
          WHERE
            server_id = sserver_id AND
            sample_id BETWEEN start_id + 1 AND end_id
          GROUP BY datid, indexrelid) AS rs
          JOIN sample_stat_indexes sst ON
            (sst.server_id, sst.sample_id, sst.datid, sst.indexrelid) =
            (sserver_id, rs.sid, rs.datid, rs.indexrelid)
      )
    SELECT
      ix.datid,
      ix.relid,
      ix.indexrelid,
      ix.dbname,
      ix.tablespacename,
      ix.schemaname,
      ix.relname,
      ix.indexrelname,

      NULLIF(ix.idx_scan, 0) AS idx_scan,
      NULLIF(ix.tbl_n_tup_ins, 0) AS tbl_n_tup_ins,
      NULLIF(ix.tbl_n_tup_upd, 0) AS tbl_n_tup_upd,
      NULLIF(ix.tbl_n_tup_del, 0) AS tbl_n_tup_del,
      NULLIF(ix.tbl_n_tup_hot_upd, 0) AS tbl_n_tup_hot_upd,
      NULLIF(ix.idx_blks_read, 0) AS idx_blks_read,
      NULLIF(ix.idx_blks_fetch, 0) AS idx_blks_fetch,
      NULLIF(ix.vacuum_count, 0) AS vacuum_count,
      NULLIF(ix.autovacuum_count, 0) AS autovacuum_count,

      CASE WHEN rsa.growth_avail THEN
        pg_size_pretty(NULLIF(ix.growth, 0))
      ELSE
        '['||pg_size_pretty(NULLIF(ix.relpagegrowth_bytes, 0))||']'
      END AS growth_pretty,
      
      COALESCE(
        pg_size_pretty(NULLIF(rsa.relsize, 0)),
        '['||pg_size_pretty(NULLIF(rsa.relpages_bytes, 0))||']'
      ) AS indexrelsize_pretty,
      
      COALESCE(
        pg_size_pretty(NULLIF(ix.vacuum_bytes_relsize, 0)),
        '['||pg_size_pretty(NULLIF(ix.vacuum_bytes_relpages, 0))||']'
      ) AS vacuum_bytes_pretty,
      
      COALESCE(
        pg_size_pretty(NULLIF(ix.avg_indexrelsize, 0)),
        '['||pg_size_pretty(NULLIF(ix.avg_indexrelpages_bytes, 0))||']'
      ) AS avg_indexrelsize_pretty,

      COALESCE(
        pg_size_pretty(NULLIF(ix.avg_relsize, 0)),
        '['||pg_size_pretty(NULLIF(ix.avg_relpages_bytes, 0))||']'
      ) AS avg_relsize_pretty,

      CASE WHEN
        ((rsa.growth_avail AND ix.growth > 0) OR ix.relpagegrowth_bytes > 0)
      THEN
        row_number() OVER (ORDER BY
          CASE WHEN rsa.growth_avail THEN ix.growth ELSE ix.relpagegrowth_bytes END 
          DESC NULLS LAST,
          datid, indexrelid)::integer
      ELSE NULL END AS ord_growth,

      CASE WHEN
        COALESCE(ix.idx_scan, 0) = 0 AND NOT ix.indisunique AND
        COALESCE(ix.tbl_n_tup_ins, 0) + COALESCE(ix.tbl_n_tup_upd, 0) + COALESCE(ix.tbl_n_tup_del, 0) > 0
      THEN
        row_number() OVER (ORDER BY
          COALESCE(ix.tbl_n_tup_ins, 0) + COALESCE(ix.tbl_n_tup_upd, 0) + COALESCE(ix.tbl_n_tup_del, 0)
          DESC NULLS LAST,
          datid, indexrelid)::integer
      ELSE NULL END AS ord_unused,

      CASE WHEN
        COALESCE(ix.vacuum_count, 0) + COALESCE(ix.autovacuum_count, 0) > 0
      THEN
        row_number() OVER (ORDER BY
          COALESCE(ix.vacuum_bytes_relsize, ix.vacuum_bytes_relpages)
          DESC NULLS LAST,
          datid, indexrelid)::integer
      ELSE NULL END AS ord_vac
    FROM
      top_indexes(sserver_id, start_id, end_id) ix
      JOIN rsa USING (datid, indexrelid)
$$ LANGUAGE sql;

CREATE FUNCTION top_indexes_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid                oid,
    relid                oid,
    indexrelid           oid,
    dbname               name,
    tablespacename       name,
    schemaname           name,
    relname              text,
    indexrelname         text,

    idx_scan1            bigint,
    tbl_n_tup_ins1       bigint,
    tbl_n_tup_upd1       bigint,
    tbl_n_tup_del1       bigint,
    tbl_n_tup_hot_upd1   bigint,
    idx_blks_read1       bigint,
    idx_blks_fetch1      bigint,
    vacuum_count1        bigint,
    autovacuum_count1    bigint,
    
    growth_pretty1       text,
    indexrelsize_pretty1 text,
    vacuum_bytes_pretty1 text,
    avg_indexrelsize_pretty1 text,
    avg_relsize_pretty1  text,

    idx_scan2            bigint,
    tbl_n_tup_ins2       bigint,
    tbl_n_tup_upd2       bigint,
    tbl_n_tup_del2       bigint,
    tbl_n_tup_hot_upd2   bigint,
    idx_blks_read2       bigint,
    idx_blks_fetch2      bigint,
    vacuum_count2        bigint,
    autovacuum_count2    bigint,
    
    growth_pretty2       text,
    indexrelsize_pretty2 text,
    vacuum_bytes_pretty2 text,
    avg_indexrelsize_pretty2 text,
    avg_relsize_pretty2  text,

    ord_growth           integer,
    ord_vac              integer
  )
SET search_path=@extschema@ AS $$
    WITH rsa1 AS (
        SELECT
          rs.datid,
          rs.indexrelid,
          rs.growth_avail,
          sst.relsize,
          sst.relpages_bytes
        FROM
          (SELECT
            datid,
            indexrelid,
            max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
            min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
            CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
              max(sample_id) FILTER (WHERE relsize IS NOT NULL)
            ELSE
              max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
            END AS sid
          FROM
            sample_stat_indexes
          WHERE
            server_id = sserver_id AND
            sample_id BETWEEN start1_id + 1 AND end1_id
          GROUP BY datid, indexrelid) AS rs
          JOIN sample_stat_indexes sst ON
            (sst.server_id, sst.sample_id, sst.datid, sst.indexrelid) =
            (sserver_id, rs.sid, rs.datid, rs.indexrelid)
      ),
    rsa2 AS (
        SELECT
          rs.datid,
          rs.indexrelid,
          rs.growth_avail,
          sst.relsize,
          sst.relpages_bytes
        FROM
          (SELECT
            datid,
            indexrelid,
            max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
            min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
            CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
              max(sample_id) FILTER (WHERE relsize IS NOT NULL)
            ELSE
              max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
            END AS sid
          FROM
            sample_stat_indexes
          WHERE
            server_id = sserver_id AND
            sample_id BETWEEN start2_id + 1 AND end2_id
          GROUP BY datid, indexrelid) AS rs
          JOIN sample_stat_indexes sst ON
            (sst.server_id, sst.sample_id, sst.datid, sst.indexrelid) =
            (sserver_id, rs.sid, rs.datid, rs.indexrelid)
      )
    SELECT
      COALESCE(ix1.datid, ix2.datid),
      COALESCE(ix1.relid, ix2.relid),
      COALESCE(ix1.indexrelid, ix2.indexrelid),
      COALESCE(ix1.dbname, ix2.dbname),
      COALESCE(ix1.tablespacename, ix2.tablespacename),
      COALESCE(ix1.schemaname, ix2.schemaname),
      COALESCE(ix1.relname, ix2.relname),
      COALESCE(ix1.indexrelname, ix2.indexrelname),

      NULLIF(ix1.idx_scan, 0) AS idx_scan1,
      NULLIF(ix1.tbl_n_tup_ins, 0) AS tbl_n_tup_ins1,
      NULLIF(ix1.tbl_n_tup_upd, 0) AS tbl_n_tup_upd1,
      NULLIF(ix1.tbl_n_tup_del, 0) AS tbl_n_tup_del1,
      NULLIF(ix1.tbl_n_tup_hot_upd, 0) AS tbl_n_tup_hot_upd1,
      NULLIF(ix1.idx_blks_read, 0) AS idx_blks_read1,
      NULLIF(ix1.idx_blks_fetch, 0) AS idx_blks_fetch1,
      NULLIF(ix1.vacuum_count, 0) AS vacuum_count1,
      NULLIF(ix1.autovacuum_count, 0) AS autovacuum_count1,

      CASE WHEN rsa1.growth_avail THEN
        pg_size_pretty(NULLIF(ix1.growth, 0))
      ELSE
        '['||pg_size_pretty(NULLIF(ix1.relpagegrowth_bytes, 0))||']'
      END AS growth_pretty1,
      
      COALESCE(
        pg_size_pretty(NULLIF(rsa1.relsize, 0)),
        '['||pg_size_pretty(NULLIF(rsa1.relpages_bytes, 0))||']'
      ) AS indexrelsize_pretty1,
      
      COALESCE(
        pg_size_pretty(NULLIF(ix1.vacuum_bytes_relsize, 0)),
        '['||pg_size_pretty(NULLIF(ix1.vacuum_bytes_relpages, 0))||']'
      ) AS vacuum_bytes_pretty1,
      
      COALESCE(
        pg_size_pretty(NULLIF(ix1.avg_indexrelsize, 0)),
        '['||pg_size_pretty(NULLIF(ix1.avg_indexrelpages_bytes, 0))||']'
      ) AS avg_indexrelsize_pretty1,

      COALESCE(
        pg_size_pretty(NULLIF(ix1.avg_relsize, 0)),
        '['||pg_size_pretty(NULLIF(ix1.avg_relpages_bytes, 0))||']'
      ) AS avg_relsize_pretty1,

      NULLIF(ix2.idx_scan, 0) AS idx_scan2,
      NULLIF(ix2.tbl_n_tup_ins, 0) AS tbl_n_tup_ins2,
      NULLIF(ix2.tbl_n_tup_upd, 0) AS tbl_n_tup_upd2,
      NULLIF(ix2.tbl_n_tup_del, 0) AS tbl_n_tup_del2,
      NULLIF(ix2.tbl_n_tup_hot_upd, 0) AS tbl_n_tup_hot_upd2,
      NULLIF(ix2.idx_blks_read, 0) AS idx_blks_read2,
      NULLIF(ix2.idx_blks_fetch, 0) AS idx_blks_fetch2,
      NULLIF(ix2.vacuum_count, 0) AS vacuum_count2,
      NULLIF(ix2.autovacuum_count, 0) AS autovacuum_count2,

      CASE WHEN rsa2.growth_avail THEN
        pg_size_pretty(NULLIF(ix2.growth, 0))
      ELSE
        '['||pg_size_pretty(NULLIF(ix2.relpagegrowth_bytes, 0))||']'
      END AS growth_pretty2,
      
      COALESCE(
        pg_size_pretty(NULLIF(rsa2.relsize, 0)),
        '['||pg_size_pretty(NULLIF(rsa2.relpages_bytes, 0))||']'
      ) AS indexrelsize_pretty2,
      
      COALESCE(
        pg_size_pretty(NULLIF(ix2.vacuum_bytes_relsize, 0)),
        '['||pg_size_pretty(NULLIF(ix2.vacuum_bytes_relpages, 0))||']'
      ) AS vacuum_bytes_pretty2,
      
      COALESCE(
        pg_size_pretty(NULLIF(ix2.avg_indexrelsize, 0)),
        '['||pg_size_pretty(NULLIF(ix2.avg_indexrelpages_bytes, 0))||']'
      ) AS avg_indexrelsize_pretty2,

      COALESCE(
        pg_size_pretty(NULLIF(ix2.avg_relsize, 0)),
        '['||pg_size_pretty(NULLIF(ix2.avg_relpages_bytes, 0))||']'
      ) AS avg_relsize_pretty2,

      CASE WHEN
        ((rsa1.growth_avail AND ix1.growth > 0) OR ix1.relpagegrowth_bytes > 0) OR
        ((rsa2.growth_avail AND ix2.growth > 0) OR ix2.relpagegrowth_bytes > 0)
      THEN
        row_number() OVER (ORDER BY
          CASE WHEN rsa1.growth_avail THEN ix1.growth ELSE ix1.relpagegrowth_bytes END +
          CASE WHEN rsa2.growth_avail THEN ix2.growth ELSE ix2.relpagegrowth_bytes END
          DESC NULLS LAST,
          COALESCE(ix1.datid, ix2.datid),
          COALESCE(ix1.indexrelid, ix2.indexrelid))::integer
      ELSE NULL END AS ord_growth,

      CASE WHEN
        COALESCE(ix1.vacuum_count, 0) + COALESCE(ix1.autovacuum_count, 0) +
        COALESCE(ix2.vacuum_count, 0) + COALESCE(ix2.autovacuum_count, 0) > 0
      THEN
        row_number() OVER (ORDER BY
          COALESCE(ix1.vacuum_bytes_relsize, ix1.vacuum_bytes_relpages, 0) +
          COALESCE(ix2.vacuum_bytes_relsize, ix2.vacuum_bytes_relpages, 0)
          DESC NULLS LAST,
          COALESCE(ix1.datid, ix2.datid),
          COALESCE(ix1.indexrelid, ix2.indexrelid))::integer
      ELSE NULL END AS ord_vac
    FROM
        (top_indexes(sserver_id, start1_id, end1_id) ix1
        JOIN rsa1 USING (datid, indexrelid))
      FULL OUTER JOIN
        (top_indexes(sserver_id, start2_id, end2_id) ix2
        JOIN rsa2 USING (datid, indexrelid))
      USING (datid, indexrelid)
$$ LANGUAGE sql;
