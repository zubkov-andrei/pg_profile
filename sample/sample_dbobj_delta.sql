CREATE FUNCTION sample_dbobj_delta(IN properties jsonb, IN sserver_id integer, IN s_id integer,
  IN topn integer, IN skip_sizes boolean) RETURNS jsonb AS $$
DECLARE
    result  jsonb := sample_dbobj_delta.properties;
BEGIN

    /* This function will calculate statistics increments for database objects
    * and store top objects values in sample.
    * Due to relations between objects we need to mark top objects (and their
    * dependencies) first, and calculate increments later
    */
    result := log_sample_timings(result, 'calculate tables stats', 'start');

    -- Marking functions
    UPDATE last_stat_user_functions ulf
    SET in_sample = true
    FROM
        (SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.funcid,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.total_time - COALESCE(lst.total_time,0) DESC) time_rank,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.self_time - COALESCE(lst.self_time,0) DESC) stime_rank,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.calls - COALESCE(lst.calls,0) DESC) calls_rank
        FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
          LEFT OUTER JOIN last_stat_database dblst ON
            (dblst.server_id, dblst.datid, dblst.sample_id) =
            (sserver_id, dbcur.datid, s_id - 1)
            AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
          LEFT OUTER JOIN last_stat_user_functions lst ON
            (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
            (sserver_id, s_id - 1, dblst.datid, cur.funcid)
        WHERE
            (cur.server_id, cur.sample_id) =
            (sserver_id, s_id)
            AND cur.calls - COALESCE(lst.calls,0) > 0) diff
    WHERE
      least(
        time_rank,
        calls_rank,
        stime_rank
      ) <= topn
      AND (ulf.server_id, ulf.sample_id, ulf.datid, ulf.funcid) =
        (diff.server_id, diff.sample_id, diff.datid, diff.funcid);

    -- Marking indexes
    UPDATE last_stat_indexes uli
    SET in_sample = true
    FROM
      (SELECT
          cur.server_id,
          cur.sample_id,
          cur.datid,
          cur.indexrelid,
          -- Index ranks
          row_number() OVER (ORDER BY cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) DESC) read_rank,
          row_number() OVER (ORDER BY cur.idx_blks_read+cur.idx_blks_hit-
            COALESCE(lst.idx_blks_read+lst.idx_blks_hit,0) DESC) gets_rank,
          row_number() OVER (PARTITION BY cur.idx_scan - COALESCE(lst.idx_scan,0) = 0
            ORDER BY tblcur.n_tup_ins - COALESCE(tbllst.n_tup_ins,0) +
            tblcur.n_tup_upd - COALESCE(tbllst.n_tup_upd,0) +
            tblcur.n_tup_del - COALESCE(tbllst.n_tup_del,0) DESC) dml_unused_rank,
          row_number() OVER (ORDER BY (tblcur.vacuum_count - COALESCE(tbllst.vacuum_count,0) +
            tblcur.autovacuum_count - COALESCE(tbllst.autovacuum_count,0)) *
              -- Coalesce is used here in case of skipped size collection
              COALESCE(cur.relsize,lst.relsize) DESC) vacuum_bytes_rank
      FROM last_stat_indexes cur JOIN last_stat_tables tblcur USING (server_id, sample_id, datid, relid)
        JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
        LEFT OUTER JOIN last_stat_database dblst ON
          (dblst.server_id, dblst.datid, dblst.sample_id) =
          (sserver_id, dbcur.datid, s_id - 1)
          AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
        LEFT OUTER JOIN last_stat_indexes lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
          (sserver_id, s_id - 1, dblst.datid, cur.relid, cur.indexrelid)
        LEFT OUTER JOIN last_stat_tables tbllst ON
          (tbllst.server_id, tbllst.sample_id, tbllst.datid, tbllst.relid) =
          (sserver_id, s_id - 1, dblst.datid, lst.relid)
      WHERE
        (cur.server_id, cur.sample_id) =
        (sserver_id, s_id)
      ) diff
    WHERE
      (least(
        read_rank,
        gets_rank,
        vacuum_bytes_rank
      ) <= topn
      OR (dml_unused_rank <= topn AND idx_scan = 0))
      AND (uli.server_id, uli.sample_id, uli.datid, uli.indexrelid, uli.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, diff.indexrelid, false);

    -- Growth rank is to be calculated independently of database stats_reset value
    UPDATE last_stat_indexes uli
    SET in_sample = true
    FROM
      (SELECT
          cur.server_id,
          cur.sample_id,
          cur.datid,
          cur.indexrelid,
          cur.relsize IS NOT NULL AS relsize_avail,
          cur.relpages_bytes IS NOT NULL AS relpages_avail,
          -- Index ranks
          row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize,0) DESC NULLS LAST) growth_rank,
          row_number() OVER (ORDER BY cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) DESC NULLS LAST) pagegrowth_rank
      FROM last_stat_indexes cur
        LEFT OUTER JOIN last_stat_indexes lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
          (sserver_id, s_id - 1, cur.datid, cur.relid, cur.indexrelid)
      WHERE
        (cur.server_id, cur.sample_id) =
        (sserver_id, s_id)
      ) diff
    WHERE
      ((relsize_avail AND growth_rank <= topn) OR
      ((NOT relsize_avail) AND relpages_avail AND pagegrowth_rank <= topn))
      AND (uli.server_id, uli.sample_id, uli.datid, uli.indexrelid, uli.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, diff.indexrelid, false);

    -- Marking tables
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM (
      SELECT
          cur.server_id AS server_id,
          cur.sample_id AS sample_id,
          cur.datid AS datid,
          cur.relid AS relid,
          tcur.relid AS toastrelid,
          -- Seq. scanned blocks rank
          row_number() OVER (ORDER BY
            (cur.seq_scan - COALESCE(lst.seq_scan,0)) * (cur.relpages_bytes / 8192) +
            (tcur.seq_scan - COALESCE(tlst.seq_scan,0)) * (tcur.relpages_bytes / 8192) DESC) scan_rank,
          row_number() OVER (ORDER BY cur.n_tup_ins + cur.n_tup_upd + cur.n_tup_del -
            COALESCE(lst.n_tup_ins + lst.n_tup_upd + lst.n_tup_del, 0) +
            COALESCE(tcur.n_tup_ins + tcur.n_tup_upd + tcur.n_tup_del, 0) -
            COALESCE(tlst.n_tup_ins + tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) dml_rank,
          row_number() OVER (ORDER BY cur.n_tup_upd+cur.n_tup_del -
            COALESCE(lst.n_tup_upd + lst.n_tup_del, 0) +
            COALESCE(tcur.n_tup_upd + tcur.n_tup_del, 0) -
            COALESCE(tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) vacuum_dml_rank,
          row_number() OVER (ORDER BY
            cur.n_dead_tup / NULLIF(cur.n_live_tup+cur.n_dead_tup, 0)
            DESC NULLS LAST) dead_pct_rank,
          row_number() OVER (ORDER BY
            cur.n_mod_since_analyze / NULLIF(cur.n_live_tup, 0)
            DESC NULLS LAST) mod_pct_rank,
          -- Read rank
          row_number() OVER (ORDER BY
            cur.heap_blks_read - COALESCE(lst.heap_blks_read,0) +
            cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) +
            cur.toast_blks_read - COALESCE(lst.toast_blks_read,0) +
            cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0) DESC) read_rank,
          -- Page processing rank
          row_number() OVER (ORDER BY cur.heap_blks_read+cur.heap_blks_hit+cur.idx_blks_read+cur.idx_blks_hit+
            cur.toast_blks_read+cur.toast_blks_hit+cur.tidx_blks_read+cur.tidx_blks_hit-
            COALESCE(lst.heap_blks_read+lst.heap_blks_hit+lst.idx_blks_read+lst.idx_blks_hit+
            lst.toast_blks_read+lst.toast_blks_hit+lst.tidx_blks_read+lst.tidx_blks_hit, 0) DESC) gets_rank,
          -- Vacuum rank
          row_number() OVER (ORDER BY cur.vacuum_count - COALESCE(lst.vacuum_count, 0) +
            cur.autovacuum_count - COALESCE(lst.autovacuum_count, 0) DESC) vacuum_count_rank,
          row_number() OVER (ORDER BY cur.analyze_count - COALESCE(lst.analyze_count,0) +
            cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) DESC) analyze_count_rank,
          row_number() OVER (ORDER BY cur.total_vacuum_time - COALESCE(lst.total_vacuum_time, 0) +
            cur.total_autovacuum_time - COALESCE(lst.total_autovacuum_time, 0) DESC) vacuum_time_rank,
          row_number() OVER (ORDER BY cur.total_analyze_time - COALESCE(lst.total_analyze_time,0) +
            cur.total_autoanalyze_time - COALESCE(lst.total_autoanalyze_time,0) DESC) analyze_time_rank,

          -- Newpage updates rank (since PG16)
          CASE WHEN cur.n_tup_newpage_upd IS NOT NULL THEN
            row_number() OVER (ORDER BY cur.n_tup_newpage_upd -
              COALESCE(lst.n_tup_newpage_upd, 0) DESC)
          ELSE
            NULL
          END newpage_upd_rank
      FROM
        -- main relations diff
        last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
        LEFT OUTER JOIN last_stat_database dblst ON
          (dblst.server_id, dblst.datid, dblst.sample_id) =
          (sserver_id, dbcur.datid, s_id - 1)
          AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
        LEFT OUTER JOIN last_stat_tables lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
          (sserver_id, s_id - 1, dblst.datid, cur.relid)
        -- toast relations diff
        LEFT OUTER JOIN last_stat_tables tcur ON
          (tcur.server_id, tcur.sample_id, tcur.datid, tcur.relid) =
          (sserver_id, s_id, dbcur.datid, cur.reltoastrelid)
        LEFT OUTER JOIN last_stat_tables tlst ON
          (tlst.server_id, tlst.sample_id, tlst.datid, tlst.relid) =
          (sserver_id, s_id - 1, dblst.datid, lst.reltoastrelid)
      WHERE
        (cur.server_id, cur.sample_id, cur.in_sample) =
        (sserver_id, s_id, false)
        AND cur.relkind IN ('r','m')) diff
    WHERE
      least(
        scan_rank,
        dml_rank,
        dead_pct_rank,
        mod_pct_rank,
        vacuum_dml_rank,
        read_rank,
        gets_rank,
        vacuum_count_rank,
        analyze_count_rank,
        vacuum_time_rank,
        analyze_time_rank,
        newpage_upd_rank
      ) <= topn
      AND (ulst.server_id, ulst.sample_id, ulst.datid, ulst.in_sample) =
        (sserver_id, s_id, diff.datid, false)
      AND (ulst.relid = diff.relid OR ulst.relid = diff.toastrelid);

    -- Growth rank is to be calculated independently of database stats_reset value
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM (
      SELECT
          cur.server_id AS server_id,
          cur.sample_id AS sample_id,
          cur.datid AS datid,
          cur.relid AS relid,
          tcur.relid AS toastrelid,
          cur.relsize IS NOT NULL AS relsize_avail,
          cur.relpages_bytes IS NOT NULL AS relpages_avail,
          row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize, 0) +
            COALESCE(tcur.relsize,0) - COALESCE(tlst.relsize, 0) DESC NULLS LAST) growth_rank,
          row_number() OVER (ORDER BY cur.relpages_bytes - COALESCE(lst.relpages_bytes, 0) +
            COALESCE(tcur.relpages_bytes,0) - COALESCE(tlst.relpages_bytes, 0) DESC NULLS LAST) pagegrowth_rank
      FROM
        -- main relations diff
        last_stat_tables cur
        LEFT OUTER JOIN last_stat_tables lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
          (sserver_id, s_id - 1, cur.datid, cur.relid)
        -- toast relations diff
        LEFT OUTER JOIN last_stat_tables tcur ON
          (tcur.server_id, tcur.sample_id, tcur.datid, tcur.relid) =
          (sserver_id, s_id, cur.datid, cur.reltoastrelid)
        LEFT OUTER JOIN last_stat_tables tlst ON
          (tlst.server_id, tlst.sample_id, tlst.datid, tlst.relid) =
          (sserver_id, s_id - 1, lst.datid, lst.reltoastrelid)
      WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
        AND cur.relkind IN ('r','m')) diff
    WHERE
      ((relsize_avail AND growth_rank <= topn) OR
      ((NOT relsize_avail) AND relpages_avail AND pagegrowth_rank <= topn))
      AND (ulst.server_id, ulst.sample_id, ulst.datid, in_sample) =
        (sserver_id, s_id, diff.datid, false)
      AND (ulst.relid = diff.relid OR ulst.relid = diff.toastrelid);

    /* Also mark tables having marked indexes on them including main
    * table in case of a TOAST index and TOAST table if index is on
    * main table
    */
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM
      last_stat_indexes ix
      JOIN last_stat_tables tbl ON
        (tbl.server_id, tbl.sample_id, tbl.datid, tbl.relid) =
        (sserver_id, s_id, ix.datid, ix.relid)
      LEFT JOIN last_stat_tables mtbl ON
        (mtbl.server_id, mtbl.sample_id, mtbl.datid, mtbl.reltoastrelid) =
        (sserver_id, s_id, tbl.datid, tbl.relid)
    WHERE
      (ix.server_id, ix.sample_id, ix.in_sample) =
      (sserver_id, s_id, true)
      AND (ulst.server_id, ulst.sample_id, ulst.datid, ulst.in_sample) =
        (sserver_id, s_id, tbl.datid, false)
      AND ulst.relid IN (tbl.relid, tbl.reltoastrelid, mtbl.relid);

    -- Insert marked objects statistics increments
    -- New table names
    INSERT INTO tables_list AS itl (
      server_id,
      last_sample_id,
      datid,
      relid,
      relkind,
      schemaname,
      relname
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.relid,
      cur.relkind,
      cur.schemaname,
      cur.relname
    FROM
      last_stat_tables cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_tables_list DO
      UPDATE SET
        (last_sample_id, schemaname, relname) =
        (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.relname)
      WHERE
        (itl.last_sample_id, itl.schemaname, itl.relname) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.schemaname, EXCLUDED.relname);

    -- Tables
    INSERT INTO sample_stat_tables (
      server_id,
      sample_id,
      datid,
      relid,
      reltoastrelid,
      tablespaceid,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_tup_ins,
      n_tup_upd,
      n_tup_del,
      n_tup_hot_upd,
      n_live_tup,
      n_dead_tup,
      n_mod_since_analyze,
      n_ins_since_vacuum,
      last_vacuum,
      last_autovacuum,
      last_analyze,
      last_autoanalyze,
      vacuum_count,
      autovacuum_count,
      analyze_count,
      autoanalyze_count,
      total_vacuum_time,
      total_autovacuum_time,
      total_analyze_time,
      total_autoanalyze_time,
      heap_blks_read,
      heap_blks_hit,
      idx_blks_read,
      idx_blks_hit,
      toast_blks_read,
      toast_blks_hit,
      tidx_blks_read,
      tidx_blks_hit,
      relsize,
      relsize_diff,
      relpages_bytes,
      relpages_bytes_diff,
      last_seq_scan,
      last_idx_scan,
      n_tup_newpage_upd
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.relid AS relid,
      cur.reltoastrelid AS reltoastrelid,
      cur.tablespaceid AS tablespaceid,
      cur.seq_scan - COALESCE(lst.seq_scan,0) AS seq_scan,
      cur.seq_tup_read - COALESCE(lst.seq_tup_read,0) AS seq_tup_read,
      cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
      cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
      cur.n_tup_ins - COALESCE(lst.n_tup_ins,0) AS n_tup_ins,
      cur.n_tup_upd - COALESCE(lst.n_tup_upd,0) AS n_tup_upd,
      cur.n_tup_del - COALESCE(lst.n_tup_del,0) AS n_tup_del,
      cur.n_tup_hot_upd - COALESCE(lst.n_tup_hot_upd,0) AS n_tup_hot_upd,
      cur.n_live_tup AS n_live_tup,
      cur.n_dead_tup AS n_dead_tup,
      cur.n_mod_since_analyze AS n_mod_since_analyze,
      cur.n_ins_since_vacuum AS n_ins_since_vacuum,
      cur.last_vacuum AS last_vacuum,
      cur.last_autovacuum AS last_autovacuum,
      cur.last_analyze AS last_analyze,
      cur.last_autoanalyze AS last_autoanalyze,
      cur.vacuum_count - COALESCE(lst.vacuum_count,0) AS vacuum_count,
      cur.autovacuum_count - COALESCE(lst.autovacuum_count,0) AS autovacuum_count,
      cur.analyze_count - COALESCE(lst.analyze_count,0) AS analyze_count,
      cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) AS autoanalyze_count,
      cur.total_vacuum_time - COALESCE(lst.total_vacuum_time,0) AS total_vacuum_time,
      cur.total_autovacuum_time - COALESCE(lst.total_autovacuum_time,0) AS total_autovacuum_time,
      cur.total_analyze_time - COALESCE(lst.total_analyze_time,0) AS total_analyze_time,
      cur.total_autoanalyze_time - COALESCE(lst.total_autoanalyze_time,0) AS total_autoanalyze_time,
      cur.heap_blks_read - COALESCE(lst.heap_blks_read,0) AS heap_blks_read,
      cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0) AS heap_blks_hit,
      cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
      cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
      cur.toast_blks_read - COALESCE(lst.toast_blks_read,0) AS toast_blks_read,
      cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0) AS toast_blks_hit,
      cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0) AS tidx_blks_read,
      cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0) AS tidx_blks_hit,
      cur.relsize AS relsize,
      cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
      cur.relpages_bytes AS relpages_bytes,
      cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) AS relpages_bytes_diff,
      cur.last_seq_scan AS last_seq_scan,
      cur.last_idx_scan AS last_idx_scan,
      cur.n_tup_newpage_upd - COALESCE(lst.n_tup_newpage_upd,0) AS n_tup_newpage_upd
    FROM
      last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
    ORDER BY cur.reltoastrelid NULLS FIRST;

    -- Update incorrectly calculated relation growth in case of database stats reset
    UPDATE sample_stat_tables usst
    SET
      relsize_diff = cur.relsize - COALESCE(lst.relsize,0),
      relpages_bytes_diff = cur.relpages_bytes - COALESCE(lst.relpages_bytes,0)
    FROM
      last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
      AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
      AND (usst.server_id, usst.sample_id, usst.datid, usst.relid) =
        (sserver_id, s_id, cur.datid, cur.relid);

    -- Total table stats
    INSERT INTO sample_stat_tables_total(
      server_id,
      sample_id,
      datid,
      tablespaceid,
      relkind,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_tup_ins,
      n_tup_upd,
      n_tup_del,
      n_tup_hot_upd,
      vacuum_count,
      autovacuum_count,
      analyze_count,
      autoanalyze_count,
      total_vacuum_time,
      total_autovacuum_time,
      total_analyze_time,
      total_autoanalyze_time,
      heap_blks_read,
      heap_blks_hit,
      idx_blks_read,
      idx_blks_hit,
      toast_blks_read,
      toast_blks_hit,
      tidx_blks_read,
      tidx_blks_hit,
      relsize_diff,
      n_tup_newpage_upd
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      cur.tablespaceid,
      cur.relkind,
      sum(cur.seq_scan - COALESCE(lst.seq_scan,0)),
      sum(cur.seq_tup_read - COALESCE(lst.seq_tup_read,0)),
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.n_tup_ins - COALESCE(lst.n_tup_ins,0)),
      sum(cur.n_tup_upd - COALESCE(lst.n_tup_upd,0)),
      sum(cur.n_tup_del - COALESCE(lst.n_tup_del,0)),
      sum(cur.n_tup_hot_upd - COALESCE(lst.n_tup_hot_upd,0)),
      sum(cur.vacuum_count - COALESCE(lst.vacuum_count,0)),
      sum(cur.autovacuum_count - COALESCE(lst.autovacuum_count,0)),
      sum(cur.analyze_count - COALESCE(lst.analyze_count,0)),
      sum(cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0)),
      sum(cur.total_vacuum_time - COALESCE(lst.total_vacuum_time,0)),
      sum(cur.total_autovacuum_time - COALESCE(lst.total_autovacuum_time,0)),
      sum(cur.total_analyze_time - COALESCE(lst.total_analyze_time,0)),
      sum(cur.total_autoanalyze_time - COALESCE(lst.total_autoanalyze_time,0)),
      sum(cur.heap_blks_read - COALESCE(lst.heap_blks_read,0)),
      sum(cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      sum(cur.toast_blks_read - COALESCE(lst.toast_blks_read,0)),
      sum(cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0)),
      sum(cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0)),
      sum(cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0)),
      CASE
        WHEN skip_sizes THEN NULL
        ELSE sum(cur.relsize - COALESCE(lst.relsize,0))
      END,
      sum(cur.n_tup_newpage_upd - COALESCE(lst.n_tup_newpage_upd,0))
    FROM last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.datid, dblst.sample_id) =
        (sserver_id, dbcur.datid, s_id - 1)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid;

    IF NOT skip_sizes THEN
    /* Update incorrectly calculated aggregated tables growth in case of
     * database statistics reset
     */
      UPDATE sample_stat_tables_total usstt
      SET relsize_diff = calc.relsize_diff
      FROM (
          SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.relkind,
            cur.tablespaceid,
            sum(cur.relsize - COALESCE(lst.relsize,0)) AS relsize_diff
          FROM last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
            JOIN last_stat_database dblst ON
              (dblst.server_id, dblst.sample_id, dblst.datid) =
              (sserver_id, s_id - 1, dbcur.datid)
            LEFT OUTER JOIN last_stat_tables lst ON
              (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
              (sserver_id, s_id - 1, dblst.datid, cur.relid)
          WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
            AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
          GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid
        ) calc
      WHERE (usstt.server_id, usstt.sample_id, usstt.datid, usstt.relkind, usstt.tablespaceid) =
        (sserver_id, s_id, calc.datid, calc.relkind, calc.tablespaceid);
    END IF;

    /*
    Preserve previous relation sizes in if we couldn't collect
    size this time (for example, due to locked relation)*/
    UPDATE last_stat_tables cur
    SET relsize = lst.relsize
    FROM last_stat_tables lst
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
      AND (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
      (cur.server_id, s_id - 1, cur.datid, cur.relid)
      AND cur.relsize IS NULL;

    result := log_sample_timings(result, 'calculate tables stats', 'end');
    result := log_sample_timings(result, 'calculate indexes stats', 'start');

    -- New index names
    INSERT INTO indexes_list AS iil (
      server_id,
      last_sample_id,
      datid,
      indexrelid,
      relid,
      schemaname,
      indexrelname
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.indexrelid,
      cur.relid,
      cur.schemaname,
      cur.indexrelname
    FROM
      last_stat_indexes cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_indexes_list DO
      UPDATE SET
        (last_sample_id, relid, schemaname, indexrelname) =
        (EXCLUDED.last_sample_id, EXCLUDED.relid, EXCLUDED.schemaname, EXCLUDED.indexrelname)
      WHERE
        (iil.last_sample_id, iil.relid, iil.schemaname, iil.indexrelname) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.relid, EXCLUDED.schemaname, EXCLUDED.indexrelname);

    -- Index stats
    INSERT INTO sample_stat_indexes (
      server_id,
      sample_id,
      datid,
      indexrelid,
      tablespaceid,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch,
      idx_blks_read,
      idx_blks_hit,
      relsize,
      relsize_diff,
      indisunique,
      relpages_bytes,
      relpages_bytes_diff,
      last_idx_scan
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.indexrelid AS indexrelid,
      cur.tablespaceid AS tablespaceid,
      cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
      cur.idx_tup_read - COALESCE(lst.idx_tup_read,0) AS idx_tup_read,
      cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
      cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
      cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
      cur.relsize,
      cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
      cur.indisunique,
      cur.relpages_bytes AS relpages_bytes,
      cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) AS relpages_bytes_diff,
      cur.last_idx_scan
    FROM
      last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Update incorrectly calculated relation growth in case of database stats reset
    UPDATE sample_stat_indexes ussi
    SET
      relsize_diff = cur.relsize - COALESCE(lst.relsize,0),
      relpages_bytes_diff = cur.relpages_bytes - COALESCE(lst.relpages_bytes,0)
    FROM
      last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
      AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
      AND (ussi.server_id, ussi.sample_id, ussi.datid, ussi.indexrelid) =
        (sserver_id, s_id, cur.datid, cur.indexrelid);

    -- Total indexes stats
    INSERT INTO sample_stat_indexes_total(
      server_id,
      sample_id,
      datid,
      tablespaceid,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch,
      idx_blks_read,
      idx_blks_hit,
      relsize_diff
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      cur.tablespaceid,
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_read - COALESCE(lst.idx_tup_read,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      CASE
        WHEN skip_sizes THEN NULL
        ELSE sum(cur.relsize - COALESCE(lst.relsize,0))
      END
    FROM last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.tablespaceid;

    /* Update incorrectly calculated aggregated index growth in case of
     * database statistics reset
     */
    IF NOT skip_sizes THEN
      UPDATE sample_stat_indexes_total ussit
      SET relsize_diff = calc.relsize_diff
      FROM (
          SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.tablespaceid,
            sum(cur.relsize - COALESCE(lst.relsize,0)) AS relsize_diff
          FROM last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
            JOIN last_stat_database dblst ON
              (dblst.server_id, dblst.sample_id, dblst.datid) =
              (sserver_id, s_id - 1, dbcur.datid)
            LEFT OUTER JOIN last_stat_indexes lst ON
              (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
              (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
          WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
            AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
          GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.tablespaceid
        ) calc
      WHERE (ussit.server_id, ussit.sample_id, ussit.datid, ussit.tablespaceid) =
        (sserver_id, s_id, calc.datid, calc.tablespaceid);
    END IF;
    /*
    Preserve previous relation sizes in if we couldn't collect
    size this time (for example, due to locked relation)*/
    UPDATE last_stat_indexes cur
    SET relsize = lst.relsize
    FROM last_stat_indexes lst
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
      AND (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
      (sserver_id, s_id - 1, cur.datid, cur.indexrelid)
      AND cur.relsize IS NULL;

    result := log_sample_timings(result, 'calculate indexes stats', 'end');
    result := log_sample_timings(result, 'calculate functions stats', 'start');

    -- New function names
    INSERT INTO funcs_list AS ifl (
      server_id,
      last_sample_id,
      datid,
      funcid,
      schemaname,
      funcname,
      funcargs
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.funcid,
      cur.schemaname,
      cur.funcname,
      cur.funcargs
    FROM
      last_stat_user_functions cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_funcs_list DO
      UPDATE SET
        (last_sample_id, funcid, schemaname, funcname, funcargs) =
        (EXCLUDED.last_sample_id, EXCLUDED.funcid, EXCLUDED.schemaname,
          EXCLUDED.funcname, EXCLUDED.funcargs)
      WHERE
        (ifl.last_sample_id, ifl.funcid, ifl.schemaname,
          ifl.funcname, ifl.funcargs) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.funcid, EXCLUDED.schemaname,
          EXCLUDED.funcname, EXCLUDED.funcargs);

    -- Function stats
    INSERT INTO sample_stat_user_functions (
      server_id,
      sample_id,
      datid,
      funcid,
      calls,
      total_time,
      self_time,
      trg_fn
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.funcid,
      cur.calls - COALESCE(lst.calls,0) AS calls,
      cur.total_time - COALESCE(lst.total_time,0) AS total_time,
      cur.self_time - COALESCE(lst.self_time,0) AS self_time,
      cur.trg_fn
    FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
        (sserver_id, s_id - 1, dblst.datid, cur.funcid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Total functions stats
    INSERT INTO sample_stat_user_func_total(
      server_id,
      sample_id,
      datid,
      calls,
      total_time,
      trg_fn
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      sum(cur.calls - COALESCE(lst.calls,0)),
      sum(cur.total_time - COALESCE(lst.total_time,0)),
      cur.trg_fn
    FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
        (sserver_id, s_id - 1, dblst.datid, cur.funcid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.trg_fn;

    result := log_sample_timings(result, 'calculate functions stats', 'end');
    result := log_sample_timings(result, 'merge new extensions version', 'start');

    UPDATE extension_versions ev
    SET last_sample_id = s_id - 1
    FROM last_extension_versions prev_lev
      LEFT JOIN last_extension_versions cur_lev ON
        (cur_lev.server_id, cur_lev.datid, cur_lev.sample_id, cur_lev.extname, cur_lev.extversion) =
        (sserver_id, prev_lev.datid, s_id, prev_lev.extname, prev_lev.extversion)
    WHERE
      (prev_lev.server_id, prev_lev.sample_id) = (sserver_id, s_id - 1) AND
      (ev.server_id, ev.datid, ev.extname, ev.extversion) =
      (sserver_id, prev_lev.datid, prev_lev.extname, prev_lev.extversion) AND
      ev.last_sample_id IS NULL AND
      cur_lev.extname IS NULL;

    INSERT INTO extension_versions (
      server_id,
      datid,
      first_seen,
      extname,
      extversion
    )
    SELECT
      cur_lev.server_id,
      cur_lev.datid,
      s.sample_time as first_seen,
      cur_lev.extname,
      cur_lev.extversion
    FROM last_extension_versions cur_lev
      JOIN samples s on (s.server_id, s.sample_id) = (sserver_id, s_id)
      LEFT JOIN last_extension_versions prev_lev ON
        (prev_lev.server_id, prev_lev.datid, prev_lev.sample_id, prev_lev.extname, prev_lev.extversion) =
        (sserver_id, cur_lev.datid, s_id - 1, cur_lev.extname, cur_lev.extversion)
    WHERE
      (cur_lev.server_id, cur_lev.sample_id) = (sserver_id, s_id) AND
      prev_lev.extname IS NULL;

    result := log_sample_timings(result, 'merge new extensions version', 'end');
    result := log_sample_timings(result, 'merge new relation storage parameters', 'start');

    UPDATE table_storage_parameters tsp
    SET last_sample_id = s_id - 1
    FROM last_stat_tables prev_lst
      LEFT JOIN last_stat_tables cur_lst ON
        (cur_lst.server_id, cur_lst.datid, cur_lst.relid, cur_lst.sample_id, cur_lst.reloptions) =
        (sserver_id, prev_lst.datid, prev_lst.relid, s_id, prev_lst.reloptions)
    WHERE
      (prev_lst.server_id, prev_lst.sample_id) = (sserver_id, s_id - 1) AND
      prev_lst.reloptions IS NOT NULL AND
      (tsp.server_id, tsp.datid, tsp.relid, tsp.reloptions) =
      (sserver_id, prev_lst.datid, prev_lst.relid, prev_lst.reloptions) AND
      tsp.last_sample_id IS NULL AND
      cur_lst IS NULL;

    INSERT INTO table_storage_parameters (
      server_id,
      datid,
      relid,
      first_seen,
      reloptions
    )
    SELECT
      cur_lst.server_id,
      cur_lst.datid,
      cur_lst.relid,
      s.sample_time as first_seen,
      cur_lst.reloptions
    FROM last_stat_tables cur_lst
      JOIN samples s on (s.server_id, s.sample_id) = (sserver_id, s_id)
      LEFT JOIN last_stat_tables prev_lst ON
        (prev_lst.server_id, prev_lst.datid, prev_lst.relid, prev_lst.sample_id, prev_lst.in_sample, prev_lst.reloptions) =
        (sserver_id, cur_lst.datid, cur_lst.relid, s_id - 1, true, cur_lst.reloptions)
      LEFT JOIN table_storage_parameters tsp ON
        (tsp.server_id, tsp.datid, tsp.relid, tsp.reloptions) =
        (sserver_id, cur_lst.datid, cur_lst.relid, cur_lst.reloptions)
    WHERE
      (cur_lst.server_id, cur_lst.sample_id, cur_lst.in_sample) = (sserver_id, s_id, true) AND
      cur_lst.reloptions IS NOT NULL AND
      prev_lst IS NULL AND
      tsp IS NULL;

    UPDATE index_storage_parameters tsp
    SET last_sample_id = s_id - 1
      FROM last_stat_indexes prev_lsi
      LEFT JOIN last_stat_indexes cur_lsi ON
        (cur_lsi.server_id, cur_lsi.datid, cur_lsi.relid, cur_lsi.indexrelid, cur_lsi.sample_id, cur_lsi.reloptions) =
        (sserver_id, prev_lsi.datid, prev_lsi.relid, prev_lsi.indexrelid, s_id, prev_lsi.reloptions)
    WHERE (prev_lsi.server_id, prev_lsi.sample_id) = (sserver_id, s_id - 1)AND
      prev_lsi.reloptions IS NOT NULL AND
      (tsp.server_id, tsp.datid, tsp.relid, tsp.indexrelid, tsp.reloptions) =
      (sserver_id, prev_lsi.datid, prev_lsi.relid, prev_lsi.indexrelid, prev_lsi.reloptions) AND
      tsp.last_sample_id IS NULL AND
      cur_lsi IS NULL;

    INSERT INTO index_storage_parameters (
      server_id,
      datid,
      relid,
      indexrelid,
      first_seen,
      reloptions
    )
    SELECT
      cur_lsi.server_id,
      cur_lsi.datid,
      cur_lsi.relid,
      cur_lsi.indexrelid,
      s.sample_time as first_seen,
      cur_lsi.reloptions
    FROM last_stat_indexes cur_lsi
      JOIN samples s on (s.server_id, s.sample_id) = (sserver_id, s_id)
      LEFT JOIN last_stat_indexes prev_lsi ON
        (prev_lsi.server_id, prev_lsi.datid, prev_lsi.relid, prev_lsi.indexrelid, prev_lsi.sample_id, prev_lsi.in_sample, prev_lsi.reloptions) =
        (sserver_id, cur_lsi.datid, cur_lsi.relid, cur_lsi.indexrelid, s_id - 1, true, cur_lsi.reloptions)
      LEFT JOIN index_storage_parameters isp ON
        (isp.server_id, isp.datid, isp.relid, isp.indexrelid, isp.reloptions) =
        (sserver_id, cur_lsi.datid, cur_lsi.relid, cur_lsi.indexrelid, cur_lsi.reloptions)
    WHERE
      (cur_lsi.server_id, cur_lsi.sample_id, cur_lsi.in_sample) = (sserver_id, s_id, true) AND
      cur_lsi.reloptions IS NOT NULL AND
      prev_lsi IS NULL AND
      isp IS NULL;

    result := log_sample_timings(result, 'merge new relation storage parameters', 'end');
    result := log_sample_timings(result, 'clear last_ tables', 'start');

    -- Clear data in last_ tables, holding data only for next diff sample
    DELETE FROM last_stat_tables WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_indexes WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_user_functions WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_extension_versions WHERE server_id = sserver_id AND sample_id != s_id;

    result := log_sample_timings(result, 'clear last_ tables', 'end');

    RETURN result;
END;
$$ LANGUAGE plpgsql;
