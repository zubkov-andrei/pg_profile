/* ===== Tables stats functions ===== */

CREATE FUNCTION top_tables(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id integer,
    datid oid,
    relid oid,
    reltoastrelid oid,
    dbname name,
    tablespacename name,
    schemaname name,
    relname name,
    seq_scan bigint,
    seq_tup_read bigint,
    idx_scan bigint,
    idx_tup_fetch bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint,
    growth bigint,
    toastseq_scan bigint,
    toastseq_tup_read bigint,
    toastidx_scan bigint,
    toastidx_tup_fetch bigint,
    toastn_tup_ins bigint,
    toastn_tup_upd bigint,
    toastn_tup_del bigint,
    toastn_tup_hot_upd bigint,
    toastvacuum_count bigint,
    toastautovacuum_count bigint,
    toastanalyze_count bigint,
    toastautoanalyze_count bigint,
    toastgrowth bigint,
    relpagegrowth_bytes bigint,
    toastrelpagegrowth_bytes bigint,
    seqscan_bytes_relsize bigint,
    seqscan_bytes_relpages bigint,
    seqscan_relsize_avail boolean,
    t_seqscan_bytes_relsize bigint,
    t_seqscan_bytes_relpages bigint,
    t_seqscan_relsize_avail boolean
) SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        st.reltoastrelid,
        sample_db.datname AS dbname,
        tl.tablespacename,
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
        sum(st.vacuum_count)::bigint AS vacuum_count,
        sum(st.autovacuum_count)::bigint AS autovacuum_count,
        sum(st.analyze_count)::bigint AS analyze_count,
        sum(st.autoanalyze_count)::bigint AS autoanalyze_count,
        sum(st.relsize_diff)::bigint AS growth,
        sum(stt.seq_scan)::bigint AS toastseq_scan,
        sum(stt.seq_tup_read)::bigint AS toastseq_tup_read,
        sum(stt.idx_scan)::bigint AS toastidx_scan,
        sum(stt.idx_tup_fetch)::bigint AS toastidx_tup_fetch,
        sum(stt.n_tup_ins)::bigint AS toastn_tup_ins,
        sum(stt.n_tup_upd)::bigint AS toastn_tup_upd,
        sum(stt.n_tup_del)::bigint AS toastn_tup_del,
        sum(stt.n_tup_hot_upd)::bigint AS toastn_tup_hot_upd,
        sum(stt.vacuum_count)::bigint AS toastvacuum_count,
        sum(stt.autovacuum_count)::bigint AS toastautovacuum_count,
        sum(stt.analyze_count)::bigint AS toastanalyze_count,
        sum(stt.autoanalyze_count)::bigint AS toastautoanalyze_count,
        sum(stt.relsize_diff)::bigint AS toastgrowth,
        sum(st.relpages_bytes_diff)::bigint AS relpagegrowth_bytes,
        sum(stt.relpages_bytes_diff)::bigint AS toastrelpagegrowth_bytes,
        sum(st.seq_scan * st.relsize)::bigint AS seqscan_bytes_relsize,
        sum(st.seq_scan * st.relpages_bytes)::bigint AS seqscan_bytes_relpages,
        bool_and(COALESCE(st.seq_scan, 0) = 0 OR st.relsize IS NOT NULL) AS seqscan_relsize_avail,
        sum(stt.seq_scan * stt.relsize)::bigint AS t_seqscan_bytes_relsize,
        sum(stt.seq_scan * stt.relpages_bytes)::bigint AS t_seqscan_bytes_relpages,
        bool_and(COALESCE(stt.seq_scan, 0) = 0 OR stt.relsize IS NOT NULL) AS t_seqscan_relsize_avail
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
        JOIN tablespaces_list tl USING (server_id, tablespaceid)
        LEFT OUTER JOIN sample_stat_tables stt ON -- TOAST stats
          (st.server_id, st.sample_id, st.datid, st.reltoastrelid) =
          (stt.server_id, stt.sample_id, stt.datid, stt.relid)
    WHERE st.server_id = sserver_id AND st.relkind IN ('r','m') AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,st.reltoastrelid,sample_db.datname,tl.tablespacename,st.schemaname,st.relname
$$ LANGUAGE sql;

/* ===== Tables report functions ===== */
CREATE FUNCTION top_scan_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        reltoastrelid,
        NULLIF(seq_scan, 0) as seq_scan,
        NULLIF(best_seqscan_bytes, 0) as seq_scan_bytes,
        seqscan_relsize_avail,
        NULLIF(idx_scan, 0) as idx_scan,
        NULLIF(idx_tup_fetch, 0) as idx_tup_fetch,
        NULLIF(n_tup_ins, 0) as n_tup_ins,
        NULLIF(n_tup_upd, 0) as n_tup_upd,
        NULLIF(n_tup_del, 0) as n_tup_del,
        NULLIF(n_tup_hot_upd, 0) as n_tup_hot_upd,
        NULLIF(toastseq_scan, 0) as toastseq_scan,
        NULLIF(best_t_seqscan_bytes, 0) as toast_seq_scan_bytes,
        t_seqscan_relsize_avail,
        NULLIF(toastidx_scan, 0) as toastidx_scan,
        NULLIF(toastidx_tup_fetch, 0) as toastidx_tup_fetch,
        NULLIF(toastn_tup_ins, 0) as toastn_tup_ins,
        NULLIF(toastn_tup_upd, 0) as toastn_tup_upd,
        NULLIF(toastn_tup_del, 0) as toastn_tup_del,
        NULLIF(toastn_tup_hot_upd, 0) as toastn_tup_hot_upd
    FROM top_tables1 tt
    WHERE
      COALESCE(best_seqscan_bytes, 0) + COALESCE(best_t_seqscan_bytes, 0) > 0
    ORDER BY
      COALESCE(best_seqscan_bytes, 0) + COALESCE(best_t_seqscan_bytes, 0) DESC,
      tt.datid ASC,
      tt.relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    --- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Estimated number of bytes, fetched by sequential scans">~SeqBytes</th>'
            '<th title="Number of sequential scans initiated on this table">SeqScan</th>'
            '<th title="Number of index scans initiated on this table">IxScan</th>'
            '<th title="Number of live rows fetched by index scans">IxFet</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr {reltr}>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'rel_wtoast_tpl',
        '<tr {reltr}>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {toasttr}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);


    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        IF r_result.reltoastrelid IS NULL THEN
          report := report||format(
              jtab_tpl #>> ARRAY['rel_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              CASE WHEN r_result.seqscan_relsize_avail THEN
                  pg_size_pretty(r_result.seq_scan_bytes)
                ELSE
                  '['||pg_size_pretty(r_result.seq_scan_bytes)||']'
              END,
              r_result.seq_scan,
              r_result.idx_scan,
              r_result.idx_tup_fetch,
              r_result.n_tup_ins,
              r_result.n_tup_upd,
              r_result.n_tup_del,
              r_result.n_tup_hot_upd
          );
        ELSE
          report := report||format(
              jtab_tpl #>> ARRAY['rel_wtoast_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              CASE WHEN r_result.seqscan_relsize_avail THEN
                  pg_size_pretty(r_result.seq_scan_bytes)
                ELSE
                  '['||pg_size_pretty(r_result.seq_scan_bytes)||']'
              END,
              r_result.seq_scan,
              r_result.idx_scan,
              r_result.idx_tup_fetch,
              r_result.n_tup_ins,
              r_result.n_tup_upd,
              r_result.n_tup_del,
              r_result.n_tup_hot_upd,
              r_result.relname||'(TOAST)',
              CASE WHEN r_result.t_seqscan_relsize_avail THEN
                  pg_size_pretty(r_result.toast_seq_scan_bytes)
                ELSE
                  '['||pg_size_pretty(r_result.toast_seq_scan_bytes)||']'
              END,
              r_result.toastseq_scan,
              r_result.toastidx_scan,
              r_result.toastidx_tup_fetch,
              r_result.toastn_tup_ins,
              r_result.toastn_tup_upd,
              r_result.toastn_tup_del,
              r_result.toastn_tup_hot_upd
          );
        END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_scan_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) AS dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) AS schemaname,
        COALESCE(tbl1.relname,tbl2.relname) AS relname,
        NULLIF(tbl1.seq_scan, 0) AS seq_scan1,
        tbl1_seq_scan.approximated AS seq_scan_bytes_approximated1,
        NULLIF(tbl1_seq_scan.seq_scan_bytes, 0) AS seq_scan_bytes1,
        NULLIF(tbl1.idx_scan, 0) AS idx_scan1,
        NULLIF(tbl1.idx_tup_fetch, 0) AS idx_tup_fetch1,
        NULLIF(tbl1.toastseq_scan, 0) AS toastseq_scan1,
        toast1_seq_scan.approximated AS toastseq_scan_bytes_approximated1,
        NULLIF(toast1_seq_scan.seq_scan_bytes, 0) AS toastseq_scan_bytes1,
        NULLIF(tbl1.toastidx_scan, 0) AS toastidx_scan1,
        NULLIF(tbl1.toastidx_tup_fetch, 0) AS toastidx_tup_fetch1,
        NULLIF(tbl2.seq_scan, 0) AS seq_scan2,
        tbl2_seq_scan.approximated AS seq_scan_bytes_approximated2,
        NULLIF(tbl2_seq_scan.seq_scan_bytes, 0) AS seq_scan_bytes2,
        NULLIF(tbl2.idx_scan, 0) AS idx_scan2,
        NULLIF(tbl2.idx_tup_fetch, 0) AS idx_tup_fetch2,
        NULLIF(tbl2.toastseq_scan, 0) AS toastseq_scan2,
        toast2_seq_scan.approximated AS toastseq_scan_bytes_approximated2,
        NULLIF(toast2_seq_scan.seq_scan_bytes, 0) AS toastseq_scan_bytes2,
        NULLIF(tbl2.toastidx_scan, 0) AS toastidx_scan2,
        NULLIF(tbl2.toastidx_tup_fetch, 0) AS toastidx_tup_fetch2,
        row_number() over (ORDER BY
          COALESCE(tbl1_seq_scan.seq_scan_bytes, 0) + COALESCE(toast1_seq_scan.seq_scan_bytes, 0)
          DESC NULLS LAST) AS rn_seqpg1,
        row_number() over (ORDER BY
          COALESCE(tbl2_seq_scan.seq_scan_bytes, 0) + COALESCE(toast2_seq_scan.seq_scan_bytes, 0)
          DESC NULLS LAST) AS rn_seqpg2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id, datid, relid)
    LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        relid,
        round(sum(seq_scan * relsize))::bigint as seq_scan_bytes,
        count(nullif(relsize, 0)) != count(nullif(seq_scan, 0)) as approximated
      FROM sample_stat_tables
      WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
      GROUP BY
        server_id,
        datid,
        relid
    ) tbl1_seq_scan ON (tbl1.server_id,tbl1.datid,tbl1.relid) =
      (tbl1_seq_scan.server_id,tbl1_seq_scan.datid,tbl1_seq_scan.relid)
    LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        relid,
        round(sum(seq_scan * relsize))::bigint as seq_scan_bytes,
        count(nullif(relsize, 0)) != count(nullif(seq_scan, 0)) as approximated
      FROM sample_stat_tables
      WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
      GROUP BY
        server_id,
        datid,
        relid
    ) toast1_seq_scan ON (tbl1.server_id,tbl1.datid,tbl1.reltoastrelid) =
      (toast1_seq_scan.server_id,toast1_seq_scan.datid,toast1_seq_scan.relid)
    LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        relid,
        round(sum(seq_scan * relsize))::bigint as seq_scan_bytes,
        count(nullif(relsize, 0)) != count(nullif(seq_scan, 0)) as approximated
      FROM sample_stat_tables
      WHERE server_id = sserver_id AND sample_id BETWEEN start2_id + 1 AND end2_id
      GROUP BY
        server_id,
        datid,
        relid
    ) tbl2_seq_scan ON (tbl2.server_id,tbl2.datid,tbl2.relid) =
      (tbl2_seq_scan.server_id,tbl2_seq_scan.datid,tbl2_seq_scan.relid)
    LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        relid,
        round(sum(seq_scan * relsize))::bigint as seq_scan_bytes,
        count(nullif(relsize, 0)) != count(nullif(seq_scan, 0)) as approximated
      FROM sample_stat_tables
      WHERE server_id = sserver_id AND sample_id BETWEEN start2_id + 1 AND end2_id
      GROUP BY
        server_id,
        datid,
        relid
    ) toast2_seq_scan ON (tbl2.server_id,tbl2.datid,tbl2.reltoastrelid) =
      (toast2_seq_scan.server_id,toast2_seq_scan.datid,toast2_seq_scan.relid)
    WHERE COALESCE(tbl1_seq_scan.seq_scan_bytes, 0) +
      COALESCE(toast1_seq_scan.seq_scan_bytes, 0) +
      COALESCE(tbl2_seq_scan.seq_scan_bytes, 0) +
      COALESCE(toast2_seq_scan.seq_scan_bytes, 0) > 0
    ORDER BY
      COALESCE(tbl1_seq_scan.seq_scan_bytes, 0) +
      COALESCE(toast1_seq_scan.seq_scan_bytes, 0) +
      COALESCE(tbl2_seq_scan.seq_scan_bytes, 0) +
      COALESCE(toast2_seq_scan.seq_scan_bytes, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_seqpg1,
        rn_seqpg2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="4">Table</th>'
            '<th colspan="4">TOAST</th>'
          '</tr>'
          '<tr>'
            '<th title="Estimated number of blocks, fetched by sequential scans">~SeqBytes</th>'
            '<th title="Number of sequential scans initiated on this table">SeqScan</th>'
            '<th title="Number of index scans initiated on this table">IxScan</th>'
            '<th title="Number of live rows fetched by index scans">IxFet</th>'
            '<th title="Estimated number of blocks, fetched by sequential scans">~SeqBytes</th>'
            '<th title="Number of sequential scans initiated on this table">SeqScan</th>'
            '<th title="Number of index scans initiated on this table">IxScan</th>'
            '<th title="Number of live rows fetched by index scans">IxFet</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,start2_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            CASE WHEN r_result.seq_scan_bytes_approximated1 THEN '~'
              ELSE ''
            END||pg_size_pretty(r_result.seq_scan_bytes1),
            r_result.seq_scan1,
            r_result.idx_scan1,
            r_result.idx_tup_fetch1,
            CASE WHEN r_result.toastseq_scan_bytes_approximated1 THEN '~'
              ELSE ''
            END||pg_size_pretty(r_result.toastseq_scan_bytes1),
            r_result.toastseq_scan1,
            r_result.toastidx_scan1,
            r_result.toastidx_tup_fetch1,
            CASE WHEN r_result.seq_scan_bytes_approximated2 THEN '~'
              ELSE ''
            END||pg_size_pretty(r_result.seq_scan_bytes2),
            r_result.seq_scan2,
            r_result.idx_scan2,
            r_result.idx_tup_fetch2,
            CASE WHEN r_result.toastseq_scan_bytes_approximated2 THEN '~'
              ELSE ''
            END||pg_size_pretty(r_result.toastseq_scan_bytes2),
            r_result.toastseq_scan2,
            r_result.toastidx_scan2,
            r_result.toastidx_tup_fetch2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_dml_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        reltoastrelid,
        NULLIF(seq_scan, 0) as seq_scan,
        NULLIF(seq_tup_read, 0) as seq_tup_read,
        NULLIF(idx_scan, 0) as idx_scan,
        NULLIF(idx_tup_fetch, 0) as idx_tup_fetch,
        NULLIF(n_tup_ins, 0) as n_tup_ins,
        NULLIF(n_tup_upd, 0) as n_tup_upd,
        NULLIF(n_tup_del, 0) as n_tup_del,
        NULLIF(n_tup_hot_upd, 0) as n_tup_hot_upd,
        NULLIF(toastseq_scan, 0) as toastseq_scan,
        NULLIF(toastseq_tup_read, 0) as toastseq_tup_read,
        NULLIF(toastidx_scan, 0) as toastidx_scan,
        NULLIF(toastidx_tup_fetch, 0) as toastidx_tup_fetch,
        NULLIF(toastn_tup_ins, 0) as toastn_tup_ins,
        NULLIF(toastn_tup_upd, 0) as toastn_tup_upd,
        NULLIF(toastn_tup_del, 0) as toastn_tup_del,
        NULLIF(toastn_tup_hot_upd, 0) as toastn_tup_hot_upd
    FROM top_tables1
    WHERE COALESCE(n_tup_ins, 0) + COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) +
      COALESCE(toastn_tup_ins, 0) + COALESCE(toastn_tup_upd, 0) + COALESCE(toastn_tup_del, 0) > 0
    ORDER BY COALESCE(n_tup_ins, 0) + COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) +
      COALESCE(toastn_tup_ins, 0) + COALESCE(toastn_tup_upd, 0) + COALESCE(toastn_tup_del, 0) DESC,
      datid ASC,
      relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Number of sequential scans initiated on this table">SeqScan</th>'
            '<th title="Number of live rows fetched by sequential scans">SeqFet</th>'
            '<th title="Number of index scans initiated on this table">IxScan</th>'
            '<th title="Number of live rows fetched by index scans">IxFet</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr {reltr}>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'rel_wtoast_tpl',
        '<tr {reltr}>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {toasttr}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        IF r_result.reltoastrelid IS NULL THEN
          report := report||format(
              jtab_tpl #>> ARRAY['rel_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              r_result.n_tup_ins,
              r_result.n_tup_upd,
              r_result.n_tup_del,
              r_result.n_tup_hot_upd,
              r_result.seq_scan,
              r_result.seq_tup_read,
              r_result.idx_scan,
              r_result.idx_tup_fetch
          );
        ELSE
          report := report||format(
              jtab_tpl #>> ARRAY['rel_wtoast_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              r_result.n_tup_ins,
              r_result.n_tup_upd,
              r_result.n_tup_del,
              r_result.n_tup_hot_upd,
              r_result.seq_scan,
              r_result.seq_tup_read,
              r_result.idx_scan,
              r_result.idx_tup_fetch,
              r_result.relname||'(TOAST)',
              r_result.toastn_tup_ins,
              r_result.toastn_tup_upd,
              r_result.toastn_tup_del,
              r_result.toastn_tup_hot_upd,
              r_result.toastseq_scan,
              r_result.toastseq_tup_read,
              r_result.toastidx_scan,
              r_result.toastidx_tup_fetch
          );
        END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_dml_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) AS dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) AS schemaname,
        COALESCE(tbl1.relname,tbl2.relname) AS relname,
        NULLIF(tbl1.n_tup_ins, 0) AS n_tup_ins1,
        NULLIF(tbl1.n_tup_upd, 0) AS n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) AS n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) AS n_tup_hot_upd1,
        NULLIF(tbl1.toastn_tup_ins, 0) AS toastn_tup_ins1,
        NULLIF(tbl1.toastn_tup_upd, 0) AS toastn_tup_upd1,
        NULLIF(tbl1.toastn_tup_del, 0) AS toastn_tup_del1,
        NULLIF(tbl1.toastn_tup_hot_upd, 0) AS toastn_tup_hot_upd1,
        NULLIF(tbl2.n_tup_ins, 0) AS n_tup_ins2,
        NULLIF(tbl2.n_tup_upd, 0) AS n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) AS n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) AS n_tup_hot_upd2,
        NULLIF(tbl2.toastn_tup_ins, 0) AS toastn_tup_ins2,
        NULLIF(tbl2.toastn_tup_upd, 0) AS toastn_tup_upd2,
        NULLIF(tbl2.toastn_tup_del, 0) AS toastn_tup_del2,
        NULLIF(tbl2.toastn_tup_hot_upd, 0) AS toastn_tup_hot_upd2,
        row_number() OVER (ORDER BY COALESCE(tbl1.n_tup_ins, 0) + COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
          COALESCE(tbl1.toastn_tup_ins, 0) + COALESCE(tbl1.toastn_tup_upd, 0) + COALESCE(tbl1.toastn_tup_del, 0) DESC NULLS LAST) AS rn_dml1,
        row_number() OVER (ORDER BY COALESCE(tbl2.n_tup_ins, 0) + COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) +
          COALESCE(tbl2.toastn_tup_ins, 0) + COALESCE(tbl2.toastn_tup_upd, 0) + COALESCE(tbl2.toastn_tup_del, 0) DESC NULLS LAST) AS rn_dml2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id, datid, relid)
    WHERE COALESCE(tbl1.n_tup_ins, 0) + COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
        COALESCE(tbl1.toastn_tup_ins, 0) + COALESCE(tbl1.toastn_tup_upd, 0) + COALESCE(tbl1.toastn_tup_del, 0) +
        COALESCE(tbl2.n_tup_ins, 0) + COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) +
        COALESCE(tbl2.toastn_tup_ins, 0) + COALESCE(tbl2.toastn_tup_upd, 0) + COALESCE(tbl2.toastn_tup_del, 0) > 0
    ORDER BY COALESCE(tbl1.n_tup_ins, 0) + COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
          COALESCE(tbl1.toastn_tup_ins, 0) + COALESCE(tbl1.toastn_tup_upd, 0) + COALESCE(tbl1.toastn_tup_del, 0) +
          COALESCE(tbl2.n_tup_ins, 0) + COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) +
          COALESCE(tbl2.toastn_tup_ins, 0) + COALESCE(tbl2.toastn_tup_upd, 0) + COALESCE(tbl2.toastn_tup_del, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_dml1,
        rn_dml2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="4">Table</th>'
            '<th colspan="4">TOAST</th>'
          '</tr>'
          '<tr>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.toastn_tup_ins1,
            r_result.toastn_tup_upd1,
            r_result.toastn_tup_del1,
            r_result.toastn_tup_hot_upd1,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2,
            r_result.toastn_tup_ins2,
            r_result.toastn_tup_upd2,
            r_result.toastn_tup_del2,
            r_result.toastn_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_upd_vac_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        tablespacename,
        schemaname,
        relname,
        reltoastrelid,
        NULLIF(n_tup_upd, 0) as n_tup_upd,
        NULLIF(n_tup_del, 0) as n_tup_del,
        NULLIF(n_tup_hot_upd, 0) as n_tup_hot_upd,
        NULLIF(vacuum_count, 0) as vacuum_count,
        NULLIF(autovacuum_count, 0) as autovacuum_count,
        NULLIF(analyze_count, 0) as analyze_count,
        NULLIF(autoanalyze_count, 0) as autoanalyze_count,
        NULLIF(toastn_tup_upd, 0) as toastn_tup_upd,
        NULLIF(toastn_tup_del, 0) as toastn_tup_del,
        NULLIF(toastn_tup_hot_upd, 0) as toastn_tup_hot_upd,
        NULLIF(toastvacuum_count, 0) as toastvacuum_count,
        NULLIF(toastautovacuum_count, 0) as toastautovacuum_count,
        NULLIF(toastanalyze_count, 0) as toastanalyze_count,
        NULLIF(toastautoanalyze_count, 0) as toastautoanalyze_count
    FROM top_tables1
    WHERE COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) +
      COALESCE(toastn_tup_upd, 0) + COALESCE(toastn_tup_del, 0) > 0
    ORDER BY COALESCE(n_tup_upd, 0) + COALESCE(n_tup_del, 0) +
      COALESCE(toastn_tup_upd, 0) + COALESCE(toastn_tup_del, 0) DESC,
      datid ASC,
      relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of times this table has been manually vacuumed (not counting VACUUM FULL)">Vacuum</th>'
            '<th title="Number of times this table has been vacuumed by the autovacuum daemon">AutoVacuum</th>'
            '<th title="Number of times this table has been manually analyzed">Analyze</th>'
            '<th title="Number of times this table has been analyzed by the autovacuum daemon">AutoAnalyze</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr {reltr}>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'rel_wtoast_tpl',
        '<tr {reltr}>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {toasttr}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        IF r_result.reltoastrelid IS NULL THEN
          report := report||format(
              jtab_tpl #>> ARRAY['rel_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              r_result.n_tup_upd,
              r_result.n_tup_hot_upd,
              r_result.n_tup_del,
              r_result.vacuum_count,
              r_result.autovacuum_count,
              r_result.analyze_count,
              r_result.autoanalyze_count
          );
        ELSE
          report := report||format(
              jtab_tpl #>> ARRAY['rel_wtoast_tpl'],
              r_result.dbname,
              r_result.tablespacename,
              r_result.schemaname,
              r_result.relname,
              r_result.n_tup_upd,
              r_result.n_tup_hot_upd,
              r_result.n_tup_del,
              r_result.vacuum_count,
              r_result.autovacuum_count,
              r_result.analyze_count,
              r_result.autoanalyze_count,
              r_result.relname||'(TOAST)',
              r_result.toastn_tup_upd,
              r_result.toastn_tup_hot_upd,
              r_result.toastn_tup_del,
              r_result.toastvacuum_count,
              r_result.toastautovacuum_count,
              r_result.toastanalyze_count,
              r_result.toastautoanalyze_count
          );
        END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_upd_vac_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        NULLIF(tbl1.n_tup_upd, 0) as n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) as n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) as n_tup_hot_upd1,
        NULLIF(tbl1.vacuum_count, 0) as vacuum_count1,
        NULLIF(tbl1.autovacuum_count, 0) as autovacuum_count1,
        NULLIF(tbl1.analyze_count, 0) as analyze_count1,
        NULLIF(tbl1.autoanalyze_count, 0) as autoanalyze_count1,
        NULLIF(tbl2.n_tup_upd, 0) as n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) as n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) as n_tup_hot_upd2,
        NULLIF(tbl2.vacuum_count, 0) as vacuum_count2,
        NULLIF(tbl2.autovacuum_count, 0) as autovacuum_count2,
        NULLIF(tbl2.analyze_count, 0) as analyze_count2,
        NULLIF(tbl2.autoanalyze_count, 0) as autoanalyze_count2,
        row_number() OVER (ORDER BY COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) DESC NULLS LAST) as rn_vactpl1,
        row_number() OVER (ORDER BY COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) DESC NULLS LAST) as rn_vactpl2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id, datid, relid)
    WHERE COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
          COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) > 0
    ORDER BY COALESCE(tbl1.n_tup_upd, 0) + COALESCE(tbl1.n_tup_del, 0) +
          COALESCE(tbl2.n_tup_upd, 0) + COALESCE(tbl2.n_tup_del, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_vactpl1,
        rn_vactpl2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>I</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of times this table has been manually vacuumed (not counting VACUUM FULL)">Vacuum</th>'
            '<th title="Number of times this table has been vacuumed by the autovacuum daemon">AutoVacuum</th>'
            '<th title="Number of times this table has been manually analyzed">Analyze</th>'
            '<th title="Number of times this table has been analyzed by the autovacuum daemon">AutoAnalyze</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.n_tup_upd1,
            r_result.n_tup_hot_upd1,
            r_result.n_tup_del1,
            r_result.vacuum_count1,
            r_result.autovacuum_count1,
            r_result.analyze_count1,
            r_result.autoanalyze_count1,
            r_result.n_tup_upd2,
            r_result.n_tup_hot_upd2,
            r_result.n_tup_del2,
            r_result.vacuum_count2,
            r_result.autovacuum_count2,
            r_result.analyze_count2,
            r_result.autoanalyze_count2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_growth_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(end1_id integer, topn integer) FOR
    SELECT
        dbname,
        top.tablespacename,
        top.schemaname,
        top.relname,
        top.reltoastrelid,
        NULLIF(top.n_tup_ins, 0) as n_tup_ins,
        NULLIF(top.n_tup_upd, 0) as n_tup_upd,
        NULLIF(top.n_tup_del, 0) as n_tup_del,
        NULLIF(top.n_tup_hot_upd, 0) as n_tup_hot_upd,
        NULLIF(top.best_growth, 0) AS growth,
        NULLIF(st_last.relsize, 0) AS relsize,
        NULLIF(st_last.relpages_bytes, 0) AS relpages_bytes,
        NULLIF(top.toastn_tup_ins, 0) as toastn_tup_ins,
        NULLIF(top.toastn_tup_upd, 0) as toastn_tup_upd,
        NULLIF(top.toastn_tup_del, 0) as toastn_tup_del,
        NULLIF(top.toastn_tup_hot_upd, 0) as toastn_tup_hot_upd,
        NULLIF(top.best_toastgrowth, 0) AS toastgrowth,
        NULLIF(stt_last.relsize, 0) AS toastrelsize,
        NULLIF(stt_last.relpages_bytes, 0) AS toastrelpages_bytes,
        top.relsize_growth_avail,
        top.relsize_toastgrowth_avail
    FROM top_tables1 top
        JOIN sample_stat_tables st_last
          USING (server_id, datid, relid)
        LEFT OUTER JOIN sample_stat_tables stt_last ON
          (stt_last.server_id, stt_last.datid, stt_last.relid, stt_last.sample_id) =
          (top.server_id, top.datid, top.reltoastrelid, end1_id)
    WHERE st_last.sample_id = end1_id AND
      COALESCE(top.best_growth,0) + COALESCE(top.best_toastgrowth,0) > 0
    ORDER BY
      COALESCE(top.best_growth,0) + COALESCE(top.best_toastgrowth,0) DESC,
      top.datid ASC,
      top.relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Table size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Table size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr {reltr}>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td {reltdhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'rel_wtoast_tpl',
        '<tr {reltr}>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td {reltdspanhdr}>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {toasttr}>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
      IF r_result.reltoastrelid IS NULL THEN
        report := report||format(
            jtab_tpl #>> ARRAY['rel_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            COALESCE(
              pg_size_pretty(r_result.relsize),
              '['||pg_size_pretty(r_result.relpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail
              THEN pg_size_pretty(r_result.growth)
              ELSE '['||pg_size_pretty(r_result.growth)||']'
            END,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd
        );
      ELSE
        report := report||format(
            jtab_tpl #>> ARRAY['rel_wtoast_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            COALESCE(
              pg_size_pretty(r_result.relsize),
              '['||pg_size_pretty(r_result.relpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail
              THEN pg_size_pretty(r_result.growth)
              ELSE '['||pg_size_pretty(r_result.growth)||']'
            END,
            r_result.n_tup_ins,
            r_result.n_tup_upd,
            r_result.n_tup_del,
            r_result.n_tup_hot_upd,
            r_result.relname||'(TOAST)',
            COALESCE(
              pg_size_pretty(r_result.toastrelsize),
              '['||pg_size_pretty(r_result.toastrelpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_toastgrowth_avail
              THEN pg_size_pretty(r_result.toastgrowth)
              ELSE '['||pg_size_pretty(r_result.toastgrowth)||']'
            END,
            r_result.toastn_tup_ins,
            r_result.toastn_tup_upd,
            r_result.toastn_tup_del,
            r_result.toastn_tup_hot_upd
        );
      END IF;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_growth_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(end1_id integer,
      end2_id integer, topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        NULLIF(tbl1.n_tup_ins, 0) as n_tup_ins1,
        NULLIF(tbl1.n_tup_upd, 0) as n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) as n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) as n_tup_hot_upd1,
        NULLIF(tbl1.best_growth, 0) as growth1,
        NULLIF(st_last1.relsize, 0) as relsize1,
        NULLIF(st_last1.relpages_bytes, 0) AS relpages_bytes1,
        NULLIF(tbl1.toastn_tup_ins, 0) as toastn_tup_ins1,
        NULLIF(tbl1.toastn_tup_upd, 0) as toastn_tup_upd1,
        NULLIF(tbl1.toastn_tup_del, 0) as toastn_tup_del1,
        NULLIF(tbl1.toastn_tup_hot_upd, 0) as toastn_tup_hot_upd1,
        NULLIF(tbl1.best_toastgrowth, 0) AS toastgrowth1,
        NULLIF(stt_last1.relsize, 0) AS toastrelsize1,
        NULLIF(stt_last1.relpages_bytes, 0) AS toastrelpages_bytes1,
        NULLIF(tbl2.n_tup_ins, 0) as n_tup_ins2,
        NULLIF(tbl2.n_tup_upd, 0) as n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) as n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) as n_tup_hot_upd2,
        NULLIF(tbl2.best_growth, 0) as growth2,
        NULLIF(st_last2.relsize, 0) as relsize2,
        NULLIF(st_last2.relpages_bytes, 0) AS relpages_bytes2,
        NULLIF(tbl2.toastn_tup_ins, 0) as toastn_tup_ins2,
        NULLIF(tbl2.toastn_tup_upd, 0) as toastn_tup_upd2,
        NULLIF(tbl2.toastn_tup_del, 0) as toastn_tup_del2,
        NULLIF(tbl2.toastn_tup_hot_upd, 0) as toastn_tup_hot_upd2,
        NULLIF(tbl2.best_toastgrowth, 0) AS toastgrowth2,
        NULLIF(stt_last2.relsize, 0) AS toastrelsize2,
        NULLIF(stt_last2.relpages_bytes, 0) AS toastrelpages_bytes2,
        tbl1.relsize_growth_avail as relsize_growth_avail1,
        tbl1.relsize_toastgrowth_avail as relsize_toastgrowth_avail1,
        tbl2.relsize_growth_avail as relsize_growth_avail2,
        tbl2.relsize_toastgrowth_avail as relsize_toastgrowth_avail2,
        row_number() OVER (ORDER BY COALESCE(tbl1.best_growth, 0) + COALESCE(tbl1.best_toastgrowth, 0) DESC NULLS LAST) as rn_growth1,
        row_number() OVER (ORDER BY COALESCE(tbl2.best_growth, 0) + COALESCE(tbl2.best_toastgrowth, 0) DESC NULLS LAST) as rn_growth2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id,datid,relid)
        LEFT OUTER JOIN sample_stat_tables st_last1 ON
          (st_last1.server_id, st_last1.datid, st_last1.relid, st_last1.sample_id) =
          (tbl1.server_id, tbl1.datid, tbl1.relid, end1_id)
        LEFT OUTER JOIN sample_stat_tables st_last2 ON
          (st_last2.server_id, st_last2.datid, st_last2.relid, st_last2.sample_id) =
          (tbl2.server_id, tbl2.datid, tbl2.relid, end2_id)
        -- join toast tables last sample stats (to get relsize)
        LEFT OUTER JOIN sample_stat_tables stt_last1 ON
          (stt_last1.server_id, stt_last1.datid, stt_last1.relid, stt_last1.sample_id) =
          (st_last1.server_id, st_last1.datid, tbl1.reltoastrelid, st_last1.sample_id)
        LEFT OUTER JOIN sample_stat_tables stt_last2 ON
          (stt_last2.server_id, stt_last2.datid, stt_last2.relid, stt_last2.sample_id) =
          (st_last2.server_id, st_last2.datid, tbl2.reltoastrelid, st_last2.sample_id)
    WHERE COALESCE(tbl1.best_growth, 0) + COALESCE(tbl1.best_toastgrowth, 0) +
      COALESCE(tbl2.best_growth, 0) + COALESCE(tbl2.best_toastgrowth, 0) > 0
    ORDER BY COALESCE(tbl1.best_growth, 0) + COALESCE(tbl1.best_toastgrowth, 0) +
      COALESCE(tbl2.best_growth, 0) + COALESCE(tbl2.best_toastgrowth, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_growth1,
        rn_growth2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="6">Table</th>'
            '<th colspan="6">TOAST</th>'
          '</tr>'
          '<tr>'
            '<th title="Table size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Table size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
            '<th title="Table size, as it was at the moment of last sample in report interval (TOAST)">Size</th>'
            '<th title="Table size increment during report interval (TOAST)">Growth</th>'
            '<th title="Number of rows inserted (TOAST)">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows) (TOAST)">Upd</th>'
            '<th title="Number of rows deleted (TOAST)">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required) (TOAST)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,end2_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            COALESCE(
              pg_size_pretty(r_result.relsize1),
              '['||pg_size_pretty(r_result.relpages_bytes1)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail1
              THEN pg_size_pretty(r_result.growth1)
              ELSE '['||pg_size_pretty(r_result.growth1)||']'
            END,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            COALESCE(
              pg_size_pretty(r_result.toastrelsize1),
              '['||pg_size_pretty(r_result.toastrelpages_bytes1)||']'
            ),
            CASE WHEN r_result.relsize_toastgrowth_avail1
              THEN pg_size_pretty(r_result.toastgrowth1)
              ELSE '['||pg_size_pretty(r_result.toastgrowth1)||']'
            END,
            r_result.toastn_tup_ins1,
            r_result.toastn_tup_upd1,
            r_result.toastn_tup_del1,
            r_result.toastn_tup_hot_upd1,
            COALESCE(
              pg_size_pretty(r_result.relsize2),
              '['||pg_size_pretty(r_result.relpages_bytes2)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail2
              THEN pg_size_pretty(r_result.growth2)
              ELSE '['||pg_size_pretty(r_result.growth2)||']'
            END,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2,
            COALESCE(
              pg_size_pretty(r_result.toastrelsize2),
              '['||pg_size_pretty(r_result.toastrelpages_bytes2)||']'
            ),
            CASE WHEN r_result.relsize_toastgrowth_avail2
              THEN pg_size_pretty(r_result.toastgrowth2)
              ELSE '['||pg_size_pretty(r_result.toastgrowth2)||']'
            END,
            r_result.toastn_tup_ins2,
            r_result.toastn_tup_upd2,
            r_result.toastn_tup_del2,
            r_result.toastn_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_vacuumed_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        top.tablespacename,
        top.schemaname,
        top.relname,
        NULLIF(top.vacuum_count, 0) as vacuum_count,
        NULLIF(top.autovacuum_count, 0) as autovacuum_count,
        NULLIF(top.n_tup_ins, 0) as n_tup_ins,
        NULLIF(top.n_tup_upd, 0) as n_tup_upd,
        NULLIF(top.n_tup_del, 0) as n_tup_del,
        NULLIF(top.n_tup_hot_upd, 0) as n_tup_hot_upd
    FROM top_tables1 top
    WHERE COALESCE(top.vacuum_count, 0) + COALESCE(top.autovacuum_count, 0) > 0
    ORDER BY COALESCE(top.vacuum_count, 0) + COALESCE(top.autovacuum_count, 0) DESC,
      top.datid ASC,
      top.relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Number of times this table has been manually vacuumed (not counting VACUUM FULL)">Vacuum count</th>'
            '<th title="Number of times this table has been vacuumed by the autovacuum daemon">Autovacuum count</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
    );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
      report := report||format(
          jtab_tpl #>> ARRAY['rel_tpl'],
          r_result.dbname,
          r_result.tablespacename,
          r_result.schemaname,
          r_result.relname,
          r_result.vacuum_count,
          r_result.autovacuum_count,
          r_result.n_tup_ins,
          r_result.n_tup_upd,
          r_result.n_tup_del,
          r_result.n_tup_hot_upd
      );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_vacuumed_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        NULLIF(tbl1.vacuum_count, 0) as vacuum_count1,
        NULLIF(tbl1.autovacuum_count, 0) as autovacuum_count1,
        NULLIF(tbl1.n_tup_ins, 0) as n_tup_ins1,
        NULLIF(tbl1.n_tup_upd, 0) as n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) as n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) as n_tup_hot_upd1,
        NULLIF(tbl2.vacuum_count, 0) as vacuum_count2,
        NULLIF(tbl2.autovacuum_count, 0) as autovacuum_count2,
        NULLIF(tbl2.n_tup_ins, 0) as n_tup_ins2,
        NULLIF(tbl2.n_tup_upd, 0) as n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) as n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) as n_tup_hot_upd2,
        row_number() OVER (ORDER BY COALESCE(tbl1.vacuum_count, 0) + COALESCE(tbl1.autovacuum_count, 0) DESC) as rn_vacuum1,
        row_number() OVER (ORDER BY COALESCE(tbl2.vacuum_count, 0) + COALESCE(tbl2.autovacuum_count, 0) DESC) as rn_vacuum2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id,datid,relid)
    WHERE COALESCE(tbl1.vacuum_count, 0) + COALESCE(tbl1.autovacuum_count, 0) +
          COALESCE(tbl2.vacuum_count, 0) + COALESCE(tbl2.autovacuum_count, 0) > 0
    ORDER BY COALESCE(tbl1.vacuum_count, 0) + COALESCE(tbl1.autovacuum_count, 0) +
          COALESCE(tbl2.vacuum_count, 0) + COALESCE(tbl2.autovacuum_count, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_vacuum1,
        rn_vacuum2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>I</th>'
            '<th title="Number of times this table has been manually vacuumed (not counting VACUUM FULL)">Vacuum count</th>'
            '<th title="Number of times this table has been vacuumed by the autovacuum daemon">Autovacuum count</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.vacuum_count1,
            r_result.autovacuum_count1,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.vacuum_count2,
            r_result.autovacuum_count2,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_analyzed_tables_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT
        dbname,
        top.tablespacename,
        top.schemaname,
        top.relname,
        NULLIF(top.analyze_count, 0) as analyze_count,
        NULLIF(top.autoanalyze_count, 0) as autoanalyze_count,
        NULLIF(top.n_tup_ins, 0) as n_tup_ins,
        NULLIF(top.n_tup_upd, 0) as n_tup_upd,
        NULLIF(top.n_tup_del, 0) as n_tup_del,
        NULLIF(top.n_tup_hot_upd, 0) as n_tup_hot_upd
    FROM top_tables1 top
    WHERE COALESCE(top.analyze_count, 0) + COALESCE(top.autoanalyze_count, 0) > 0
    ORDER BY COALESCE(top.analyze_count, 0) + COALESCE(top.autoanalyze_count, 0) DESC,
      top.datid ASC,
      top.relid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN

    -- Populate templates
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th title="Number of times this table has been manually analyzed">Analyze count</th>'
            '<th title="Number of times this table has been analyzed by the autovacuum daemon">Autoanalyze count</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'rel_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
    );

    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
      report := report||format(
          jtab_tpl #>> ARRAY['rel_tpl'],
          r_result.dbname,
          r_result.tablespacename,
          r_result.schemaname,
          r_result.relname,
          r_result.analyze_count,
          r_result.autoanalyze_count,
          r_result.n_tup_ins,
          r_result.n_tup_upd,
          r_result.n_tup_del,
          r_result.n_tup_hot_upd
      );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_analyzed_tables_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for tables stats
    c_tbl_stats CURSOR(topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(tbl1.dbname,tbl2.dbname) as dbname,
        COALESCE(tbl1.tablespacename,tbl2.tablespacename) AS tablespacename,
        COALESCE(tbl1.schemaname,tbl2.schemaname) as schemaname,
        COALESCE(tbl1.relname,tbl2.relname) as relname,
        NULLIF(tbl1.analyze_count, 0) as analyze_count1,
        NULLIF(tbl1.autoanalyze_count, 0) as autoanalyze_count1,
        NULLIF(tbl1.n_tup_ins, 0) as n_tup_ins1,
        NULLIF(tbl1.n_tup_upd, 0) as n_tup_upd1,
        NULLIF(tbl1.n_tup_del, 0) as n_tup_del1,
        NULLIF(tbl1.n_tup_hot_upd, 0) as n_tup_hot_upd1,
        NULLIF(tbl2.analyze_count, 0) as analyze_count2,
        NULLIF(tbl2.autoanalyze_count, 0) as autoanalyze_count2,
        NULLIF(tbl2.n_tup_ins, 0) as n_tup_ins2,
        NULLIF(tbl2.n_tup_upd, 0) as n_tup_upd2,
        NULLIF(tbl2.n_tup_del, 0) as n_tup_del2,
        NULLIF(tbl2.n_tup_hot_upd, 0) as n_tup_hot_upd2,
        row_number() OVER (ORDER BY COALESCE(tbl1.analyze_count, 0) + COALESCE(tbl1.autoanalyze_count, 0) DESC) as rn_analyze1,
        row_number() OVER (ORDER BY COALESCE(tbl2.analyze_count, 0) + COALESCE(tbl2.autoanalyze_count, 0) DESC) as rn_analyze2
    FROM top_tables1 tbl1
        FULL OUTER JOIN top_tables2 tbl2 USING (server_id,datid,relid)
    WHERE COALESCE(tbl1.analyze_count, 0) + COALESCE(tbl1.autoanalyze_count, 0) +
          COALESCE(tbl2.analyze_count, 0) + COALESCE(tbl2.autoanalyze_count, 0) > 0
    ORDER BY COALESCE(tbl1.analyze_count, 0) + COALESCE(tbl1.autoanalyze_count, 0) +
          COALESCE(tbl2.analyze_count, 0) + COALESCE(tbl2.autoanalyze_count, 0) DESC,
      COALESCE(tbl1.datid,tbl2.datid) ASC,
      COALESCE(tbl1.relid,tbl2.relid) ASC
    ) t1
    WHERE least(
        rn_analyze1,
        rn_analyze2
      ) <= topn;

    r_result RECORD;
BEGIN
    -- Tables stats template
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {difftbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>I</th>'
            '<th title="Number of times this table has been manually analyzed">Analyze count</th>'
            '<th title="Number of times this table has been analyzed by the autovacuum daemon">Autoanalyze count</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (includes HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
            '<th title="Number of rows HOT updated (i.e., with no separate index update required)">Upd(HOT)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats(
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.analyze_count1,
            r_result.autoanalyze_count1,
            r_result.n_tup_ins1,
            r_result.n_tup_upd1,
            r_result.n_tup_del1,
            r_result.n_tup_hot_upd1,
            r_result.analyze_count2,
            r_result.autoanalyze_count2,
            r_result.n_tup_ins2,
            r_result.n_tup_upd2,
            r_result.n_tup_del2,
            r_result.n_tup_hot_upd2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
