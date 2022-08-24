/* ===== Indexes stats functions ===== */

CREATE FUNCTION top_indexes(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    datid               oid,
    relid               oid,
    indexrelid          oid,
    indisunique         boolean,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    indexrelname        name,
    idx_scan            bigint,
    growth              bigint,
    tbl_n_tup_ins       bigint,
    tbl_n_tup_upd       bigint,
    tbl_n_tup_del       bigint,
    tbl_n_tup_hot_upd   bigint,
    relpagegrowth_bytes bigint,
    idx_blks_read       bigint,
    idx_blks_fetch      bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id,
        st.datid,
        st.relid,
        st.indexrelid,
        st.indisunique,
        sample_db.datname,
        tablespaces_list.tablespacename,
        COALESCE(mtbl.schemaname,st.schemaname)::name AS schemaname,
        COALESCE(mtbl.relname||'(TOAST)',st.relname)::name as relname,
        st.indexrelname,
        sum(st.idx_scan)::bigint as idx_scan,
        sum(st.relsize_diff)::bigint as growth,
        sum(tbl.n_tup_ins)::bigint as tbl_n_tup_ins,
        sum(tbl.n_tup_upd)::bigint as tbl_n_tup_upd,
        sum(tbl.n_tup_del)::bigint as tbl_n_tup_del,
        sum(tbl.n_tup_hot_upd)::bigint as tbl_n_tup_hot_upd,
        sum(st.relpages_bytes_diff)::bigint as relpagegrowth_bytes,
        sum(st.idx_blks_read)::bigint as idx_blks_read,
        sum(st.idx_blks_hit)::bigint + sum(st.idx_blks_read)::bigint as idx_blks_fetch
    FROM v_sample_stat_indexes st JOIN sample_stat_tables tbl USING (server_id, sample_id, datid, relid)
        -- Database name
        JOIN sample_stat_database sample_db USING (server_id, sample_id, datid)
        JOIN tablespaces_list ON (st.server_id, st.tablespaceid) = (tablespaces_list.server_id, tablespaces_list.tablespaceid)
        -- join main table for indexes on toast
        LEFT OUTER JOIN tables_list mtbl ON
          (mtbl.server_id, mtbl.datid, mtbl.reltoastrelid) =
          (st.server_id, st.datid, st.relid)
    WHERE st.server_id=sserver_id AND NOT sample_db.datistemplate AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,st.indexrelid,st.indisunique,sample_db.datname,
      COALESCE(mtbl.schemaname,st.schemaname),COALESCE(mtbl.relname||'(TOAST)',st.relname), tablespaces_list.tablespacename,st.indexrelname
$$ LANGUAGE sql;

CREATE FUNCTION top_growth_indexes_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Indexes stats template
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(end1_id integer, topn integer) FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        NULLIF(st.best_growth, 0) as growth,
        NULLIF(st_last.relsize, 0) as relsize,
        NULLIF(st_last.relpages_bytes, 0) as relpages_bytes,
        NULLIF(tbl_n_tup_ins, 0) as tbl_n_tup_ins,
        NULLIF(tbl_n_tup_upd - COALESCE(tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd,
        NULLIF(tbl_n_tup_del, 0) as tbl_n_tup_del,
        st.relsize_growth_avail
    FROM top_indexes1 st
        JOIN sample_stat_indexes st_last USING (server_id,datid,indexrelid)
    WHERE st_last.sample_id = end1_id
      AND st.best_growth > 0
    ORDER BY st.best_growth DESC,
      COALESCE(tbl_n_tup_ins,0) + COALESCE(tbl_n_tup_upd,0) + COALESCE(tbl_n_tup_del,0) DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespace</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            COALESCE(
              pg_size_pretty(r_result.relsize),
              '['||pg_size_pretty(r_result.relpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail
              THEN pg_size_pretty(r_result.growth)
              ELSE '['||pg_size_pretty(r_result.growth)||']'
            END,
            r_result.tbl_n_tup_ins,
            r_result.tbl_n_tup_upd,
            r_result.tbl_n_tup_del
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_growth_indexes_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(end1_id integer, end2_id integer, topn integer) FOR
    SELECT * FROM (SELECT
        COALESCE(ix1.dbname,ix2.dbname) as dbname,
        COALESCE(ix1.tablespacename,ix2.tablespacename) as tablespacename,
        COALESCE(ix1.schemaname,ix2.schemaname) as schemaname,
        COALESCE(ix1.relname,ix2.relname) as relname,
        COALESCE(ix1.indexrelname,ix2.indexrelname) as indexrelname,
        NULLIF(ix1.best_growth, 0) as growth1,
        NULLIF(ix_last1.relsize, 0) as relsize1,
        NULLIF(ix_last1.relpages_bytes, 0) as relpages_bytes1,
        NULLIF(ix1.tbl_n_tup_ins, 0) as tbl_n_tup_ins1,
        NULLIF(ix1.tbl_n_tup_upd - COALESCE(ix1.tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd1,
        NULLIF(ix1.tbl_n_tup_del, 0) as tbl_n_tup_del1,
        NULLIF(ix2.best_growth, 0) as growth2,
        NULLIF(ix_last2.relsize, 0) as relsize2,
        NULLIF(ix_last2.relpages_bytes, 0) as relpages_bytes2,
        NULLIF(ix2.tbl_n_tup_ins, 0) as tbl_n_tup_ins2,
        NULLIF(ix2.tbl_n_tup_upd - COALESCE(ix2.tbl_n_tup_hot_upd,0), 0) as tbl_n_tup_upd2,
        NULLIF(ix2.tbl_n_tup_del, 0) as tbl_n_tup_del2,
        ix1.relsize_growth_avail as relsize_growth_avail1,
        ix2.relsize_growth_avail as relsize_growth_avail2,
        row_number() over (ORDER BY ix1.best_growth DESC NULLS LAST) as rn_growth1,
        row_number() over (ORDER BY ix2.best_growth DESC NULLS LAST) as rn_growth2
    FROM top_indexes1 ix1
        FULL OUTER JOIN top_indexes2 ix2 USING (server_id, datid, indexrelid)
        LEFT OUTER JOIN sample_stat_indexes ix_last1 ON
          (ix_last1.sample_id, ix_last1.server_id, ix_last1.datid, ix_last1.indexrelid) =
          (end1_id, ix1.server_id, ix1.datid, ix1.indexrelid)
        LEFT OUTER JOIN sample_stat_indexes ix_last2 ON
          (ix_last2.sample_id, ix_last2.server_id, ix_last2.datid, ix_last2.indexrelid) =
          (end2_id, ix2.server_id, ix2.datid, ix2.indexrelid)
    WHERE COALESCE(ix1.best_growth, 0) + COALESCE(ix2.best_growth, 0) > 0
    ORDER BY COALESCE(ix1.best_growth, 0) + COALESCE(ix2.best_growth, 0) DESC,
      COALESCE(ix1.tbl_n_tup_ins,0) + COALESCE(ix1.tbl_n_tup_upd,0) + COALESCE(ix1.tbl_n_tup_del,0) +
      COALESCE(ix2.tbl_n_tup_ins,0) + COALESCE(ix2.tbl_n_tup_upd,0) + COALESCE(ix2.tbl_n_tup_del,0) DESC,
      COALESCE(ix1.datid,ix2.datid) ASC,
      COALESCE(ix1.relid,ix2.relid) ASC,
      COALESCE(ix1.indexrelid,ix2.indexrelid) ASC
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
            '<th rowspan="2">Index</th>'
            '<th rowspan="2">I</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
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
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
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
    FOR r_result IN c_ix_stats(
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
            r_result.indexrelname,
            COALESCE(
              pg_size_pretty(r_result.relsize1),
              '['||pg_size_pretty(r_result.relpages_bytes1)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail1
              THEN pg_size_pretty(r_result.growth1)
              ELSE '['||pg_size_pretty(r_result.growth1)||']'
            END,
            r_result.tbl_n_tup_ins1,
            r_result.tbl_n_tup_upd1,
            r_result.tbl_n_tup_del1,
            COALESCE(
              pg_size_pretty(r_result.relsize2),
              '['||pg_size_pretty(r_result.relpages_bytes2)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail2
              THEN pg_size_pretty(r_result.growth2)
              ELSE '['||pg_size_pretty(r_result.growth2)||']'
            END,
            r_result.tbl_n_tup_ins2,
            r_result.tbl_n_tup_upd2,
            r_result.tbl_n_tup_del2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION ix_unused_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(end1_id integer, topn integer) FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        NULLIF(st.best_growth, 0) as growth,
        NULLIF(st_last.relsize, 0) as relsize,
        NULLIF(st_last.relpages_bytes, 0) as relpages_bytes,
        NULLIF(tbl_n_tup_ins, 0) as tbl_n_tup_ins,
        NULLIF(tbl_n_tup_upd - COALESCE(tbl_n_tup_hot_upd,0), 0) as tbl_n_ind_upd,
        NULLIF(tbl_n_tup_del, 0) as tbl_n_tup_del,
        st.relsize_growth_avail
    FROM top_indexes1 st
        JOIN sample_stat_indexes st_last using (server_id,datid,indexrelid)
    WHERE st_last.sample_id=end1_id
      AND COALESCE(st.idx_scan, 0) = 0 AND NOT st.indisunique
      AND COALESCE(tbl_n_tup_ins, 0) + COALESCE(tbl_n_tup_upd, 0) + COALESCE(tbl_n_tup_del, 0) > 0
    ORDER BY
      COALESCE(tbl_n_tup_ins, 0) + COALESCE(tbl_n_tup_upd, 0) + COALESCE(tbl_n_tup_del, 0) DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th rowspan="2">DB</th>'
            '<th rowspan="2">Tablespaces</th>'
            '<th rowspan="2">Schema</th>'
            '<th rowspan="2">Table</th>'
            '<th rowspan="2">Index</th>'
            '<th colspan="2">Index</th>'
            '<th colspan="3">Table</th>'
          '</tr>'
          '<tr>'
            '<th title="Index size, as it was at the moment of last sample in report interval">Size</th>'
            '<th title="Index size increment during report interval">Growth</th>'
            '<th title="Number of rows inserted">Ins</th>'
            '<th title="Number of rows updated (without HOT updated rows)">Upd</th>'
            '<th title="Number of rows deleted">Del</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_ix_stats(
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            COALESCE(
              pg_size_pretty(r_result.relsize),
              '['||pg_size_pretty(r_result.relpages_bytes)||']'
            ),
            CASE WHEN r_result.relsize_growth_avail
              THEN pg_size_pretty(r_result.growth)
              ELSE '['||pg_size_pretty(r_result.growth)||']'
            END,
            r_result.tbl_n_tup_ins,
            r_result.tbl_n_ind_upd,
            r_result.tbl_n_tup_del
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION top_vacuumed_indexes_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Indexes stats template
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(start1_id integer, end1_id integer, topn integer) FOR
    SELECT
        st.dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        st.indexrelname,
        NULLIF(vac.vacuum_count, 0) as vacuum_count,
        NULLIF(vac.autovacuum_count, 0) as autovacuum_count,
        NULLIF(vac.vacuum_bytes, 0) as vacuum_bytes,
        NULLIF(vac.avg_indexrelsize, 0) as avg_ix_relsize,
        NULLIF(vac.avg_relsize, 0) as avg_relsize,
        NULLIF(vac.relpages_vacuum_bytes, 0) as relpages_vacuum_bytes,
        NULLIF(vac.avg_indexrelpages_bytes, 0) as avg_indexrelpages_bytes,
        NULLIF(vac.avg_relpages_bytes, 0) as avg_relpages_bytes,
        vac.relsize_collected as relsize_collected
    FROM top_indexes1 st
      JOIN (
        SELECT
          server_id,
          datid,
          indexrelid,
          sum(vacuum_count) as vacuum_count,
          sum(autovacuum_count) as autovacuum_count,
          round(sum(i.relsize
      * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as vacuum_bytes,
          round(
            avg(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          )::bigint as avg_indexrelsize,
          round(
            avg(t.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          )::bigint as avg_relsize,

          round(sum(i.relpages_bytes
      * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as relpages_vacuum_bytes,
          round(
            avg(i.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          )::bigint as avg_indexrelpages_bytes,
          round(
            avg(t.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          )::bigint as avg_relpages_bytes,
          count(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0) =
          count(*) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
            as relsize_collected
        FROM sample_stat_indexes i
      JOIN indexes_list il USING (server_id,datid,indexrelid)
      JOIN sample_stat_tables t USING
        (server_id, sample_id, datid, relid)
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start1_id + 1 AND end1_id
        GROUP BY
          server_id, datid, indexrelid
      ) vac USING (server_id, datid, indexrelid)
    WHERE COALESCE(vac.vacuum_count, 0) + COALESCE(vac.autovacuum_count, 0) > 0
    ORDER BY CASE WHEN relsize_collected THEN vacuum_bytes ELSE relpages_vacuum_bytes END DESC,
      st.datid ASC,
      st.relid ASC,
      st.indexrelid ASC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>DB</th>'
            '<th>Tablespace</th>'
            '<th>Schema</th>'
            '<th>Table</th>'
            '<th>Index</th>'
            '<th title="Estimated implicit vacuum load caused by table indexes">~Vacuum bytes</th>'
            '<th title="Vacuum count on underlying table">Vacuum cnt</th>'
            '<th title="Autovacuum count on underlying table">Autovacuum cnt</th>'
            '<th title="Average index size during report interval">IX size</th>'
            '<th title="Average relation size during report interval">Relsize</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(report_context, jtab_tpl);
    -- Reporting table stats
    FOR r_result IN c_ix_stats(
        (report_context #>> '{report_properties,start1_id}')::integer,
        (report_context #>> '{report_properties,end1_id}')::integer,
        (report_context #>> '{report_properties,topn}')::integer
      )
    LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['row_tpl'],
            r_result.dbname,
            r_result.tablespacename,
            r_result.schemaname,
            r_result.relname,
            r_result.indexrelname,
            CASE WHEN r_result.relsize_collected THEN
              pg_size_pretty(r_result.vacuum_bytes)
            ELSE
              '['||pg_size_pretty(r_result.relpages_vacuum_bytes)||']'
            END,
            r_result.vacuum_count,
            r_result.autovacuum_count,
            CASE WHEN r_result.relsize_collected THEN
              pg_size_pretty(r_result.avg_ix_relsize)
            ELSE
              '['||pg_size_pretty(r_result.avg_indexrelpages_bytes)||']'
            END,
            CASE WHEN r_result.relsize_collected THEN
              pg_size_pretty(r_result.avg_relsize)
            ELSE
              '['||pg_size_pretty(r_result.avg_relpages_bytes)||']'
            END
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION top_vacuumed_indexes_diff_htbl(IN report_context jsonb, IN sserver_id integer)
RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';

    -- Table elements template collection
    jtab_tpl    jsonb;

    --Cursor for indexes stats
    c_ix_stats CURSOR(start1_id integer, end1_id integer,
      start2_id integer, end2_id integer, topn integer)
    FOR
    SELECT * FROM (SELECT
        COALESCE(ix1.dbname,ix2.dbname) as dbname,
        COALESCE(ix1.tablespacename,ix2.tablespacename) as tablespacename,
        COALESCE(ix1.schemaname,ix2.schemaname) as schemaname,
        COALESCE(ix1.relname,ix2.relname) as relname,
        COALESCE(ix1.indexrelname,ix2.indexrelname) as indexrelname,
        NULLIF(vac1.vacuum_count, 0) as vacuum_count1,
        NULLIF(vac1.autovacuum_count, 0) as autovacuum_count1,
        CASE WHEN vac1.relsize_collected THEN
          NULLIF(vac1.vacuum_bytes, 0)
        ELSE
          NULLIF(vac1.relpages_vacuum_bytes, 0)
        END as best_vacuum_bytes1,
--        NULLIF(vac1.vacuum_bytes, 0) as vacuum_bytes1,
        NULLIF(vac1.avg_indexrelsize, 0) as avg_ix_relsize1,
        NULLIF(vac1.avg_relsize, 0) as avg_relsize1,
--        NULLIF(vac1.relpages_vacuum_bytes, 0) as relpages_vacuum_bytes1,
        NULLIF(vac1.avg_indexrelpages_bytes, 0) as avg_indexrelpages_bytes1,
        NULLIF(vac1.avg_relpages_bytes, 0) as avg_relpages_bytes1,
        vac1.relsize_collected as relsize_collected1,
        NULLIF(vac2.vacuum_count, 0) as vacuum_count2,
        NULLIF(vac2.autovacuum_count, 0) as autovacuum_count2,
        CASE WHEN vac2.relsize_collected THEN
          NULLIF(vac2.vacuum_bytes, 0)
        ELSE
          NULLIF(vac2.relpages_vacuum_bytes, 0)
        END as best_vacuum_bytes2,
--        NULLIF(vac2.vacuum_bytes, 0) as vacuum_bytes2,
        NULLIF(vac2.avg_indexrelsize, 0) as avg_ix_relsize2,
        NULLIF(vac2.avg_relsize, 0) as avg_relsize2,
        --NULLIF(vac2.relpages_vacuum_bytes, 0) as relpages_vacuum_bytes2,
        NULLIF(vac2.avg_indexrelpages_bytes, 0) as avg_indexrelpages_bytes2,
        NULLIF(vac2.avg_relpages_bytes, 0) as avg_relpages_bytes2,
        vac2.relsize_collected as relsize_collected2,
        row_number() over (ORDER BY
          CASE WHEN vac1.relsize_collected THEN vac1.vacuum_bytes ELSE vac1.relpages_vacuum_bytes END
          DESC NULLS LAST)
          as rn_vacuum_bytes1,
        row_number() over (ORDER BY
          CASE WHEN vac2.relsize_collected THEN vac2.vacuum_bytes ELSE vac2.relpages_vacuum_bytes END
          DESC NULLS LAST)
          as rn_vacuum_bytes2
    FROM top_indexes1 ix1
        FULL OUTER JOIN top_indexes2 ix2 USING (server_id, datid, indexrelid)
        -- Join interpolated data of interval 1
        LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        indexrelid,
        sum(vacuum_count) as vacuum_count,
        sum(autovacuum_count) as autovacuum_count,
        round(sum(i.relsize
          * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as vacuum_bytes,
        round(avg(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_indexrelsize,
        round(avg(t.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_relsize,
        round(sum(i.relpages_bytes
          * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as relpages_vacuum_bytes,
        round(avg(i.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_indexrelpages_bytes,
        round(avg(t.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_relpages_bytes,
        count(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0) =
        count(*) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          as relsize_collected
      FROM sample_stat_indexes i
        JOIN indexes_list il USING (server_id,datid,indexrelid)
        JOIN sample_stat_tables t USING
          (server_id, sample_id, datid, relid)
      WHERE
        server_id = sserver_id AND
        sample_id BETWEEN start1_id + 1 AND end1_id
      GROUP BY
        server_id, datid, indexrelid
        ) vac1 USING (server_id, datid, indexrelid)
        -- Join interpolated data of interval 2
        LEFT OUTER JOIN (
      SELECT
        server_id,
        datid,
        indexrelid,
        sum(vacuum_count) as vacuum_count,
        sum(autovacuum_count) as autovacuum_count,
        round(sum(i.relsize
          * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as vacuum_bytes,
        round(avg(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_indexrelsize,
        round(avg(t.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_relsize,
        round(sum(i.relpages_bytes
          * (COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0))))::bigint as relpages_vacuum_bytes,
        round(avg(i.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_indexrelpages_bytes,
        round(avg(t.relpages_bytes) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0))::bigint as avg_relpages_bytes,
        count(i.relsize) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0) =
        count(*) FILTER (WHERE COALESCE(vacuum_count,0) + COALESCE(autovacuum_count,0) > 0)
          as relsize_collected
      FROM sample_stat_indexes i
        JOIN indexes_list il USING (server_id,datid,indexrelid)
        JOIN sample_stat_tables t USING
          (server_id, sample_id, datid, relid)
      WHERE
        server_id = sserver_id AND
        sample_id BETWEEN start2_id + 1 AND end2_id
      GROUP BY
        server_id, datid, indexrelid
        ) vac2 USING (server_id, datid, indexrelid)
    WHERE COALESCE(vac1.vacuum_count, 0) + COALESCE(vac1.autovacuum_count, 0) +
        COALESCE(vac2.vacuum_count, 0) + COALESCE(vac2.autovacuum_count, 0) > 0
    ) t1
    WHERE least(
        rn_vacuum_bytes1,
        rn_vacuum_bytes2
      ) <= topn
    ORDER BY
      COALESCE(best_vacuum_bytes1, 0) + COALESCE(best_vacuum_bytes2, 0) DESC,
      dbname ASC,
      schemaname ASC,
      indexrelname ASC
    ;

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
            '<th>Index</th>'
            '<th>I</th>'
            '<th title="Estimated implicit vacuum load caused by table indexes">~Vacuum bytes</th>'
            '<th title="Vacuum count on underlying table">Vacuum cnt</th>'
            '<th title="Autovacuum count on underlying table">Autovacuum cnt</th>'
            '<th title="Average index size during report interval">IX size</th>'
            '<th title="Average relation size during report interval">Relsize</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'row_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr}>%s</td>'
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
        '</tr>'
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
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
    FOR r_result IN c_ix_stats(
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
            r_result.indexrelname,
            CASE WHEN r_result.relsize_collected1 THEN
              pg_size_pretty(r_result.best_vacuum_bytes1)
            ELSE
              '['||pg_size_pretty(r_result.best_vacuum_bytes1)||']'
            END,
            r_result.vacuum_count1,
            r_result.autovacuum_count1,
            CASE WHEN r_result.relsize_collected1 THEN
              pg_size_pretty(r_result.avg_ix_relsize1)
            ELSE
              '['||pg_size_pretty(r_result.avg_indexrelpages_bytes1)||']'
            END,
            CASE WHEN r_result.relsize_collected1 THEN
              pg_size_pretty(r_result.avg_relsize1)
            ELSE
              '['||pg_size_pretty(r_result.avg_relpages_bytes1)||']'
            END,
            CASE WHEN r_result.relsize_collected2 THEN
              pg_size_pretty(r_result.best_vacuum_bytes2)
            ELSE
              '['||pg_size_pretty(r_result.best_vacuum_bytes2)||']'
            END,
            r_result.vacuum_count2,
            r_result.autovacuum_count2,
            CASE WHEN r_result.relsize_collected2 THEN
              pg_size_pretty(r_result.avg_ix_relsize2)
            ELSE
              '['||pg_size_pretty(r_result.avg_indexrelpages_bytes2)||']'
            END,
            CASE WHEN r_result.relsize_collected2 THEN
              pg_size_pretty(r_result.avg_relsize2)
            ELSE
              '['||pg_size_pretty(r_result.avg_relpages_bytes2)||']'
            END
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
