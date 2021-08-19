/* ===== Cluster stats functions ===== */
CREATE FUNCTION profile_checkavail_walstats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS BOOLEAN
SET search_path=@extschema@ AS $$
-- Check if there is table sizes collected in both bounds
  SELECT
    count(wal_bytes) > 0
  FROM sample_stat_wal
  WHERE
    server_id = sserver_id
    AND sample_id BETWEEN start_id + 1 AND end_id
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        server_id               integer,
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        wal_buffers_full    bigint,
        wal_write           bigint,
        wal_sync            bigint,
        wal_write_time      double precision,
        wal_sync_time       double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id as server_id,
        sum(wal_records)::bigint as wal_records,
        sum(wal_fpi)::bigint as wal_fpi,
        sum(wal_bytes)::numeric as wal_bytes,
        sum(wal_buffers_full)::bigint as wal_buffers_full,
        sum(wal_write)::bigint as wal_write,
        sum(wal_sync)::bigint as wal_sync,
        sum(wal_write_time)::double precision as wal_write_time,
        sum(wal_sync_time)::double precision as wal_sync_time
    FROM sample_stat_wal st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        sample_id        integer,
        wal_stats_reset  timestamp with time zone
)
SET search_path=@extschema@ AS $$
  SELECT
      ws1.sample_id as sample_id,
      nullif(ws1.stats_reset,ws0.stats_reset)
  FROM sample_stat_wal ws1
      JOIN sample_stat_wal ws0 ON (ws1.server_id = ws0.server_id AND ws1.sample_id = ws0.sample_id + 1)
  WHERE ws1.server_id = sserver_id AND ws1.sample_id BETWEEN start_id + 1 AND end_id
    AND
      nullif(ws1.stats_reset,ws0.stats_reset) IS NOT NULL
  ORDER BY ws1.sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION wal_stats_reset_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        sample_id,
        wal_stats_reset
    FROM wal_stats_reset(sserver_id,start_id,end_id)
    ORDER BY wal_stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Sample</th>'
            '<th>WAL stats reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl',
        '<tr>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['sample_tpl'],
            r_result.sample_id,
            r_result.wal_stats_reset
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION wal_stats_reset_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        interval_num,
        sample_id,
        wal_stats_reset
    FROM
      (SELECT 1 AS interval_num, sample_id, wal_stats_reset
        FROM wal_stats_reset(sserver_id,start1_id,end1_id)
      UNION ALL
      SELECT 2 AS interval_num, sample_id, wal_stats_reset
        FROM wal_stats_reset(sserver_id,start2_id,end2_id)) AS samples
    ORDER BY interval_num, wal_stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>I</th>'
            '<th>Sample</th>'
            '<th>WAL stats reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl1',
        '<tr {interval1}>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'sample_tpl2',
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
      CASE r_result.interval_num
        WHEN 1 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl1'],
              r_result.sample_id,
              r_result.wal_stats_reset
          );
        WHEN 2 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl2'],
              r_result.sample_id,
              r_result.wal_stats_reset
          );
        END CASE;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION wal_stats_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    report_duration float = (jreportset #> ARRAY['report_properties','interval_duration_sec'])::float;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        NULLIF(wal_records, 0) as wal_records,
        NULLIF(wal_fpi, 0) as wal_fpi,
        NULLIF(wal_bytes, 0) as wal_bytes,
        NULLIF(wal_buffers_full, 0) as wal_buffers_full,
        NULLIF(wal_write, 0) as wal_write,
        NULLIF(wal_sync, 0) as wal_sync,
        NULLIF(wal_write_time, 0.0) as wal_write_time,
        NULLIF(wal_sync_time, 0.0) as wal_sync_time
    FROM wal_stats(sserver_id,start_id,end_id);

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Metric</th>'
            '<th>Value</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'val_tpl',
        '<tr>'
          '<td title="%s">%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of WAL generated', 'WAL generated', pg_size_pretty(r_result.wal_bytes));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average amount of WAL generated per second', 'WAL per second',
          pg_size_pretty(
            round(
              r_result.wal_bytes/report_duration
            )::bigint
          ));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total number of WAL records generated', 'WAL records', r_result.wal_records);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total number of WAL full page images generated', 'WAL FPI', r_result.wal_fpi);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL data was written to disk because WAL buffers became full',
          'WAL buffers full', r_result.wal_buffers_full);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL buffers were written out to disk via XLogWrite request',
          'WAL writes', r_result.wal_write);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average number of times WAL buffers were written out to disk via XLogWrite request per second',
          'WAL writes per second',
          round((r_result.wal_write/report_duration)::numeric,2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL files were synced to disk via issue_xlog_fsync request (if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)',
          'WAL sync', r_result.wal_sync);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average number of times WAL files were synced to disk via issue_xlog_fsync request per second',
          'WAL syncs per second',
          round((r_result.wal_sync/report_duration)::numeric,2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of time spent writing WAL buffers to disk via XLogWrite request, in milliseconds (if track_wal_io_timing is enabled, otherwise zero). This includes the sync time when wal_sync_method is either open_datasync or open_sync',
          'WAL write time (s)',
          round(cast(r_result.wal_write_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'WAL write time as a percentage of the report duration time',
          'WAL write duty',
          round((r_result.wal_write_time/10/report_duration)::numeric,2) || '%');
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of time spent syncing WAL files to disk via issue_xlog_fsync request, in milliseconds (if track_wal_io_timing is enabled, fsync is on, and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)',
          'WAL sync time (s)',
          round(cast(r_result.wal_sync_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'WAL sync time as a percentage of the report duration time',
          'WAL sync duty',
          round((r_result.wal_sync_time/10/report_duration)::numeric,2) || '%');
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION wal_stats_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@ AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    report1_duration float = (jreportset #> ARRAY['report_properties','interval1_duration_sec'])::float;
    report2_duration float = (jreportset #> ARRAY['report_properties','interval2_duration_sec'])::float;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        NULLIF(stat1.wal_records, 0) as wal_records1,
        NULLIF(stat1.wal_fpi, 0) as wal_fpi1,
        NULLIF(stat1.wal_bytes, 0) as wal_bytes1,
        NULLIF(stat1.wal_buffers_full, 0) as wal_buffers_full1,
        NULLIF(stat1.wal_write, 0) as wal_write1,
        NULLIF(stat1.wal_sync, 0) as wal_sync1,
        NULLIF(stat1.wal_write_time, 0.0) as wal_write_time1,
        NULLIF(stat1.wal_sync_time, 0.0) as wal_sync_time1,
        NULLIF(stat2.wal_records, 0) as wal_records2,
        NULLIF(stat2.wal_fpi, 0) as wal_fpi2,
        NULLIF(stat2.wal_bytes, 0) as wal_bytes2,
        NULLIF(stat2.wal_buffers_full, 0) as wal_buffers_full2,
        NULLIF(stat2.wal_write, 0) as wal_write2,
        NULLIF(stat2.wal_sync, 0) as wal_sync2,
        NULLIF(stat2.wal_write_time, 0.0) as wal_write_time2,
        NULLIF(stat2.wal_sync_time, 0.0) as wal_sync_time2
    FROM wal_stats(sserver_id,start1_id,end1_id) stat1
        FULL OUTER JOIN wal_stats(sserver_id,start2_id,end2_id) stat2 USING (server_id);

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table {stattbl}>'
          '<tr>'
            '<th>Metric</th>'
            '<th {title1}>Value (1)</th>'
            '<th {title2}>Value (2)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'val_tpl',
        '<tr>'
          '<td title="%s">%s</td>'
          '<td {interval1}><div {value}>%s</div></td>'
          '<td {interval2}><div {value}>%s</div></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of WAL generated', 'WAL generated',
          pg_size_pretty(r_result.wal_bytes1), pg_size_pretty(r_result.wal_bytes2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average amount of WAL generated per second', 'WAL per second',
          pg_size_pretty(
            round(
              r_result.wal_bytes1/report1_duration
            )::bigint
          ),
          pg_size_pretty(
            round(
              r_result.wal_bytes2/report2_duration
            )::bigint
          ));

        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total number of WAL records generated', 'WAL records', r_result.wal_records1, r_result.wal_records2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total number of WAL full page images generated', 'WAL FPI', r_result.wal_fpi1, r_result.wal_fpi2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL data was written to disk because WAL buffers became full', 'WAL buffers full', r_result.wal_buffers_full1, r_result.wal_buffers_full2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL buffers were written out to disk via XLogWrite request', 'WAL writes',
          r_result.wal_write1, r_result.wal_write2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average number of times WAL buffers were written out to disk via XLogWrite request per second',
          'WAL writes per second',
          round((r_result.wal_write1/report1_duration)::numeric,2),
          round((r_result.wal_write2/report2_duration)::numeric,2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Number of times WAL files were synced to disk via issue_xlog_fsync request (if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)',
          'WAL sync', r_result.wal_sync1, r_result.wal_sync2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Average number of times WAL files were synced to disk via issue_xlog_fsync request per second',
          'WAL syncs per second',
          round((r_result.wal_sync1/report1_duration)::numeric,2),
          round((r_result.wal_sync2/report2_duration)::numeric,2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of time spent writing WAL buffers to disk via XLogWrite request, in milliseconds (if track_wal_io_timing is enabled, otherwise zero). This includes the sync time when wal_sync_method is either open_datasync or open_sync',
          'WAL write time (s)',
          round(cast(r_result.wal_write_time1/1000 as numeric),2),
          round(cast(r_result.wal_write_time2/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'WAL write time as a percentage of the report duration time',
          'WAL write duty',
          round((r_result.wal_write_time1/10/report1_duration)::numeric,2) || '%',
          round((r_result.wal_write_time2/10/report2_duration)::numeric,2) || '%');
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'Total amount of time spent syncing WAL files to disk via issue_xlog_fsync request, in milliseconds (if track_wal_io_timing is enabled, fsync is on, and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)',
          'WAL sync time (s)',
          round(cast(r_result.wal_sync_time1/1000 as numeric),2),
          round(cast(r_result.wal_sync_time2/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],
          'WAL sync time as a percentage of the report duration time',
          'WAL sync duty',
          round((r_result.wal_sync_time1/10/report1_duration)::numeric,2) || '%',
          round((r_result.wal_sync_time2/10/report2_duration)::numeric,2) || '%');
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
