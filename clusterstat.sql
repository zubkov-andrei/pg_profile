/* ===== Cluster stats functions ===== */

CREATE OR REPLACE FUNCTION cluster_stats(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        server_id               integer,
        checkpoints_timed     bigint,
        checkpoints_req       bigint,
        checkpoint_write_time double precision,
        checkpoint_sync_time  double precision,
        buffers_checkpoint    bigint,
        buffers_clean         bigint,
        buffers_backend       bigint,
        buffers_backend_fsync bigint,
        maxwritten_clean      bigint,
        buffers_alloc         bigint,
        wal_size              bigint,
        archived_count        bigint,
        failed_count          bigint
)
SET search_path=@extschema@,public AS $$
    SELECT
        st.server_id as server_id,
        sum(checkpoints_timed)::bigint as checkpoints_timed,
        sum(checkpoints_req)::bigint as checkpoints_req,
        sum(checkpoint_write_time)::double precision as checkpoint_write_time,
        sum(checkpoint_sync_time)::double precision as checkpoint_sync_time,
        sum(buffers_checkpoint)::bigint as buffers_checkpoint,
        sum(buffers_clean)::bigint as buffers_clean,
        sum(buffers_backend)::bigint as buffers_backend,
        sum(buffers_backend_fsync)::bigint as buffers_backend_fsync,
        sum(maxwritten_clean)::bigint as maxwritten_clean,
        sum(buffers_alloc)::bigint as buffers_alloc,
        sum(wal_size)::bigint as wal_size,
        sum(archived_count)::bigint as archived_count,
        sum(failed_count)::bigint as failed_count
    FROM sample_stat_cluster st
        LEFT OUTER JOIN sample_stat_archiver sa USING (server_id, sample_id)
        /* Start sample existance condition
        Start sample stats does not account in report, but we must be sure
        that start sample exists, as it is reference point of next sample
        */
        JOIN samples sample_s ON (st.server_id = sample_s.server_id AND sample_s.sample_id = start_id)
        /* End sample existance condition
        Make sure that end sample exists, so we really account full interval
        */
        JOIN samples sample_e ON (st.server_id = sample_e.server_id AND sample_e.sample_id = end_id)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN sample_s.sample_id + 1 AND sample_e.sample_id
    GROUP BY st.server_id
    --HAVING max(stats_reset)=min(stats_reset);
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION cluster_stats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        sample_id               integer,
        bgwriter_stats_reset  timestamp with time zone,
        archiver_stats_reset  timestamp with time zone
)
SET search_path=@extschema@,public AS $$
  SELECT
      bgwr1.sample_id as sample_id,
      nullif(bgwr1.stats_reset,coalesce(bgwr2.stats_reset,bgwr1.stats_reset)),
      nullif(sta1.stats_reset,coalesce(sta2.stats_reset,sta1.stats_reset))
  FROM sample_stat_cluster bgwr1
      LEFT OUTER JOIN sample_stat_archiver sta1 USING (server_id,sample_id)
      JOIN sample_stat_cluster bgwr2 ON (bgwr1.server_id = bgwr2.server_id AND bgwr1.sample_id = bgwr2.sample_id + 1)
      LEFT OUTER JOIN sample_stat_archiver sta2 ON (sta1.server_id = sta2.server_id AND sta1.sample_id = sta2.sample_id + 1)
  WHERE bgwr1.server_id = sserver_id AND bgwr1.sample_id BETWEEN start_id + 1 AND end_id
    AND
      COALESCE(
        nullif(bgwr1.stats_reset,coalesce(bgwr2.stats_reset,bgwr1.stats_reset)),
        nullif(sta1.stats_reset,coalesce(sta2.stats_reset,sta1.stats_reset))
      ) IS NOT NULL
  ORDER BY bgwr1.sample_id ASC
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION cluster_stats_reset_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        sample_id,
        bgwriter_stats_reset,
        archiver_stats_reset
    FROM cluster_stats_reset(sserver_id,start_id,end_id)
    ORDER BY COALESCE(bgwriter_stats_reset,archiver_stats_reset) ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Sample</th>'
            '<th>BGWriter reset time</th>'
            '<th>Archiver reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl',
        '<tr>'
          '<td {value}>%s</td>'
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
            r_result.bgwriter_stats_reset,
            r_result.archiver_stats_reset
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_stats_reset_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        interval_num,
        sample_id,
        bgwriter_stats_reset,
        archiver_stats_reset
    FROM
      (SELECT 1 AS interval_num, sample_id, bgwriter_stats_reset, archiver_stats_reset
        FROM cluster_stats_reset(sserver_id,start1_id,end1_id)
      UNION ALL
      SELECT 2 AS interval_num, sample_id, bgwriter_stats_reset, archiver_stats_reset
        FROM cluster_stats_reset(sserver_id,start2_id,end2_id)) AS samples
    ORDER BY interval_num, COALESCE(bgwriter_stats_reset, archiver_stats_reset) ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>I</th>'
            '<th>Sample</th>'
            '<th>BGWriter reset time</th>'
            '<th>Archiver reset time</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'sample_tpl1',
        '<tr {interval1}>'
          '<td {label} {title1}>1</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>',
      'sample_tpl2',
        '<tr {interval2}>'
          '<td {label} {title2}>2</td>'
          '<td {value}>%s</td>'
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
              r_result.bgwriter_stats_reset,
              r_result.archiver_stats_reset
          );
        WHEN 2 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['sample_tpl2'],
              r_result.sample_id,
              r_result.bgwriter_stats_reset,
              r_result.archiver_stats_reset
          );
        END CASE;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_stats_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        buffers_backend,
        buffers_backend_fsync,
        maxwritten_clean,
        buffers_alloc,
        pg_size_pretty(wal_size) as wal_size,
        archived_count,
        failed_count
    FROM cluster_stats(sserver_id,start_id,end_id);

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Metric</th>'
            '<th>Value</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'val_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Scheduled checkpoints',r_result.checkpoints_timed);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Requested checkpoints',r_result.checkpoints_req);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint write time (s)',round(cast(r_result.checkpoint_write_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint sync time (s)',round(cast(r_result.checkpoint_sync_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoints buffers written',r_result.buffers_checkpoint);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Background buffers written',r_result.buffers_clean);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend buffers written',r_result.buffers_backend);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend fsync count',r_result.buffers_backend_fsync);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Number of buffers allocated',r_result.buffers_alloc);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL generated',r_result.wal_size);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL segments archived',r_result.archived_count);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL segments archive failed',r_result.failed_count);
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_stats_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        stat1.checkpoints_timed as checkpoints_timed1,
        stat1.checkpoints_req as checkpoints_req1,
        stat1.checkpoint_write_time as checkpoint_write_time1,
        stat1.checkpoint_sync_time as checkpoint_sync_time1,
        stat1.buffers_checkpoint as buffers_checkpoint1,
        stat1.buffers_clean as buffers_clean1,
        stat1.buffers_backend as buffers_backend1,
        stat1.buffers_backend_fsync as buffers_backend_fsync1,
        stat1.maxwritten_clean as maxwritten_clean1,
        stat1.buffers_alloc as buffers_alloc1,
        pg_size_pretty(stat1.wal_size) as wal_size1,
        stat1.archived_count as archived_count1,
        stat1.failed_count as failed_count1,
        stat2.checkpoints_timed as checkpoints_timed2,
        stat2.checkpoints_req as checkpoints_req2,
        stat2.checkpoint_write_time as checkpoint_write_time2,
        stat2.checkpoint_sync_time as checkpoint_sync_time2,
        stat2.buffers_checkpoint as buffers_checkpoint2,
        stat2.buffers_clean as buffers_clean2,
        stat2.buffers_backend as buffers_backend2,
        stat2.buffers_backend_fsync as buffers_backend_fsync2,
        stat2.maxwritten_clean as maxwritten_clean2,
        stat2.buffers_alloc as buffers_alloc2,
        pg_size_pretty(stat2.wal_size) as wal_size2,
        stat2.archived_count as archived_count2,
        stat2.failed_count as failed_count2
    FROM cluster_stats(sserver_id,start1_id,end1_id) stat1
        FULL OUTER JOIN cluster_stats(sserver_id,start2_id,end2_id) stat2 USING (server_id);

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Metric</th>'
            '<th {title1}>Value (1)</th>'
            '<th {title2}>Value (2)</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'val_tpl',
        '<tr>'
          '<td>%s</td>'
          '<td {interval1}><div {value}>%s</div></td>'
          '<td {interval2}><div {value}>%s</div></td>'
        '</tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Scheduled checkpoints',r_result.checkpoints_timed1,r_result.checkpoints_timed2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Requested checkpoints',r_result.checkpoints_req1,r_result.checkpoints_req2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint write time (s)',
            round(cast(r_result.checkpoint_write_time1/1000 as numeric),2),
            round(cast(r_result.checkpoint_write_time2/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint sync time (s)',
            round(cast(r_result.checkpoint_sync_time1/1000 as numeric),2),
            round(cast(r_result.checkpoint_sync_time2/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoints buffers written',r_result.buffers_checkpoint1,r_result.buffers_checkpoint2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Background buffers written',r_result.buffers_clean1,r_result.buffers_clean2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend buffers written',r_result.buffers_backend1,r_result.buffers_backend2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend fsync count',r_result.buffers_backend_fsync1,r_result.buffers_backend_fsync2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean1,r_result.maxwritten_clean2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Number of buffers allocated',r_result.buffers_alloc1,r_result.buffers_alloc2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL generated',r_result.wal_size1,r_result.wal_size2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL segments archived',r_result.archived_count1,r_result.archived_count2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL segments archive failed',r_result.failed_count1,r_result.failed_count2);
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
