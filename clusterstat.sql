/* ===== Cluster stats functions ===== */

CREATE OR REPLACE FUNCTION cluster_stats(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        node_id integer,
        checkpoints_timed bigint,
        checkpoints_req bigint,
        checkpoint_write_time double precision,
        checkpoint_sync_time double precision,
        buffers_checkpoint bigint,
        buffers_clean bigint,
        buffers_backend bigint,
        buffers_backend_fsync bigint,
        maxwritten_clean bigint,
        buffers_alloc bigint,
        wal_size bigint
)
SET search_path=@extschema@,public AS $$
    SELECT
        st.node_id as node_id,
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
        sum(wal_size)::bigint as wal_size
    FROM snap_stat_cluster st
        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY st.node_id
    --HAVING max(stats_reset)=min(stats_reset);
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION cluster_stats_reset(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
        snap_id         integer,
        stats_reset     timestamp with time zone
)
SET search_path=@extschema@,public AS $$
    SELECT
        st.snap_id as snap_id,
        st.stats_reset as stats_reset
    FROM snap_stat_cluster st
        JOIN snap_stat_cluster stfirst ON (st.node_id = stfirst.node_id AND stfirst.snap_id = start_id)
        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
    WHERE st.node_id = snode_id AND st.snap_id BETWEEN snap_s.snap_id AND snap_e.snap_id
      AND st.stats_reset != stfirst.stats_reset
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION cluster_stats_reset_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        snap_id,
        stats_reset
    FROM cluster_stats_reset(snode_id,start_id,end_id)
    ORDER BY stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Snapshot</th><th>Reset time</th></tr>{rows}</table>',
      'snap_tpl','<tr><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['snap_tpl'],
            r_result.snap_id,
            r_result.stats_reset
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_stats_reset_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        interval_num,
        snap_id,
        stats_reset
    FROM
      (SELECT 1 AS interval_num, snap_id, stats_reset
        FROM cluster_stats_reset(snode_id,start1_id,end1_id)
      UNION ALL
      SELECT 2 AS interval_num, snap_id, stats_reset
        FROM cluster_stats_reset(snode_id,start2_id,end2_id)) AS snapshots
    ORDER BY interval_num, stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>I</th><th>Snapshot</th><th>Reset time</th></tr>{rows}</table>',
      'snap_tpl1','<tr {interval1}><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td></tr>',
      'snap_tpl2','<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
      CASE r_result.interval_num
        WHEN 1 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['snap_tpl1'],
              r_result.snap_id,
              r_result.stats_reset
          );
        WHEN 2 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['snap_tpl2'],
              r_result.snap_id,
              r_result.stats_reset
          );
        END CASE;
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_stats_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
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
        pg_size_pretty(wal_size) as wal_size
    FROM cluster_stats(snode_id,start_id,end_id);

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Metric</th><th>Value</th></tr>{rows}</table>',
      'val_tpl','<tr><td>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting summary bgwriter stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Scheduled checkpoints',r_result.checkpoints_timed);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Requested checkpoints',r_result.checkpoints_req);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint write time (s)',round(cast(r_result.checkpoint_write_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoint sync time (s)',round(cast(r_result.checkpoint_sync_time/1000 as numeric),2));
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoints pages written',r_result.buffers_checkpoint);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Background pages written',r_result.buffers_clean);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend pages written',r_result.buffers_backend);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend fsync count',r_result.buffers_backend_fsync);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Number of buffers allocated',r_result.buffers_alloc);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL generated',r_result.wal_size);
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cluster_stats_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
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
        pg_size_pretty(stat2.wal_size) as wal_size2
    FROM cluster_stats(snode_id,start1_id,end1_id) stat1
        FULL OUTER JOIN cluster_stats(snode_id,start2_id,end2_id) stat2 USING (node_id);

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Metric</th><th {title1}>Value (1)</th><th {title2}>Value (2)</th></tr>{rows}</table>',
      'val_tpl','<tr><td>%s</td><td {interval1}><div {value}>%s</div></td><td {interval2}><div {value}>%s</div></td></tr>');
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
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Checkpoints pages written',r_result.buffers_checkpoint1,r_result.buffers_checkpoint2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Background pages written',r_result.buffers_clean1,r_result.buffers_clean2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend pages written',r_result.buffers_backend1,r_result.buffers_backend2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Backend fsync count',r_result.buffers_backend_fsync1,r_result.buffers_backend_fsync2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Bgwriter interrupts (too many buffers)',r_result.maxwritten_clean1,r_result.maxwritten_clean2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'Number of buffers allocated',r_result.buffers_alloc1,r_result.buffers_alloc2);
        report := report||format(jtab_tpl #>> ARRAY['val_tpl'],'WAL generated',r_result.wal_size1,r_result.wal_size2);
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
