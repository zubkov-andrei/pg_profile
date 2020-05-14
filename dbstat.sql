/* ========= Reporting functions ========= */

/* ========= Cluster databases report functions ========= */

CREATE OR REPLACE FUNCTION dbstats(IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer)
RETURNS TABLE(
    datid oid,
    dbname name,
    xact_commit     bigint,
    xact_rollback   bigint,
    blks_read       bigint,
    blks_hit        bigint,
    tup_returned    bigint,
    tup_fetched     bigint,
    tup_inserted    bigint,
    tup_updated     bigint,
    tup_deleted     bigint,
    temp_files      bigint,
    temp_bytes      bigint,
    datsize         bigint,
    datsize_delta   bigint,
    deadlocks       bigint,
    blks_hit_pct    double precision)
SET search_path=@extschema@,public AS $$
    SELECT
        st.datid AS datid,
        st.datname AS dbname,
        sum(xact_commit)::bigint AS xact_commit,
        sum(xact_rollback)::bigint AS xact_rollback,
        sum(blks_read)::bigint AS blks_read,
        sum(blks_hit)::bigint AS blks_hit,
        sum(tup_returned)::bigint AS tup_returned,
        sum(tup_fetched)::bigint AS tup_fetched,
        sum(tup_inserted)::bigint AS tup_inserted,
        sum(tup_updated)::bigint AS tup_updated,
        sum(tup_deleted)::bigint AS tup_deleted,
        sum(temp_files)::bigint AS temp_files,
        sum(temp_bytes)::bigint AS temp_bytes,
        sum(datsize)::bigint AS datsize,
        sum(datsize_delta)::bigint AS datsize_delta,
        sum(deadlocks)::bigint AS deadlocks,
        sum(blks_hit)*100/GREATEST(sum(blks_hit)+sum(blks_read),1)::double precision AS blks_hit_pct
    FROM snap_stat_database st
        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
    WHERE st.node_id = snode_id AND datname NOT LIKE 'template_' AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY st.datid,st.datname
    --HAVING max(stats_reset)=min(stats_reset);
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION dbstats_reset(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
  datname       name,
  stats_reset   timestamp with time zone,
  snap_id       integer
)
SET search_path=@extschema@,public AS $$
    SELECT
        st.datname,
        st.stats_reset,
        st.snap_id
    FROM snap_stat_database st
        JOIN snap_stat_database stfirst ON
          (stfirst.node_id = st.node_id AND stfirst.snap_id = start_id AND stfirst.datid = st.datid)
        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
    WHERE st.node_id = snode_id AND st.datname NOT LIKE 'template_' AND st.snap_id BETWEEN snap_s.snap_id AND snap_e.snap_id
      AND st.stats_reset != stfirst.stats_reset
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION dbstats_reset_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        datname,
        snap_id,
        stats_reset
    FROM dbstats_reset(snode_id,start_id,end_id)
      ORDER BY stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Database</th><th>Snapshot</th><th>Reset time</th></tr>{rows}</table>',
      'snap_tpl','<tr><td>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['snap_tpl'],
            r_result.datname,
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

CREATE OR REPLACE FUNCTION dbstats_reset_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        interval_num,
        datname,
        snap_id,
        stats_reset
    FROM
      (SELECT 1 AS interval_num, datname, snap_id, stats_reset
        FROM dbstats_reset(snode_id,start1_id,end1_id)
      UNION ALL
      SELECT 2 AS interval_num, datname, snap_id, stats_reset
        FROM dbstats_reset(snode_id,start2_id,end2_id)) AS snapshots
    ORDER BY interval_num, stats_reset ASC;

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>I</th><th>Database</th><th>Snapshot</th><th>Reset time</th></tr>{rows}</table>',
      'snap_tpl1','<tr {interval1}><td {label} {title1}>1</td><td>%s</td><td {value}>%s</td><td {value}>%s</td></tr>',
      'snap_tpl2','<tr {interval2}><td {label} {title2}>2</td><td>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
      CASE r_result.interval_num
        WHEN 1 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['snap_tpl1'],
              r_result.datname,
              r_result.snap_id,
              r_result.stats_reset
          );
        WHEN 2 THEN
          report := report||format(
              jtab_tpl #>> ARRAY['snap_tpl2'],
              r_result.datname,
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

CREATE OR REPLACE FUNCTION dbstats_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        dbname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        temp_files,
        pg_size_pretty(temp_bytes) AS temp_bytes,
        pg_size_pretty(datsize) AS datsize,
        pg_size_pretty(datsize_delta) AS datsize_delta,
        deadlocks,
        blks_hit_pct
    FROM dbstats(snode_id,start_id,end_id,topn);

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Database</th><th>Commits</th><th>Rollbacks</th><th>Deadlocks</th><th>BlkHit%(read/hit)</th><th>Tup Ret/Fet</th><th>Tup Ins</th><th>Tup Upd</th><th>Tup Del</th><th>Temp Size(Files)</th><th>Size</th><th>Growth</th></tr>{rows}</table>',
      'db_tpl','<tr><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s%%(%s/%s)</td><td {value}>%s/%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s(%s)</td><td {value}>%s</td><td {value}>%s</td></tr>');
          -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            r_result.xact_commit,
            r_result.xact_rollback,
            r_result.deadlocks,
            round(CAST(r_result.blks_hit_pct AS numeric),2),
            r_result.blks_read,
            r_result.blks_hit,
            r_result.tup_returned,
            r_result.tup_fetched,
            r_result.tup_inserted,
            r_result.tup_updated,
            r_result.tup_deleted,
            r_result.temp_bytes,
            r_result.temp_files,
            r_result.datsize,
            r_result.datsize_delta
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dbstats_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
   IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for db stats
    c_dbstats CURSOR FOR
    SELECT
        COALESCE(dbs1.dbname,dbs2.dbname) AS dbname,
        dbs1.xact_commit AS xact_commit1,
        dbs1.xact_rollback AS xact_rollback1,
        dbs1.blks_read AS blks_read1,
        dbs1.blks_hit AS blks_hit1,
        dbs1.tup_returned AS tup_returned1,
        dbs1.tup_fetched AS tup_fetched1,
        dbs1.tup_inserted AS tup_inserted1,
        dbs1.tup_updated AS tup_updated1,
        dbs1.tup_deleted AS tup_deleted1,
        dbs1.temp_files AS temp_files1,
        pg_size_pretty(dbs1.temp_bytes) AS temp_bytes1,
        pg_size_pretty(dbs1.datsize) AS datsize1,
        pg_size_pretty(dbs1.datsize_delta) AS datsize_delta1,
        dbs1.deadlocks AS deadlocks1,
        dbs1.blks_hit_pct AS blks_hit_pct1,
        dbs2.xact_commit AS xact_commit2,
        dbs2.xact_rollback AS xact_rollback2,
        dbs2.blks_read AS blks_read2,
        dbs2.blks_hit AS blks_hit2,
        dbs2.tup_returned AS tup_returned2,
        dbs2.tup_fetched AS tup_fetched2,
        dbs2.tup_inserted AS tup_inserted2,
        dbs2.tup_updated AS tup_updated2,
        dbs2.tup_deleted AS tup_deleted2,
        dbs2.temp_files AS temp_files2,
        pg_size_pretty(dbs2.temp_bytes) AS temp_bytes2,
        pg_size_pretty(dbs2.datsize) AS datsize2,
        pg_size_pretty(dbs2.datsize_delta) AS datsize_delta2,
        dbs2.deadlocks AS deadlocks2,
        dbs2.blks_hit_pct AS blks_hit_pct2
    FROM dbstats(snode_id,start1_id,end1_id,topn) dbs1 FULL OUTER JOIN dbstats(snode_id,start2_id,end2_id,topn) dbs2
        USING (datid);

    r_result RECORD;
BEGIN
    -- Database stats TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Database</th><th>I</th><th>Commits</th><th>Rollbacks</th><th>Deadlocks</th><th>BlkHit%(read/hit)</th><th>Tup Ret/Fet</th><th>Tup Ins</th><th>Tup Upd</th><th>Tup Del</th><th>Temp Size(Files)</th><th>Size</th><th>Growth</th></tr>{rows}</table>',
     'db_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s%%(%s/%s)</td><td {value}>%s/%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s(%s)</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s%%(%s/%s)</td><td {value}>%s/%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s(%s)</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');
    -- apply settings to templates

    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_dbstats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['db_tpl'],
            r_result.dbname,
            r_result.xact_commit1,
            r_result.xact_rollback1,
            r_result.deadlocks1,
            round(CAST(r_result.blks_hit_pct1 AS numeric),2),
            r_result.blks_read1,
            r_result.blks_hit1,
            r_result.tup_returned1,
            r_result.tup_fetched1,
            r_result.tup_inserted1,
            r_result.tup_updated1,
            r_result.tup_deleted1,
            r_result.temp_bytes1,
            r_result.temp_files1,
            r_result.datsize1,
            r_result.datsize_delta1,
            r_result.xact_commit2,
            r_result.xact_rollback2,
            r_result.deadlocks2,
            round(CAST(r_result.blks_hit_pct2 AS numeric),2),
            r_result.blks_read2,
            r_result.blks_hit2,
            r_result.tup_returned2,
            r_result.tup_fetched2,
            r_result.tup_inserted2,
            r_result.tup_updated2,
            r_result.tup_deleted2,
            r_result.temp_bytes2,
            r_result.temp_files2,
            r_result.datsize2,
            r_result.datsize_delta2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;
