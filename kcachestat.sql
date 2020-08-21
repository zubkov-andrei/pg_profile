/* ===== Statements stats functions ===== */

CREATE OR REPLACE FUNCTION top_kcache_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id integer,
    datid oid,
    dbname name,
    userid              oid,
    queryid             bigint,
    queryid_md5         char(10),
    user_time           double precision, --  User CPU time used
    user_time_pct       float, --  User CPU time used
    system_time         double precision, --  System CPU time used
    system_time_pct     float, --  User CPU time used
    minflts             bigint, -- Number of page reclaims (soft page faults)
    majflts             bigint, -- Number of page faults (hard page faults)
    nswaps              bigint, -- Number of swaps
    reads               bigint, -- Number of bytes read by the filesystem layer
    --reads_blks          bigint, -- Number of 8K blocks read by the filesystem layer
    writes              bigint, -- Number of bytes written by the filesystem layer
    --writes_blks         bigint, -- Number of 8K blocks written by the filesystem layer
    msgsnds             bigint, -- Number of IPC messages sent
    msgrcvs             bigint, -- Number of IPC messages received
    nsignals            bigint, -- Number of signals received
    nvcsws              bigint, -- Number of voluntary context switches
    nivcsws             bigint,
    reads_total_pct     float,
    writes_total_pct    float
) SET search_path=@extschema@,public AS $$
  WITH tot AS (
        SELECT
            sum(user_time) AS user_time,
            sum(system_time) AS system_time,
            sum(reads) AS reads,
            sum(writes) AS writes
        FROM sample_kcache_total
        WHERE server_id = sserver_id AND sample_id BETWEEN start_id + 1 AND end_id)
    SELECT
        kc.server_id as server_id,
        kc.datid as datid,
        sample_db.datname as dbname,
        kc.userid as userid,
        kc.queryid as queryid,
        kc.queryid_md5 as queryid_md5,
        sum(kc.user_time) as user_time,
        (sum(kc.user_time)*100/NULLIF(min(tot.user_time),0))::float AS user_time_pct,
        sum(kc.system_time) as system_time,
        (sum(kc.system_time)*100/NULLIF(min(tot.system_time),0))::float AS system_time_pct,
        sum(kc.minflts)::bigint as minflts,
        sum(kc.majflts)::bigint as majflts,
        sum(kc.nswaps)::bigint as nswaps,
        sum(kc.reads)::bigint as reads,
        sum(kc.writes)::bigint as writes,
        sum(kc.msgsnds)::bigint as msgsnds,
        sum(kc.msgrcvs)::bigint as msgrcvs,
        sum(kc.nsignals)::bigint as nsignals,
        sum(kc.nvcsws)::bigint as nvcsws,
        sum(kc.nivcsws)::bigint as nivcsws,
        (sum(kc.reads)*100/NULLIF(min(tot.reads),0))::float AS reads_total_pct,
        (sum(kc.writes)*100/NULLIF(min(tot.writes),0))::float AS writes_total_pct
   FROM v_sample_kcache kc
        -- Database name
        JOIN sample_stat_database sample_db
        ON (kc.server_id=sample_db.server_id AND kc.sample_id=sample_db.sample_id AND kc.datid=sample_db.datid)
        /* Start sample existance condition
        Start sample stats does not account in report, but we must be sure
        that start sample exists, as it is reference point of next sample
        */
        JOIN samples sample_s ON (kc.server_id = sample_s.server_id AND sample_s.sample_id = start_id)
        /* End sample existance condition
        Make sure that end sample exists, so we really account full interval
        */
        JOIN samples sample_e ON (kc.server_id = sample_e.server_id AND sample_e.sample_id = end_id)
        -- Total stats
        CROSS JOIN tot
    WHERE kc.server_id = sserver_id AND kc.sample_id BETWEEN sample_s.sample_id + 1 AND sample_e.sample_id
    GROUP BY kc.server_id,kc.datid,sample_db.datname,kc.userid,kc.queryid,kc.queryid_md5
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION top_cpu_time_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by epapsed time
    c_elapsed_time CURSOR FOR
    SELECT
        kc.queryid_md5 as queryid,
        kc.dbname,
        kc.user_time as user_time,
        kc.user_time_pct as user_time_pct,
        kc.system_time as system_time,
        kc.system_time_pct as system_time_pct
    FROM top_kcache_statements(sserver_id, start_id, end_id) kc
    WHERE least(kc.user_time,kc.system_time) > 0
    ORDER BY (kc.user_time + kc.system_time) DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
        '<tr>'
          '<th>Query ID</th>'
          '<th>Database</th>'
          '<th title="User CPU time elapsed">User time(s)</th>'
          '<th title="User CPU time elapsed by this statement as a percentage of total user CPU time">%Total</th>'
          '<th title="System CPU time elapsed">System time(s)</th>'
          '<th title="System CPU time elapsed by this statement as a percentage of total system CPU time">%Total</th>'
        '</tr>'
        '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.user_time AS numeric),2),
            round(CAST(r_result.user_time_pct AS numeric),2),
            round(CAST(r_result.system_time AS numeric),2),
            round(CAST(r_result.system_time_pct AS numeric),2)
        );
        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_cpu_time_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by elapsed time
    c_elapsed_time CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(kc1.queryid_md5,kc2.queryid_md5) as queryid,
        COALESCE(kc1.dbname,kc2.dbname) as dbname,
        kc1.user_time as user_time1,
        kc1.user_time_pct as user_time_pct1,
        kc1.system_time as system_time1,
        kc1.system_time_pct as system_time_pct1,
        kc2.user_time as user_time2,
        kc2.user_time_pct as user_time_pct2,
        kc2.system_time as system_time2,
        kc2.system_time_pct as system_time_pct2,
        row_number() over (ORDER BY kc1.user_time+kc1.system_time DESC NULLS LAST) as time1,
        row_number() over (ORDER BY kc2.user_time+kc2.system_time DESC NULLS LAST) as time2
    FROM top_kcache_statements(sserver_id, start1_id, end1_id) kc1
        FULL OUTER JOIN top_kcache_statements(sserver_id, start2_id, end2_id) kc2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(kc1.user_time,0) + COALESCE(kc2.user_time,0) + COALESCE(kc1.system_time,0) + COALESCE(kc2.system_time,0) > 0
    ORDER BY COALESCE(kc1.user_time,0) + COALESCE(kc2.user_time,0) + COALESCE(kc1.system_time,0) + COALESCE(kc2.system_time,0) DESC ) t1
    WHERE time1 <= topn OR time2 <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
        '<tr>'
          '<th>Query ID</th>'
          '<th>Database</th>'
          '<th>I</th>'
          '<th title="User CPU time elapsed">User time(s)</th>'
          '<th title="User CPU time elapsed by this statement as a percentage of total user CPU time">%Total</th>'
          '<th title="System CPU time elapsed">System time(s)</th>'
          '<th title="System CPU time elapsed by this statement as a percentage of total system CPU time">%Total</th>'
        '</tr>'
        '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
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
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            round(CAST(r_result.user_time1 AS numeric),2),
            round(CAST(r_result.user_time_pct1 AS numeric),2),
            round(CAST(r_result.system_time1 AS numeric),2),
            round(CAST(r_result.system_time_pct1 AS numeric),2),
            round(CAST(r_result.user_time2 AS numeric),2),
            round(CAST(r_result.user_time_pct2 AS numeric),2),
            round(CAST(r_result.system_time2 AS numeric),2),
            round(CAST(r_result.system_time_pct2 AS numeric),2)
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_io_filesystem_htbl(IN jreportset jsonb, IN sserver_id integer, IN start_id integer, IN end_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by epapsed time
    c_elapsed_time CURSOR FOR
    SELECT
        kc.queryid_md5 as queryid,
        kc.dbname,
        kc.reads as reads,
        kc.reads_total_pct as reads_total_pct,
        kc.writes  as writes,
        kc.writes_total_pct as writes_total_pct
    FROM top_kcache_statements(sserver_id, start_id, end_id) kc
    WHERE kc.reads + kc.writes > 0
    ORDER BY kc.reads + kc.writes DESC
    LIMIT topn;

    r_result RECORD;
BEGIN
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th title="Filesystem read amount">Reads</th>'
            '<th title="Filesystem read amount of this statement as a percentage of all statements FS read amount">%Total</th>'
            '<th title="Filesystem write amount">Writes</th>'
            '<th title="Filesystem write amount of this statement as a percentage of all statements FS write amount">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr>'
          '<td {mono}><a HREF="#%s">%s</a></td>'
          '<td>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
          '<td {value}>%s</td>'
        '</tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);
    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            pg_size_pretty(r_result.reads),
            round(CAST(r_result.reads_total_pct AS numeric),2),
            pg_size_pretty(r_result.writes),
            round(CAST(r_result.writes_total_pct AS numeric),2)
        );
        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION top_io_filesystem_diff_htbl(IN jreportset jsonb, IN sserver_id integer, IN start1_id integer, IN end1_id integer,
    IN start2_id integer, IN end2_id integer, IN topn integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for top(cnt) queries ordered by epapsed time
    c_elapsed_time CURSOR FOR
    SELECT * FROM (SELECT
        COALESCE(kc1.queryid_md5,kc2.queryid_md5) as queryid,
        COALESCE(kc1.dbname,kc2.dbname) as dbname,
        kc1.reads as reads1,
        kc1.reads_total_pct as reads_total_pct1,
        kc1.writes  as writes1,
        kc1.writes_total_pct as writes_total_pct1,
        kc2.reads as reads2,
        kc2.reads_total_pct as reads_total_pct2,
        kc2.writes as writes2,
        kc2.writes_total_pct as writes_total_pct2,
        row_number() over (ORDER BY kc1.reads + kc1.writes DESC NULLS LAST) as io_count1,
        row_number() over (ORDER BY kc2.reads + kc2.writes  DESC NULLS LAST) as io_count2
    FROM top_kcache_statements(sserver_id, start1_id, end1_id) kc1
        FULL OUTER JOIN top_kcache_statements(sserver_id, start2_id, end2_id) kc2 USING (server_id, datid, userid, queryid_md5)
    WHERE COALESCE(kc1.writes,0) + COALESCE(kc2.writes,0) + COALESCE(kc1.reads,0) + COALESCE(kc2.reads,0) > 0
    ORDER BY COALESCE(kc1.writes,0) + COALESCE(kc2.writes,0) + COALESCE(kc1.reads,0) + COALESCE(kc2.reads,0) DESC ) t1
    WHERE io_count1 <= topn OR io_count2 <= topn;

    r_result RECORD;
BEGIN
    -- Elapsed time sorted list TPLs
    jtab_tpl := jsonb_build_object(
      'tab_hdr',
        '<table>'
          '<tr>'
            '<th>Query ID</th>'
            '<th>Database</th>'
            '<th>I</th>'
            '<th title="Filesystem read amount">Reads</th>'
            '<th title="Filesystem read amount of this statement as a percentage of all statements FS read amount">%Total</th>'
            '<th title="Filesystem write amount">Writes</th>'
            '<th title="Filesystem write amount of this statement as a percentage of all statements FS write amount">%Total</th>'
          '</tr>'
          '{rows}'
        '</table>',
      'stmt_tpl',
        '<tr {interval1}>'
          '<td {rowtdspanhdr_mono}><a HREF="#%s">%s</a></td>'
          '<td {rowtdspanhdr}>%s</td>'
          '<td {label} {title1}>1</td>'
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
        '</tr>'
        '<tr style="visibility:collapse"></tr>');
    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting on top queries by elapsed time
    FOR r_result IN c_elapsed_time LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['stmt_tpl'],
            r_result.queryid,
            r_result.queryid,
            r_result.dbname,
            pg_size_pretty(r_result.reads1),
            round(CAST(r_result.reads_total_pct1 AS numeric),2),
            pg_size_pretty(r_result.writes1),
            round(CAST(r_result.writes_total_pct1 AS numeric),2),
            pg_size_pretty(r_result.reads2),
            round(CAST(r_result.reads_total_pct2 AS numeric),2),
            pg_size_pretty(r_result.writes2),
            round(CAST(r_result.writes_total_pct2 AS numeric),2)
        );

        PERFORM collect_queries(r_result.queryid);
    END LOOP;

    IF report != '' THEN
        RETURN replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    ELSE
        RETURN '';
    END IF;
END;
$$ LANGUAGE plpgsql;

