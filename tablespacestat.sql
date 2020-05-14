/* ===== Tables stats functions ===== */

CREATE OR REPLACE FUNCTION tablespace_stats(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    node_id integer,
    tablespaceid oid,
    tablespacename name,
    tablespacepath text,
    size bigint,
    size_delta bigint
) SET search_path=@extschema@,public AS $$
    SELECT
        st.node_id,
        st.tablespaceid,
        st.tablespacename,
        st.tablespacepath,
        sum(st.size)::bigint AS size,
        sum(st.size_delta)::bigint AS size_delta
    FROM v_snap_stat_tablespaces st

        /* Start snapshot existance condition
        Start snapshot stats does not account in report, but we must be sure
        that start snapshot exists, as it is reference point of next snapshot
        */
        JOIN snapshots snap_s ON (st.node_id = snap_s.node_id AND snap_s.snap_id = start_id)
        /* End snapshot existance condition
        Make sure that end snapshot exists, so we really account full interval
        */
        JOIN snapshots snap_e ON (st.node_id = snap_e.node_id AND snap_e.snap_id = end_id)
    WHERE st.node_id = snode_id
      AND st.snap_id BETWEEN snap_s.snap_id + 1 AND snap_e.snap_id
    GROUP BY st.node_id, st.tablespaceid, st.tablespacename, st.tablespacepath
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION tablespaces_stats_htbl(IN jreportset jsonb, IN snode_id integer, IN start_id integer, IN end_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        tablespacename,
        tablespacepath,
        pg_size_pretty(size) as size,
        pg_size_pretty(size_delta) as size_delta
    FROM tablespace_stats(snode_id,start_id,end_id);

    r_result RECORD;
BEGIN
       --- Populate templates

    jtab_tpl := jsonb_build_object(
      'tab_hdr','<table><tr><th>Tablespace</th><th>Path</th><th>Size</th><th>Growth</th></tr>{rows}</table>',
      'ts_tpl','<tr><td>%s</td><td {value}>%s</td><td {value}>%s</td><td {value}>%s</td></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting table stats
    FOR r_result IN c_tbl_stats LOOP
          report := report||format(
              jtab_tpl #>> ARRAY['ts_tpl'],
              r_result.tablespacename,
              r_result.tablespacepath,
              r_result.size,
              r_result.size_delta
          );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;


    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tablespaces_stats_diff_htbl(IN jreportset jsonb, IN snode_id integer, IN start1_id integer, IN end1_id integer,
IN start2_id integer, IN end2_id integer) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    report text := '';
    jtab_tpl    jsonb;

    --Cursor for stats
    c_tbl_stats CURSOR FOR
    SELECT
        COALESCE(stat1.tablespacename,stat2.tablespacename) AS tablespacename,
        COALESCE(stat1.tablespacepath,stat2.tablespacepath) AS tablespacepath,
        pg_size_pretty(stat1.size) as size1,
        pg_size_pretty(stat2.size) as size2,
        pg_size_pretty(stat1.size_delta) as size_delta1,
        pg_size_pretty(stat2.size_delta) as size_delta2
    FROM tablespace_stats(snode_id,start1_id,end1_id) stat1
        FULL OUTER JOIN tablespace_stats(snode_id,start2_id,end2_id) stat2 USING (node_id,tablespaceid);

    r_result RECORD;
BEGIN
     -- Tablespace stats template
     jtab_tpl := jsonb_build_object(
      'tab_hdr','<table {difftbl}><tr><th>Tablespace</th><th>Path</th><th>I</th><th>Size</th><th>Growth</th></tr>{rows}</table>',
      'ts_tpl','<tr {interval1}><td {rowtdspanhdr}>%s</td><td {rowtdspanhdr_mono}>%s</td><td {label} {title1}>1</td><td {value}>%s</td><td {value}>%s</td></tr>'||
        '<tr {interval2}><td {label} {title2}>2</td><td {value}>%s</td><td {value}>%s</td></tr><tr style="visibility:collapse"></tr>');

    -- apply settings to templates
    jtab_tpl := jsonb_replace(jreportset #> ARRAY['htbl'], jtab_tpl);

    -- Reporting summary databases stats
    FOR r_result IN c_tbl_stats LOOP
        report := report||format(
            jtab_tpl #>> ARRAY['ts_tpl'],
            r_result.tablespacename,
            r_result.tablespacepath,
            r_result.size1,
            r_result.size_delta1,
            r_result.size2,
            r_result.size_delta2
        );
    END LOOP;

    IF report != '' THEN
        report := replace(jtab_tpl #>> ARRAY['tab_hdr'],'{rows}',report);
    END IF;
    RETURN report;

END;
$$ LANGUAGE plpgsql;
