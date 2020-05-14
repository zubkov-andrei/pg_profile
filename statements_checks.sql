/* ===== pg_stat_statements checks ===== */

CREATE OR REPLACE FUNCTION check_stmt_cnt(IN snode_id integer, IN start_id integer = 0, IN end_id integer = 0) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tab_tpl CONSTANT text := '<table><tr><th>Snapshot ID</th><th>Snapshot Time</th><th>Stmts Captured</th><th>pg_stat_statements.max</th></tr>{rows}</table>';
    row_tpl CONSTANT text := '<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>';

    report text := '';

    c_stmt_all_stats CURSOR FOR
    SELECT snap_id,snap_time,stmt_cnt,prm.setting AS max_cnt
    FROM snapshots
        JOIN (
            SELECT snap_id,sum(statements) stmt_cnt
            FROM snap_statements_total
            WHERE node_id = snode_id
            GROUP BY snap_id
        ) snap_stmt_cnt USING(snap_id)
        JOIN v_snap_settings prm USING (node_id, snap_id)
    WHERE node_id = snode_id AND prm.name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer);

    c_stmt_stats CURSOR (s_id integer, e_id integer) FOR
    SELECT snap_id,snap_time,stmt_cnt,prm.setting AS max_cnt
    FROM snapshots
        JOIN (
            SELECT snap_id,sum(statements) stmt_cnt
            FROM snap_statements_total
            WHERE node_id = snode_id AND snap_id BETWEEN s_id + 1 AND e_id
            GROUP BY snap_id
        ) snap_stmt_cnt USING(snap_id)
        JOIN v_snap_settings prm USING (node_id,snap_id)
    WHERE node_id = snode_id AND prm.name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer);

    r_result RECORD;
BEGIN
    IF start_id = 0 THEN
        FOR r_result IN c_stmt_all_stats LOOP
            report := report||format(
                row_tpl,
                r_result.snap_id,
                r_result.snap_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    ELSE
        FOR r_result IN c_stmt_stats(start_id,end_id) LOOP
            report := report||format(
                row_tpl,
                r_result.snap_id,
                r_result.snap_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    END IF;

    IF report != '' THEN
        report := replace(tab_tpl,'{rows}',report);
    END IF;

    RETURN  report;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION check_stmt_all_setting(IN snode_id integer, IN start_id integer, IN end_id integer)
RETURNS integer SET search_path=@extschema@,public AS $$
    SELECT count(1)::integer
    FROM v_snap_settings
    WHERE node_id = snode_id AND name = 'pg_stat_statements.track'
        AND setting = 'all' AND snap_id BETWEEN start_id + 1 AND end_id;
$$ LANGUAGE sql;
