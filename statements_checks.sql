/* ===== pg_stat_statements checks ===== */

CREATE OR REPLACE FUNCTION check_stmt_cnt(IN sserver_id integer, IN start_id integer = 0, IN end_id integer = 0) RETURNS text SET search_path=@extschema@,public AS $$
DECLARE
    tab_tpl CONSTANT text :=
      '<table>'
        '<tr>'
          '<th>Sample ID</th>'
          '<th>Sample Time</th>'
          '<th>Stmts Captured</th>'
          '<th>pg_stat_statements.max</th>'
        '</tr>'
        '{rows}'
      '</table>';
    row_tpl CONSTANT text :=
      '<tr>'
        '<td>%s</td>'
        '<td>%s</td>'
        '<td>%s</td>'
        '<td>%s</td>'
      '</tr>';

    report text := '';

    c_stmt_all_stats CURSOR FOR
    SELECT sample_id,sample_time,stmt_cnt,prm.setting AS max_cnt
    FROM samples
        JOIN (
            SELECT sample_id,sum(statements) stmt_cnt
            FROM sample_statements_total
            WHERE server_id = sserver_id
            GROUP BY sample_id
        ) sample_stmt_cnt USING(sample_id)
        JOIN v_sample_settings prm USING (server_id, sample_id)
    WHERE server_id = sserver_id AND prm.name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer)
    ORDER BY sample_id ASC;

    c_stmt_stats CURSOR (s_id integer, e_id integer) FOR
    SELECT sample_id,sample_time,stmt_cnt,prm.setting AS max_cnt
    FROM samples
        JOIN (
            SELECT sample_id,sum(statements) stmt_cnt
            FROM sample_statements_total
            WHERE server_id = sserver_id AND sample_id BETWEEN s_id + 1 AND e_id
            GROUP BY sample_id
        ) sample_stmt_cnt USING(sample_id)
        JOIN v_sample_settings prm USING (server_id,sample_id)
    WHERE server_id = sserver_id AND prm.name='pg_stat_statements.max' AND stmt_cnt >= 0.9*cast(prm.setting AS integer)
    ORDER BY sample_id ASC;

    r_result RECORD;
BEGIN
    IF start_id = 0 THEN
        FOR r_result IN c_stmt_all_stats LOOP
            report := report||format(
                row_tpl,
                r_result.sample_id,
                r_result.sample_time,
                r_result.stmt_cnt,
                r_result.max_cnt
            );
        END LOOP;
    ELSE
        FOR r_result IN c_stmt_stats(start_id,end_id) LOOP
            report := report||format(
                row_tpl,
                r_result.sample_id,
                r_result.sample_time,
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

CREATE OR REPLACE FUNCTION check_stmt_all_setting(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS integer SET search_path=@extschema@,public AS $$
    SELECT count(1)::integer
    FROM v_sample_settings
    WHERE server_id = sserver_id AND name = 'pg_stat_statements.track'
        AND setting = 'all' AND sample_id BETWEEN start_id + 1 AND end_id;
$$ LANGUAGE sql;
