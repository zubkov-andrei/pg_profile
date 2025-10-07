CREATE FUNCTION delete_obsolete_samples(IN sserver_id integer, IN ssample_id integer
) RETURNS void SET search_path=@extschema@ AS $$
declare
    ret    integer;
begin
    -- Updating dictionary tables setting last_sample_id
    UPDATE tablespaces_list utl SET last_sample_id = ssample_id - 1
    FROM tablespaces_list tl LEFT JOIN sample_stat_tablespaces cur
      ON (cur.server_id, cur.sample_id, cur.tablespaceid) =
        (sserver_id, ssample_id, tl.tablespaceid)
    WHERE
      tl.last_sample_id IS NULL AND
      (utl.server_id, utl.tablespaceid) = (sserver_id, tl.tablespaceid) AND
      tl.server_id = sserver_id AND cur.server_id IS NULL;

    UPDATE funcs_list ufl SET last_sample_id = ssample_id - 1
    FROM funcs_list fl LEFT JOIN sample_stat_user_functions cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.funcid) =
        (sserver_id, ssample_id, fl.datid, fl.funcid)
    WHERE
      fl.last_sample_id IS NULL AND
      fl.server_id = sserver_id AND cur.server_id IS NULL AND
      (ufl.server_id, ufl.datid, ufl.funcid) =
      (sserver_id, fl.datid, fl.funcid);

    UPDATE indexes_list uil SET last_sample_id = ssample_id - 1
    FROM indexes_list il LEFT JOIN sample_stat_indexes cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.indexrelid) =
        (sserver_id, ssample_id, il.datid, il.indexrelid)
    WHERE
      il.last_sample_id IS NULL AND
      il.server_id = sserver_id AND cur.server_id IS NULL AND
      (uil.server_id, uil.datid, uil.indexrelid) =
      (sserver_id, il.datid, il.indexrelid);

    UPDATE tables_list utl SET last_sample_id = ssample_id - 1
    FROM tables_list tl LEFT JOIN sample_stat_tables cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.relid) =
        (sserver_id, ssample_id, tl.datid, tl.relid)
    WHERE
      tl.last_sample_id IS NULL AND
      tl.server_id = sserver_id AND cur.server_id IS NULL AND
      (utl.server_id, utl.datid, utl.relid) =
      (sserver_id, tl.datid, tl.relid);

    UPDATE stmt_list slu SET last_sample_id = ssample_id - 1
    FROM sample_statements ss RIGHT JOIN stmt_list sl
      ON (ss.server_id, ss.sample_id, ss.queryid_md5) =
        (sserver_id, ssample_id, sl.queryid_md5)
    WHERE
      sl.server_id = sserver_id AND
      sl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (slu.server_id, slu.queryid_md5) = (sserver_id, sl.queryid_md5);

    UPDATE roles_list rlu SET last_sample_id = ssample_id - 1
    FROM
        sample_statements ss
      RIGHT JOIN roles_list rl
      ON (ss.server_id, ss.sample_id, ss.userid) =
        (sserver_id, ssample_id, rl.userid)
    WHERE
      rl.server_id = sserver_id AND
      rl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (rlu.server_id, rlu.userid) = (sserver_id, rl.userid);

    -- Deleting obsolete baselines
    DELETE FROM baselines
    WHERE keep_until < now()
      AND server_id = sserver_id;

    -- Getting max_sample_age setting
    BEGIN
        ret := COALESCE(current_setting('{pg_profile}.max_sample_age')::integer);
    EXCEPTION
        WHEN OTHERS THEN ret := 7;
    END;

    -- Deleting obsolete samples
    PERFORM num_nulls(min(s.sample_id),max(s.sample_id)) > 0 OR
      delete_samples(sserver_id, min(s.sample_id), max(s.sample_id)) > 0
    FROM samples s JOIN
      servers n USING (server_id)
    WHERE s.server_id = sserver_id
        AND s.sample_time < now() - (COALESCE(n.max_sample_age,ret) || ' days')::interval
        AND (s.server_id,s.sample_id) NOT IN (SELECT server_id,sample_id FROM bl_samples WHERE server_id = sserver_id);
end;
$$ LANGUAGE plpgsql;