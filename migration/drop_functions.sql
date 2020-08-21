/* Drop all previous functions and create new functions */
DO LANGUAGE plpgsql
$$DECLARE
    func_drop_sql   record;
BEGIN
FOR func_drop_sql IN (SELECT 'drop function '||proc.pronamespace::regnamespace||'.'||proc.proname||'('||pg_get_function_identity_arguments(proc.oid)||');' AS query
    FROM pg_depend dep
        JOIN pg_extension ext ON (dep.refobjid = ext.oid)
        JOIN pg_proc proc ON (proc.oid = dep.objid)
    WHERE ext.extname='{pg_profile}' AND dep.deptype='e' AND dep.classid='pg_proc'::regclass)
LOOP
    EXECUTE func_drop_sql.query;
END LOOP;
END$$;
