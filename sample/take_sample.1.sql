CREATE FUNCTION take_sample(IN server name, IN skip_sizes boolean = NULL)
RETURNS TABLE (
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
DECLARE
    sserver_id          integer;
    server_sampleres    integer;
    etext               text := '';
    edetail             text := '';
    econtext            text := '';

    qres                record;
    conname             text;
    start_clock         timestamp (2) with time zone;
BEGIN
    SELECT server_id INTO sserver_id FROM servers WHERE server_name = take_sample.server;
    IF sserver_id IS NULL THEN
        RAISE 'Server not found';
    ELSE
        /*
        * We should include dblink schema to perform disconnections
        * on exception conditions
        */
        SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
        IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
          EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
        END IF;

        BEGIN
            start_clock := clock_timestamp()::timestamp (2) with time zone;
            server_sampleres := take_sample(sserver_id, take_sample.skip_sizes);
            elapsed := clock_timestamp()::timestamp (2) with time zone - start_clock;
            CASE server_sampleres
              WHEN 0 THEN
                result := 'OK';
              ELSE
                result := 'FAIL';
            END CASE;
            RETURN NEXT;
        EXCEPTION
            WHEN OTHERS THEN
                BEGIN
                    GET STACKED DIAGNOSTICS etext = MESSAGE_TEXT,
                        edetail = PG_EXCEPTION_DETAIL,
                        econtext = PG_EXCEPTION_CONTEXT;
                    result := format (E'%s\n%s\n%s', etext, econtext, edetail);
                    elapsed := clock_timestamp()::timestamp (2) with time zone - start_clock;
                    RETURN NEXT;
                    /*
                      Cleanup dblink connections
                    */
                    FOREACH conname IN ARRAY
                        coalesce(dblink_get_connections(), array[]::text[])
                    LOOP
                        IF conname IN ('server_connection', 'server_db_connection') THEN
                            PERFORM dblink_disconnect(conname);
                        END IF;
                    END LOOP;
                END;
        END;
    END IF;
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION take_sample(IN server name, IN skip_sizes boolean) IS
  'Statistics sample creation function (by server name)';
