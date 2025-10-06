CREATE FUNCTION log_sample_timings(server_properties jsonb, sampling_event text, exec_point text)
    RETURNS jsonb SET search_path=@extschema@
AS $function$
BEGIN
    IF (server_properties #>> '{collect_timings}')::boolean THEN
        server_properties :=
            jsonb_set(
                server_properties,
                '{timings}',
                coalesce(server_properties -> 'timings', '[]'::jsonb) ||
                    jsonb_build_object(
                        'sampling_event', sampling_event,
                        'exec_point', exec_point,
                        'event_tm', clock_timestamp()));
    END IF;
    return server_properties;
END;
$function$ LANGUAGE plpgsql immutable;
COMMENT ON FUNCTION log_sample_timings(server_properties jsonb, sampling_event text, exec_point text) IS
  'log event to sample_timings';