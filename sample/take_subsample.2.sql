CREATE FUNCTION take_subsample() RETURNS TABLE (
    server      name,
    result      text,
    elapsed     interval day to second (2)
)
SET search_path=@extschema@ AS $$
  SELECT * FROM take_subsample_subset(1,0);
$$ LANGUAGE sql;

COMMENT ON FUNCTION take_subsample() IS 'Subsample taking function (for all enabled servers).';
