/*===== Relation storage parameters reporting functions =====*/
CREATE FUNCTION table_storage_parameters_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer = NULL, IN end2_id integer = NULL)
RETURNS TABLE (
    dbname      name,
    dbid        oid,
    schemaname  name,
    relname     name,
    relid       oid,
    reloptions  jsonb,
    first_seen  text,
    ord_param   integer -- report header ordering
  )
SET search_path=@extschema@ AS $$
  SELECT
    evd.dbname,
    evd.dbid,
    evd.schemaname,
    evd.relname,
    evd.relid,
    evd.reloptions,
    CASE
      WHEN s_first.sample_id IS NOT NULL AND
        s_first.sample_id > least(start1_id, start2_id) OR
        evd.last_sample_id IS NOT NULL AND
        evd.last_sample_id < greatest(end1_id, end2_id)
      THEN evd.first_seen::text
      ELSE NULL
    END as first_seen,
    row_number() over (order by evd.dbname, evd.schemaname, evd.relname, evd.first_seen)::integer as ord_param
  FROM (
    SELECT DISTINCT
      ssd.datname as dbname,
      tsp.datid as dbid,
      tl.schemaname,
      tl.relname,
      tsp.relid,
      tsp.first_seen,
      tsp.last_sample_id,
      tsp.reloptions
    FROM v_table_storage_parameters tsp
    JOIN tables_list tl USING (server_id, datid, relid)
    JOIN sample_stat_database ssd USING (server_id, datid, sample_id)
    WHERE
      tsp.server_id = sserver_id AND
      (tsp.sample_id BETWEEN start1_id AND end1_id OR
      tsp.sample_id BETWEEN start2_id AND end2_id)
    ) evd
    LEFT JOIN samples s_first ON (s_first.server_id, s_first.sample_time) = (sserver_id, evd.first_seen)
$$ LANGUAGE sql;

CREATE FUNCTION index_storage_parameters_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer = NULL, IN end2_id integer = NULL)
RETURNS TABLE (
    dbname        name,
    dbid          oid,
    schemaname    name,
    relname       name,
    relid         oid,
    indexrelname  name,
    indexrelid    oid,
    reloptions    jsonb,
    first_seen    text,
    ord_param     integer -- report header ordering
  )
SET search_path=@extschema@ AS $$
  SELECT
    evd.dbname,
    evd.dbid,
    evd.schemaname,
    evd.relname,
    evd.relid,
    evd.indexrelname,
    evd.indexrelid,
    evd.reloptions,
    CASE
      WHEN s_first.sample_id IS NOT NULL AND
        s_first.sample_id > least(start1_id, start2_id) OR
        evd.last_sample_id IS NOT NULL AND
        evd.last_sample_id < greatest(end1_id, end2_id)
      THEN evd.first_seen::text
      ELSE NULL
    END as first_seen,
    row_number() over (order by evd.dbname, evd.schemaname, evd.relname, evd.indexrelname, evd.first_seen)::integer as ord_param
  FROM (
    SELECT DISTINCT
      ssd.datname as dbname,
      isp.datid as dbid,
      tl.schemaname,
      tl.relname,
      isp.relid,
      il.indexrelname,
      isp.indexrelid,
      isp.first_seen,
      isp.last_sample_id,
      isp.reloptions
    FROM v_index_storage_parameters isp
    JOIN tables_list tl USING (server_id, datid, relid)
    JOIN indexes_list il USING (server_id, datid, relid, indexrelid)
    JOIN sample_stat_database ssd USING (server_id, datid, sample_id)
    WHERE
      isp.server_id = sserver_id AND
      (isp.sample_id BETWEEN start1_id AND end1_id OR
      isp.sample_id BETWEEN start2_id AND end2_id)
    ) evd
    LEFT JOIN samples s_first ON (s_first.server_id, s_first.sample_time) = (sserver_id, evd.first_seen)
$$ LANGUAGE sql;
