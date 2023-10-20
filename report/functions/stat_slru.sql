CREATE FUNCTION cluster_stat_slru(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id     integer,
    name          text,
    blks_zeroed   bigint,
    blks_hit      bigint,
    blks_read     bigint,
    blks_written  bigint,
    blks_exists   bigint,
    flushes       bigint,
    truncates     bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.name AS name,
        SUM(blks_zeroed)::bigint AS blks_zeroed,
        SUM(blks_hit)::bigint AS blks_hit,
        SUM(blks_read)::bigint AS blks_read,
        SUM(blks_written)::bigint AS blks_written,
        SUM(blks_exists)::bigint AS blks_exists,
        SUM(flushes)::bigint AS flushes,
        SUM(truncates)::bigint AS truncates
    FROM sample_stat_slru st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.name
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_slru_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    name          text,

    blks_zeroed   bigint,
    blks_hit      bigint,
    blks_read     bigint,
    hit_pct       numeric,
    blks_written  bigint,
    blks_exists   bigint,
    flushes       bigint,
    truncates     bigint
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(name, 'Total') AS name,

    NULLIF(SUM(blks_zeroed), 0) AS blks_zeroed,
    NULLIF(SUM(blks_hit), 0) AS blks_hit,
    NULLIF(SUM(blks_read), 0) AS blks_read,
    ROUND(NULLIF(SUM(blks_hit), 0)::numeric * 100 /
      NULLIF(COALESCE(SUM(blks_hit), 0) + COALESCE(SUM(blks_read), 0), 0), 2)
      AS hit_pct,
    NULLIF(SUM(blks_written), 0) AS blks_written,
    NULLIF(SUM(blks_exists), 0) AS blks_exists,
    NULLIF(SUM(flushes), 0) AS flushes,
    NULLIF(SUM(truncates), 0) AS truncates

  FROM cluster_stat_slru(sserver_id, start_id, end_id)
  GROUP BY ROLLUP(name)
  ORDER BY NULLIF(name, 'Total') ASC NULLS LAST
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_slru_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    name          text,

    blks_zeroed1  bigint,
    blks_hit1     bigint,
    blks_read1    bigint,
    hit_pct1      numeric,
    blks_written1 bigint,
    blks_exists1  bigint,
    flushes1      bigint,
    truncates1    bigint,

    blks_zeroed2  bigint,
    blks_hit2     bigint,
    blks_read2    bigint,
    hit_pct2      numeric,
    blks_written2 bigint,
    blks_exists2  bigint,
    flushes2      bigint,
    truncates2    bigint
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(name, 'Total') AS name,

    NULLIF(SUM(st1.blks_zeroed), 0) AS blks_zeroed1,
    NULLIF(SUM(st1.blks_hit), 0) AS blks_hit1,
    NULLIF(SUM(st1.blks_read), 0) AS blks_read1,
    ROUND(NULLIF(SUM(st1.blks_hit), 0)::numeric * 100 /
      NULLIF(COALESCE(SUM(st1.blks_hit), 0) + COALESCE(SUM(st1.blks_read), 0), 0), 2)
      AS hit_pct1,
    NULLIF(SUM(st1.blks_written), 0) AS blks_written1,
    NULLIF(SUM(st1.blks_exists), 0) AS blks_exists1,
    NULLIF(SUM(st1.flushes), 0) AS flushes1,
    NULLIF(SUM(st1.truncates), 0) AS truncates1,

    NULLIF(SUM(st2.blks_zeroed), 0) AS blks_zeroed2,
    NULLIF(SUM(st2.blks_hit), 0) AS blks_hit2,
    NULLIF(SUM(st2.blks_read), 0) AS blks_read2,
    ROUND(NULLIF(SUM(st2.blks_hit), 0)::numeric * 100 /
      NULLIF(COALESCE(SUM(st2.blks_hit), 0) + COALESCE(SUM(st2.blks_read), 0), 0), 2)
      AS hit_pct2,
    NULLIF(SUM(st2.blks_written), 0) AS blks_written2,
    NULLIF(SUM(st2.blks_exists), 0) AS blks_exists2,
    NULLIF(SUM(st2.flushes), 0) AS flushes2,
    NULLIF(SUM(st2.truncates), 0) AS truncates2
    
  FROM cluster_stat_slru(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN cluster_stat_slru(sserver_id, start2_id, end2_id) st2
    USING (server_id, name)
  GROUP BY ROLLUP(name)
  ORDER BY NULLIF(name, 'Total') ASC NULLS LAST
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_slru_resets(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id     integer,
    sample_id     integer,
    name          text,
    stats_reset   timestamp with time zone
)
SET search_path=@extschema@ AS $$
    WITH first_val AS (
      SELECT name, stats_reset
      FROM sample_stat_slru st JOIN (
         SELECT name, MIN(sample_id) AS sample_id
         FROM sample_stat_slru
         WHERE server_id = sserver_id AND
           sample_id BETWEEN start_id AND end_id
         GROUP BY name
       ) f USING (name,sample_id)
     WHERE st.server_id = sserver_id
    )
    SELECT
      server_id,
      min(sample_id),
      name,
      st.stats_reset
    FROM sample_stat_slru st JOIN first_val USING (name)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
      AND st.stats_reset IS DISTINCT FROM first_val.stats_reset
    GROUP BY server_id, name, st.stats_reset
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_slru_reset_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    sample_id     integer,
    name          text,
    stats_reset  timestamp with time zone
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    name,
    stats_reset
  FROM cluster_stat_slru_resets(sserver_id, start_id, end_id)
  ORDER BY sample_id ASC
$$ LANGUAGE sql;

CREATE FUNCTION cluster_stat_slru_reset_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer, IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    sample_id     integer,
    name          text,
    stats_reset  timestamp with time zone
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    name,
    stats_reset
  FROM (
    SELECT
      sample_id,
      name,
      stats_reset
    FROM cluster_stat_slru_resets(sserver_id, start1_id, end1_id)
    UNION
    SELECT
      sample_id,
      name,
      stats_reset
    FROM cluster_stat_slru_resets(sserver_id, start2_id, end2_id)
    ) st
  ORDER BY sample_id ASC
$$ LANGUAGE sql;
