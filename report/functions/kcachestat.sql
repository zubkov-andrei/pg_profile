/* ===== Statements stats functions ===== */

CREATE FUNCTION top_kcache_statements(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id                integer,
    datid                    oid,
    dbname                   name,
    userid                   oid,
    username                 name,
    queryid                  bigint,
    toplevel                 boolean,
    exec_user_time           double precision, --  User CPU time used
    user_time                double precision, --  User CPU time used percentage
    exec_system_time         double precision, --  System CPU time used
    system_time              double precision, --  System CPU time used percentage
    exec_minflts             bigint, -- Number of page reclaims (soft page faults)
    exec_majflts             bigint, -- Number of page faults (hard page faults)
    exec_nswaps              bigint, -- Number of swaps
    exec_reads               bigint, -- Number of bytes read by the filesystem layer
    exec_writes              bigint, -- Number of bytes written by the filesystem layer
    exec_msgsnds             bigint, -- Number of IPC messages sent
    exec_msgrcvs             bigint, -- Number of IPC messages received
    exec_nsignals            bigint, -- Number of signals received
    exec_nvcsws              bigint, -- Number of voluntary context switches
    exec_nivcsws             bigint,
    reads                    numeric,
    writes                   numeric,
    plan_user_time           double precision, --  User CPU time used
    plan_system_time         double precision, --  System CPU time used
    plan_minflts             bigint, -- Number of page reclaims (soft page faults)
    plan_majflts             bigint, -- Number of page faults (hard page faults)
    plan_nswaps              bigint, -- Number of swaps
    plan_reads               bigint, -- Number of bytes read by the filesystem layer
    plan_writes              bigint, -- Number of bytes written by the filesystem layer
    plan_msgsnds             bigint, -- Number of IPC messages sent
    plan_msgrcvs             bigint, -- Number of IPC messages received
    plan_nsignals            bigint, -- Number of signals received
    plan_nvcsws              bigint, -- Number of voluntary context switches
    plan_nivcsws             bigint,
    skipped_time             double precision
) SET search_path=@extschema@ AS $$
    SELECT
        kc.server_id AS server_id,
        kc.datid AS datid,
        sample_db.datname AS dbname,
        kc.userid AS userid,
        rl.username AS username,
        kc.queryid AS queryid,
        kc.toplevel AS toplevel,
        COALESCE(sum(kc.exec_user_time), 0) AS exec_user_time,
        COALESCE(sum(kc.exec_user_time), 0.0) + COALESCE(sum(kc.plan_user_time), 0.0) as user_time,
        COALESCE(sum(kc.exec_system_time), 0) AS exec_system_time,
        COALESCE(sum(kc.exec_system_time), 0.0) + COALESCE(sum(kc.plan_system_time), 0.0) as system_time,
        COALESCE(sum(kc.exec_minflts), 0)::bigint AS exec_minflts,
        COALESCE(sum(kc.exec_majflts), 0)::bigint AS exec_majflts,
        COALESCE(sum(kc.exec_nswaps), 0)::bigint AS exec_nswaps,
        COALESCE(sum(kc.exec_reads), 0)::bigint AS exec_reads,
        COALESCE(sum(kc.exec_writes), 0)::bigint AS exec_writes,
        COALESCE(sum(kc.exec_msgsnds), 0)::bigint AS exec_msgsnds,
        COALESCE(sum(kc.exec_msgrcvs), 0)::bigint AS exec_msgrcvs,
        COALESCE(sum(kc.exec_nsignals), 0)::bigint AS exec_nsignals,
        COALESCE(sum(kc.exec_nvcsws), 0)::bigint AS exec_nvcsws,
        COALESCE(sum(kc.exec_nivcsws), 0)::bigint AS exec_nivcsws,
        COALESCE(sum(kc.exec_reads), 0) + COALESCE(sum(kc.plan_reads), 0) as reads,
        COALESCE(sum(kc.exec_writes), 0) + COALESCE(sum(kc.plan_writes), 0) as writes,
        COALESCE(sum(kc.plan_user_time), 0) AS plan_user_time,
        COALESCE(sum(kc.plan_system_time), 0) AS plan_system_time,
        COALESCE(sum(kc.plan_minflts), 0)::bigint AS plan_minflts,
        COALESCE(sum(kc.plan_majflts), 0)::bigint AS plan_majflts,
        COALESCE(sum(kc.plan_nswaps), 0)::bigint AS plan_nswaps,
        COALESCE(sum(kc.plan_reads), 0)::bigint AS plan_reads,
        COALESCE(sum(kc.plan_writes), 0)::bigint AS plan_writes,
        COALESCE(sum(kc.plan_msgsnds), 0)::bigint AS plan_msgsnds,
        COALESCE(sum(kc.plan_msgrcvs), 0)::bigint AS plan_msgrcvs,
        COALESCE(sum(kc.plan_nsignals), 0)::bigint AS plan_nsignals,
        COALESCE(sum(kc.plan_nvcsws), 0)::bigint AS plan_nvcsws,
        COALESCE(sum(kc.plan_nivcsws), 0)::bigint AS plan_nivcsws,
        extract(epoch from sum(CASE
            WHEN kc.stats_since <= s_prev.sample_time THEN '0'::interval
            ELSE kc.stats_since - s_prev.sample_time
          END)) AS skipped_time
   FROM sample_kcache kc
        -- User name
        JOIN roles_list rl USING (server_id, userid)
        -- Database name
        JOIN sample_stat_database sample_db
        USING (server_id, sample_id, datid)
        -- Prev sample is needed to calc stat coverage
        JOIN samples s_prev ON (s_prev.server_id, s_prev.sample_id) = (sserver_id, kc.sample_id - 1)
    WHERE kc.server_id = sserver_id AND kc.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY
      kc.server_id,
      kc.datid,
      sample_db.datname,
      kc.userid,
      rl.username,
      kc.queryid,
      kc.toplevel
$$ LANGUAGE sql;

CREATE FUNCTION top_rusage_statements_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid                    oid,
    dbname                   name,
    userid                   oid,
    username                 name,
    queryid                  text,
    hexqueryid               text,
    toplevel                 boolean,
    hashed_ids               text,
    exec_user_time           numeric,
    user_time_pct            numeric,
    exec_system_time         numeric,
    system_time_pct          numeric,
    exec_minflts             bigint,
    exec_majflts             bigint,
    exec_nswaps              bigint,
    exec_reads               text,
    exec_writes              text,
    exec_msgsnds             bigint,
    exec_msgrcvs             bigint,
    exec_nsignals            bigint,
    exec_nvcsws              bigint,
    exec_nivcsws             bigint,
    reads_total_pct          numeric,
    writes_total_pct         numeric,
    plan_user_time           numeric,
    plan_system_time         numeric,
    plan_minflts             bigint,
    plan_majflts             bigint,
    plan_nswaps              bigint,
    plan_reads               text,
    plan_writes              text,
    plan_msgsnds             bigint,
    plan_msgrcvs             bigint,
    plan_nsignals            bigint,
    plan_nvcsws              bigint,
    plan_nivcsws             bigint,
    rusage_cover             numeric,

    sum_cpu_time             numeric,
    sum_io_bytes             numeric,
    ord_cpu_time             bigint,
    ord_io_bytes             bigint
)
SET search_path=@extschema@ AS $$
  SELECT
    datid,
    dbname,
    userid,
    username,
    queryid::text,
    to_hex(st.queryid) AS hexqueryid,
    toplevel,
    left(encode(sha224(convert_to(
      st.userid::text || st.datid::text || st.queryid::text,
      'UTF8')
    ), 'hex'), 10) AS hashed_ids,
    round(CAST(NULLIF(st.exec_user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.user_time * 100 /
      NULLIF(tot.user_time, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.exec_system_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.system_time * 100 /
      NULLIF(tot.system_time, 0), 0.0) AS numeric), 2),
    NULLIF(st.exec_minflts, 0),
    NULLIF(st.exec_majflts, 0),
    NULLIF(st.exec_nswaps, 0),
    pg_size_pretty(NULLIF(st.exec_reads, 0)),
    pg_size_pretty(NULLIF(st.exec_writes, 0)),
    NULLIF(st.exec_msgsnds, 0),
    NULLIF(st.exec_msgrcvs, 0),
    NULLIF(st.exec_nsignals, 0),
    NULLIF(st.exec_nvcsws, 0),
    NULLIF(st.exec_nivcsws, 0),
    round(CAST(NULLIF(st.reads * 100 /
      NULLIF(tot.reads, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.writes * 100 /
      NULLIF(tot.writes, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.plan_user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st.plan_system_time, 0.0) AS numeric), 2),
    NULLIF(st.plan_minflts, 0),
    NULLIF(st.plan_majflts, 0),
    NULLIF(st.plan_nswaps, 0),
    pg_size_pretty(NULLIF(st.plan_reads, 0)),
    pg_size_pretty(NULLIF(st.plan_writes, 0)),
    NULLIF(st.plan_msgsnds, 0),
    NULLIF(st.plan_msgrcvs, 0),
    NULLIF(st.plan_nsignals, 0),
    NULLIF(st.plan_nvcsws, 0),
    NULLIF(st.plan_nivcsws, 0),
    round(CAST(NULLIF(100 - st.skipped_time * 100 /
      extract(epoch from r_samples_dur.duration), 0.0) AS numeric))
      as rusage_cover,

    (st.user_time + st.system_time)::numeric AS sum_cpu_time,
    st.reads + st.writes AS sum_io_bytes,
    CASE WHEN st.user_time + st.system_time > 0 THEN
      row_number() OVER (ORDER BY
        st.user_time + st.system_time
      DESC NULLS LAST,
        datid, userid, queryid, toplevel)
    ELSE NULL END AS ord_cpu_time,
    CASE WHEN st.reads + st.writes > 0 THEN
      row_number() OVER (ORDER BY
        st.reads + st.writes DESC NULLS LAST,
        datid, userid, queryid, toplevel)
    ELSE NULL END AS ord_io_bytes
  FROM
    top_kcache_statements(sserver_id, start_id, end_id) st
    CROSS JOIN total_kcache_statements(sserver_id, start_id, end_id) tot
    CROSS JOIN report_smpl_duration(sserver_id, start_id, end_id) r_samples_dur
$$ LANGUAGE sql;

CREATE FUNCTION top_rusage_statements_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid                    oid,
    dbname                   name,
    userid                   oid,
    username                 name,
    queryid                  text,
    hexqueryid               text,
    toplevel                 boolean,
    hashed_ids               text,
    exec_user_time1          numeric, --First interval
    user_time_pct1           numeric,
    exec_system_time1        numeric,
    system_time_pct1         numeric,
    exec_minflts1            bigint,
    exec_majflts1            bigint,
    exec_nswaps1             bigint,
    exec_reads1              text,
    exec_writes1             text,
    exec_msgsnds1            bigint,
    exec_msgrcvs1            bigint,
    exec_nsignals1           bigint,
    exec_nvcsws1             bigint,
    exec_nivcsws1            bigint,
    reads_total_pct1         numeric,
    writes_total_pct1        numeric,
    plan_user_time1          numeric,
    plan_system_time1        numeric,
    plan_minflts1            bigint,
    plan_majflts1            bigint,
    plan_nswaps1             bigint,
    plan_reads1              text,
    plan_writes1             text,
    plan_msgsnds1            bigint,
    plan_msgrcvs1            bigint,
    plan_nsignals1           bigint,
    plan_nvcsws1             bigint,
    plan_nivcsws1            bigint,
    rusage_cover1            numeric,
    exec_user_time2          numeric, -- Second interval
    user_time_pct2           numeric,
    exec_system_time2        numeric,
    system_time_pct2         numeric,
    exec_minflts2            bigint,
    exec_majflts2            bigint,
    exec_nswaps2             bigint,
    exec_reads2              text,
    exec_writes2             text,
    exec_msgsnds2            bigint,
    exec_msgrcvs2            bigint,
    exec_nsignals2           bigint,
    exec_nvcsws2             bigint,
    exec_nivcsws2            bigint,
    reads_total_pct2         numeric,
    writes_total_pct2        numeric,
    plan_user_time2          numeric,
    plan_system_time2        numeric,
    plan_minflts2            bigint,
    plan_majflts2            bigint,
    plan_nswaps2             bigint,
    plan_reads2              text,
    plan_writes2             text,
    plan_msgsnds2            bigint,
    plan_msgrcvs2            bigint,
    plan_nsignals2           bigint,
    plan_nvcsws2             bigint,
    plan_nivcsws2            bigint,
    rusage_cover2            numeric,
    sum_cpu_time             double precision, -- Filter and ordering fields
    sum_io_bytes             numeric,
    ord_cpu_time             bigint,
    ord_io_bytes             bigint
)
SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(st1.datid,st2.datid) as datid,
    COALESCE(st1.dbname,st2.dbname) as dbname,
    COALESCE(st1.userid,st2.userid) as userid,
    COALESCE(st1.username,st2.username) as username,
    COALESCE(st1.queryid,st2.queryid)::text AS queryid,
    to_hex(COALESCE(st1.queryid,st2.queryid)) as hexqueryid,
    COALESCE(st1.toplevel,st2.toplevel) as toplevel,
    left(encode(sha224(convert_to(
      COALESCE(st1.userid,st2.userid)::text ||
      COALESCE(st1.datid,st2.datid)::text ||
      COALESCE(st1.queryid,st2.queryid)::text,
      'UTF8')
    ), 'hex'), 10) AS hashed_ids,
    -- First interval
    round(CAST(NULLIF(st1.exec_user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.user_time * 100 /
      NULLIF(tot1.user_time, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.exec_system_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.system_time * 100 /
      NULLIF(tot1.system_time, 0), 0.0) AS numeric), 2),
    NULLIF(st1.exec_minflts, 0),
    NULLIF(st1.exec_majflts, 0),
    NULLIF(st1.exec_nswaps, 0),
    pg_size_pretty(NULLIF(st1.exec_reads, 0)),
    pg_size_pretty(NULLIF(st1.exec_writes, 0)),
    NULLIF(st1.exec_msgsnds, 0),
    NULLIF(st1.exec_msgrcvs, 0),
    NULLIF(st1.exec_nsignals, 0),
    NULLIF(st1.exec_nvcsws, 0),
    NULLIF(st1.exec_nivcsws, 0),
    round(CAST(NULLIF(st1.reads * 100 /
      NULLIF(tot1.reads, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.writes * 100 /
      NULLIF(tot1.writes, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.plan_user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st1.plan_system_time, 0.0) AS numeric), 2),
    NULLIF(st1.plan_minflts, 0),
    NULLIF(st1.plan_majflts, 0),
    NULLIF(st1.plan_nswaps, 0),
    pg_size_pretty(NULLIF(st1.plan_reads, 0)),
    pg_size_pretty(NULLIF(st1.plan_writes, 0)),
    NULLIF(st1.plan_msgsnds, 0),
    NULLIF(st1.plan_msgrcvs, 0),
    NULLIF(st1.plan_nsignals, 0),
    NULLIF(st1.plan_nvcsws, 0),
    NULLIF(st1.plan_nivcsws, 0),
    round(CAST(NULLIF(100 - st1.skipped_time * 100 /
      extract(epoch from r_samples_dur1.duration), 0.0) AS numeric))
      as rusage_cover1,
    -- Second interval
    round(CAST(NULLIF(st2.exec_user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.user_time * 100 /
      NULLIF(tot2.user_time, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.exec_system_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.system_time * 100 /
      NULLIF(tot2.system_time, 0), 0.0) AS numeric), 2),
    NULLIF(st2.exec_minflts, 0),
    NULLIF(st2.exec_majflts, 0),
    NULLIF(st2.exec_nswaps, 0),
    pg_size_pretty(NULLIF(st2.exec_reads, 0)),
    pg_size_pretty(NULLIF(st2.exec_writes, 0)),
    NULLIF(st2.exec_msgsnds, 0),
    NULLIF(st2.exec_msgrcvs, 0),
    NULLIF(st2.exec_nsignals, 0),
    NULLIF(st2.exec_nvcsws, 0),
    NULLIF(st2.exec_nivcsws, 0),
    round(CAST(NULLIF(st2.reads * 100 /
      NULLIF(tot2.reads, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.writes * 100 /
      NULLIF(tot2.writes, 0), 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.plan_user_time, 0.0) AS numeric), 2),
    round(CAST(NULLIF(st2.plan_system_time, 0.0) AS numeric), 2),
    NULLIF(st2.plan_minflts, 0),
    NULLIF(st2.plan_majflts, 0),
    NULLIF(st2.plan_nswaps, 0),
    pg_size_pretty(NULLIF(st2.plan_reads, 0)),
    pg_size_pretty(NULLIF(st2.plan_writes, 0)),
    NULLIF(st2.plan_msgsnds, 0),
    NULLIF(st2.plan_msgrcvs, 0),
    NULLIF(st2.plan_nsignals, 0),
    NULLIF(st2.plan_nvcsws, 0),
    NULLIF(st2.plan_nivcsws, 0),
    round(CAST(NULLIF(100 - st2.skipped_time * 100 /
      extract(epoch from r_samples_dur2.duration), 0.0) AS numeric))
      as rusage_cover2,
    -- Filter and ordering fields
    st1.user_time + st1.system_time +
      st2.user_time + st2.system_time AS sum_cpu_time,
    st1.reads + st1.writes + st2.reads + st2.writes AS sum_io_bytes,
    CASE WHEN st1.user_time + st1.system_time +
        st2.user_time + st2.system_time > 0
    THEN
      row_number() OVER (ORDER BY st1.plan_user_time +
        st1.user_time +
        st1.system_time +
        st2.user_time +
        st2.system_time DESC NULLS LAST,
        COALESCE(st1.datid,st2.datid),
        COALESCE(st1.userid,st2.userid),
        COALESCE(st1.queryid,st2.queryid),
        COALESCE(st1.toplevel,st2.toplevel))
    ELSE NULL END AS ord_cpu_time,
    CASE WHEN st1.reads + st1.writes +
        st2.reads + st2.writes > 0
    THEN
      row_number() OVER (ORDER BY st1.reads +
        st1.writes + st2.reads +
        st2.writes DESC NULLS LAST,
        COALESCE(st1.datid,st2.datid),
        COALESCE(st1.userid,st2.userid),
        COALESCE(st1.queryid,st2.queryid),
        COALESCE(st1.toplevel,st2.toplevel))
    ELSE NULL END AS ord_io_bytes
  FROM top_kcache_statements(sserver_id, start1_id, end1_id) st1
      FULL OUTER JOIN top_kcache_statements(sserver_id, start2_id, end2_id) st2 USING
        (server_id, datid, userid, queryid, toplevel)
    CROSS JOIN total_kcache_statements(sserver_id, start1_id, end1_id) tot1
    CROSS JOIN report_smpl_duration(sserver_id, start1_id, end1_id) r_samples_dur1
    CROSS JOIN total_kcache_statements(sserver_id, start2_id, end2_id) tot2
    CROSS JOIN report_smpl_duration(sserver_id, start2_id, end2_id) r_samples_dur2
$$ LANGUAGE sql;
