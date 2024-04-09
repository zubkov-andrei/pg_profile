INSERT INTO import_queries_version_order VALUES
('pg_profile','4.6','pg_profile','4.5')
;

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;

DO $$
DECLARE
  sserver_id    integer;
BEGIN
  FOR sserver_id IN (SELECT * FROM servers) LOOP
    -- Create last_stat_activity table partition
    EXECUTE format(
      'CREATE TABLE last_stat_activity_srv%1$s PARTITION OF last_stat_activity '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_activity_srv%1$s '
      'ADD CONSTRAINT pk_last_stat_activity_srv%1$s '
        'PRIMARY KEY (server_id, sample_id, pid, subsample_ts), '
      'ADD CONSTRAINT fk_last_stat_activity_sample_srv%1$s '
        'FOREIGN KEY (server_id, sample_id) '
        'REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT',
        sserver_id);

    -- Create last_stat_activity table partition
    EXECUTE format(
      'CREATE TABLE last_stat_activity_count_srv%1$s PARTITION OF last_stat_activity_count '
      'FOR VALUES IN (%1$s)',
      sserver_id);
    EXECUTE format(
      'ALTER TABLE last_stat_activity_count_srv%1$s '
      'ADD CONSTRAINT fk_last_stat_activity_count_sample_srv%1$s '
        'FOREIGN KEY (server_id, sample_id) '
        'REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT',
        sserver_id);
  END LOOP;
END;
$$ LANGUAGE plpgsql;
