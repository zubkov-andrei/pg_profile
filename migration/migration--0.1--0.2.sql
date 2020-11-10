CREATE TABLE sample_timings (
    server_id   integer NOT NULL,
    sample_id   integer NOT NULL,
    event       text,
    time_spent  interval MINUTE TO SECOND (2),
    CONSTRAINT pk_sample_timings PRIMARY KEY (server_id, sample_id, event),
    CONSTRAINT fk_sample_timings_sample FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE CASCADE
);
COMMENT ON TABLE sample_timings IS 'Sample taking time statistics';

CREATE VIEW v_sample_timings AS
SELECT
  srv.server_name,
  smp.sample_id,
  smp.sample_time,
  tm.event,
  tm.time_spent
FROM
  sample_timings tm
  JOIN servers srv USING (server_id)
  JOIN samples smp USING (server_id, sample_id);
COMMENT ON VIEW v_sample_timings IS 'Sample taking time statistics with server names and sample times';
