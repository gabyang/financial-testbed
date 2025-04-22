CREATE MATERIALIZED VIEW ca_weekly_avg
WITH (timescaledb.continuous) AS
SELECT symbol,
       time_bucket(INTERVAL '1 week', time) AS time_window,
       AVG(close) AS avg_close
  FROM stock_ticks
 GROUP BY symbol, time_window;

CREATE MATERIALIZED VIEW ca_biweekly_avg
WITH (timescaledb.continuous) AS
SELECT symbol,
       time_bucket(INTERVAL '2 weeks', time) AS time_window,
       AVG(close) AS avg_close
  FROM stock_ticks
 GROUP BY symbol, time_window;


CREATE MATERIALIZED VIEW industry_weekly_ca
WITH (timescaledb.continuous) AS
SELECT industry,
       time_bucket(INTERVAL '1 week', time) AS time_window,
       AVG(close) AS avg_close
  FROM stock_ticks
 GROUP BY symbol, time_window;

CREATE MATERIALIZED VIEW industry_biweekly_ca
WITH (timescaledb.continuous) AS
SELECT industry,
       time_bucket(INTERVAL '2 weeks', time) AS time_window,
       AVG(close) AS avg_close
  FROM stock_ticks
 GROUP BY symbol, time_window;


SELECT add_continuous_aggregate_policy('ca_weekly_avg',
                                      start_offset => NA,
                                      end_offset   => INTERVAL '1 hour',
                                      schedule_interval => INTERVAL '1 hour');

 

SELECT add_continuous_aggregate_policy('ca_biweekly_avg',
                                      start_offset => NA,
                                      end_offset   => INTERVAL '1 hour',
                                      schedule_interval => INTERVAL '1 hour');                               



SELECT add_continuous_aggregate_policy('industry_weekly_ca',
                                      start_offset => NA,
                                      end_offset   => INTERVAL '1 hour',
                                      schedule_interval => INTERVAL '1 hour');




SELECT add_continuous_aggregate_policy('industry_weekly_ca',
                                      start_offset => NA,
                                      end_offset   => INTERVAL '1 hour',
                                      schedule_interval => INTERVAL '1 hour');