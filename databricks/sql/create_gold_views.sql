CREATE OR REPLACE VIEW vw_weather_daily_kpi AS
SELECT
  c.city_name,
  k.weather_date,
  k.avg_temperature_2m,
  k.min_temperature_2m,
  k.max_temperature_2m,
  k.total_precipitation_mm,
  k.avg_relative_humidity,
  k.max_wind_speed_10m,
  k.frost_hours,
  k.comfort_hours
FROM gold_kpi_weather_daily k
JOIN gold_dim_city c
  ON k.city_id = c.city_id;

  CREATE OR REPLACE VIEW vw_weather_hourly_star AS
SELECT
  c.city_name,
  d.date,
  d.year,
  d.month,
  d.day_of_week,
  f.observation_ts,
  f.temperature_2m,
  f.relative_humidity_2m,
  f.precipitation_mm,
  f.wind_speed_10m
FROM gold_fact_weather_hourly f
JOIN gold_dim_city c
  ON f.city_id = c.city_id
JOIN gold_dim_date d
  ON f.date_id = d.date_id;