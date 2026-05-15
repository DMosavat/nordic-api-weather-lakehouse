spark.sql(f"CREATE TABLE IF NOT EXISTS gold_kpi_weather_daily USING DELTA LOCATION '{kpi_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_dim_city USING DELTA LOCATION '{dim_city_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_dim_date USING DELTA LOCATION '{dim_date_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_fact_weather_hourly USING DELTA LOCATION '{fact_path}'")