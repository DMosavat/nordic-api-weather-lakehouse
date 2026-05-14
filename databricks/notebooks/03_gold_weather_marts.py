# Databricks notebook source

# ---------------------------
# CONFIG
# ---------------------------

storage_account = "<your-storage-account>"
container = "lakehouse"

silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/weather/weather_hourly/"
gold_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/gold/weather/"

kpi_path = gold_path + "kpi_weather_daily/"
dim_city_path = gold_path + "dim_city/"
dim_date_path = gold_path + "dim_date/"
fact_path = gold_path + "fact_weather_hourly/"

# ---------------------------
# READ SILVER
# ---------------------------

from pyspark.sql import functions as F

silver_df = spark.read.format("delta").load(silver_path)

display(silver_df.limit(5))

# ---------------------------
# KPI TABLE
# ---------------------------

kpi_df = (
    silver_df
    .groupBy(
        "city_id",
        F.to_date("observation_ts").alias("weather_date")
    )
    .agg(
        F.avg("temperature_2m").alias("avg_temperature_2m"),
        F.min("temperature_2m").alias("min_temperature_2m"),
        F.max("temperature_2m").alias("max_temperature_2m"),
        F.sum("precipitation_mm").alias("total_precipitation_mm"),
        F.avg("relative_humidity_2m").alias("avg_relative_humidity"),
        F.max("wind_speed_10m").alias("max_wind_speed_10m"),

        F.sum(F.when(F.col("temperature_2m") < 0, 1).otherwise(0)).alias("frost_hours"),
        F.sum(F.when((F.col("temperature_2m") >= 18) & (F.col("temperature_2m") <= 24), 1).otherwise(0)).alias("comfort_hours")
    )
    .withColumn("gold_updated_at_utc", F.current_timestamp())
)

display(kpi_df.limit(10))

# Write KPI
kpi_df.write.format("delta").mode("overwrite").save(kpi_path)

# ---------------------------
# DIM CITY
# ---------------------------

dim_city_df = (
    silver_df
    .select("city_id", "latitude", "longitude", "timezone")
    .dropDuplicates(["city_id"])
    .withColumn(
        "city_name",
        F.when((F.col("latitude") == 59.3293) & (F.col("longitude") == 18.0686), "Stockholm")
         .when((F.col("latitude") == 57.7089) & (F.col("longitude") == 11.9746), "Göteborg")
         .when((F.col("latitude") == 55.6050) & (F.col("longitude") == 13.0038), "Malmö")
         .otherwise("Unknown")
    )
    .withColumn("country", F.lit("Sweden"))
)

display(dim_city_df)

dim_city_df.write.format("delta").mode("overwrite").save(dim_city_path)

# ---------------------------
# DIM DATE
# ---------------------------

dim_date_df = (
    silver_df
    .select(F.to_date("observation_ts").alias("date"))
    .dropDuplicates()
    .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.date_format("date", "E"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("is_weekend", F.col("day_of_week").isin("Sat", "Sun"))
)

display(dim_date_df)

dim_date_df.write.format("delta").mode("overwrite").save(dim_date_path)

# ---------------------------
# FACT TABLE
# ---------------------------

fact_df = (
    silver_df
    .select(
        "weather_hourly_id",
        "city_id",
        F.date_format(F.to_date("observation_ts"), "yyyyMMdd").cast("int").alias("date_id"),
        "observation_ts",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation_mm",
        "wind_speed_10m"
    )
)

display(fact_df.limit(10))

fact_df.write.format("delta").mode("overwrite").save(fact_path)

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_kpi_weather_daily USING DELTA LOCATION '{kpi_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_dim_city USING DELTA LOCATION '{dim_city_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_dim_date USING DELTA LOCATION '{dim_date_path}'")
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_fact_weather_hourly USING DELTA LOCATION '{fact_path}'")