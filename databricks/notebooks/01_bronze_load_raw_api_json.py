# Databricks notebook source

# ---------------------------
# CONFIG (edit this)
# ---------------------------

storage_account = "<your-storage-account>"
container = "lakehouse"

raw_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/raw/open_meteo/weather_hourly/"
bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/weather/"

bronze_table = "bronze_open_meteo_weather_raw"


# ---------------------------
# READ RAW JSON
# ---------------------------

raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(raw_path)
)

display(raw_df.limit(5))


# ---------------------------
# ADD METADATA
# ---------------------------

from pyspark.sql import functions as F

bronze_df = (
    raw_df
    .withColumn("source_system", F.lit("open_meteo"))
    .withColumn("ingested_at_utc", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
)

display(bronze_df.limit(5))


# ---------------------------
# WRITE DELTA (APPEND ONLY)
# ---------------------------

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .save(bronze_path)
)

# ---------------------------
# CREATE TABLE (optional but recommended)
# ---------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_table}
USING DELTA
LOCATION '{bronze_path}'
""")