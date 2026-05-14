# Databricks notebook source

# ---------------------------
# CONFIG
# ---------------------------

storage_account = "<your-storage-account>"
container = "lakehouse"

bronze_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/bronze/weather/"
silver_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/weather/weather_hourly/"
quarantine_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/silver/weather/weather_hourly_quarantine/"

silver_table = "silver_weather_hourly"
quarantine_table = "silver_weather_hourly_quarantine"


# ---------------------------
# READ BRONZE
# ---------------------------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

bronze_df = spark.read.format("delta").load(bronze_path)

display(bronze_df.limit(5))


# ---------------------------
# FLATTEN HOURLY ARRAYS
# ---------------------------

flattened_df = (
    bronze_df
    .withColumn(
        "hourly_zipped",
        F.arrays_zip(
            F.col("hourly.time").alias("time"),
            F.col("hourly.temperature_2m").alias("temperature_2m"),
            F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
            F.col("hourly.precipitation").alias("precipitation"),
            F.col("hourly.wind_speed_10m").alias("wind_speed_10m")
        )
    )
    .withColumn("hourly_row", F.explode("hourly_zipped"))
    .select(
        F.sha2(
            F.concat_ws("|", F.col("latitude").cast("string"), F.col("longitude").cast("string")),
            256
        ).alias("city_id"),

        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
        F.col("timezone").cast("string").alias("timezone"),

        F.to_timestamp(F.col("hourly_row.time")).alias("observation_ts"),
        F.col("hourly_row.temperature_2m").cast("double").alias("temperature_2m"),
        F.col("hourly_row.relative_humidity_2m").cast("double").alias("relative_humidity_2m"),
        F.col("hourly_row.precipitation").cast("double").alias("precipitation_mm"),
        F.col("hourly_row.wind_speed_10m").cast("double").alias("wind_speed_10m"),

        F.col("source_system"),
        F.col("source_file"),
        F.col("ingested_at_utc")
    )
    .withColumn(
        "weather_hourly_id",
        F.sha2(
            F.concat_ws("|", F.col("city_id"), F.col("observation_ts").cast("string")),
            256
        )
    )
    .withColumn("silver_updated_at_utc", F.current_timestamp())
)

display(flattened_df.limit(10))

# ---------------------------
# DATA QUALITY CHECKS
# ---------------------------

dq_df = (
    flattened_df
    .withColumn(
        "dq_errors",
        F.array_remove(
            F.array(
                F.when(F.col("city_id").isNull(), F.lit("city_id_null")),
                F.when(F.col("weather_hourly_id").isNull(), F.lit("weather_hourly_id_null")),
                F.when(F.col("observation_ts").isNull(), F.lit("observation_ts_null")),
                F.when(F.col("temperature_2m").isNull(), F.lit("temperature_2m_null")),
                F.when((F.col("temperature_2m") < -60) | (F.col("temperature_2m") > 60), F.lit("temperature_2m_out_of_range")),
                F.when((F.col("relative_humidity_2m") < 0) | (F.col("relative_humidity_2m") > 100), F.lit("relative_humidity_2m_out_of_range")),
                F.when(F.col("precipitation_mm") < 0, F.lit("precipitation_mm_negative")),
                F.when(F.col("wind_speed_10m") < 0, F.lit("wind_speed_10m_negative"))
            ),
            None
        )
    )
    .withColumn("is_valid", F.size(F.col("dq_errors")) == 0)
)

valid_df = (
    dq_df
    .filter(F.col("is_valid") == True)
    .drop("dq_errors", "is_valid")
)

invalid_df = (
    dq_df
    .filter(F.col("is_valid") == False)
)

print("Valid rows:", valid_df.count())
print("Invalid rows:", invalid_df.count())

display(valid_df.limit(10))
display(invalid_df.limit(10))

# ---------------------------
# DEDUPLICATION
# ---------------------------

valid_dedup_df = (
    valid_df
    .dropDuplicates(["weather_hourly_id"])
)

print("Valid rows after dedup:", valid_dedup_df.count())

# ---------------------------
# WRITE SILVER WITH DELTA MERGE / UPSERT
# ---------------------------

if DeltaTable.isDeltaTable(spark, silver_path):
    silver_target = DeltaTable.forPath(spark, silver_path)

    (
        silver_target.alias("target")
        .merge(
            valid_dedup_df.alias("source"),
            "target.weather_hourly_id = source.weather_hourly_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

else:
    (
        valid_dedup_df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )


# ---------------------------
# WRITE QUARANTINE
# ---------------------------

if invalid_df.count() > 0:
    (
        invalid_df.write
        .format("delta")
        .mode("append")
        .save(quarantine_path)
    )


# ---------------------------
# CREATE TABLES IF NOT EXISTS
# ---------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table}
USING DELTA
LOCATION '{silver_path}'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {quarantine_table}
USING DELTA
LOCATION '{quarantine_path}'
""")