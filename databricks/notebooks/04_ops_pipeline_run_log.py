# Databricks notebook source

from pyspark.sql import functions as F

run_log_df = spark.createDataFrame(
    [
        (
            dbutils.widgets.get("pipeline_run_id"),
            dbutils.widgets.get("pipeline_name"),
            dbutils.widgets.get("env"),
            dbutils.widgets.get("processing_date"),
            dbutils.widgets.get("status"),
        )
    ],
    ["pipeline_run_id", "pipeline_name", "env", "processing_date", "status"]
).withColumn("logged_at_utc", F.current_timestamp())

run_log_df.write.format("delta").mode("append").save(
    "abfss://lakehouse@<storage-account>.dfs.core.windows.net/logs/pipeline_run_log/"
)