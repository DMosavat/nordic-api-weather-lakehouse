# Data Model

## Overview

This document defines the data model for the Nordic API Weather Lakehouse project.

The solution follows the Medallion Architecture pattern:

- Bronze Layer → Raw API payloads
- Silver Layer → Cleaned and validated hourly weather observations
- Gold Layer → Analytical star schema and KPI tables

---

# Source API

The project uses the Open-Meteo Historical Weather API.

Example API response fields:

```text
latitude
longitude
timezone
hourly.time
hourly.temperature_2m
hourly.relative_humidity_2m
hourly.precipitation
hourly.wind_speed_10m
```

---

# Bronze Layer

## Table: `bronze.open_meteo_weather_raw`

### Purpose

Stores raw JSON API responses together with ingestion metadata.

---

## Schema

| Column                | Type      | Description                 |
| --------------------- | --------- | --------------------------- |
| latitude              | double    | Source latitude             |
| longitude             | double    | Source longitude            |
| generationtime_ms     | double    | API generation time         |
| utc_offset_seconds    | long      | API timezone offset         |
| timezone              | string    | API timezone                |
| timezone_abbreviation | string    | Timezone abbreviation       |
| elevation             | double    | Location elevation          |
| hourly_units          | struct    | Units for hourly metrics    |
| hourly                | struct    | Raw hourly arrays           |
| source_system         | string    | Source system name          |
| processing_date       | date      | Business processing date    |
| pipeline_run_id       | string    | ADF pipeline run identifier |
| ingested_at_utc       | timestamp | Ingestion timestamp         |
| source_file           | string    | Raw file path               |

---

## Partitioning Strategy

```text
processing_date
```

---

# Silver Layer

## Table: `silver.weather_hourly`

### Purpose

Stores cleaned, validated and deduplicated hourly weather observations.

---

## Schema

| Column                | Type      | Description                  |
| --------------------- | --------- | ---------------------------- |
| weather_hourly_id     | string    | Surrogate hash key           |
| city_id               | string    | City hash key                |
| latitude              | double    | City latitude                |
| longitude             | double    | City longitude               |
| timezone              | string    | Timezone                     |
| observation_ts        | timestamp | Hourly observation timestamp |
| temperature_2m        | double    | Air temperature at 2 meters  |
| relative_humidity_2m  | double    | Relative humidity            |
| precipitation_mm      | double    | Precipitation in millimeters |
| wind_speed_10m        | double    | Wind speed at 10 meters      |
| processing_date       | date      | Business processing date     |
| pipeline_run_id       | string    | ADF pipeline run identifier  |
| silver_updated_at_utc | timestamp | Last Silver update timestamp |

---

## Primary Business Key

```text
city_id + observation_ts
```

---

## Surrogate Key

```text
weather_hourly_id = sha2(city_id + observation_ts)
```

---

## Incremental Strategy

```sql
MERGE INTO silver.weather_hourly
ON weather_hourly_id
```

---

## Table: `silver.weather_hourly_quarantine`

### Purpose

Stores invalid records that failed data quality validation rules.

---

## Schema

| Column                | Type          | Description                  |
| --------------------- | ------------- | ---------------------------- |
| weather_hourly_id     | string        | Surrogate hash key           |
| city_id               | string        | City hash key                |
| latitude              | double        | City latitude                |
| longitude             | double        | City longitude               |
| timezone              | string        | Timezone                     |
| observation_ts        | timestamp     | Hourly observation timestamp |
| temperature_2m        | double        | Air temperature              |
| relative_humidity_2m  | double        | Relative humidity            |
| precipitation_mm      | double        | Precipitation                |
| wind_speed_10m        | double        | Wind speed                   |
| processing_date       | date          | Business processing date     |
| pipeline_run_id       | string        | ADF pipeline run identifier  |
| dq_error              | array<string> | Failed data quality rules    |
| silver_updated_at_utc | timestamp     | Quarantine timestamp         |

---

## Partitioning Strategy

```text
processing_date
```

---

# Gold Layer

## Table: `gold.dim_city`

### Purpose

Dimension table containing city attributes.

---

## Schema

| Column    | Type   | Description   |
| --------- | ------ | ------------- |
| city_id   | string | City hash key |
| city_name | string | City name     |
| country   | string | Country       |
| latitude  | double | Latitude      |
| longitude | double | Longitude     |
| timezone  | string | Timezone      |

---

## Table: `gold.dim_date`

### Purpose

Date dimension table for analytical queries.

---

## Schema

| Column       | Type    | Description                 |
| ------------ | ------- | --------------------------- |
| date_id      | int     | Date key in yyyyMMdd format |
| date         | date    | Calendar date               |
| year         | int     | Year                        |
| month        | int     | Month                       |
| month_name   | string  | Month name                  |
| day          | int     | Day of month                |
| day_of_week  | string  | Day of week                 |
| week_of_year | int     | ISO week number             |
| is_weekend   | boolean | Weekend flag                |

---

## Table: `gold.fact_weather_hourly`

### Purpose

Fact table storing hourly weather observations for analytics.

---

## Schema

| Column               | Type      | Description              |
| -------------------- | --------- | ------------------------ |
| weather_hourly_id    | string    | Fact key                 |
| city_id              | string    | Foreign key to dim_city  |
| date_id              | int       | Foreign key to dim_date  |
| observation_ts       | timestamp | Observation timestamp    |
| temperature_2m       | double    | Air temperature          |
| relative_humidity_2m | double    | Relative humidity        |
| precipitation_mm     | double    | Precipitation            |
| wind_speed_10m       | double    | Wind speed               |
| processing_date      | date      | Business processing date |

---

## Grain

```text
One row per city per hour
```

---

## Table: `gold.kpi_weather_daily`

### Purpose

Daily KPI table optimized for BI dashboards.

---

## Schema

| Column                 | Type      | Description                           |
| ---------------------- | --------- | ------------------------------------- |
| city_id                | string    | City hash key                         |
| weather_date           | date      | Weather date                          |
| avg_temperature_2m     | double    | Average daily temperature             |
| min_temperature_2m     | double    | Minimum daily temperature             |
| max_temperature_2m     | double    | Maximum daily temperature             |
| total_precipitation_mm | double    | Total daily precipitation             |
| avg_relative_humidity  | double    | Average daily humidity                |
| max_wind_speed_10m     | double    | Maximum wind speed                    |
| frost_hours            | long      | Number of hours below 0°C             |
| comfort_hours          | long      | Number of hours between 18°C and 24°C |
| gold_updated_at_utc    | timestamp | Last Gold update timestamp            |

---

## Grain

```text
One row per city per day
```

---

# Gold Views

## View: `gold.vw_weather_daily_kpi`

Business-friendly KPI view joining:

- Daily KPI metrics
- City dimension attributes

Used by:

- Power BI dashboards
- Executive reporting

---

## View: `gold.vw_weather_hourly_star`

Star schema analytical view joining:

- fact_weather_hourly
- dim_city
- dim_date

Used for:

- Ad hoc analysis
- Power BI exploration
- SQL analytics

---

# Data Quality Rules

| Rule Type  | Column               | Condition                  | Action                |
| ---------- | -------------------- | -------------------------- | --------------------- |
| Not Null   | city_id              | city_id IS NOT NULL        | Reject                |
| Not Null   | observation_ts       | observation_ts IS NOT NULL | Reject                |
| Not Null   | temperature_2m       | temperature_2m IS NOT NULL | Reject                |
| Range      | temperature_2m       | Between -60 and 60         | Reject                |
| Range      | relative_humidity_2m | Between 0 and 100          | Reject                |
| Range      | precipitation_mm     | Greater than or equal to 0 | Reject                |
| Range      | wind_speed_10m       | Greater than or equal to 0 | Reject                |
| Uniqueness | weather_hourly_id    | Unique per Silver table    | Deduplicate or Upsert |

---

# Incremental Processing Strategy

## Bronze Layer

Bronze is append-only.

New API responses are stored using:

```text
processing_date
city
pipeline_run_id
```

---

## Silver Layer

Silver uses Delta MERGE for incremental upserts.

### Match Condition

```sql
target.weather_hourly_id = source.weather_hourly_id
```

### Behavior

- Existing rows are updated
- New rows are inserted
- Invalid rows are written to quarantine tables

---

## Gold Layer

Gold tables are rebuilt only for affected business dates.

### KPI Tables

```text
Overwrite affected weather_date
```

### Fact Tables

```sql
MERGE by weather_hourly_id
```

---

# BI Consumption

Power BI consumes only Gold views through Databricks SQL Warehouse.

Power BI does not connect directly to:

- Bronze tables
- Silver tables
- ADLS Gen2 files

This separation improves:

- Governance
- Security
- Query performance
- Access control
- Semantic consistency

## Bronze Implementation

- Raw JSON data is ingested from ADLS
- Metadata columns added:
  - source_system
  - ingested_at_utc
  - source_file
- Data stored as Delta table
- Append-only pattern implemented

## Silver Implementation

- Bronze Delta data is read from ADLS
- Hourly arrays are flattened using arrays_zip and explode
- Surrogate keys are generated:
  - city_id
  - weather_hourly_id
- Data quality checks are applied
- Valid records are written to Silver
- Invalid records are written to Quarantine
- Duplicate records are removed using weather_hourly_id

## Silver Incremental Processing

Silver uses Delta MERGE / UPSERT instead of full overwrite.

Merge key:

```text
weather_hourly_id
```

### Merge behavior:

```text

WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

This allows safe reprocessing, deduplication and incremental loads.

## Gold Implementation

- KPI table created using daily aggregation
- Star schema implemented:
  - dim_city
  - dim_date
  - fact_weather_hourly
- Gold tables optimized for BI and reporting
