# Nordic API Weather Lakehouse

## Overview

Nordic API Weather Lakehouse is a cloud-native Azure Data Engineering project designed to ingest, process and serve weather data using a modern Medallion Architecture.

The platform consumes hourly weather data from the Open-Meteo REST API, processes it through Bronze, Silver and Gold layers using Databricks and Delta Lake, and exposes curated analytical datasets to Power BI through Databricks SQL Warehouse.

---

# Project Objective

The objective of this project is to simulate a production-grade Azure Data Engineering solution with:

- REST API ingestion
- Parameterized orchestration
- Incremental data processing
- Medallion Architecture
- Delta Lake storage
- Monitoring and alerting
- CI/CD deployment workflows
- Power BI reporting integration

---

# Data Source

The project uses the Open-Meteo REST API as the source system.

## Example Cities

| City      | Country | Latitude | Longitude |
| --------- | ------- | -------: | --------: |
| Stockholm | Sweden  |  59.3293 |   18.0686 |
| Göteborg  | Sweden  |  57.7089 |   11.9746 |
| Malmö     | Sweden  |  55.6050 |   13.0038 |
| Uppsala   | Sweden  |  59.8586 |   17.6389 |

---

# High-Level Architecture

```text
Open-Meteo REST API
        |
        | Daily parameterized ingestion
        v
Azure Data Factory
        |
        | Stores raw JSON responses
        v
Azure Data Lake Storage Gen2 - Raw Zone
        |
        | Reads raw JSON and appends metadata
        v
Databricks Bronze Layer
        |
        | Cleans, validates, flattens and upserts data
        v
Databricks Silver Layer
        |
        | Builds KPI tables and star schema
        v
Databricks Gold Layer
        |
        | SQL serving layer
        v
Databricks SQL Warehouse
        |
        | Semantic reporting
        v
Power BI
```

---

# Medallion Architecture

The project follows the Medallion Architecture design pattern.

## Bronze Layer

The Bronze layer stores raw API data with minimal transformation.

### Responsibilities

- Preserve raw API responses
- Store ingestion metadata
- Support replay and backfill
- Partition data by processing date
- Keep append-only history

### Example Raw Path

```text
raw/open_meteo/weather_hourly/
  processing_date=2026-05-10/
  city=stockholm/
  run_id=<adf_pipeline_run_id>/
  response.json
```

### Bronze Table

```text
bronze.open_meteo_weather_raw
```

---

## Silver Layer

The Silver layer contains cleaned and validated structured data.

### Responsibilities

- Parse and flatten JSON
- Enforce schema
- Apply data quality checks
- Quarantine invalid records
- Deduplicate records
- Apply incremental processing using Delta MERGE / UPSERT

### Silver Tables

```text
silver.weather_hourly
silver.weather_hourly_quarantine
```

### Natural Key

```text
weather_hourly_id = sha2(city_id + observation_ts)
```

---

## Gold Layer

The Gold layer contains business-ready analytical datasets.

### Responsibilities

- Create KPI tables
- Create star schema
- Serve reporting use cases
- Optimize for BI consumption

### Gold Tables

```text
gold.kpi_weather_daily
gold.dim_city
gold.dim_date
gold.fact_weather_hourly
```

### Gold Views

```text
gold.vw_weather_daily_kpi
gold.vw_weather_hourly_star
```

---

# Orchestration

Azure Data Factory orchestrates the end-to-end pipeline.

## ADF Pipeline

```text
pl_ingest_open_meteo_weather_daily
```

## Pipeline Parameters

| Parameter       | Description                            |
| --------------- | -------------------------------------- |
| env             | Target environment such as dev or prod |
| processing_date | Business date to process               |
| city_list       | List of cities to ingest               |
| run_id          | Pipeline run identifier                |

## Pipeline Flow

1. Resolve processing date
2. Load city configuration
3. Call Open-Meteo REST API for each city
4. Store raw JSON in ADLS Gen2
5. Execute Bronze Databricks notebook
6. Execute Silver Databricks notebook
7. Execute Gold Databricks notebook
8. Write pipeline run logs
9. Send alerts on failure

---

# Storage Design

Azure Data Lake Storage Gen2 is used as the centralized storage layer.

## Recommended Container

```text
lakehouse
```

## Recommended Folder Structure

```text
lakehouse/
├── raw/
│   └── open_meteo/
├── bronze/
│   └── weather/
├── silver/
│   └── weather/
├── gold/
│   └── weather/
└── logs/
    └── pipeline_run_log/
```

---

# Security Design

## Security Principles

- No secrets stored in source code
- Azure Key Vault for secret management
- Service Principal authentication for automation
- Least privilege access model
- Power BI users access only Gold views
- Separate dev and prod environments

## Key Vault Secrets

```text
databricks-client-id
databricks-client-secret
tenant-id
storage-account-name
```

---

# Monitoring and Alerting

## Monitoring Components

- Azure Data Factory monitoring
- Azure Monitor
- Log Analytics Workspace
- Action Group notifications
- Delta-based operational logging

## Operational Log Table

```text
ops.pipeline_run_log
```

## Example Logged Fields

| Field                    | Description                     |
| ------------------------ | ------------------------------- |
| pipeline_run_id          | Unique pipeline run identifier  |
| pipeline_name            | ADF pipeline name               |
| env                      | Environment                     |
| processing_date          | Processed business date         |
| status                   | Success or failed               |
| bronze_row_count         | Bronze record count             |
| silver_valid_row_count   | Valid Silver record count       |
| silver_invalid_row_count | Quarantined Silver record count |
| gold_row_count           | Gold record count               |
| error_message            | Failure details                 |

---

# Serving Layer

Databricks SQL Warehouse is used as the analytical serving layer.

Power BI connects directly to Databricks SQL Warehouse instead of ADLS Gen2.

This architecture provides:

- Better governance
- SQL-based access control
- Query optimization
- Separation between storage and reporting layers

---

# CI/CD Design

GitHub Actions is used for CI/CD automation.

## CI/CD Responsibilities

- Validate repository structure
- Run Python unit tests
- Lint source code
- Deploy infrastructure
- Deploy Databricks notebooks and SQL assets
- Deploy Azure Data Factory artifacts

## Environment Strategy

```text
develop -> dev environment
main    -> prod environment
```

---

# Design Decisions

| Decision                 | Reason                                |
| ------------------------ | ------------------------------------- |
| REST API instead of CSV  | Closer to real production ingestion   |
| Azure Data Factory       | Native orchestration and monitoring   |
| Databricks               | Scalable Spark-based processing       |
| Delta Lake               | ACID transactions and MERGE support   |
| Medallion Architecture   | Clear separation of processing layers |
| Databricks SQL Warehouse | Optimized BI serving layer            |
| Azure Key Vault          | Secure secret management              |
| Service Principal        | Secure automation authentication      |
| Log Analytics            | Centralized operational monitoring    |
| GitHub Actions           | Repeatable CI/CD workflows            |

---

# Suggested Repository Structure

```text
nordic-api-weather-lakehouse/
├── adf/
│   ├── pipelines/
│   ├── datasets/
│   ├── linked_services/
│   └── triggers/
│
├── databricks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── sql/
│   └── shared/
│
├── infrastructure/
│   ├── bicep/
│   └── terraform/
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── configs/
│   ├── dev/
│   └── prod/
│
├── docs/
│
├── .github/
│   └── workflows/
│
├── requirements.txt
├── README.md
└── .gitignore
```

## Implemented Infrastructure (Phase 1)

- Resource Group: rg-nordic-weather-dev
- Storage Account: stnordicweatherdevXXX
- ADLS Gen2 enabled
- Container: lakehouse
- Folder structure initialized (raw, bronze, silver, gold, logs)

## Compute Layer

Azure Databricks is used as the compute engine.

- Workspace: dbw-nordic-weather-dev
- Cluster: cl-nordic-weather-dev
- Runtime: Latest LTS
- Auto termination enabled

Databricks is connected to ADLS Gen2 using storage account key (temporary approach for development).
