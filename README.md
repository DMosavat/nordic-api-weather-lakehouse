# Nordic API Weather Lakehouse

End-to-end Azure Data Engineering project using a real REST API and a production-like cloud-native architecture.

## Project Goal

This project demonstrates a modern Azure data engineering pipeline using:

- Azure Data Factory for orchestration and API ingestion
- Azure Data Lake Storage Gen2 for lake storage
- Azure Databricks with PySpark and Delta Lake for processing
- Medallion Architecture: Bronze, Silver, Gold
- Data Quality checks in Silver
- Delta MERGE / UPSERT for incremental processing
- Databricks SQL Warehouse as serving layer
- Power BI for reporting
- Azure Key Vault for secret management
- Service Principal authentication
- Log Analytics and Azure Monitor for monitoring and alerting
- GitHub Actions for CI/CD

## Data Source

The project uses the Open-Meteo REST API to ingest hourly weather data for selected Swedish cities.

## Architecture

```text
Open-Meteo REST API
        |
        v
Azure Data Factory
        |
        v
ADLS Gen2 Raw Zone
        |
        v
Azure Databricks - Bronze
        |
        v
Azure Databricks - Silver
        |
        v
Azure Databricks - Gold
        |
        v
Databricks SQL Warehouse
        |
        v
Power BI
```
