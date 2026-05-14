## Phase 1 - Initial API Ingestion

- Created Azure Data Factory instance
- Configured REST linked service (Open-Meteo)
- Configured ADLS Gen2 linked service
- Created REST dataset for API ingestion
- Created ADLS dataset for raw storage
- Implemented initial Copy Activity pipeline
- Successfully ingested weather data into ADLS

## Phase 2 - Dynamic API Ingestion

- Added pipeline parameters (processing_date, city_list)
- Implemented ForEach activity for multi-city ingestion
- Parameterized REST API calls
- Implemented dynamic sink path using processing_date and run_id
- Enabled scalable and reusable ingestion pattern
