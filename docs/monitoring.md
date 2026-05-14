# Monitoring and Alerting

## Components

- Azure Data Factory monitoring
- Log Analytics Workspace
- Azure Monitor alerts
- Action Groups (email notifications)
- Custom Delta log table: pipeline_run_log

## Logging

ADF diagnostic logs are sent to Log Analytics:

- PipelineRuns
- ActivityRuns
- TriggerRuns

## Alerting

Alert rule:

- Trigger: Pipeline failure
- Query:

```kusto
ADFPipelineRun
| where Status == "Failed"
| where TimeGenerated > ago(5m)
```

- Notification: Email via Action Group

## Observability Strategy

- Technical logs → Azure Monitor
- Business logs → Delta table (pipeline_run_log)
- Real-time alerts → Azure Alerts
