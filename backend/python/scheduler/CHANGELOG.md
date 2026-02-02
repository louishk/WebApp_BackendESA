# PBI Scheduler Changelog

All notable changes to the PBI Scheduler will be documented in this file.

## [1.0.0] - 2026-01-19

### Added
- Initial release of PBI Scheduler
- APScheduler-based job scheduling engine
- Web UI for job management and monitoring
- Pipeline configuration via YAML (`config/pipelines.yaml`)
- Auto-reload config on file changes (10-second polling)
- Job history tracking in PostgreSQL
- Resource management and conflict resolution
- Support for cron, interval, and date-based schedules
- Real-time job execution streaming via SSE
- CLI for scheduler management (`run_scheduler.py`)
- Retry logic with exponential backoff
- Priority-based job queuing

### Pipelines Supported
- siteinfo - Site dimension table population
- unitsinfo - Unit information from SOAP API
- fxrate - Foreign exchange rates from Yahoo Finance
- rentroll - Unit inventory and occupancy data
- discount - Discount and promotion data
- mimo - Move-in/Move-out tenant data
- sugarcrm - CRM lead synchronization
