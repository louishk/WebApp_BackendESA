# New Pipeline

Scaffold a new ETL data pipeline: YAML config entry + datalayer module.

Usage: `/new-pipeline <pipeline-name> <description>`
Example: `/new-pipeline fx_rates Fetch daily FX rates from API and store in esa_pbi`

Pipeline: $ARGUMENTS

---

## Step 1: Parse Input

Extract:
- **pipeline_name**: snake_case identifier (e.g., `fx_rates`)
- **display_name**: Human readable (e.g., `FX Rates`)
- **description**: What the pipeline does

---

## Step 2: Study Existing Patterns

Read these files to understand the conventions:
1. `backend/python/config/pipelines.yaml` — see existing pipeline configs
2. One existing datalayer module (e.g., `backend/python/datalayer/populate_siteinfo.py` or similar) — see the module pattern
3. `backend/python/common/models.py` — see existing model definitions

---

## Step 3: Add Pipeline Config

Add an entry to `backend/python/config/pipelines.yaml` following the existing pattern:

```yaml
  <pipeline_name>:
    display_name: <Display Name>
    description: <description>
    module_path: datalayer.<pipeline_name>
    enabled: false  # Start disabled until tested
    schedule:
      type: cron
      cron: "0 5 * * *"  # Default daily 5am SGT — adjust as needed
    priority: 5
    resource_group: soap_api  # or http_api, depending on source
    max_db_connections: 2
    estimated_duration_seconds: 120
    retry:
      max_attempts: 3
      delay_seconds: 300
      backoff_multiplier: 2
    timeout_seconds: 600
    data_freshness:
      table: '<target_table>'
      date_column: 'created_at'
```

Ask the user about:
- Schedule (how often should it run?)
- Resource group (soap_api or http_api?)
- Target table name

---

## Step 4: Create Datalayer Module

Create `backend/python/datalayer/<pipeline_name>.py` following the existing module pattern.

The module must have a `run()` function (this is what the scheduler calls):

```python
"""
<Display Name> Pipeline
<description>
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def run(session=None, mode='full', **kwargs):
    """
    Main pipeline entry point.

    Args:
        session: SQLAlchemy session (provided by scheduler)
        mode: 'full' or 'incremental'
        **kwargs: Additional parameters from scheduler

    Returns:
        dict with 'records_processed' count
    """
    logger.info(f"Starting <pipeline_name> pipeline (mode={mode})")

    records = 0

    try:
        # TODO: Implement data extraction
        # TODO: Implement transformation
        # TODO: Implement loading to database

        logger.info(f"<pipeline_name> complete: {records} records processed")
        return {'records_processed': records}

    except Exception as e:
        logger.error(f"<pipeline_name> failed: {e}")
        raise
```

---

## Step 5: Create Model (if needed)

If the pipeline needs a new table, ask the user for the schema and add the model to `backend/python/common/models.py` following existing patterns (use `TimestampMixin`, `BaseModel`).

Also create a SQL migration in `backend/python/migrations/` to create the table.

---

## Step 6: Summary

Report what was created:
1. Pipeline config added to `pipelines.yaml`
2. Module created at `datalayer/<pipeline_name>.py`
3. Model added (if applicable)
4. Migration created (if applicable)

Remind the user to:
- Run any migrations on the database
- Implement the actual extraction/transformation logic in the module
- Test with: manual run from scheduler web UI
- Enable the pipeline in `pipelines.yaml` once tested (set `enabled: true`)

---

## Rules
- Start with `enabled: false` — never auto-enable new pipelines
- Follow the exact module pattern from existing datalayer modules
- Use `logging.getLogger(__name__)`, never `print()`
- Return `{'records_processed': N}` from `run()`
- Don't add pip dependencies without flagging
- Use SQLAlchemy ORM or parameterized queries — no string formatting for SQL
