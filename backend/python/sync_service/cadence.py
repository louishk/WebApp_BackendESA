"""
Cadence helpers for the orchestrator UI.

derive_frequency_category(schedule_config) returns a coarse bucket
('high' | 'med' | 'low') based on the cron expression in the pipeline's
schedule_config. Used to group the Data Freshness panel by how often
each pipeline runs.

Boundaries:
  - delta between two consecutive fires ≤ 3600s   → 'high'
  - delta ≤ 86400s                                → 'med'
  - delta > 86400s                                → 'low'

Returns None for on_demand pipelines or any cron we can't parse.
"""

from datetime import datetime
from typing import Optional


def derive_frequency_category(schedule_config: Optional[dict]) -> Optional[str]:
    if not schedule_config:
        return None
    cron = schedule_config.get('cron')
    if not cron:
        return None
    try:
        from croniter import croniter
        base = datetime(2026, 1, 1)
        c = croniter(cron, base)
        t1 = c.get_next(datetime)
        t2 = c.get_next(datetime)
        delta_seconds = (t2 - t1).total_seconds()
    except Exception:
        return None
    if delta_seconds <= 3600:
        return 'high'
    if delta_seconds <= 86400:
        return 'med'
    return 'low'
