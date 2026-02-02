"""
Scheduler utility functions.
"""

from typing import Optional


def cron_to_human(cron_expr: str) -> str:
    """
    Convert a cron expression to human-readable format.

    Args:
        cron_expr: Standard 5-field cron expression (minute hour day month weekday)

    Returns:
        Human-readable description of the schedule

    Examples:
        - "0 6 * * *" → "Daily at 6:00 AM"
        - "0 4 * * 0" → "Sundays at 4:00 AM"
        - "*/15 * * * *" → "Every 15 minutes"
        - "0 0 1 * *" → "Monthly on day 1 at 12:00 AM"
        - "30 14 * * 1-5" → "Weekdays at 2:30 PM"
    """
    if not cron_expr or not isinstance(cron_expr, str):
        return cron_expr or "N/A"

    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return cron_expr  # Return original if not standard format

    minute, hour, day, month, weekday = parts

    # Format time
    def format_time(h: str, m: str) -> str:
        try:
            hour_int = int(h)
            min_int = int(m)
            period = "AM" if hour_int < 12 else "PM"
            display_hour = hour_int % 12 or 12
            if min_int == 0:
                return f"{display_hour}:00 {period}"
            else:
                return f"{display_hour}:{min_int:02d} {period}"
        except ValueError:
            return f"{h}:{m}"

    # Weekday names
    weekday_names = {
        '0': 'Sundays',
        '1': 'Mondays',
        '2': 'Tuesdays',
        '3': 'Wednesdays',
        '4': 'Thursdays',
        '5': 'Fridays',
        '6': 'Saturdays',
        '7': 'Sundays',  # Some systems use 7 for Sunday
    }

    # Check for common patterns

    # Every N minutes: */N * * * *
    if minute.startswith('*/') and hour == '*' and day == '*' and month == '*' and weekday == '*':
        interval = minute[2:]
        return f"Every {interval} minutes"

    # Every hour: 0 * * * *
    if minute == '0' and hour == '*' and day == '*' and month == '*' and weekday == '*':
        return "Every hour"

    # Every N hours: 0 */N * * *
    if minute == '0' and hour.startswith('*/') and day == '*' and month == '*' and weekday == '*':
        interval = hour[2:]
        return f"Every {interval} hours"

    # Specific time daily: M H * * *
    if day == '*' and month == '*' and weekday == '*':
        try:
            time_str = format_time(hour, minute)
            return f"Daily at {time_str}"
        except:
            pass

    # Specific weekday(s): M H * * W
    if day == '*' and month == '*' and weekday != '*':
        time_str = format_time(hour, minute)

        # Single weekday
        if weekday in weekday_names:
            return f"{weekday_names[weekday]} at {time_str}"

        # Weekday range (e.g., 1-5 for Mon-Fri)
        if '-' in weekday:
            start, end = weekday.split('-')
            if start == '1' and end == '5':
                return f"Weekdays at {time_str}"
            elif start == '0' and end == '6':
                return f"Daily at {time_str}"

        # Multiple weekdays (e.g., 1,3,5)
        if ',' in weekday:
            days = weekday.split(',')
            day_names = [weekday_names.get(d, d) for d in days]
            return f"{', '.join(day_names)} at {time_str}"

        return f"Weekday {weekday} at {time_str}"

    # Monthly: M H D * *
    if month == '*' and weekday == '*' and day != '*':
        time_str = format_time(hour, minute)
        if day == '1':
            return f"Monthly on 1st at {time_str}"
        elif day == '15':
            return f"Monthly on 15th at {time_str}"
        else:
            return f"Monthly on day {day} at {time_str}"

    # Yearly: M H D M *
    if weekday == '*' and month != '*' and day != '*':
        time_str = format_time(hour, minute)
        month_names = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        try:
            month_name = month_names[int(month)]
            return f"Yearly on {month_name} {day} at {time_str}"
        except:
            pass

    # Default: return the cron expression with a hint
    return f"Cron: {cron_expr}"


def human_to_cron(description: str) -> Optional[str]:
    """
    Convert a human-readable schedule description to cron expression.

    Args:
        description: Human-readable schedule like "Daily at 6:00 AM"

    Returns:
        Cron expression or None if not parseable
    """
    desc = description.lower().strip()

    # Parse time from description
    import re
    time_match = re.search(r'(\d{1,2}):?(\d{2})?\s*(am|pm)?', desc, re.IGNORECASE)

    if time_match:
        hour = int(time_match.group(1))
        minute = int(time_match.group(2) or 0)
        period = (time_match.group(3) or '').lower()

        if period == 'pm' and hour < 12:
            hour += 12
        elif period == 'am' and hour == 12:
            hour = 0
    else:
        hour = 0
        minute = 0

    # Determine frequency
    if 'every' in desc and 'minute' in desc:
        interval_match = re.search(r'every\s+(\d+)\s+minute', desc)
        if interval_match:
            return f"*/{interval_match.group(1)} * * * *"

    if 'every' in desc and 'hour' in desc:
        interval_match = re.search(r'every\s+(\d+)\s+hour', desc)
        if interval_match:
            return f"0 */{interval_match.group(1)} * * *"

    if 'daily' in desc:
        return f"{minute} {hour} * * *"

    if 'weekday' in desc or 'monday' in desc and 'friday' in desc:
        return f"{minute} {hour} * * 1-5"

    weekdays = {
        'sunday': '0', 'monday': '1', 'tuesday': '2', 'wednesday': '3',
        'thursday': '4', 'friday': '5', 'saturday': '6'
    }
    for day_name, day_num in weekdays.items():
        if day_name in desc:
            return f"{minute} {hour} * * {day_num}"

    if 'monthly' in desc:
        day_match = re.search(r'(\d{1,2})(st|nd|rd|th)?', desc)
        day = day_match.group(1) if day_match else '1'
        return f"{minute} {hour} {day} * *"

    return None


# Common schedule presets for UI dropdown
SCHEDULE_PRESETS = [
    {"label": "Daily at 5:00 AM", "cron": "0 5 * * *"},
    {"label": "Daily at 6:00 AM", "cron": "0 6 * * *"},
    {"label": "Daily at 7:00 AM", "cron": "0 7 * * *"},
    {"label": "Daily at 8:00 AM", "cron": "0 8 * * *"},
    {"label": "Daily at 9:00 AM", "cron": "0 9 * * *"},
    {"label": "Daily at 10:00 AM", "cron": "0 10 * * *"},
    {"label": "Daily at 11:00 AM", "cron": "0 11 * * *"},
    {"label": "Daily at 12:00 PM", "cron": "0 12 * * *"},
    {"label": "Every 15 minutes", "cron": "*/15 * * * *"},
    {"label": "Every 30 minutes", "cron": "*/30 * * * *"},
    {"label": "Hourly", "cron": "0 * * * *"},
    {"label": "Every 2 hours", "cron": "0 */2 * * *"},
    {"label": "Weekdays at 6:00 AM", "cron": "0 6 * * 1-5"},
    {"label": "Sundays at 4:00 AM", "cron": "0 4 * * 0"},
    {"label": "Monthly on 1st at 6:00 AM", "cron": "0 6 1 * *"},
]
