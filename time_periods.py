"""
Time period parsing — text + Telegram buttons
"""
from datetime import datetime, timedelta, date
import calendar

def parse_time_period(text: str) -> tuple:
    """
    Parse natural language time period.
    Returns (start_date, end_date, label)
    """
    text = text.lower().strip()
    today = date.today()

    if any(x in text for x in ["this week", "current week"]):
        start = today - timedelta(days=today.weekday())
        return start, today, "This week"

    if any(x in text for x in ["last week", "previous week"]):
        start = today - timedelta(days=today.weekday() + 7)
        end = start + timedelta(days=6)
        return start, end, "Last week"

    if any(x in text for x in ["this month", "current month"]):
        start = today.replace(day=1)
        return start, today, "This month"

    if any(x in text for x in ["last month", "previous month"]):
        first_this = today.replace(day=1)
        last_month_end = first_this - timedelta(days=1)
        start = last_month_end.replace(day=1)
        return start, last_month_end, "Last month"

    if any(x in text for x in ["last 3 months", "3 months", "past 3 months"]):
        start = today - timedelta(days=90)
        return start, today, "Last 3 months"

    if any(x in text for x in ["last 6 months", "6 months", "past 6 months"]):
        start = today - timedelta(days=180)
        return start, today, "Last 6 months"

    if any(x in text for x in ["this year", "current year"]):
        start = today.replace(month=1, day=1)
        return start, today, "This year"

    # Default — last 30 days
    return today - timedelta(days=30), today, "Last 30 days"

def get_time_period_buttons() -> list:
    """Returns Telegram inline keyboard for time period selection."""
    return [
        [{"text": "📅 This week", "callback_data": "period_this_week"},
         {"text": "📅 Last week", "callback_data": "period_last_week"}],
        [{"text": "📅 This month", "callback_data": "period_this_month"},
         {"text": "📅 Last month", "callback_data": "period_last_month"}],
        [{"text": "📅 Last 3 months", "callback_data": "period_3m"},
         {"text": "📅 Last 6 months", "callback_data": "period_6m"}],
        [{"text": "📅 This year", "callback_data": "period_year"},
         {"text": "📅 Custom range", "callback_data": "period_custom"}],
    ]

CALLBACK_TO_PERIOD = {
    "period_this_week":  "this week",
    "period_last_week":  "last week",
    "period_this_month": "this month",
    "period_last_month": "last month",
    "period_3m":         "last 3 months",
    "period_6m":         "last 6 months",
    "period_year":       "this year",
}
