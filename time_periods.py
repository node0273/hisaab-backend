"""Time period parsing — text + Telegram buttons"""
from datetime import datetime, timedelta, date

def parse_time_period(text: str) -> tuple:
    """Returns (start_date, end_date, label)"""
    text = text.lower().strip()
    today = date.today()

    if "this week" in text:
        return today - timedelta(days=today.weekday()), today, "This week"
    if "last week" in text:
        start = today - timedelta(days=today.weekday() + 7)
        return start, start + timedelta(days=6), "Last week"
    if "this month" in text:
        return today.replace(day=1), today, "This month"
    if "last month" in text:
        first = today.replace(day=1)
        end = first - timedelta(days=1)
        return end.replace(day=1), end, "Last month"
    if "3 month" in text:
        return today - timedelta(days=90), today, "Last 3 months"
    if "6 month" in text:
        return today - timedelta(days=180), today, "Last 6 months"
    if "this year" in text:
        return today.replace(month=1, day=1), today, "This year"
    return today - timedelta(days=30), today, "Last 30 days"

def get_time_period_buttons() -> list:
    return [
        [{"text": "This week", "callback_data": "period_this_week"},
         {"text": "Last week", "callback_data": "period_last_week"}],
        [{"text": "This month", "callback_data": "period_this_month"},
         {"text": "Last month", "callback_data": "period_last_month"}],
        [{"text": "Last 3 months", "callback_data": "period_3m"},
         {"text": "Last 6 months", "callback_data": "period_6m"}],
        [{"text": "This year", "callback_data": "period_year"},
         {"text": "Custom range", "callback_data": "period_custom"}],
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
