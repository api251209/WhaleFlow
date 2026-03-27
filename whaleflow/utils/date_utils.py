from datetime import date, timedelta


def get_last_friday(reference: date | None = None) -> date:
    """Return the most recent Friday on or before the reference date."""
    d = reference or date.today()
    # weekday(): Monday=0, Friday=4
    days_since_friday = (d.weekday() - 4) % 7
    return d - timedelta(days=days_since_friday)


def normalize_to_week(d: date) -> date:
    """Normalize any date to the Friday of its ISO week (TDCC settlement day)."""
    return d + timedelta(days=(4 - d.weekday()) % 7)


def get_friday_range(start: date, end: date) -> list[date]:
    """Return all Fridays between start and end (inclusive)."""
    fridays = []
    current = normalize_to_week(start)
    while current <= end:
        fridays.append(current)
        current += timedelta(weeks=1)
    return fridays


def weeks_between(d1: date, d2: date) -> int:
    """Return the number of weeks between two Friday dates."""
    delta = abs((d2 - d1).days)
    return delta // 7


def date_to_str(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def str_to_date(s: str) -> date:
    return date.fromisoformat(s)
