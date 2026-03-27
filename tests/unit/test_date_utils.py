from datetime import date

import pytest

from whaleflow.utils.date_utils import (
    date_to_str,
    get_friday_range,
    get_last_friday,
    normalize_to_week,
    str_to_date,
    weeks_between,
)


def test_get_last_friday_on_friday():
    assert get_last_friday(date(2026, 3, 20)) == date(2026, 3, 20)  # Friday


def test_get_last_friday_on_saturday():
    assert get_last_friday(date(2026, 3, 21)) == date(2026, 3, 20)


def test_get_last_friday_on_monday():
    assert get_last_friday(date(2026, 3, 23)) == date(2026, 3, 20)


def test_normalize_to_week_friday():
    assert normalize_to_week(date(2026, 3, 20)) == date(2026, 3, 20)


def test_normalize_to_week_monday():
    assert normalize_to_week(date(2026, 3, 16)) == date(2026, 3, 20)


def test_get_friday_range():
    fridays = get_friday_range(date(2026, 3, 1), date(2026, 3, 20))
    assert date(2026, 3, 6) in fridays
    assert date(2026, 3, 13) in fridays
    assert date(2026, 3, 20) in fridays
    assert len(fridays) == 3


def test_weeks_between():
    assert weeks_between(date(2026, 3, 6), date(2026, 3, 20)) == 2
    assert weeks_between(date(2026, 3, 20), date(2026, 3, 6)) == 2


def test_date_to_str():
    assert date_to_str(date(2026, 3, 20)) == "2026-03-20"


def test_str_to_date():
    assert str_to_date("2026-03-20") == date(2026, 3, 20)
