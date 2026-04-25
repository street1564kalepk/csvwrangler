"""Tests for CSVWeekdaySplitter."""
from __future__ import annotations

import pytest
from csvwrangler.splitter_by_weekday import CSVWeekdaySplitter


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    def rows(self):
        return iter(self._rows)


def _source():
    hdrs = ["id", "name", "date"]
    data = [
        {"id": "1", "name": "Alice", "date": "2024-01-01"},   # Monday
        {"id": "2", "name": "Bob",   "date": "2024-01-02"},   # Tuesday
        {"id": "3", "name": "Carol", "date": "2024-01-06"},   # Saturday
        {"id": "4", "name": "Dave",  "date": "2024-01-07"},   # Sunday
        {"id": "5", "name": "Eve",   "date": "2024-01-08"},   # Monday
        {"id": "6", "name": "Frank", "date": "not-a-date"},   # _unparsed
    ]
    return _FakeSource(hdrs, data)


def test_headers_unchanged():
    sp = CSVWeekdaySplitter(_source(), "date")
    assert sp.headers == ["id", "name", "date"]


def test_monday_group_count():
    sp = CSVWeekdaySplitter(_source(), "date")
    assert len(sp.group("Monday")) == 2


def test_tuesday_group_count():
    sp = CSVWeekdaySplitter(_source(), "date")
    assert len(sp.group("Tuesday")) == 1


def test_saturday_group():
    sp = CSVWeekdaySplitter(_source(), "date")
    rows = sp.group("Saturday")
    assert len(rows) == 1
    assert rows[0]["name"] == "Carol"


def test_sunday_group():
    sp = CSVWeekdaySplitter(_source(), "date")
    rows = sp.group("Sunday")
    assert rows[0]["name"] == "Dave"


def test_unparsed_bucket():
    sp = CSVWeekdaySplitter(_source(), "date")
    rows = sp.group("_unparsed")
    assert len(rows) == 1
    assert rows[0]["name"] == "Frank"


def test_group_keys_non_empty_only():
    sp = CSVWeekdaySplitter(_source(), "date")
    keys = sp.group_keys
    assert "Monday" in keys
    assert "Wednesday" not in keys


def test_group_count():
    sp = CSVWeekdaySplitter(_source(), "date")
    # Monday, Tuesday, Saturday, Sunday, _unparsed
    assert sp.group_count == 5


def test_rows_iterator():
    sp = CSVWeekdaySplitter(_source(), "date")
    names = [r["name"] for r in sp.rows("Monday")]
    assert names == ["Alice", "Eve"]


def test_empty_group_returns_empty_list():
    sp = CSVWeekdaySplitter(_source(), "date")
    assert sp.group("Wednesday") == []
