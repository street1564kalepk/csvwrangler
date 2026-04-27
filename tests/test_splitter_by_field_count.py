"""Tests for CSVFieldCountSplitter."""

import pytest
from csvwrangler.splitter_by_field_count import CSVFieldCountSplitter


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
    hdrs = ["name", "city", "score"]
    data = [
        {"name": "Alice", "city": "London", "score": "90"},   # 3 non-empty
        {"name": "Bob",   "city": "",       "score": "80"},   # 2 non-empty
        {"name": "Carol", "city": "Paris",  "score": ""},    # 2 non-empty
        {"name": "",      "city": "",       "score": "70"},   # 1 non-empty
        {"name": "Dave",  "city": "Rome",   "score": "60"},   # 3 non-empty
    ]
    return _FakeSource(hdrs, data)


def test_headers_unchanged():
    s = CSVFieldCountSplitter(_source())
    assert s.headers == ["name", "city", "score"]


def test_group_keys_sorted():
    s = CSVFieldCountSplitter(_source())
    assert s.group_keys == [1, 2, 3]


def test_group_count():
    s = CSVFieldCountSplitter(_source())
    assert s.group_count() == 3


def test_rows_for_three_non_empty():
    s = CSVFieldCountSplitter(_source())
    result = s.rows_for(3)
    names = [r["name"] for r in result]
    assert names == ["Alice", "Dave"]


def test_rows_for_two_non_empty():
    s = CSVFieldCountSplitter(_source())
    result = s.rows_for(2)
    assert len(result) == 2


def test_rows_for_one_non_empty():
    s = CSVFieldCountSplitter(_source())
    result = s.rows_for(1)
    assert len(result) == 1
    assert result[0]["score"] == "70"


def test_rows_for_missing_key_returns_empty():
    s = CSVFieldCountSplitter(_source())
    assert s.rows_for(0) == []


def test_all_groups_total_rows():
    s = CSVFieldCountSplitter(_source())
    groups = s.all_groups()
    total = sum(len(v) for v in groups.values())
    assert total == 5


def test_all_groups_returns_copy():
    s = CSVFieldCountSplitter(_source())
    g1 = s.all_groups()
    g1[3].clear()
    g2 = s.all_groups()
    assert len(g2[3]) == 2
