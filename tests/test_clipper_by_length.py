"""Tests for CSVLengthClipper."""

import pytest
from csvwrangler.clipper_by_length import CSVLengthClipper


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(row) for row in self._data)


def _source():
    return _FakeSource(
        ["name", "city"],
        [
            {"name": "Al", "city": "Rome"},
            {"name": "Bob", "city": "Paris"},
            {"name": "Charlie", "city": "London"},
            {"name": "Di", "city": "NY"},
        ],
    )


def test_headers_unchanged():
    c = CSVLengthClipper(_source(), "name", "gt", 2)
    assert c.headers == ["name", "city"]


def test_filter_gt():
    rows = list(CSVLengthClipper(_source(), "name", "gt", 2).rows())
    names = [r["name"] for r in rows]
    assert names == ["Bob", "Charlie"]


def test_filter_gte():
    rows = list(CSVLengthClipper(_source(), "name", "gte", 3).rows())
    names = [r["name"] for r in rows]
    assert names == ["Bob", "Charlie"]


def test_filter_lt():
    rows = list(CSVLengthClipper(_source(), "name", "lt", 3).rows())
    names = [r["name"] for r in rows]
    assert names == ["Al", "Di"]


def test_filter_eq():
    rows = list(CSVLengthClipper(_source(), "name", "eq", 3).rows())
    names = [r["name"] for r in rows]
    assert names == ["Bob"]


def test_filter_ne():
    rows = list(CSVLengthClipper(_source(), "name", "ne", 2).rows())
    names = [r["name"] for r in rows]
    assert names == ["Bob", "Charlie"]


def test_row_count():
    c = CSVLengthClipper(_source(), "name", "gte", 3)
    assert c.row_count == 2


def test_unknown_op_raises():
    with pytest.raises(ValueError, match="Unknown op"):
        CSVLengthClipper(_source(), "name", "bad_op", 3)


def test_missing_column_treated_as_empty():
    src = _FakeSource(["name"], [{"name": "Al"}, {"name": "Bob"}])
    rows = list(CSVLengthClipper(src, "city", "eq", 0).rows())
    # missing key → empty string → length 0
    assert len(rows) == 2
