"""Tests for CSVProfiler."""
import pytest
from csvwrangler.profiler import CSVProfiler


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    @property
    def rows(self):
        return iter(self._rows)


def _source():
    return _FakeSource(
        ["name", "age", "city"],
        [
            {"name": "Alice", "age": "30", "city": "London"},
            {"name": "Bob",   "age": "25", "city": "Paris"},
            {"name": "Alice", "age": "",   "city": "London"},
            {"name": "Carol", "age": "40", "city": ""},
        ],
    )


def test_headers_unchanged():
    p = CSVProfiler(_source())
    assert p.headers == ["name", "age", "city"]


def test_profile_returns_all_columns():
    p = CSVProfiler(_source())
    prof = p.profile()
    assert set(prof.keys()) == {"name", "age", "city"}


def test_count():
    p = CSVProfiler(_source())
    assert p.column("name")["count"] == 4


def test_nulls():
    p = CSVProfiler(_source())
    assert p.column("age")["nulls"] == 1
    assert p.column("city")["nulls"] == 1
    assert p.column("name")["nulls"] == 0


def test_unique():
    p = CSVProfiler(_source())
    # Alice appears twice but unique count should be 2 (Alice, Bob, Carol minus dup)
    assert p.column("name")["unique"] == 3
    assert p.column("city")["unique"] == 2  # London, Paris (empty excluded)


def test_numeric_min_max():
    p = CSVProfiler(_source())
    s = p.column("age")
    assert s["min"] == 25.0
    assert s["max"] == 40.0


def test_string_min_max():
    p = CSVProfiler(_source())
    s = p.column("name")
    assert s["min"] == "Alice"
    assert s["max"] == "Carol"


def test_unknown_column_raises():
    p = CSVProfiler(_source())
    with pytest.raises(KeyError):
        p.column("nonexistent")


def test_summary_rows_structure():
    p = CSVProfiler(_source())
    rows = p.summary_rows()
    assert len(rows) == 3
    keys = set(rows[0].keys())
    assert keys == {"column", "count", "nulls", "unique", "min", "max"}


def test_summary_rows_order():
    p = CSVProfiler(_source())
    rows = p.summary_rows()
    assert [r["column"] for r in rows] == ["name", "age", "city"]


def test_empty_source():
    src = _FakeSource(["x", "y"], [])
    p = CSVProfiler(src)
    s = p.column("x")
    assert s["count"] == 0
    assert s["nulls"] == 0
    assert s["unique"] == 0
    assert s["min"] is None
    assert s["max"] is None
