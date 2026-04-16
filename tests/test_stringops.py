"""Tests for CSVStringOps."""
import pytest
from csvwrangler.stringops import CSVStringOps


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._data


def _source():
    return _FakeSource(
        ["id", "name", "city"],
        [
            {"id": "1", "name": "  alice  ", "city": "new york"},
            {"id": "2", "name": "bob", "city": "london"},
            {"id": "3", "name": "CAROL", "city": "  Paris  "},
        ],
    )


def test_headers_unchanged():
    ops = CSVStringOps(_source(), {"name": "strip"})
    assert ops.headers == ["id", "name", "city"]


def test_upper():
    ops = CSVStringOps(_source(), {"city": "upper"})
    results = list(ops.rows())
    assert results[0]["city"] == "NEW YORK"
    assert results[1]["city"] == "LONDON"


def test_lower():
    ops = CSVStringOps(_source(), {"name": "lower"})
    results = list(ops.rows())
    assert results[2]["name"] == "carol"


def test_strip():
    ops = CSVStringOps(_source(), {"name": "strip", "city": "strip"})
    results = list(ops.rows())
    assert results[0]["name"] == "alice"
    assert results[2]["city"] == "Paris"


def test_title():
    ops = CSVStringOps(_source(), {"city": "title"})
    results = list(ops.rows())
    assert results[0]["city"] == "New York"


def test_multiple_columns():
    ops = CSVStringOps(_source(), {"name": "strip", "city": "upper"})
    results = list(ops.rows())
    assert results[0]["name"] == "alice"
    assert results[0]["city"] == "NEW YORK"


def test_non_string_value_skipped():
    src = _FakeSource(["id", "val"], [{"id": "1", "val": 42}])
    ops = CSVStringOps(src, {"val": "upper"})
    results = list(ops.rows())
    assert results[0]["val"] == 42


def test_unsupported_op_raises():
    with pytest.raises(ValueError, match="Unsupported"):
        CSVStringOps(_source(), {"name": "reverse"})


def test_missing_column_raises():
    with pytest.raises(ValueError, match="not found"):
        CSVStringOps(_source(), {"nonexistent": "upper"})


def test_row_count():
    ops = CSVStringOps(_source(), {"name": "upper"})
    assert ops.row_count() == 3
