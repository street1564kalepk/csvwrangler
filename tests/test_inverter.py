"""Tests for CSVInverter."""
import pytest
from csvwrangler.inverter import CSVInverter


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    def headers(self):
        return list(self._headers)

    def rows(self):
        return iter([dict(r) for r in self._rows])


def _source():
    return _FakeSource(
        ["name", "active", "verified"],
        [
            {"name": "Alice", "active": "1",     "verified": "yes"},
            {"name": "Bob",   "active": "0",     "verified": "no"},
            {"name": "Carol", "active": "true",  "verified": "1"},
            {"name": "Dave",  "active": "false", "verified": "0"},
        ],
    )


def test_headers_unchanged():
    inv = CSVInverter(_source(), ["active"])
    assert inv.headers() == ["name", "active", "verified"]


def test_invert_truthy_becomes_zero():
    inv = CSVInverter(_source(), ["active"])
    results = list(inv.rows())
    assert results[0]["active"] == "0"   # '1' -> '0'
    assert results[2]["active"] == "0"   # 'true' -> '0'


def test_invert_falsy_becomes_one():
    inv = CSVInverter(_source(), ["active"])
    results = list(inv.rows())
    assert results[1]["active"] == "1"   # '0' -> '1'
    assert results[3]["active"] == "1"   # 'false' -> '1'


def test_invert_multiple_columns():
    inv = CSVInverter(_source(), ["active", "verified"])
    results = list(inv.rows())
    assert results[0]["active"] == "0"
    assert results[0]["verified"] == "0"  # 'yes' -> '0'
    assert results[1]["active"] == "1"
    assert results[1]["verified"] == "1"  # 'no' -> '1'


def test_non_inverted_column_untouched():
    inv = CSVInverter(_source(), ["active"])
    results = list(inv.rows())
    assert results[0]["name"] == "Alice"
    assert results[0]["verified"] == "yes"


def test_row_count():
    inv = CSVInverter(_source(), ["active"])
    assert inv.row_count() == 4


def test_empty_columns_raises():
    with pytest.raises(ValueError, match="columns must not be empty"):
        CSVInverter(_source(), [])


def test_unknown_column_raises():
    inv = CSVInverter(_source(), ["nonexistent"])
    with pytest.raises(ValueError, match="Unknown columns"):
        list(inv.rows())
