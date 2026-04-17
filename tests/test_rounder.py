"""Tests for CSVRounder."""
import pytest
from csvwrangler.rounder import CSVRounder


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(row)


def _source():
    return _FakeSource(
        ["name", "score", "ratio"],
        [
            {"name": "Alice", "score": "3.14159", "ratio": "0.666667"},
            {"name": "Bob",   "score": "2.71828", "ratio": "1.333333"},
            {"name": "Carol", "score": "1.0",     "ratio": ""},
        ],
    )


def test_headers_unchanged():
    r = CSVRounder(_source(), {"score": 2})
    assert r.headers == ["name", "score", "ratio"]


def test_round_single_column():
    rows = list(CSVRounder(_source(), {"score": 2}).rows())
    assert rows[0]["score"] == 3.14
    assert rows[1]["score"] == 2.72


def test_round_multiple_columns():
    rows = list(CSVRounder(_source(), {"score": 1, "ratio": 3}).rows())
    assert rows[0]["score"] == 3.1
    assert rows[0]["ratio"] == 0.667


def test_empty_value_left_untouched():
    rows = list(CSVRounder(_source(), {"ratio": 2}).rows())
    assert rows[2]["ratio"] == ""


def test_non_numeric_value_left_untouched():
    src = _FakeSource(["val"], [{"val": "N/A"}, {"val": "1.5"}])
    rows = list(CSVRounder(src, {"val": 1}).rows())
    assert rows[0]["val"] == "N/A"
    assert rows[1]["val"] == 1.5


def test_row_count():
    r = CSVRounder(_source(), {"score": 2})
    assert r.row_count == 3


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVRounder(_source(), {"nonexistent": 2})


def test_empty_columns_raises():
    with pytest.raises(ValueError, match="must not be empty"):
        CSVRounder(_source(), {})


def test_zero_decimals():
    rows = list(CSVRounder(_source(), {"score": 0}).rows())
    assert rows[0]["score"] == 3.0
    assert rows[1]["score"] == 3.0
