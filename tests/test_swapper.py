"""Tests for CSVSwapper."""
import pytest
from csvwrangler.swapper import CSVSwapper


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
        ["name", "city", "score"],
        [
            {"name": "Alice", "city": "London", "score": "10"},
            {"name": "Bob",   "city": "Paris",  "score": "20"},
            {"name": "Carol", "city": "Rome",   "score": "30"},
        ],
    )


def test_headers_unchanged():
    sw = CSVSwapper(_source(), "name", "city")
    assert sw.headers == ["name", "city", "score"]


def test_swap_values():
    sw = CSVSwapper(_source(), "name", "city")
    result = list(sw.rows())
    assert result[0]["name"] == "London"
    assert result[0]["city"] == "Alice"


def test_swap_does_not_affect_other_columns():
    sw = CSVSwapper(_source(), "name", "city")
    result = list(sw.rows())
    assert result[1]["score"] == "20"


def test_swap_all_rows():
    sw = CSVSwapper(_source(), "name", "score")
    result = list(sw.rows())
    assert result[0]["name"] == "10"
    assert result[0]["score"] == "Alice"
    assert result[2]["name"] == "30"
    assert result[2]["score"] == "Carol"


def test_row_count():
    sw = CSVSwapper(_source(), "name", "city")
    assert sw.row_count == 3


def test_missing_col_a_raises():
    with pytest.raises(ValueError, match="'missing'"):
        CSVSwapper(_source(), "missing", "city")


def test_missing_col_b_raises():
    with pytest.raises(ValueError, match="'missing'"):
        CSVSwapper(_source(), "name", "missing")


def test_same_column_raises():
    with pytest.raises(ValueError, match="different"):
        CSVSwapper(_source(), "name", "name")
