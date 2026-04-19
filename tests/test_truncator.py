"""Tests for CSVTruncator."""

import pytest
from csvwrangler.truncator import CSVTruncator


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))


def _source():
    return _FakeSource(
        ["name", "bio", "city"],
        [
            ["Alice", "Software engineer at ACME Corp", "New York"],
            ["Bob", "Hi", "LA"],
            ["Charlie", "Data scientist and ML researcher", "San Francisco"],
        ],
    )


def test_headers_unchanged():
    t = CSVTruncator(_source(), {"bio": 10})
    assert t.headers == ["name", "bio", "city"]


def test_truncates_long_value():
    t = CSVTruncator(_source(), {"bio": 10})
    result = list(t.rows())
    assert result[0]["bio"] == "Software e…"


def test_short_value_unchanged():
    t = CSVTruncator(_source(), {"bio": 10})
    result = list(t.rows())
    assert result[1]["bio"] == "Hi"


def test_multiple_columns():
    t = CSVTruncator(_source(), {"name": 3, "city": 5})
    result = list(t.rows())
    assert result[0]["name"] == "Ali…"
    assert result[0]["city"] == "New Y…"
    assert result[1]["name"] == "Bob"


def test_custom_ellipsis():
    t = CSVTruncator(_source(), {"bio": 8}, ellipsis_str="...")
    result = list(t.rows())
    assert result[0]["bio"] == "Software..."


def test_row_count():
    t = CSVTruncator(_source(), {"bio": 5})
    assert t.row_count() == 3


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVTruncator(_source(), {"nonexistent": 5})


def test_invalid_max_len_raises():
    with pytest.raises(ValueError, match="positive integer"):
        CSVTruncator(_source(), {"bio": 0})


def test_exact_length_not_truncated():
    t = CSVTruncator(_source(), {"name": 5})
    result = list(t.rows())
    assert result[0]["name"] == "Alice"
