"""Tests for CSVInterpolator."""
import pytest
from csvwrangler.interpolator import CSVInterpolator


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        for row in self._data:
            yield dict(row)


def _source(data):
    headers = ["name", "score", "value"]
    return _FakeSource(headers, data)


def _rows(interp):
    return list(interp.rows())


def test_headers_unchanged():
    src = _source([])
    interp = CSVInterpolator(src, ["score"])
    assert interp.headers == ["name", "score", "value"]


def test_no_missing_values_unchanged():
    src = _source([
        {"name": "a", "score": "10", "value": "1"},
        {"name": "b", "score": "20", "value": "2"},
    ])
    rows = _rows(CSVInterpolator(src, ["score"]))
    assert rows[0]["score"] == "10"
    assert rows[1]["score"] == "20"


def test_interpolates_middle_gap():
    src = _source([
        {"name": "a", "score": "10", "value": "1"},
        {"name": "b", "score": "",   "value": "2"},
        {"name": "c", "score": "20", "value": "3"},
    ])
    rows = _rows(CSVInterpolator(src, ["score"]))
    assert rows[1]["score"] == "15"


def test_interpolates_multiple_gaps():
    src = _source([
        {"name": "a", "score": "0",  "value": "1"},
        {"name": "b", "score": "",   "value": "2"},
        {"name": "c", "score": "",   "value": "3"},
        {"name": "d", "score": "30", "value": "4"},
    ])
    rows = _rows(CSVInterpolator(src, ["score"]))
    assert rows[1]["score"] == "10"
    assert rows[2]["score"] == "20"


def test_trailing_gap_fills_last_known():
    src = _source([
        {"name": "a", "score": "5", "value": "1"},
        {"name": "b", "score": "",  "value": "2"},
    ])
    rows = _rows(CSVInterpolator(src, ["score"]))
    assert rows[1]["score"] == "5"


def test_leading_gap_fills_first_known():
    src = _source([
        {"name": "a", "score": "",   "value": "1"},
        {"name": "b", "score": "42", "value": "2"},
    ])
    rows = _rows(CSVInterpolator(src, ["score"]))
    assert rows[0]["score"] == "42"


def test_multiple_columns_interpolated():
    src = _source([
        {"name": "a", "score": "0",  "value": "100"},
        {"name": "b", "score": "",   "value": ""},
        {"name": "c", "score": "10", "value": "200"},
    ])
    rows = _rows(CSVInterpolator(src, ["score", "value"]))
    assert rows[1]["score"] == "5"
    assert rows[1]["value"] == "150"


def test_unknown_column_raises():
    src = _source([])
    with pytest.raises(ValueError, match="unknown columns"):
        CSVInterpolator(src, ["nonexistent"])


def test_row_count():
    src = _source([
        {"name": "a", "score": "1", "value": "x"},
        {"name": "b", "score": "",  "value": "y"},
        {"name": "c", "score": "3", "value": "z"},
    ])
    interp = CSVInterpolator(src, ["score"])
    assert interp.row_count == 3
