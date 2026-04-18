"""Tests for CSVShifter."""
import pytest
from csvwrangler.shifter import CSVShifter


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    @property
    def rows(self):
        return iter(self._data)


def _source():
    return _FakeSource(
        ["name", "score", "date"],
        [
            {"name": "Alice", "score": "10", "date": "2024-01-15"},
            {"name": "Bob",   "score": "20", "date": "2024-03-01"},
            {"name": "Carol", "score": "5",  "date": "2023-12-31"},
        ],
    )


def test_headers_unchanged():
    s = CSVShifter(_source(), {"score": 5})
    assert s.headers == ["name", "score", "date"]


def test_shift_integer_column():
    s = CSVShifter(_source(), {"score": 5})
    results = list(s.rows)
    assert results[0]["score"] == "15"
    assert results[1]["score"] == "25"
    assert results[2]["score"] == "10"


def test_shift_negative_offset():
    s = CSVShifter(_source(), {"score": -3})
    results = list(s.rows)
    assert results[0]["score"] == "7"


def test_shift_date_by_days():
    s = CSVShifter(_source(), {"date": {"days": 10}})
    results = list(s.rows)
    assert results[0]["date"] == "2024-01-25"
    assert results[2]["date"] == "2024-01-10"


def test_shift_date_negative_days():
    s = CSVShifter(_source(), {"date": {"days": -1}})
    results = list(s.rows)
    assert results[0]["date"] == "2024-01-14"


def test_non_numeric_value_left_unchanged():
    src = _FakeSource(["name", "score"], [{"name": "Alice", "score": "N/A"}])
    s = CSVShifter(src, {"score": 5})
    assert list(s.rows)[0]["score"] == "N/A"


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="not found"):
        CSVShifter(_source(), {"missing": 1})


def test_unshifted_columns_preserved():
    s = CSVShifter(_source(), {"score": 1})
    row = list(s.rows)[0]
    assert row["name"] == "Alice"
    assert row["date"] == "2024-01-15"


def test_row_count():
    s = CSVShifter(_source(), {"score": 0})
    assert s.row_count == 3
