"""Tests for CSVMerger."""
import pytest
from csvwrangler.merger import CSVMerger


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


def _left():
    return _FakeSource(
        ["id", "name", "city"],
        [
            ["1", "Alice", "London"],
            ["2", "Bob", "Paris"],
            ["3", "Carol", "Berlin"],
        ],
    )


def _right():
    return _FakeSource(
        ["id", "score"],
        [
            ["1", "95"],
            ["2", "80"],
        ],
    )


def _right_conflict():
    """Right source shares 'city' column with left."""
    return _FakeSource(
        ["id", "city", "score"],
        [
            ["1", "NYC", "95"],
        ],
    )


def test_headers_no_conflict():
    m = CSVMerger(_left(), _right(), on="id")
    assert m.headers == ["id", "name", "city", "score"]


def test_row_count_equals_left():
    m = CSVMerger(_left(), _right(), on="id")
    assert m.row_count == 3


def test_matched_row_has_right_values():
    m = CSVMerger(_left(), _right(), on="id")
    rows = list(m.rows())
    assert rows[0]["score"] == "95"
    assert rows[1]["score"] == "80"


def test_unmatched_row_has_empty_right_values():
    m = CSVMerger(_left(), _right(), on="id")
    rows = list(m.rows())
    assert rows[2]["score"] == ""


def test_left_values_preserved():
    m = CSVMerger(_left(), _right(), on="id")
    rows = list(m.rows())
    assert rows[0]["name"] == "Alice"
    assert rows[0]["city"] == "London"


def test_conflicting_column_renamed():
    m = CSVMerger(_left(), _right_conflict(), on="id")
    assert "city_left" in m.headers
    assert "city_right" in m.headers
    rows = list(m.rows())
    assert rows[0]["city_left"] == "London"
    assert rows[0]["city_right"] == "NYC"


def test_invalid_key_raises():
    with pytest.raises(ValueError, match="Key column"):
        CSVMerger(_left(), _right(), on="missing")
