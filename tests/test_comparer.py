"""Tests for CSVComparer."""
import pytest
from csvwrangler.comparer import CSVComparer


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))


def _left():
    return _FakeSource(
        ["name", "score", "city"],
        [
            ["Alice", "90", "London"],
            ["Bob", "75", "Paris"],
            ["Carol", "88", "Berlin"],
        ],
    )


def _right():
    return _FakeSource(
        ["name", "score", "city"],
        [
            ["Alice", "90", "London"],
            ["Bob", "80", "Paris"],
            ["Carol", "88", "Rome"],
        ],
    )


def test_comparer_headers():
    c = CSVComparer(_left(), _right())
    assert c.headers() == ["row_index", "column", "left_value", "right_value", "match"]


def test_comparer_row_count_all_columns():
    c = CSVComparer(_left(), _right())
    # 3 rows * 3 columns = 9
    assert c.row_count() == 9


def test_comparer_row_count_selected_columns():
    c = CSVComparer(_left(), _right(), columns=["score"])
    assert c.row_count() == 3


def test_comparer_match_values():
    c = CSVComparer(_left(), _right(), columns=["name"])
    result = list(c.rows())
    assert all(r["match"] == "true" for r in result)


def test_comparer_mismatch_detected():
    c = CSVComparer(_left(), _right(), columns=["score", "city"])
    mismatches = list(c.mismatches())
    # Bob score differs, Carol city differs => 2 mismatches
    assert len(mismatches) == 2


def test_comparer_mismatch_count():
    c = CSVComparer(_left(), _right())
    assert c.mismatch_count() == 2


def test_comparer_mismatch_row_details():
    c = CSVComparer(_left(), _right(), columns=["score"])
    mismatches = list(c.mismatches())
    assert len(mismatches) == 1
    m = mismatches[0]
    assert m["column"] == "score"
    assert m["left_value"] == "75"
    assert m["right_value"] == "80"
    assert m["row_index"] == "2"


def test_comparer_common_columns_only():
    left = _FakeSource(["name", "score"], [["Alice", "10"]])
    right = _FakeSource(["name", "city"], [["Alice", "NYC"]])
    c = CSVComparer(left, right)
    result = list(c.rows())
    assert all(r["column"] == "name" for r in result)
    assert len(result) == 1
