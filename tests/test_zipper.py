"""Tests for CSVZipper."""
import pytest
from csvwrangler.zipper import CSVZipper


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))


def _left():
    return _FakeSource(["id", "name"], [("1", "Alice"), ("2", "Bob"), ("3", "Carol")])


def _right():
    return _FakeSource(["city", "score"], [("NYC", "90"), ("LA", "85")])


def _right_overlap():
    return _FakeSource(["id", "city"], [("10", "NYC"), ("20", "LA")])


def test_headers_no_overlap():
    z = CSVZipper(_left(), _right())
    assert z.headers == ["id", "name", "city", "score"]


def test_headers_with_overlap():
    z = CSVZipper(_left(), _right_overlap())
    assert z.headers == ["id", "name", "id_right", "city"]


def test_rows_limited_to_shorter_source():
    z = CSVZipper(_left(), _right())
    result = list(z.rows())
    assert len(result) == 2  # right has only 2 rows


def test_rows_merged_correctly():
    z = CSVZipper(_left(), _right())
    result = list(z.rows())
    assert result[0] == {"id": "1", "name": "Alice", "city": "NYC", "score": "90"}
    assert result[1] == {"id": "2", "name": "Bob", "city": "LA", "score": "85"}


def test_rows_with_overlap_renamed():
    z = CSVZipper(_left(), _right_overlap())
    result = list(z.rows())
    assert result[0]["id"] == "1"
    assert result[0]["id_right"] == "10"
    assert result[0]["city"] == "NYC"


def test_row_count():
    z = CSVZipper(_left(), _right())
    assert z.row_count() == 2


def test_empty_right():
    right = _FakeSource(["city"], [])
    z = CSVZipper(_left(), right)
    assert list(z.rows()) == []


def test_empty_left():
    left = _FakeSource(["id", "name"], [])
    z = CSVZipper(left, _right())
    assert list(z.rows()) == []
