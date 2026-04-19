"""Tests for CSVSpliceor."""
import pytest
from csvwrangler.spliceor import CSVSpliceor


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(row)

    def row_count(self):
        return len(self._data)


def _left():
    return _FakeSource(
        ["name", "dept"],
        [
            {"name": "Alice", "dept": "Eng"},
            {"name": "Bob", "dept": "HR"},
            {"name": "Carol", "dept": "Eng"},
        ],
    )


def _right():
    return _FakeSource(
        ["name", "dept"],
        [
            {"name": "Dave", "dept": "Sales"},
        ],
    )


def test_headers_unchanged():
    s = CSVSpliceor(_left(), _right())
    assert s.headers() == ["name", "dept"]


def test_append_by_default():
    s = CSVSpliceor(_left(), _right(), after_row=-1)
    names = [r["name"] for r in s.rows()]
    assert names == ["Alice", "Bob", "Carol", "Dave"]


def test_insert_after_first_row():
    s = CSVSpliceor(_left(), _right(), after_row=0)
    names = [r["name"] for r in s.rows()]
    assert names == ["Alice", "Dave", "Bob", "Carol"]


def test_insert_after_second_row():
    s = CSVSpliceor(_left(), _right(), after_row=1)
    names = [r["name"] for r in s.rows()]
    assert names == ["Alice", "Bob", "Dave", "Carol"]


def test_row_count_append():
    s = CSVSpliceor(_left(), _right())
    assert s.row_count() == 4


def test_missing_columns_in_other_filled_with_empty():
    other = _FakeSource(["name"], [{"name": "Eve"}])
    s = CSVSpliceor(_left(), other, after_row=-1)
    rows = list(s.rows())
    last = rows[-1]
    assert last["name"] == "Eve"
    assert last["dept"] == ""


def test_extra_columns_in_other_are_dropped():
    other = _FakeSource(["name", "dept", "extra"], [{"name": "Eve", "dept": "IT", "extra": "x"}])
    s = CSVSpliceor(_left(), other, after_row=-1)
    rows = list(s.rows())
    assert "extra" not in rows[-1]
