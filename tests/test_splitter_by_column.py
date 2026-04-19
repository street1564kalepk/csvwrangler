import pytest
from csvwrangler.splitter_by_column import CSVColumnSplitter


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._data


def _source():
    headers = ["name", "dept", "salary", "city"]
    data = [
        {"name": "Alice", "dept": "Eng", "salary": "90000", "city": "NYC"},
        {"name": "Bob",   "dept": "HR",  "salary": "60000", "city": "LA"},
        {"name": "Carol", "dept": "Eng", "salary": "85000", "city": "NYC"},
    ]
    return _FakeSource(headers, data)


def test_group_count():
    s = CSVColumnSplitter(_source(), [["name", "dept"], ["salary", "city"]])
    assert s.group_count == 2


def test_headers_for_group_0():
    s = CSVColumnSplitter(_source(), [["name", "dept"], ["salary", "city"]])
    assert s.headers_for(0) == ["name", "dept"]


def test_headers_for_group_1():
    s = CSVColumnSplitter(_source(), [["name", "dept"], ["salary", "city"]])
    assert s.headers_for(1) == ["salary", "city"]


def test_rows_for_group_0():
    s = CSVColumnSplitter(_source(), [["name", "dept"], ["salary", "city"]])
    rows = list(s.rows_for(0))
    assert rows[0] == {"name": "Alice", "dept": "Eng"}
    assert rows[1] == {"name": "Bob",   "dept": "HR"}


def test_rows_for_group_1():
    s = CSVColumnSplitter(_source(), [["name", "dept"], ["salary", "city"]])
    rows = list(s.rows_for(1))
    assert rows[0] == {"salary": "90000", "city": "NYC"}


def test_row_count_for():
    s = CSVColumnSplitter(_source(), [["name"], ["dept", "salary"]])
    assert s.row_count_for(0) == 3
    assert s.row_count_for(1) == 3


def test_column_may_appear_in_multiple_groups():
    s = CSVColumnSplitter(_source(), [["name", "dept"], ["name", "salary"]])
    rows0 = list(s.rows_for(0))
    rows1 = list(s.rows_for(1))
    assert rows0[0]["name"] == rows1[0]["name"]


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="unknown columns"):
        CSVColumnSplitter(_source(), [["name", "nonexistent"]])


def test_empty_group_raises():
    with pytest.raises(ValueError, match="empty"):
        CSVColumnSplitter(_source(), [[]])


def test_single_group_all_columns():
    s = CSVColumnSplitter(_source(), [["name", "dept", "salary", "city"]])
    rows = list(s.rows_for(0))
    assert len(rows) == 3
    assert set(rows[0].keys()) == {"name", "dept", "salary", "city"}
