"""Tests for CSVDropper."""
import pytest
from csvwrangler.dropper import CSVDropper


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        return iter(self._rows)


def _source():
    return _FakeSource(
        headers=["id", "name", "dept", "salary"],
        rows=[
            {"id": "1", "name": "Alice", "dept": "Eng", "salary": "90000"},
            {"id": "2", "name": "Bob",   "dept": "HR",  "salary": "70000"},
            {"id": "3", "name": "Carol", "dept": "Eng", "salary": "95000"},
        ],
    )


def test_headers_excludes_dropped_columns():
    d = CSVDropper(_source(), ["salary"])
    assert d.headers == ["id", "name", "dept"]


def test_headers_multiple_drops():
    d = CSVDropper(_source(), ["dept", "salary"])
    assert d.headers == ["id", "name"]


def test_rows_exclude_dropped_columns():
    d = CSVDropper(_source(), ["salary"])
    result = list(d.rows())
    assert all("salary" not in r for r in result)


def test_rows_preserve_remaining_values():
    d = CSVDropper(_source(), ["dept", "salary"])
    result = list(d.rows())
    assert result[0] == {"id": "1", "name": "Alice"}
    assert result[1] == {"id": "2", "name": "Bob"}


def test_row_count():
    d = CSVDropper(_source(), ["id"])
    assert d.row_count == 3


def test_empty_columns_raises():
    with pytest.raises(ValueError, match="columns must not be empty"):
        CSVDropper(_source(), [])


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVDropper(_source(), ["nonexistent"])


def test_original_source_headers_unchanged():
    src = _source()
    CSVDropper(src, ["salary"])
    assert src.headers == ["id", "name", "dept", "salary"]
