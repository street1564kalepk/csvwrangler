"""Tests for CSVReorderer."""
import pytest
from csvwrangler.reorderer import CSVReorderer


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    def rows(self):
        return iter(self._rows)


def _source():
    return _FakeSource(
        ["name", "age", "city", "score"],
        [
            {"name": "Alice", "age": "30", "city": "NY", "score": "9"},
            {"name": "Bob", "age": "25", "city": "LA", "score": "7"},
        ],
    )


def test_reorder_headers():
    r = CSVReorderer(_source(), ["score", "name", "city", "age"])
    assert r.headers == ["score", "name", "city", "age"]


def test_reorder_partial_with_rest():
    r = CSVReorderer(_source(), ["city", "name"])
    assert r.headers == ["city", "name", "age", "score"]


def test_reorder_drop_rest_headers():
    r = CSVReorderer(_source(), ["name", "score"], drop_rest=True)
    assert r.headers == ["name", "score"]


def test_reorder_rows_values():
    r = CSVReorderer(_source(), ["score", "name"], drop_rest=True)
    rows = list(r.rows())
    assert rows[0] == {"score": "9", "name": "Alice"}
    assert rows[1] == {"score": "7", "name": "Bob"}


def test_reorder_rows_full_order():
    r = CSVReorderer(_source(), ["age", "city", "name", "score"])
    rows = list(r.rows())
    assert list(rows[0].keys()) == ["age", "city", "name", "score"]


def test_reorder_row_count():
    r = CSVReorderer(_source(), ["name", "age"])
    assert r.row_count() == 2


def test_unknown_column_raises():
    with pytest.raises(ValueError, match="Unknown columns"):
        CSVReorderer(_source(), ["name", "missing"])


def test_drop_rest_excludes_unlisted():
    r = CSVReorderer(_source(), ["name"], drop_rest=True)
    rows = list(r.rows())
    assert all(list(row.keys()) == ["name"] for row in rows)
