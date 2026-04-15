"""Tests for CSVPivotter."""
import pytest
from csvwrangler.pivotter import CSVPivotter


class _FakeSource:
    def __init__(self, hdrs, data):
        self._headers = hdrs
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(row)


def _source():
    headers = ["name", "metric", "value"]
    data = [
        {"name": "alice", "metric": "height", "value": "170"},
        {"name": "alice", "metric": "weight", "value": "65"},
        {"name": "bob",   "metric": "height", "value": "180"},
        {"name": "bob",   "metric": "weight", "value": "80"},
    ]
    return _FakeSource(headers, data)


def make_pivotter(**kwargs):
    defaults = dict(index_col="name", pivot_col="metric", value_col="value")
    defaults.update(kwargs)
    return CSVPivotter(_source(), **defaults)


def test_pivot_headers():
    p = make_pivotter()
    assert p.headers == ["name", "height", "weight"]


def test_pivot_row_count():
    p = make_pivotter()
    assert p.row_count == 2


def test_pivot_rows_alice():
    p = make_pivotter()
    rows = list(p.rows())
    alice = next(r for r in rows if r["name"] == "alice")
    assert alice["height"] == "170"
    assert alice["weight"] == "65"


def test_pivot_rows_bob():
    p = make_pivotter()
    rows = list(p.rows())
    bob = next(r for r in rows if r["name"] == "bob")
    assert bob["height"] == "180"
    assert bob["weight"] == "80"


def test_pivot_missing_value_is_empty_string():
    headers = ["name", "metric", "value"]
    data = [
        {"name": "alice", "metric": "height", "value": "170"},
        {"name": "bob",   "metric": "weight", "value": "80"},
    ]
    src = _FakeSource(headers, data)
    p = CSVPivotter(src, index_col="name", pivot_col="metric", value_col="value")
    rows = {r["name"]: r for r in p.rows()}
    assert rows["alice"]["weight"] == ""
    assert rows["bob"]["height"] == ""


def test_pivot_preserves_insertion_order_of_pivot_values():
    headers = ["id", "attr", "val"]
    data = [
        {"id": "x", "attr": "z", "val": "1"},
        {"id": "x", "attr": "a", "val": "2"},
        {"id": "y", "attr": "z", "val": "3"},
        {"id": "y", "attr": "a", "val": "4"},
    ]
    src = _FakeSource(headers, data)
    p = CSVPivotter(src, index_col="id", pivot_col="attr", value_col="val")
    assert p.headers == ["id", "z", "a"]


def test_pivot_last_value_wins_on_duplicate_key():
    headers = ["name", "metric", "value"]
    data = [
        {"name": "alice", "metric": "height", "value": "160"},
        {"name": "alice", "metric": "height", "value": "170"},
    ]
    src = _FakeSource(headers, data)
    p = CSVPivotter(src, index_col="name", pivot_col="metric", value_col="value")
    rows = list(p.rows())
    assert rows[0]["height"] == "170"
