"""Tests for CSVParitySplitter."""

import pytest
from csvwrangler.splitter_by_parity import CSVParitySplitter


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
    hdrs = ["id", "name"]
    data = [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
        {"id": "3", "name": "Carol"},
        {"id": "4", "name": "Dave"},
        {"id": "5", "name": "Eve"},
    ]
    return _FakeSource(hdrs, data)


def test_headers_unchanged():
    sp = CSVParitySplitter(_source())
    assert sp.headers == ["id", "name"]


def test_even_count():
    sp = CSVParitySplitter(_source())
    # indices 0, 2, 4 → rows 1, 3, 5
    assert sp.even_count() == 3


def test_odd_count():
    sp = CSVParitySplitter(_source())
    # indices 1, 3 → rows 2, 4
    assert sp.odd_count() == 2


def test_even_rows_values():
    sp = CSVParitySplitter(_source())
    names = [r["name"] for r in sp.even_rows()]
    assert names == ["Alice", "Carol", "Eve"]


def test_odd_rows_values():
    sp = CSVParitySplitter(_source())
    names = [r["name"] for r in sp.odd_rows()]
    assert names == ["Bob", "Dave"]


def test_groups_keys():
    sp = CSVParitySplitter(_source())
    g = sp.groups()
    assert set(g.keys()) == {"even", "odd"}


def test_groups_total_rows():
    sp = CSVParitySplitter(_source())
    g = sp.groups()
    assert len(g["even"]) + len(g["odd"]) == 5


def test_empty_source():
    src = _FakeSource(["id", "name"], [])
    sp = CSVParitySplitter(src)
    assert sp.even_count() == 0
    assert sp.odd_count() == 0


def test_single_row_goes_to_even():
    src = _FakeSource(["id"], [{"id": "1"}])
    sp = CSVParitySplitter(src)
    assert sp.even_count() == 1
    assert sp.odd_count() == 0


def test_build_is_idempotent():
    sp = CSVParitySplitter(_source())
    _ = sp.even_count()
    _ = sp.odd_count()
    # calling again should not double-count
    assert sp.even_count() == 3
    assert sp.odd_count() == 2
