"""Tests for CSVHashSplitter."""
from __future__ import annotations

import pytest
from csvwrangler.splitter_by_hash import CSVHashSplitter


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
        ["id", "name"],
        [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
            {"id": "3", "name": "Carol"},
            {"id": "4", "name": "Dave"},
            {"id": "5", "name": "Eve"},
            {"id": "6", "name": "Frank"},
        ],
    )


def test_headers_unchanged():
    splitter = CSVHashSplitter(_source(), "id", n_buckets=2)
    assert splitter.headers == ["id", "name"]


def test_bucket_count():
    splitter = CSVHashSplitter(_source(), "id", n_buckets=3)
    assert splitter.bucket_count == 3


def test_group_keys_range():
    splitter = CSVHashSplitter(_source(), "id", n_buckets=3)
    assert splitter.group_keys == [0, 1, 2]


def test_all_rows_distributed():
    splitter = CSVHashSplitter(_source(), "id", n_buckets=2)
    total = sum(splitter.row_count(b) for b in splitter.group_keys)
    assert total == 6


def test_deterministic_assignment():
    """Same data → same bucket assignment on repeated calls."""
    s1 = CSVHashSplitter(_source(), "id", n_buckets=2)
    s2 = CSVHashSplitter(_source(), "id", n_buckets=2)
    for b in [0, 1]:
        ids1 = [r["id"] for r in s1.bucket_rows(b)]
        ids2 = [r["id"] for r in s2.bucket_rows(b)]
        assert ids1 == ids2


def test_single_bucket_contains_all():
    splitter = CSVHashSplitter(_source(), "id", n_buckets=1)
    assert splitter.row_count(0) == 6


def test_invalid_n_buckets_raises():
    with pytest.raises(ValueError):
        CSVHashSplitter(_source(), "id", n_buckets=0)


def test_missing_column_raises():
    splitter = CSVHashSplitter(_source(), "nonexistent", n_buckets=2)
    with pytest.raises(KeyError):
        splitter.bucket_rows(0)


def test_invalid_bucket_key_raises():
    splitter = CSVHashSplitter(_source(), "id", n_buckets=2)
    with pytest.raises(KeyError):
        splitter.bucket_rows(99)
