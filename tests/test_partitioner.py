"""Tests for CSVPartitioner."""
from __future__ import annotations

import pytest

from csvwrangler.partitioner import CSVPartitioner


class _FakeSource:
    def __init__(self, rows):
        self._rows = rows

    @property
    def headers(self):
        return ["id", "name", "dept"]

    def rows(self):
        yield from self._rows


def _source():
    data = [
        {"id": "1", "name": "Alice", "dept": "Eng"},
        {"id": "2", "name": "Bob",   "dept": "HR"},
        {"id": "3", "name": "Carol", "dept": "Eng"},
        {"id": "4", "name": "Dave",  "dept": "HR"},
        {"id": "5", "name": "Eve",   "dept": "Eng"},
    ]
    return _FakeSource(data)


def test_headers_unchanged():
    p = CSVPartitioner(_source(), n_parts=2)
    assert p.headers == ["id", "name", "dept"]


def test_partition_count():
    p = CSVPartitioner(_source(), n_parts=3)
    assert p.partition_count == 3


def test_sizes_sum_equals_total_rows():
    p = CSVPartitioner(_source(), n_parts=2)
    assert sum(p.sizes()) == 5


def test_sizes_roughly_equal_for_two_parts():
    p = CSVPartitioner(_source(), n_parts=2)
    sizes = p.sizes()
    assert sizes[0] == 3
    assert sizes[1] == 2


def test_round_robin_distribution():
    p = CSVPartitioner(_source(), n_parts=2)
    part0 = p.partition(0)
    part1 = p.partition(1)
    assert [r["id"] for r in part0] == ["1", "3", "5"]
    assert [r["id"] for r in part1] == ["2", "4"]


def test_single_partition_contains_all_rows():
    p = CSVPartitioner(_source(), n_parts=1)
    assert len(p.partition(0)) == 5


def test_more_parts_than_rows():
    p = CSVPartitioner(_source(), n_parts=10)
    total = sum(p.sizes())
    assert total == 5
    non_empty = [s for s in p.sizes() if s > 0]
    assert len(non_empty) == 5


def test_rows_iterator():
    p = CSVPartitioner(_source(), n_parts=2)
    result = list(p.rows(0))
    assert len(result) == 3


def test_row_count():
    p = CSVPartitioner(_source(), n_parts=3)
    assert p.row_count() == 5


def test_invalid_n_parts_raises():
    with pytest.raises(ValueError):
        CSVPartitioner(_source(), n_parts=0)


def test_index_out_of_range_raises():
    p = CSVPartitioner(_source(), n_parts=2)
    with pytest.raises(IndexError):
        p.partition(5)
