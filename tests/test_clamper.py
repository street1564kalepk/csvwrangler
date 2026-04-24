"""Tests for CSVClamper."""

from __future__ import annotations

import pytest

from csvwrangler.clamper import CSVClamper


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._rows)


def _source():
    return _FakeSource(
        ["name", "score", "age"],
        [
            {"name": "Alice", "score": "120", "age": "30"},
            {"name": "Bob",   "score": "-5",  "age": "17"},
            {"name": "Carol", "score": "75",  "age": "45"},
            {"name": "Dave",  "score": "abc", "age": "200"},
        ],
    )


def test_headers_unchanged():
    c = CSVClamper(_source(), ["score"], lower=0, upper=100)
    assert c.headers == ["name", "score", "age"]


def test_clamp_upper_only():
    c = CSVClamper(_source(), ["score"], upper=100)
    results = {r["name"]: r["score"] for r in c.rows()}
    assert results["Alice"] == 100
    assert results["Bob"] == -5
    assert results["Carol"] == 75


def test_clamp_lower_only():
    c = CSVClamper(_source(), ["score"], lower=0)
    results = {r["name"]: r["score"] for r in c.rows()}
    assert results["Alice"] == 120
    assert results["Bob"] == 0
    assert results["Carol"] == 75


def test_clamp_both_bounds():
    c = CSVClamper(_source(), ["score"], lower=0, upper=100)
    results = {r["name"]: r["score"] for r in c.rows()}
    assert results["Alice"] == 100
    assert results["Bob"] == 0
    assert results["Carol"] == 75


def test_non_numeric_left_untouched():
    c = CSVClamper(_source(), ["score"], lower=0, upper=100)
    results = {r["name"]: r["score"] for r in c.rows()}
    assert results["Dave"] == "abc"


def test_multiple_columns():
    c = CSVClamper(_source(), ["score", "age"], lower=0, upper=100)
    results = {r["name"]: r for r in c.rows()}
    assert results["Alice"]["score"] == 100
    assert results["Dave"]["age"] == 100
    assert results["Bob"]["age"] == 17


def test_raises_when_no_bounds():
    with pytest.raises(ValueError, match="At least one"):
        CSVClamper(_source(), ["score"])


def test_raises_when_lower_gt_upper():
    with pytest.raises(ValueError, match="lower"):
        CSVClamper(_source(), ["score"], lower=100, upper=0)


def test_row_count():
    c = CSVClamper(_source(), ["score"], lower=0, upper=100)
    assert c.row_count == 4
