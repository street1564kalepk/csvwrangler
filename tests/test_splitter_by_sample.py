"""Tests for CSVSampleSplitter."""
from __future__ import annotations

import pytest

from csvwrangler.splitter_by_sample import CSVSampleSplitter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeSource:
    def __init__(self, headers: list[str], rows: list[dict]) -> None:
        self._headers = headers
        self._rows = rows

    @property
    def headers(self) -> list[str]:
        return list(self._headers)

    @property
    def rows(self):
        yield from self._rows


def _source() -> _FakeSource:
    return _FakeSource(
        headers=["id", "name", "score"],
        rows=[
            {"id": "1", "name": "Alice", "score": "90"},
            {"id": "2", "name": "Bob", "score": "80"},
            {"id": "3", "name": "Carol", "score": "70"},
            {"id": "4", "name": "Dave", "score": "60"},
            {"id": "5", "name": "Eve", "score": "50"},
            {"id": "6", "name": "Frank", "score": "40"},
        ],
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    splitter = CSVSampleSplitter(_source(), n=2, seed=0)
    assert splitter.headers == ["id", "name", "score"]


def test_sample_count_matches_n():
    splitter = CSVSampleSplitter(_source(), n=3, seed=42)
    assert splitter.sample_count == 3


def test_remainder_count_is_complement():
    src = _source()
    total = len(list(src.rows))
    splitter = CSVSampleSplitter(_source(), n=3, seed=42)
    assert splitter.remainder_count == total - 3


def test_sample_plus_remainder_covers_all_rows():
    splitter = CSVSampleSplitter(_source(), n=4, seed=7)
    sampled = list(splitter.sampled_rows())
    remainder = list(splitter.remainder_rows())
    all_ids = {r["id"] for r in sampled} | {r["id"] for r in remainder}
    assert all_ids == {"1", "2", "3", "4", "5", "6"}


def test_no_overlap_between_sample_and_remainder():
    splitter = CSVSampleSplitter(_source(), n=3, seed=99)
    sampled_ids = {r["id"] for r in splitter.sampled_rows()}
    remainder_ids = {r["id"] for r in splitter.remainder_rows()}
    assert sampled_ids.isdisjoint(remainder_ids)


def test_reproducible_with_seed():
    s1 = CSVSampleSplitter(_source(), n=3, seed=123)
    s2 = CSVSampleSplitter(_source(), n=3, seed=123)
    ids1 = [r["id"] for r in s1.sampled_rows()]
    ids2 = [r["id"] for r in s2.sampled_rows()]
    assert ids1 == ids2


def test_different_seeds_may_differ():
    s1 = CSVSampleSplitter(_source(), n=3, seed=1)
    s2 = CSVSampleSplitter(_source(), n=3, seed=2)
    ids1 = sorted(r["id"] for r in s1.sampled_rows())
    ids2 = sorted(r["id"] for r in s2.sampled_rows())
    # With different seeds the samples are very likely to differ for n=3/6
    # We simply verify both are valid samples of size 3.
    assert len(ids1) == 3
    assert len(ids2) == 3


def test_n_greater_than_total_clamps_to_all():
    splitter = CSVSampleSplitter(_source(), n=100, seed=0)
    assert splitter.sample_count == 6
    assert splitter.remainder_count == 0


def test_n_zero_produces_empty_sample():
    splitter = CSVSampleSplitter(_source(), n=0, seed=0)
    assert splitter.sample_count == 0
    assert splitter.remainder_count == 6


def test_negative_n_raises():
    with pytest.raises(ValueError, match="n must be >= 0"):
        CSVSampleSplitter(_source(), n=-1)
