"""Tests for CSVSampler."""

import pytest
from csvwrangler.sampler import CSVSampler


class _FakeSource:
    def __init__(self, rows):
        self._rows = rows

    @property
    def headers(self):
        return list(self._rows[0].keys()) if self._rows else []

    def rows(self):
        yield from self._rows


def _source(n=20):
    data = [{"id": str(i), "value": str(i * 10)} for i in range(n)]
    return _FakeSource(data)


# ---------------------------------------------------------------------------
# Construction validation
# ---------------------------------------------------------------------------

def test_raises_if_neither_n_nor_frac():
    with pytest.raises(ValueError, match="Specify either"):
        CSVSampler(_source())


def test_raises_if_both_n_and_frac():
    with pytest.raises(ValueError, match="only one"):
        CSVSampler(_source(), n=5, frac=0.5)


def test_raises_if_n_negative():
    with pytest.raises(ValueError, match="non-negative"):
        CSVSampler(_source(), n=-1)


def test_raises_if_frac_out_of_range():
    with pytest.raises(ValueError, match="range"):
        CSVSampler(_source(), frac=1.5)
    with pytest.raises(ValueError, match="range"):
        CSVSampler(_source(), frac=0.0)


# ---------------------------------------------------------------------------
# Headers
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    src = _source()
    sampler = CSVSampler(src, n=5, seed=0)
    assert sampler.headers == ["id", "value"]


# ---------------------------------------------------------------------------
# Fixed-count sampling (n=)
# ---------------------------------------------------------------------------

def test_sample_n_returns_exact_count():
    sampler = CSVSampler(_source(20), n=7, seed=42)
    result = list(sampler.rows())
    assert len(result) == 7


def test_sample_n_rows_are_dicts_with_correct_keys():
    sampler = CSVSampler(_source(20), n=5, seed=1)
    for row in sampler.rows():
        assert set(row.keys()) == {"id", "value"}


def test_sample_n_larger_than_source_returns_all():
    sampler = CSVSampler(_source(5), n=100, seed=0)
    result = list(sampler.rows())
    assert len(result) == 5


def test_sample_n_zero_returns_empty():
    sampler = CSVSampler(_source(10), n=0, seed=0)
    assert list(sampler.rows()) == []


def test_sample_n_reproducible_with_seed():
    s1 = list(CSVSampler(_source(50), n=10, seed=99).rows())
    s2 = list(CSVSampler(_source(50), n=10, seed=99).rows())
    assert s1 == s2


# ---------------------------------------------------------------------------
# Fraction sampling (frac=)
# ---------------------------------------------------------------------------

def test_sample_frac_one_returns_all_rows():
    src = _source(10)
    sampler = CSVSampler(src, frac=1.0, seed=0)
    result = list(sampler.rows())
    assert len(result) == 10


def test_sample_frac_roughly_correct_size():
    # With 1000 rows and frac=0.5, expect roughly 500 ± 60 (6-sigma tolerance).
    sampler = CSVSampler(_source(1000), frac=0.5, seed=7)
    count = sum(1 for _ in sampler.rows())
    assert 400 <= count <= 600


def test_sample_frac_reproducible_with_seed():
    s1 = list(CSVSampler(_source(100), frac=0.3, seed=5).rows())
    s2 = list(CSVSampler(_source(100), frac=0.3, seed=5).rows())
    assert s1 == s2


# ---------------------------------------------------------------------------
# row_count helper
# ---------------------------------------------------------------------------

def test_row_count_matches_rows_length():
    sampler = CSVSampler(_source(20), n=8, seed=3)
    assert sampler.row_count() == len(list(CSVSampler(_source(20), n=8, seed=3).rows()))
