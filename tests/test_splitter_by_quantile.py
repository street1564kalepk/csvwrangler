"""Tests for CSVQuantileSplitter."""
from __future__ import annotations

import pytest

from csvwrangler.splitter_by_quantile import CSVQuantileSplitter


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from self._rows


def _source():
    return _FakeSource(
        ["name", "score"],
        [
            {"name": "Alice", "score": "10"},
            {"name": "Bob", "score": "20"},
            {"name": "Carol", "score": "30"},
            {"name": "Dave", "score": "40"},
            {"name": "Eve", "score": "50"},
            {"name": "Frank", "score": "60"},
            {"name": "Grace", "score": "70"},
            {"name": "Heidi", "score": "80"},
        ],
    )


def test_headers_unchanged():
    sp = CSVQuantileSplitter(_source(), "score", n_quantiles=4)
    assert sp.headers == ["name", "score"]


def test_group_keys_default_quartiles():
    sp = CSVQuantileSplitter(_source(), "score", n_quantiles=4)
    keys = sp.group_keys
    assert set(keys) == {"Q1", "Q2", "Q3", "Q4"}


def test_total_rows_preserved():
    sp = CSVQuantileSplitter(_source(), "score", n_quantiles=4)
    total = sum(sp.row_count(k) for k in sp.group_keys)
    assert total == 8


def test_rows_in_q1_are_lowest():
    sp = CSVQuantileSplitter(_source(), "score", n_quantiles=4)
    q1_scores = [float(r["score"]) for r in sp.rows("Q1")]
    q4_scores = [float(r["score"]) for r in sp.rows("Q4")]
    assert max(q1_scores) < min(q4_scores)


def test_two_quantiles_splits_evenly():
    sp = CSVQuantileSplitter(_source(), "score", n_quantiles=2)
    assert sp.row_count("Q1") == 4
    assert sp.row_count("Q2") == 4


def test_non_numeric_goes_to_q_other():
    src = _FakeSource(
        ["name", "score"],
        [
            {"name": "Alice", "score": "10"},
            {"name": "Bob", "score": "N/A"},
            {"name": "Carol", "score": "30"},
            {"name": "Dave", "score": "40"},
        ],
    )
    sp = CSVQuantileSplitter(src, "score", n_quantiles=2)
    assert sp.row_count("Q_other") == 1
    assert list(sp.rows("Q_other"))[0]["name"] == "Bob"


def test_invalid_n_quantiles_raises():
    with pytest.raises(ValueError, match="n_quantiles must be >= 2"):
        CSVQuantileSplitter(_source(), "score", n_quantiles=1)


def test_missing_column_raises():
    with pytest.raises(ValueError, match="Column 'missing' not found"):
        CSVQuantileSplitter(_source(), "missing", n_quantiles=4)


def test_row_count_unknown_key_returns_zero():
    sp = CSVQuantileSplitter(_source(), "score", n_quantiles=4)
    assert sp.row_count("Q99") == 0
