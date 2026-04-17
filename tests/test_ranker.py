"""Tests for CSVRanker."""
import pytest
from csvwrangler.ranker import CSVRanker


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._data)


def _source():
    return _FakeSource(
        ["name", "score"],
        [
            {"name": "Alice", "score": "90"},
            {"name": "Bob", "score": "70"},
            {"name": "Carol", "score": "90"},
            {"name": "Dave", "score": "50"},
        ],
    )


def test_headers_include_rank_col():
    r = CSVRanker(_source(), "score")
    assert r.headers == ["name", "score", "rank"]


def test_custom_rank_col_name():
    r = CSVRanker(_source(), "score", rank_col="position")
    assert "position" in r.headers


def test_ascending_rank():
    r = CSVRanker(_source(), "score", ascending=True)
    result = {row["name"]: row["rank"] for row in r.rows()}
    # ascending: lowest score gets rank 1
    assert result["Dave"] == 1
    assert result["Bob"] == 2


def test_descending_rank():
    r = CSVRanker(_source(), "score", ascending=False)
    result = {row["name"]: row["rank"] for row in r.rows()}
    # descending: highest score gets rank 1
    assert result["Alice"] == 1
    assert result["Carol"] == 1
    assert result["Dave"] > result["Bob"]


def test_dense_ranking_no_gaps():
    r = CSVRanker(_source(), "score", ascending=False, dense=True)
    result = {row["name"]: row["rank"] for row in r.rows()}
    ranks = sorted(set(result.values()))
    # dense: 1, 2, 3 — no gap after tied rank
    assert ranks == list(range(1, len(ranks) + 1))


def test_standard_ranking_has_gap():
    r = CSVRanker(_source(), "score", ascending=False, dense=False)
    result = {row["name"]: row["rank"] for row in r.rows()}
    # Alice and Carol both rank 1; Bob should be rank 3 (gap)
    assert result["Bob"] == 3


def test_row_count_unchanged():
    r = CSVRanker(_source(), "score")
    assert r.row_count == 4


def test_missing_column_raises():
    with pytest.raises(ValueError, match="not found"):
        CSVRanker(_source(), "missing")


def test_duplicate_rank_col_raises():
    src = _FakeSource(["name", "rank"], [{"name": "A", "rank": "1"}])
    with pytest.raises(ValueError, match="already exists"):
        CSVRanker(src, "rank")
