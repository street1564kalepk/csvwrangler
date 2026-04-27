"""Pipeline integration tests for split_by_quantile."""
from __future__ import annotations

import io
import pytest

from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_splitter_by_quantile_patch import _patch

_patch(Pipeline)


CSV_TEXT = """name,score
Alice,10
Bob,20
Carol,30
Dave,40
Eve,50
Frank,60
Grace,70
Heidi,80
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(p):
    return [r for r in p._source.rows()]


def test_split_by_quantile_method_exists():
    assert hasattr(Pipeline, "split_by_quantile")


def test_split_by_quantile_returns_dict():
    groups = pipeline().split_by_quantile("score", n_quantiles=4)
    assert isinstance(groups, dict)


def test_split_by_quantile_group_keys():
    groups = pipeline().split_by_quantile("score", n_quantiles=4)
    assert set(groups.keys()) == {"Q1", "Q2", "Q3", "Q4"}


def test_split_by_quantile_total_rows():
    groups = pipeline().split_by_quantile("score", n_quantiles=4)
    total = sum(len(_parse(p)) for p in groups.values())
    assert total == 8


def test_split_by_quantile_q1_lowest_scores():
    groups = pipeline().split_by_quantile("score", n_quantiles=4)
    q1_scores = [float(r["score"]) for r in _parse(groups["Q1"])]
    q4_scores = [float(r["score"]) for r in _parse(groups["Q4"])]
    assert max(q1_scores) < min(q4_scores)


def test_split_by_quantile_headers_preserved():
    groups = pipeline().split_by_quantile("score", n_quantiles=2)
    for p in groups.values():
        assert p._source.headers == ["name", "score"]


def test_split_by_quantile_two_equal_halves():
    groups = pipeline().split_by_quantile("score", n_quantiles=2)
    assert len(_parse(groups["Q1"])) == 4
    assert len(_parse(groups["Q2"])) == 4
