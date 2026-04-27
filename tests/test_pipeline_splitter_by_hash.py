"""Integration tests for Pipeline.split_by_hash()."""
from __future__ import annotations

import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_splitter_by_hash_patch import _patch

_patch(Pipeline)


CSV_TEXT = """id,dept
1,eng
2,eng
3,hr
4,finance
5,eng
6,hr
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(p):
    out = io.StringIO()
    p.to_stream(out)
    lines = out.getvalue().strip().splitlines()
    headers = lines[0].split(",")
    rows = [dict(zip(headers, l.split(","))) for l in lines[1:]]
    return headers, rows


def test_split_by_hash_method_exists():
    assert hasattr(Pipeline, "split_by_hash")


def test_split_by_hash_returns_dict():
    groups = pipeline().split_by_hash("id", n_buckets=2)
    assert isinstance(groups, dict)
    assert set(groups.keys()) == {0, 1}


def test_split_by_hash_total_rows_preserved():
    groups = pipeline().split_by_hash("id", n_buckets=3)
    total = sum(len(_parse(p)[1]) for p in groups.values())
    assert total == 6


def test_split_by_hash_headers_in_each_group():
    groups = pipeline().split_by_hash("id", n_buckets=2)
    for p in groups.values():
        headers, _ = _parse(p)
        assert headers == ["id", "dept"]


def test_split_by_hash_deterministic():
    g1 = pipeline().split_by_hash("id", n_buckets=2)
    g2 = pipeline().split_by_hash("id", n_buckets=2)
    for key in [0, 1]:
        _, rows1 = _parse(g1[key])
        _, rows2 = _parse(g2[key])
        assert [r["id"] for r in rows1] == [r["id"] for r in rows2]


def test_split_by_hash_single_bucket():
    groups = pipeline().split_by_hash("id", n_buckets=1)
    _, rows = _parse(groups[0])
    assert len(rows) == 6
