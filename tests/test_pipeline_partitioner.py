"""Pipeline-level tests for the .partition() method."""
from __future__ import annotations

import io
import pytest

from csvwrangler.pipeline import Pipeline
import csvwrangler.pipeline_partitioner_patch  # noqa: F401  – activates patch


_CSV = """id,name,dept
1,Alice,Eng
2,Bob,HR
3,Carol,Eng
4,Dave,HR
5,Eve,Eng
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(p: Pipeline):
    buf = io.StringIO()
    p.to_file(buf)
    buf.seek(0)
    lines = [ln.strip() for ln in buf if ln.strip()]
    headers = lines[0].split(",")
    rows = [dict(zip(headers, ln.split(","))) for ln in lines[1:]]
    return headers, rows


def test_partition_method_exists():
    assert hasattr(pipeline(), "partition")


def test_partition_returns_dict():
    parts = pipeline().partition(2)
    assert isinstance(parts, dict)
    assert set(parts.keys()) == {0, 1}


def test_partition_total_rows_preserved():
    parts = pipeline().partition(2)
    total = sum(len(_parse(p)[1]) for p in parts.values())
    assert total == 5


def test_partition_headers_same_in_each_part():
    parts = pipeline().partition(3)
    for p in parts.values():
        h, _ = _parse(p)
        assert h == ["id", "name", "dept"]


def test_partition_round_robin_order():
    parts = pipeline().partition(2)
    _, rows0 = _parse(parts[0])
    _, rows1 = _parse(parts[1])
    assert [r["id"] for r in rows0] == ["1", "3", "5"]
    assert [r["id"] for r in rows1] == ["2", "4"]


def test_partition_single_bucket():
    parts = pipeline().partition(1)
    _, rows = _parse(parts[0])
    assert len(rows) == 5
