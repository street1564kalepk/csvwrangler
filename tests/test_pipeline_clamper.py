"""Integration tests for Pipeline.clamp_columns()."""

from __future__ import annotations

import io
import csv

import pytest

from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_clamper_patch import _patch

_patch(Pipeline)


CSV_TEXT = """name,score,age
Alice,120,30
Bob,-5,17
Carol,75,45
Dave,abc,200
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(csv_string: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(csv_string)))


def test_clamp_columns_method_exists():
    assert hasattr(Pipeline, "clamp_columns")


def test_clamp_columns_headers_unchanged():
    result = _parse(pipeline().clamp_columns(["score"], lower=0, upper=100).to_string())
    assert list(result[0].keys()) == ["name", "score", "age"]


def test_clamp_upper_via_pipeline():
    result = _parse(pipeline().clamp_columns(["score"], upper=100).to_string())
    by_name = {r["name"]: r["score"] for r in result}
    assert by_name["Alice"] == "100"
    assert by_name["Carol"] == "75"


def test_clamp_lower_via_pipeline():
    result = _parse(pipeline().clamp_columns(["score"], lower=0).to_string())
    by_name = {r["name"]: r["score"] for r in result}
    assert by_name["Bob"] == "0"
    assert by_name["Alice"] == "120"


def test_clamp_both_bounds_via_pipeline():
    result = _parse(
        pipeline().clamp_columns(["score", "age"], lower=0, upper=100).to_string()
    )
    by_name = {r["name"]: r for r in result}
    assert by_name["Alice"]["score"] == "100"
    assert by_name["Dave"]["age"] == "100"
    assert by_name["Bob"]["age"] == "17"


def test_clamp_non_numeric_preserved():
    result = _parse(pipeline().clamp_columns(["score"], lower=0, upper=100).to_string())
    by_name = {r["name"]: r["score"] for r in result}
    assert by_name["Dave"] == "abc"


def test_clamp_chainable():
    p = pipeline().clamp_columns(["score"], lower=0, upper=100)
    assert p is not None
    # further chaining should work
    result = _parse(p.to_string())
    assert len(result) == 4
