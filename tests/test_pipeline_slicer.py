"""Integration tests for Pipeline.skip() and Pipeline.slice()."""
from __future__ import annotations

import csv
import io
import textwrap

import pytest

from csvwrangler.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CSV = textwrap.dedent("""\
    id,name,score
    1,Alice,90
    2,Bob,75
    3,Carol,88
    4,Dave,60
    5,Eve,95
""")


def pipeline() -> Pipeline:
    reader = csv.DictReader(io.StringIO(_CSV))

    class _Src:
        @property
        def headers(self):
            return ["id", "name", "score"]

        def rows(self):
            yield from csv.DictReader(io.StringIO(_CSV))

    return Pipeline(_Src())


def _parse(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


# ---------------------------------------------------------------------------
# Tests — skip
# ---------------------------------------------------------------------------

def test_skip_removes_first_n_rows():
    result = _parse(pipeline().skip(2).to_string())
    assert [r["name"] for r in result] == ["Carol", "Dave", "Eve"]


def test_skip_zero_keeps_all_rows():
    result = _parse(pipeline().skip(0).to_string())
    assert len(result) == 5


def test_skip_headers_preserved():
    assert pipeline().skip(3).headers == ["id", "name", "score"]


# ---------------------------------------------------------------------------
# Tests — slice
# ---------------------------------------------------------------------------

def test_slice_offset_and_limit():
    result = _parse(pipeline().slice(offset=1, limit=2).to_string())
    assert [r["name"] for r in result] == ["Bob", "Carol"]


def test_slice_limit_only():
    result = _parse(pipeline().slice(limit=3).to_string())
    assert [r["name"] for r in result] == ["Alice", "Bob", "Carol"]


def test_slice_offset_only():
    result = _parse(pipeline().slice(offset=3).to_string())
    assert [r["name"] for r in result] == ["Dave", "Eve"]


def test_slice_chained_after_filter():
    result = _parse(
        pipeline()
        .where("score", "gt", "70")
        .slice(offset=1, limit=2)
        .to_string()
    )
    # rows with score > 70: Alice(90), Bob(75), Carol(88), Eve(95)
    # after skip 1 + limit 2 → Bob, Carol
    assert [r["name"] for r in result] == ["Bob", "Carol"]


def test_slice_headers_unchanged():
    assert pipeline().slice(offset=2, limit=1).headers == ["id", "name", "score"]
