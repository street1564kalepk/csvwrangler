"""Integration tests: Pipeline.flatten() end-to-end."""
from __future__ import annotations

import csv
import io
import textwrap

import pytest

from csvwrangler.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _csv_text(text: str) -> io.StringIO:
    return io.StringIO(textwrap.dedent(text).strip())


def _parse(buf: io.StringIO) -> list[dict]:
    buf.seek(0)
    return list(csv.DictReader(buf))


@pytest.fixture()
def pipeline():
    text = (
        "id,name,tags\n"
        "1,Alice,python|csv|data\n"
        "2,Bob,java\n"
        "3,Carol,\n"
    )
    return Pipeline.from_file(_csv_text(text))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_flatten_headers(pipeline):
    buf = io.StringIO()
    pipeline.flatten("tags").to_file(buf)
    rows = _parse(buf)
    assert list(rows[0].keys()) == ["id", "name", "tags"]


def test_flatten_row_count(pipeline):
    buf = io.StringIO()
    pipeline.flatten("tags").to_file(buf)
    rows = _parse(buf)
    # Alice→3, Bob→1, Carol→1 (empty)
    assert len(rows) == 5


def test_flatten_values(pipeline):
    buf = io.StringIO()
    pipeline.flatten("tags").to_file(buf)
    rows = _parse(buf)
    tags = [r["tags"] for r in rows]
    assert tags == ["python", "csv", "data", "java", ""]


def test_flatten_then_filter(pipeline):
    buf = io.StringIO()
    pipeline.flatten("tags").where("tags", "eq", "csv").to_file(buf)
    rows = _parse(buf)
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_flatten_custom_delimiter():
    text = "id,vals\n1,a:b:c\n"
    p = Pipeline.from_file(io.StringIO(text))
    buf = io.StringIO()
    p.flatten("vals", delimiter=":").to_file(buf)
    rows = _parse(buf)
    assert [r["vals"] for r in rows] == ["a", "b", "c"]
