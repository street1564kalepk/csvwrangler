"""Integration tests for Pipeline.highlight()."""
import io
import csv as _csv

import pytest

from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_highlighter_patch import _patch

_patch(Pipeline)


CSV_TEXT = """name,dept,score
Alice,Eng,95
Bob,HR,60
Carol,Eng,82
Dave,HR,45
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(result: str):
    return list(_csv.DictReader(io.StringIO(result)))


def test_highlight_method_exists():
    assert hasattr(Pipeline, "highlight")


def test_highlight_headers_appended():
    result = pipeline().highlight(lambda r: r["dept"] == "Eng").to_string()
    rows = _parse(result)
    assert "highlighted" in rows[0]


def test_highlight_custom_flag_column():
    result = (
        pipeline()
        .highlight(lambda r: int(r["score"]) >= 80, flag_column="top_performer")
        .to_string()
    )
    rows = _parse(result)
    assert "top_performer" in rows[0]


def test_highlight_correct_values():
    result = pipeline().highlight(lambda r: r["dept"] == "Eng").to_string()
    rows = _parse(result)
    by_name = {r["name"]: r["highlighted"] for r in rows}
    assert by_name["Alice"] == "true"
    assert by_name["Bob"] == "false"
    assert by_name["Carol"] == "true"
    assert by_name["Dave"] == "false"


def test_highlight_row_count_unchanged():
    result = pipeline().highlight(lambda r: False).to_string()
    rows = _parse(result)
    assert len(rows) == 4


def test_highlight_chainable_with_filter():
    result = (
        pipeline()
        .highlight(lambda r: int(r["score"]) >= 80, true_value="yes", false_value="no")
        .where(lambda r: r["dept"] == "Eng")
        .to_string()
    )
    rows = _parse(result)
    assert all(r["dept"] == "Eng" for r in rows)
    assert rows[0]["yes" if False else "highlighted"] in ("yes", "no")
