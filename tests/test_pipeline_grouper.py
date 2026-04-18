"""Integration tests for Pipeline.group_by()."""
import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_grouper_patch import _patch

_patch()


CSV_TEXT = """dept,name,salary
eng,Alice,90000
eng,Bob,80000
hr,Carol,60000
hr,Dave,65000
eng,Eve,95000
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(p):
    buf = io.StringIO()
    p.to_file(buf)
    buf.seek(0)
    lines = [l.strip() for l in buf.readlines() if l.strip()]
    header = lines[0].split(",")
    rows = [dict(zip(header, l.split(","))) for l in lines[1:]]
    return header, rows


def test_group_by_method_exists():
    assert hasattr(pipeline(), "group_by")


def test_group_by_headers():
    p = pipeline().group_by("dept", "salary", "count")
    header, _ = _parse(p)
    assert header == ["dept", "count_salary"]


def test_group_by_count_eng():
    p = pipeline().group_by("dept", "salary", "count")
    _, rows = _parse(p)
    result = {r["dept"]: int(r["count_salary"]) for r in rows}
    assert result["eng"] == 3


def test_group_by_sum_custom_out_col():
    p = pipeline().group_by("dept", "salary", "sum", out_col="total_pay")
    header, rows = _parse(p)
    assert "total_pay" in header
    result = {r["dept"]: float(r["total_pay"]) for r in rows}
    assert result["hr"] == pytest.approx(125000.0)


def test_group_by_row_count():
    p = pipeline().group_by("dept", "salary", "count")
    _, rows = _parse(p)
    assert len(rows) == 2
