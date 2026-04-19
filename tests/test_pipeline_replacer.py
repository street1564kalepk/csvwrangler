"""Pipeline integration tests for replace_values()."""
import io
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_replacer_patch import _patch

_patch(Pipeline)


CSV_TEXT = """name,dept,status
Alice,Eng,active
Bob,HR,inactive
Carol,Eng,active
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(p):
    buf = io.StringIO()
    p.to_file(buf)
    buf.seek(0)
    lines = buf.read().splitlines()
    headers = lines[0].split(",")
    rows = [dict(zip(headers, l.split(","))) for l in lines[1:] if l]
    return headers, rows


def test_replace_method_exists():
    assert hasattr(Pipeline, "replace_values")


def test_replace_headers_unchanged():
    headers, _ = _parse(pipeline().replace_values({"dept": {"Eng": "Engineering"}}))
    assert headers == ["name", "dept", "status"]


def test_replace_exact_values():
    _, rows = _parse(pipeline().replace_values({"dept": {"Eng": "Engineering", "HR": "Human Resources"}}))
    depts = [r["dept"] for r in rows]
    assert depts == ["Engineering", "Human Resources", "Engineering"]


def test_replace_substring():
    _, rows = _parse(pipeline().replace_values({"status": {"act": "ACT"}}, substring=True))
    statuses = [r["status"] for r in rows]
    assert statuses == ["ACTive", "inACTive", "ACTive"]


def test_replace_chained_with_filter():
    _, rows = _parse(
        pipeline()
        .where("dept", "eq", "Eng")
        .replace_values({"status": {"active": "yes"}})
    )
    assert all(r["status"] == "yes" for r in rows)
    assert len(rows) == 2
