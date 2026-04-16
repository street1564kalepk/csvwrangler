"""Pipeline integration tests for the validate step."""
import io
from csvwrangler.pipeline import Pipeline


_CSV = """name,age,email
Alice,30,alice@example.com
Bob,abc,bob@example.com
,25,carol@example.com
Dave,40,not-an-email
"""


def pipeline():
    return Pipeline.from_stream(io.StringIO(_CSV))


def _parse(text: str):
    reader = io.StringIO(text)
    import csv
    return list(csv.DictReader(reader))


_RULES = {
    "name": lambda v: len(v) > 0,
    "age": lambda v: v.isdigit(),
    "email": lambda v: "@" in v,
}


def test_validate_drop_headers():
    out = io.StringIO()
    pipeline().validate(_RULES, mode="drop").to_stream(out)
    rows = _parse(out.getvalue())
    assert list(rows[0].keys()) == ["name", "age", "email"]


def test_validate_drop_row_count():
    out = io.StringIO()
    pipeline().validate(_RULES, mode="drop").to_stream(out)
    rows = _parse(out.getvalue())
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_validate_tag_headers():
    out = io.StringIO()
    pipeline().validate(_RULES, mode="tag").to_stream(out)
    rows = _parse(out.getvalue())
    assert "_errors" in rows[0]


def test_validate_tag_all_rows_present():
    out = io.StringIO()
    pipeline().validate(_RULES, mode="tag").to_stream(out)
    rows = _parse(out.getvalue())
    assert len(rows) == 4


def test_validate_tag_valid_row_has_empty_errors():
    out = io.StringIO()
    pipeline().validate(_RULES, mode="tag").to_stream(out)
    rows = _parse(out.getvalue())
    alice = next(r for r in rows if r["name"] == "Alice")
    assert alice["_errors"] == ""


def test_validate_tag_invalid_row_has_errors():
    out = io.StringIO()
    pipeline().validate(_RULES, mode="tag").to_stream(out)
    rows = _parse(out.getvalue())
    bob = next(r for r in rows if r["name"] == "Bob")
    assert "age" in bob["_errors"]
