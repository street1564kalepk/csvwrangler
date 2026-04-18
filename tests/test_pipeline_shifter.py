"""Pipeline integration tests for the shift_columns method."""
import io
import csv
from csvwrangler.pipeline import Pipeline


_CSV = """name,score,date
Alice,10,2024-01-15
Bob,20,2024-03-01
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(text: str):
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def test_shift_method_exists():
    p = pipeline()
    assert hasattr(p, "shift_columns")


def test_shift_headers_unchanged():
    p = pipeline()
    out = p.shift_columns({"score": 5}).to_string()
    rows = _parse(out)
    assert list(rows[0].keys()) == ["name", "score", "date"]


def test_shift_score_values():
    p = pipeline()
    out = p.shift_columns({"score": 5}).to_string()
    rows = _parse(out)
    assert rows[0]["score"] == "15"
    assert rows[1]["score"] == "25"


def test_shift_date_values():
    p = pipeline()
    out = p.shift_columns({"date": {"days": 1}}).to_string()
    rows = _parse(out)
    assert rows[0]["date"] == "2024-01-16"


def test_shift_chained_with_filter():
    p = pipeline()
    out = (p.where("score", "gt", "15")
             .shift_columns({"score": -5})
             .to_string())
    rows = _parse(out)
    assert len(rows) == 1
    assert rows[0]["name"] == "Bob"
    assert rows[0]["score"] == "15"
