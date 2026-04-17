"""Pipeline integration tests for the round_columns method."""
import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.reader import CSVReader


_CSV = """name,score,ratio
Alice,3.14159,0.666667
Bob,2.71828,1.333333
"""


def pipeline():
    src = CSVReader(io.StringIO(_CSV))
    return Pipeline(src)


def _parse(text: str) -> list[dict]:
    import csv
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def test_round_columns_method_exists():
    p = pipeline()
    assert hasattr(p, "round_columns")


def test_round_columns_headers_unchanged():
    out = pipeline().round_columns({"score": 2}).to_string()
    rows = _parse(out)
    assert list(rows[0].keys()) == ["name", "score", "ratio"]


def test_round_columns_values():
    out = pipeline().round_columns({"score": 2}).to_string()
    rows = _parse(out)
    assert rows[0]["score"] == "3.14"
    assert rows[1]["score"] == "2.72"


def test_round_columns_chained_with_filter():
    out = (
        pipeline()
        .where("name", "eq", "Alice")
        .round_columns({"ratio": 2})
        .to_string()
    )
    rows = _parse(out)
    assert len(rows) == 1
    assert rows[0]["ratio"] == "0.67"
