"""Pipeline integration tests for reorder_columns."""
import io
import csv
from csvwrangler.pipeline import Pipeline


CSV_TEXT = "name,age,city\nAlice,30,NY\nBob,25,LA\n"


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def test_reorder_method_exists():
    p = pipeline()
    assert hasattr(p, "reorder_columns")


def test_reorder_headers_via_pipeline():
    out = pipeline().reorder_columns(["city", "name", "age"]).to_string()
    rows = _parse(out)
    assert list(rows[0].keys()) == ["city", "name", "age"]


def test_reorder_drop_rest_via_pipeline():
    out = pipeline().reorder_columns(["name"], drop_rest=True).to_string()
    rows = _parse(out)
    assert list(rows[0].keys()) == ["name"]
    assert rows[0]["name"] == "Alice"


def test_reorder_values_preserved():
    out = pipeline().reorder_columns(["age", "name", "city"]).to_string()
    rows = _parse(out)
    assert rows[1]["name"] == "Bob"
    assert rows[1]["age"] == "25"
