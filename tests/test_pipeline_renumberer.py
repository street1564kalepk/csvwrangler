"""Integration tests: Pipeline.renumber_column wires CSVRenumberer."""

import io
import pytest
from csvwrangler.pipeline import Pipeline


CSV_TEXT = """id,name,score
5,Alice,90
10,Bob,85
99,Carol,78
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(csv_string: str) -> list[dict]:
    """Turn a CSV string back into a list of dicts."""
    import csv
    reader = csv.DictReader(io.StringIO(csv_string))
    return list(reader)


def test_renumber_method_exists():
    p = pipeline()
    assert hasattr(p, "renumber_column")


def test_renumber_headers_unchanged():
    p = pipeline()
    out = p.renumber_column("id").to_string()
    rows = _parse(out)
    assert list(rows[0].keys()) == ["id", "name", "score"]


def test_renumber_default_values():
    p = pipeline()
    out = p.renumber_column("id").to_string()
    rows = _parse(out)
    assert [r["id"] for r in rows] == ["1", "2", "3"]


def test_renumber_custom_start():
    p = pipeline()
    out = p.renumber_column("id", start=100).to_string()
    rows = _parse(out)
    assert rows[0]["id"] == "100"
    assert rows[1]["id"] == "101"


def test_renumber_custom_step():
    p = pipeline()
    out = p.renumber_column("id", start=0, step=5).to_string()
    rows = _parse(out)
    assert [r["id"] for r in rows] == ["0", "5", "10"]


def test_renumber_other_columns_intact():
    p = pipeline()
    out = p.renumber_column("id").to_string()
    rows = _parse(out)
    assert [r["name"] for r in rows] == ["Alice", "Bob", "Carol"]
