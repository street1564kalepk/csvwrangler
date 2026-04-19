"""Pipeline integration tests for truncate_columns."""

import io
import pytest
from csvwrangler.pipeline import Pipeline


_CSV = """name,bio,city
Alice,Software engineer at ACME Corp,New York
Bob,Hi,LA
Charlie,Data scientist and ML researcher,San Francisco
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(csv_text: str) -> list[dict]:
    import csv
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


def test_truncate_method_exists():
    p = pipeline()
    assert hasattr(p, "truncate_columns")


def test_truncate_headers_unchanged():
    p = pipeline()
    out = p.truncate_columns({"bio": 10}).to_string()
    rows = _parse(out)
    assert list(rows[0].keys()) == ["name", "bio", "city"]


def test_truncate_applies_limit():
    p = pipeline()
    out = p.truncate_columns({"bio": 10}).to_string()
    rows = _parse(out)
    assert rows[0]["bio"] == "Software e\u2026"


def test_truncate_short_value_unchanged():
    p = pipeline()
    out = p.truncate_columns({"bio": 10}).to_string()
    rows = _parse(out)
    assert rows[1]["bio"] == "Hi"


def test_truncate_row_count_preserved():
    p = pipeline()
    out = p.truncate_columns({"bio": 5}).to_string()
    rows = _parse(out)
    assert len(rows) == 3
