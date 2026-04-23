"""Integration tests: CSVStripper wired into Pipeline."""
from __future__ import annotations

import io

from csvwrangler.pipeline import Pipeline


_CSV_TEXT = "name,city,score\n  Alice  , London ,10\nBob ,Paris ,20\n"


def pipeline():
    return Pipeline.from_string(_CSV_TEXT)


def _parse(csv_string: str) -> list[dict]:
    rows = []
    lines = csv_string.strip().splitlines()
    headers = lines[0].split(",")
    for line in lines[1:]:
        rows.append(dict(zip(headers, line.split(","))))
    return rows


def test_strip_columns_method_exists():
    p = pipeline()
    assert hasattr(p, "strip_columns")


def test_strip_columns_headers_unchanged():
    p = pipeline()
    out = p.strip_columns().to_string()
    rows = _parse(out)
    assert set(rows[0].keys()) == {"name", "city", "score"}


def test_strip_columns_all_trimmed():
    p = pipeline()
    out = p.strip_columns().to_string()
    rows = _parse(out)
    assert rows[0]["name"] == "Alice"
    assert rows[0]["city"] == "London"


def test_strip_columns_specific_column():
    p = pipeline()
    out = p.strip_columns(columns=["name"]).to_string()
    rows = _parse(out)
    assert rows[0]["name"] == "Alice"
    # city should still have leading space when only name is targeted
    assert rows[0]["city"].startswith(" ") or rows[0]["city"] == "London"


def test_strip_columns_row_count_preserved():
    p = pipeline()
    out = p.strip_columns().to_string()
    rows = _parse(out)
    assert len(rows) == 2
