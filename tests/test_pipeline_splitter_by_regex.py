"""Pipeline-level tests for split_by_regex."""
from __future__ import annotations

import io
import pytest

from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_splitter_by_regex_patch import _patch

_patch(Pipeline)


CSV_TEXT = """name,email,dept
Alice,alice@example.com,Engineering
Bob,bob@corp.org,Marketing
Carol,carol@example.com,Engineering
Dave,dave@nowhere.net,HR
Eve,eve@corp.org,Engineering
Frank,frank@example.com,Finance
"""


def pipeline():
    return Pipeline.from_string(CSV_TEXT)


def _parse(p: "Pipeline"):
    buf = io.StringIO()
    p.to_stream(buf)
    buf.seek(0)
    lines = [l.strip() for l in buf.readlines() if l.strip()]
    header = lines[0].split(",")
    rows = [dict(zip(header, l.split(","))) for l in lines[1:]]
    return header, rows


def test_split_by_regex_method_exists():
    assert hasattr(pipeline(), "split_by_regex")


def test_split_by_regex_returns_dict():
    groups = pipeline().split_by_regex(
        "email", {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    assert isinstance(groups, dict)


def test_split_by_regex_group_keys():
    groups = pipeline().split_by_regex(
        "email", {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    assert "example" in groups
    assert "corp" in groups
    assert "__unmatched__" in groups


def test_split_by_regex_example_count():
    groups = pipeline().split_by_regex(
        "email", {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    _, rows = _parse(groups["example"])
    assert len(rows) == 3


def test_split_by_regex_corp_names():
    groups = pipeline().split_by_regex(
        "email", {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    _, rows = _parse(groups["corp"])
    names = sorted(r["name"] for r in rows)
    assert names == ["Bob", "Eve"]


def test_split_by_regex_unmatched():
    groups = pipeline().split_by_regex(
        "email", {"example": r"@example\.com", "corp": r"@corp\.org"}
    )
    _, rows = _parse(groups["__unmatched__"])
    assert len(rows) == 1
    assert rows[0]["name"] == "Dave"


def test_split_by_regex_headers_preserved():
    groups = pipeline().split_by_regex("email", {"example": r"@example\.com"})
    headers, _ = _parse(groups["example"])
    assert headers == ["name", "email", "dept"]
