"""Integration tests for Pipeline.split_by_weekday()."""
from __future__ import annotations

import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_splitter_by_weekday_patch import _patch

_patch(Pipeline)


_CSV = """id,name,date
1,Alice,2024-01-01
2,Bob,2024-01-02
3,Carol,2024-01-06
4,Dave,2024-01-07
5,Eve,2024-01-08
6,Frank,not-a-date
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(p: "Pipeline") -> list:
    buf = io.StringIO()
    p.to_stream(buf)
    buf.seek(0)
    import csv
    return list(csv.DictReader(buf))


def test_split_by_weekday_method_exists():
    assert hasattr(Pipeline, "split_by_weekday")


def test_split_by_weekday_returns_dict():
    groups = pipeline().split_by_weekday("date")
    assert isinstance(groups, dict)


def test_split_by_weekday_group_keys():
    groups = pipeline().split_by_weekday("date")
    assert "Monday" in groups
    assert "Tuesday" in groups
    assert "_unparsed" in groups


def test_split_monday_two_rows():
    groups = pipeline().split_by_weekday("date")
    rows = _parse(groups["Monday"])
    assert len(rows) == 2


def test_split_saturday_carol():
    groups = pipeline().split_by_weekday("date")
    rows = _parse(groups["Saturday"])
    assert rows[0]["name"] == "Carol"


def test_split_unparsed_frank():
    groups = pipeline().split_by_weekday("date")
    rows = _parse(groups["_unparsed"])
    assert rows[0]["name"] == "Frank"


def test_split_headers_preserved():
    groups = pipeline().split_by_weekday("date")
    rows = _parse(groups["Monday"])
    assert set(rows[0].keys()) == {"id", "name", "date"}
