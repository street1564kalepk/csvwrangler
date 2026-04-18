"""Pipeline integration tests for the annotate() method."""
import io
import csv
import pytest
from csvwrangler.pipeline import Pipeline


_CSV = "name,city\nAlice,London\nBob,Paris\nCarol,Berlin\n"


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


# ── method existence ──────────────────────────────────────────────────

def test_annotate_method_exists():
    assert hasattr(pipeline(), "annotate")


# ── headers ───────────────────────────────────────────────────────────

def test_annotate_headers():
    result = _parse(pipeline().annotate("source", "manual").to_string())
    assert result[0].keys() >= {"name", "city", "source"}


# ── values ────────────────────────────────────────────────────────────

def test_annotate_constant_value():
    result = _parse(pipeline().annotate("source", "manual").to_string())
    assert all(r["source"] == "manual" for r in result)


def test_annotate_row_count_unchanged():
    result = _parse(pipeline().annotate("tag", "v1").to_string())
    assert len(result) == 3


def test_annotate_chained_with_filter():
    result = _parse(
        pipeline()
        .annotate("region", "EU")
        .where("city", "eq", "London")
        .to_string()
    )
    assert len(result) == 1
    assert result[0]["region"] == "EU"
    assert result[0]["name"] == "Alice"
