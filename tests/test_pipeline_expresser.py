"""Integration tests for Pipeline.express()."""

import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pipeline_expresser_patch import _patch

_patch(Pipeline)


def pipeline():
    csv_text = "name,score\nAlice,10\nBob,20\nCarol,5\n"
    return Pipeline.from_string(csv_text)


def _parse(p) -> list[dict]:
    buf = io.StringIO()
    p.to_stream(buf)
    buf.seek(0)
    import csv
    return list(csv.DictReader(buf))


def test_express_method_exists():
    assert hasattr(Pipeline, "express")


def test_express_adds_column_to_headers():
    p = pipeline().express("label", "name + '!'")
    rows = _parse(p)
    assert "label" in rows[0]


def test_express_new_column_values():
    p = pipeline().express("label", "name + '!'")
    rows = _parse(p)
    assert rows[0]["label"] == "Alice!"
    assert rows[1]["label"] == "Bob!"


def test_express_overwrite_column():
    p = pipeline().express("score", "str(int(score) * 3)")
    rows = _parse(p)
    assert rows[0]["score"] == "30"
    assert rows[1]["score"] == "60"


def test_express_preserves_other_columns():
    p = pipeline().express("label", "name")
    rows = _parse(p)
    assert rows[0]["name"] == "Alice"
    assert rows[0]["score"] == "10"
