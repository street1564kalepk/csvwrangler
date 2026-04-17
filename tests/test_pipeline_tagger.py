"""Integration tests: Pipeline.tag_column."""
import io
import csv
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.reader import CSVReader


CSV_TEXT = "name,score\nAlice,92\nBob,55\nCarol,73\n"


def pipeline():
    return Pipeline(CSVReader(io.StringIO(CSV_TEXT)))


def _parse(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def test_tag_column_method_exists():
    p = pipeline()
    assert hasattr(p, "tag_column")


def test_tag_column_headers():
    rules = [("high", lambda r: int(r["score"]) >= 80),
             ("low",  lambda r: True)]
    out = pipeline().tag_column("grade", rules).to_string()
    rows = _parse(out)
    assert "grade" in rows[0]


def test_tag_column_values():
    rules = [("high", lambda r: int(r["score"]) >= 80),
             ("low",  lambda r: True)]
    out = pipeline().tag_column("grade", rules).to_string()
    rows = _parse(out)
    assert rows[0]["grade"] == "high"   # Alice 92
    assert rows[1]["grade"] == "low"    # Bob 55
    assert rows[2]["grade"] == "low"    # Carol 73 (< 80)


def test_tag_column_chained_with_filter():
    rules = [("pass", lambda r: int(r["score"]) >= 60),
             ("fail", lambda r: True)]
    out = (pipeline()
           .tag_column("result", rules)
           .where("result", "eq", "pass")
           .to_string())
    rows = _parse(out)
    assert all(r["result"] == "pass" for r in rows)
    assert len(rows) == 2  # Alice and Carol
