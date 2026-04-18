import io
import csv
from csvwrangler.pipeline import Pipeline
from csvwrangler.reader import CSVReader


CSV_TEXT = "name,math,english\nAlice,90,80\nBob,70,60\nCarol,50,100\n"


def pipeline():
    return Pipeline(CSVReader(io.StringIO(CSV_TEXT)))


def _parse(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def test_score_method_exists():
    p = pipeline()
    assert hasattr(p, "score")


def test_score_headers():
    p = pipeline()
    out = _parse(p.score({"math": 0.6, "english": 0.4}).to_string())
    assert "score" in out[0]


def test_score_alice_value():
    p = pipeline()
    out = _parse(p.score({"math": 0.6, "english": 0.4}).to_string())
    assert float(out[0]["score"]) == pytest.approx(86.0)


def test_score_custom_col_name():
    p = pipeline()
    out = _parse(p.score({"math": 1.0}, score_col="total").to_string())
    assert "total" in out[0]
    assert "score" not in out[0]


def test_score_chained_with_filter():
    p = pipeline()
    out = _parse(
        p.score({"math": 0.6, "english": 0.4})
         .where("score", "gt", 70)
         .to_string()
    )
    names = [r["name"] for r in out]
    assert "Alice" in names
    assert "Bob" not in names


import pytest
