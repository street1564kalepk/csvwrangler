import io
from csvwrangler.pipeline import Pipeline


_CSV = """name,score,age
Alice,90,30
Bob,80,25
Charlie,85,35
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(csv_text: str):
    lines = csv_text.strip().splitlines()
    headers = lines[0].split(",")
    rows = [dict(zip(headers, l.split(","))) for l in lines[1:]]
    return headers, rows


def test_summarize_method_exists():
    p = pipeline()
    assert hasattr(p, "summarize")


def test_summarize_returns_string():
    p = pipeline()
    result = p.summarize().to_string()
    assert isinstance(result, str)


def test_summarize_headers():
    p = pipeline()
    headers, _ = _parse(p.summarize().to_string())
    assert headers == ["column", "count", "unique", "min", "max", "mean"]


def test_summarize_row_count():
    p = pipeline()
    _, rows = _parse(p.summarize().to_string())
    # one summary row per source column
    assert len(rows) == 3


def test_summarize_score_mean():
    p = pipeline()
    _, rows = _parse(p.summarize().to_string())
    score_row = next(r for r in rows if r["column"] == "score")
    assert float(score_row["mean"]) == pytest.approx(85.0)


import pytest
