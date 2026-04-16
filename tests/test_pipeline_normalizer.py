"""Integration tests: normalizer wired through Pipeline."""

import io
import csv
from csvwrangler.pipeline import Pipeline


_CSV_TEXT = "name,city,score\n  Alice ,NEW YORK,10\nBOB,  london  ,20\n  carol ,Paris,30\n"


def pipeline():
    return Pipeline.from_string(_CSV_TEXT)


def _parse(text: str):
    return list(csv.DictReader(io.StringIO(text)))


def test_normalizer_headers_unchanged():
    result = pipeline().normalize(["name"], ["strip"]).to_string()
    rows = _parse(result)
    assert list(rows[0].keys()) == ["name", "city", "score"]


def test_normalizer_strip_via_pipeline():
    result = pipeline().normalize(["name"], ["strip"]).to_string()
    rows = _parse(result)
    assert rows[0]["name"] == "Alice"
    assert rows[2]["name"] == "carol"


def test_normalizer_lower_via_pipeline():
    result = pipeline().normalize(["city"], ["strip", "lower"]).to_string()
    rows = _parse(result)
    assert rows[1]["city"] == "london"


def test_normalizer_upper_via_pipeline():
    result = pipeline().normalize(["name"], ["strip", "upper"]).to_string()
    rows = _parse(result)
    assert rows[1]["name"] == "BOB"
    assert rows[0]["name"] == "ALICE"


def test_normalizer_chained_with_filter():
    result = (
        pipeline()
        .normalize(["name"], ["strip", "lower"])
        .where("name", "eq", "alice")
        .to_string()
    )
    rows = _parse(result)
    assert len(rows) == 1
    assert rows[0]["score"] == "10"
