import io
import csv
from csvwrangler.pipeline import Pipeline
from csvwrangler.reader import CSVReader

_CSV = """dept,name,score
eng,alice,10
eng,bob,20
hr,carol,5
hr,dave,15
fin,frank,30
"""


def pipeline():
    return Pipeline(CSVReader(io.StringIO(_CSV)))


def _parse(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def test_condense_method_exists():
    p = pipeline()
    assert hasattr(p, "condense")


def test_condense_first_headers():
    out = pipeline().condense("dept", agg="first").to_string()
    rows = _parse(out)
    assert list(rows[0].keys()) == ["dept", "name", "score"]


def test_condense_first_row_count():
    out = pipeline().condense("dept", agg="first").to_string()
    rows = _parse(out)
    assert len(rows) == 3


def test_condense_sum_values():
    out = pipeline().condense("dept", agg="sum").to_string()
    rows = _parse(out)
    eng = next(r for r in rows if r["dept"] == "eng")
    assert eng["score"] == "30.0"


def test_condense_join_values():
    out = pipeline().condense("dept", agg="join", sep=";").to_string()
    rows = _parse(out)
    hr = next(r for r in rows if r["dept"] == "hr")
    assert hr["name"] == "carol;dave"
