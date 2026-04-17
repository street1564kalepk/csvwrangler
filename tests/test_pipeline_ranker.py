"""Pipeline integration tests for CSVRanker."""
import io
from csvwrangler.pipeline import Pipeline

_CSV = """name,score
Alice,90
Bob,70
Carol,50
"""


def pipeline():
    return Pipeline.from_string(_CSV)


def _parse(csv_text: str) -> list[dict]:
    lines = csv_text.strip().splitlines()
    headers = lines[0].split(",")
    return [dict(zip(headers, line.split(","))) for line in lines[1:]]


def test_rank_method_exists():
    p = pipeline()
    assert hasattr(p, "rank_by")


def test_rank_headers():
    p = pipeline()
    out = _parse(p.rank_by("score").to_string())
    assert "rank" in out[0]


def test_rank_descending_alice_is_first():
    p = pipeline()
    out = _parse(p.rank_by("score", ascending=False).to_string())
    by_name = {r["name"]: int(r["rank"]) for r in out}
    assert by_name["Alice"] == 1


def test_rank_ascending_carol_is_first():
    p = pipeline()
    out = _parse(p.rank_by("score", ascending=True).to_string())
    by_name = {r["name"]: int(r["rank"]) for r in out}
    assert by_name["Carol"] == 1


def test_rank_custom_col_name():
    p = pipeline()
    out = _parse(p.rank_by("score", rank_col="position").to_string())
    assert "position" in out[0]
    assert "rank" not in out[0]
