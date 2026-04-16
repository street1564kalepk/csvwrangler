"""Pipeline integration tests for CSVDiffer."""
import io
import csv
from csvwrangler.pipeline import Pipeline


_LEFT_CSV = """id,name,city
1,Alice,NY
2,Bob,LA
3,Carol,SF
"""

_RIGHT_CSV = """id,name,city
1,Alice,NY
2,Bob,Chicago
4,Dave,Boston
"""


def _parse(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def _pipeline(left_text, right_text):
    from csvwrangler.reader import CSVReader
    left = CSVReader(io.StringIO(left_text))
    right = CSVReader(io.StringIO(right_text))
    return Pipeline(left), right


def test_differ_added_via_pipeline(tmp_path):
    p, right = _pipeline(_LEFT_CSV, _RIGHT_CSV)
    out = tmp_path / "out.csv"
    p.diff(right, key="id", mode="added").to_file(str(out))
    rows = _parse(out.read_text())
    assert len(rows) == 1
    assert rows[0]["_diff"] == "added"
    assert rows[0]["id"] == "4"


def test_differ_removed_via_pipeline(tmp_path):
    p, right = _pipeline(_LEFT_CSV, _RIGHT_CSV)
    out = tmp_path / "out.csv"
    p.diff(right, key="id", mode="removed").to_file(str(out))
    rows = _parse(out.read_text())
    assert len(rows) == 1
    assert rows[0]["id"] == "3"


def test_differ_all_via_pipeline(tmp_path):
    p, right = _pipeline(_LEFT_CSV, _RIGHT_CSV)
    out = tmp_path / "out.csv"
    p.diff(right, key="id", mode="all").to_file(str(out))
    rows = _parse(out.read_text())
    assert len(rows) == 3
    assert "_diff" in rows[0]
