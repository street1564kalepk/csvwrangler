"""Integration tests: Pipeline.pivot() end-to-end."""
import io
import csv
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.pivotter import CSVPivotter


CSV_DATA = """name,metric,value
alice,height,170
alice,weight,65
bob,height,180
bob,weight,80
"""


@pytest.fixture()
def pipeline(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text(CSV_DATA)
    return Pipeline.from_file(str(p))


def _parse(output: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(output)))


def test_pivot_headers(pipeline):
    out = io.StringIO()
    pipeline.pivot(
        index_col="name", pivot_col="metric", value_col="value"
    ).to_file(out)
    out.seek(0)
    reader = csv.reader(out)
    headers = next(reader)
    assert headers == ["name", "height", "weight"]


def test_pivot_row_count(pipeline):
    out = io.StringIO()
    pipeline.pivot(
        index_col="name", pivot_col="metric", value_col="value"
    ).to_file(out)
    rows = _parse(out.getvalue())
    assert len(rows) == 2


def test_pivot_values(pipeline):
    out = io.StringIO()
    pipeline.pivot(
        index_col="name", pivot_col="metric", value_col="value"
    ).to_file(out)
    rows = {r["name"]: r for r in _parse(out.getvalue())}
    assert rows["alice"]["height"] == "170"
    assert rows["alice"]["weight"] == "65"
    assert rows["bob"]["height"] == "180"
    assert rows["bob"]["weight"] == "80"


def test_pivot_after_filter(pipeline):
    """Filter to a single person then pivot — should yield one row."""
    out = io.StringIO()
    pipeline.where("name", "eq", "alice").pivot(
        index_col="name", pivot_col="metric", value_col="value"
    ).to_file(out)
    rows = _parse(out.getvalue())
    assert len(rows) == 1
    assert rows[0]["name"] == "alice"
