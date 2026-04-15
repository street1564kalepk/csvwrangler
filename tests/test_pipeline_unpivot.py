"""Integration tests: Pipeline.unpivot()."""
import csv
import io
import pytest
from csvwrangler.pipeline import Pipeline


RAW_CSV = """id,name,jan,feb,mar
1,Alice,100,200,150
2,Bob,300,400,350
"""


@pytest.fixture
def pipeline(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text(RAW_CSV)
    return Pipeline.from_file(str(p))


def _parse(result: str):
    reader = csv.DictReader(io.StringIO(result))
    return list(reader)


def test_unpivot_headers(pipeline):
    out = io.StringIO()
    pipeline.unpivot(
        id_cols=["id", "name"],
        value_cols=["jan", "feb", "mar"],
        var_name="month",
        value_name="sales",
    ).to_string(out)
    rows = _parse(out.getvalue())
    assert list(rows[0].keys()) == ["id", "name", "month", "sales"]


def test_unpivot_row_count(pipeline):
    out = io.StringIO()
    pipeline.unpivot(
        id_cols=["id", "name"],
        value_cols=["jan", "feb", "mar"],
    ).to_string(out)
    rows = _parse(out.getvalue())
    assert len(rows) == 6


def test_unpivot_values(pipeline):
    out = io.StringIO()
    pipeline.unpivot(
        id_cols=["id", "name"],
        value_cols=["jan", "feb", "mar"],
        var_name="month",
        value_name="sales",
    ).to_string(out)
    rows = _parse(out.getvalue())
    assert rows[0] == {"id": "1", "name": "Alice", "month": "jan", "sales": "100"}
    assert rows[3] == {"id": "2", "name": "Bob",   "month": "jan", "sales": "300"}


def test_unpivot_then_filter(pipeline):
    out = io.StringIO()
    pipeline.unpivot(
        id_cols=["id", "name"],
        value_cols=["jan", "feb", "mar"],
        var_name="month",
        value_name="sales",
    ).where("month", "eq", "feb").to_string(out)
    rows = _parse(out.getvalue())
    assert len(rows) == 2
    assert all(r["month"] == "feb" for r in rows)


def test_unpivot_default_names(pipeline):
    out = io.StringIO()
    pipeline.unpivot(
        id_cols=["id"],
        value_cols=["jan", "feb"],
    ).to_string(out)
    rows = _parse(out.getvalue())
    assert "variable" in rows[0]
    assert "value" in rows[0]
