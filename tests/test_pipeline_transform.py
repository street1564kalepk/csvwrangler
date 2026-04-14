import io
import csv
import pytest
from csvwrangler.reader import CSVReader
from csvwrangler.pipeline import Pipeline
from csvwrangler.transform import CSVTransform


SAMPLE_CSV = """id,first_name,salary,department
1,Alice,70000,Engineering
2,Bob,50000,Marketing
3,Carol,90000,Engineering
4,Dave,45000,HR
"""


@pytest.fixture
def pipeline():
    return Pipeline(CSVReader(io.StringIO(SAMPLE_CSV)))


def _parse(csv_str: str):
    reader = csv.DictReader(io.StringIO(csv_str))
    return list(reader)


def test_pipeline_transform_rename(pipeline):
    t = pipeline.transform()
    t.rename("first_name", "name")
    pipeline.continue_with(t)
    rows = _parse(pipeline.to_string())
    assert "name" in rows[0]
    assert "first_name" not in rows[0]
    assert rows[0]["name"] == "Alice"


def test_pipeline_transform_add_column(pipeline):
    t = pipeline.transform()
    t.add_column("senior", lambda row: "yes" if int(row["salary"]) >= 70000 else "no")
    pipeline.continue_with(t)
    rows = _parse(pipeline.to_string())
    assert rows[0]["senior"] == "yes"
    assert rows[1]["senior"] == "no"
    assert rows[2]["senior"] == "yes"


def test_pipeline_filter_then_transform(pipeline):
    pipeline.filter("department", "eq", "Engineering")
    t = pipeline.transform()
    t.apply("salary", lambda v: str(int(v) // 1000) + "k")
    pipeline.continue_with(t)
    rows = _parse(pipeline.to_string())
    assert len(rows) == 2
    assert rows[0]["salary"] == "70k"
    assert rows[1]["salary"] == "90k"


def test_pipeline_transform_then_select(pipeline):
    t = pipeline.transform()
    t.rename("first_name", "name")
    pipeline.continue_with(t)
    pipeline.select("id", "name")
    rows = _parse(pipeline.to_string())
    assert list(rows[0].keys()) == ["id", "name"]


def test_pipeline_to_file_with_transform(tmp_path, pipeline):
    t = pipeline.transform()
    t.add_column("label", lambda row: row["first_name"].upper())
    pipeline.continue_with(t)
    out = tmp_path / "out.csv"
    pipeline.to_file(str(out))
    result = _parse(out.read_text())
    assert result[0]["label"] == "ALICE"
    assert result[3]["label"] == "DAVE"
