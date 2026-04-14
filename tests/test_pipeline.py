import os
import pytest
import tempfile
from csvwrangler.pipeline import Pipeline
from csvwrangler.reader import CSVReader


CSV_CONTENT = """name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,New York
Diana,28,Chicago
"""


@pytest.fixture
def csv_file(tmp_path):
    f = tmp_path / "people.csv"
    f.write_text(CSV_CONTENT)
    return str(f)


def test_pipeline_to_file_no_filter(csv_file, tmp_path):
    out = str(tmp_path / "out.csv")
    pipeline = Pipeline(csv_file)
    count = pipeline.to_file(out)
    assert count == 4
    reader = CSVReader(out)
    assert reader.headers == ["name", "age", "city"]
    rows = list(reader.rows())
    assert len(rows) == 4


def test_pipeline_to_file_with_filter(csv_file, tmp_path):
    out = str(tmp_path / "filtered.csv")
    pipeline = Pipeline(csv_file)
    pipeline.filter().where("city", "==", "New York")
    count = pipeline.to_file(out)
    assert count == 2
    reader = CSVReader(out)
    rows = list(reader.rows())
    assert all(r["city"] == "New York" for r in rows)


def test_pipeline_select_columns(csv_file, tmp_path):
    out = str(tmp_path / "selected.csv")
    pipeline = Pipeline(csv_file)
    pipeline.select("name", "city")
    count = pipeline.to_file(out)
    assert count == 4
    reader = CSVReader(out)
    assert reader.headers == ["name", "city"]
    rows = list(reader.rows())
    assert "age" not in rows[0]


def test_pipeline_select_and_filter(csv_file, tmp_path):
    out = str(tmp_path / "sf Pipeline(csv_file)
    pipeline.select("name", "city")
    pipeline.filter().where("age", ">", 28)
    count = pipeline.to_file(out)
    assert count == 2
    reader = CSVReader(out)
    rows = list(reader.rows())
    assert {r["name"] for r in rows} == {"Alice", "Charlie"}


def test_pipeline_invalid(csv_file):
    pipeline = Pipeline(csv_file)
    with pytest.raises(ValueError, match="Column 'salary' not found"):
        pipeline.select("name", "salary")


def test_pipeline_repr(csv_file):
    pipeline = Pipeline(csv_file)
    assert "Pipeline" in repr(pipeline)
