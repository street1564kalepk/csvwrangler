"""Integration tests: Pipeline .cast() step."""

import io
import csv

import pytest

from csvwrangler.pipeline import Pipeline


_CSV_TEXT = """name,age,score,active
Alice,30,9.5,true
Bob,25,7.0,false
Carol,40,8.8,yes
"""


@pytest.fixture()
def pipeline(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text(_CSV_TEXT)
    return Pipeline.from_file(str(p))


def _parse(tmp_path, pipe):
    out = tmp_path / "out.csv"
    pipe.to_file(str(out))
    reader = csv.DictReader(out.read_text().splitlines())
    return list(reader)


def test_cast_headers_unchanged(pipeline, tmp_path):
    rows = _parse(tmp_path, pipeline.cast({"age": "int"}))
    assert list(rows[0].keys()) == ["name", "age", "score", "active"]


def test_cast_int_values_in_output(pipeline, tmp_path):
    rows = _parse(tmp_path, pipeline.cast({"age": "int"}))
    # CSV output is always strings, but the value should be the integer repr
    assert rows[0]["age"] == "30"
    assert rows[1]["age"] == "25"


def test_cast_float_values_in_output(pipeline, tmp_path):
    rows = _parse(tmp_path, pipeline.cast({"score": "float"}))
    assert rows[0]["score"] == "9.5"


def test_cast_bool_values_in_output(pipeline, tmp_path):
    rows = _parse(tmp_path, pipeline.cast({"active": "bool"}))
    # bool cast: "true" -> True -> "True" when written back to CSV
    assert rows[0]["active"] == "True"
    assert rows[1]["active"] == "False"


def test_cast_chained_with_filter(pipeline, tmp_path):
    rows = _parse(
        tmp_path,
        pipeline.cast({"age": "int"}).where("age", "gt", "30"),
    )
    assert len(rows) == 1
    assert rows[0]["name"] == "Carol"


def test_cast_chained_with_select(pipeline, tmp_path):
    rows = _parse(
        tmp_path,
        pipeline.cast({"age": "int", "score": "float"}).select(["name", "age"]),
    )
    assert list(rows[0].keys()) == ["name", "age"]
    assert len(rows) == 3
