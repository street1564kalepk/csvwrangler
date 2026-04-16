"""Integration tests: stringops via pipeline."""
import io
import pytest
from csvwrangler.reader import CSVReader
from csvwrangler.writer import CSVWriter
from csvwrangler.stringops import CSVStringOps


_CSV = """id,name,city
1,  alice  ,new york
2,BOB,LONDON
3,Carol,  Paris  
"""


def pipeline():
    src = CSVReader(io.StringIO(_CSV))
    return src


def _parse(text: str):
    reader = csv_reader = CSVReader(io.StringIO(text))
    return list(reader.rows())


def test_pipeline_stringops_headers():
    src = pipeline()
    ops = CSVStringOps(src, {"name": "strip"})
    out = io.StringIO()
    CSVWriter(ops).write(out)
    out.seek(0)
    rows = _parse(out.read())
    assert set(rows[0].keys()) == {"id", "name", "city"}


def test_pipeline_strip_name():
    src = pipeline()
    ops = CSVStringOps(src, {"name": "strip"})
    out = io.StringIO()
    CSVWriter(ops).write(out)
    out.seek(0)
    rows = _parse(out.read())
    assert rows[0]["name"] == "alice"


def test_pipeline_lower_city():
    src = pipeline()
    ops = CSVStringOps(src, {"city": "lower"})
    out = io.StringIO()
    CSVWriter(ops).write(out)
    out.seek(0)
    rows = _parse(out.read())
    assert rows[1]["city"] == "london"


def test_pipeline_chained_ops():
    src = pipeline()
    ops1 = CSVStringOps(src, {"name": "strip"})
    ops2 = CSVStringOps(ops1, {"name": "upper"})
    out = io.StringIO()
    CSVWriter(ops2).write(out)
    out.seek(0)
    rows = _parse(out.read())
    assert rows[0]["name"] == "ALICE"
    assert rows[1]["name"] == "BOB"
