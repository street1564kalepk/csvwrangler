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


def _run_ops(ops) -> list:
    """Write ops pipeline to a buffer and return parsed rows."""
    out = io.StringIO()
    CSVWriter(ops).write(out)
    out.seek(0)
    return _parse(out.read())


def test_pipeline_stringops_headers():
    src = pipeline()
    ops = CSVStringOps(src, {"name": "strip"})
    rows = _run_ops(ops)
    assert set(rows[0].keys()) == {"id", "name", "city"}


def test_pipeline_strip_name():
    src = pipeline()
    ops = CSVStringOps(src, {"name": "strip"})
    rows = _run_ops(ops)
    assert rows[0]["name"] == "alice"


def test_pipeline_lower_city():
    src = pipeline()
    ops = CSVStringOps(src, {"city": "lower"})
    rows = _run_ops(ops)
    assert rows[1]["city"] == "london"


def test_pipeline_chained_ops():
    src = pipeline()
    ops1 = CSVStringOps(src, {"name": "strip"})
    ops2 = CSVStringOps(ops1, {"name": "upper"})
    rows = _run_ops(ops2)
    assert rows[0]["name"] == "ALICE"
    assert rows[1]["name"] == "BOB"


def test_pipeline_strip_city():
    """Ensure strip removes surrounding whitespace from city column."""
    src = pipeline()
    ops = CSVStringOps(src, {"city": "strip"})
    rows = _run_ops(ops)
    assert rows[2]["city"] == "Paris"
