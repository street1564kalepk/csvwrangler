"""Integration tests: Pipeline.format_columns."""
import io
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.reader import CSVReader


CSV_TEXT = """first,last,age
Alice,Smith,30
Bob,Jones,25
"""


def pipeline():
    return Pipeline(CSVReader(io.StringIO(CSV_TEXT)))


def _parse(text: str):
    rows = []
    lines = text.strip().splitlines()
    headers = lines[0].split(",")
    for line in lines[1:]:
        rows.append(dict(zip(headers, line.split(","))))
    return headers, rows


def test_format_columns_method_exists():
    p = pipeline()
    assert hasattr(p, "format_columns")


def test_format_columns_headers_unchanged():
    out = pipeline().format_columns({"age": "Age:{value}"}).to_string()
    headers, _ = _parse(out)
    assert headers == ["first", "last", "age"]


def test_format_columns_applies_template():
    out = pipeline().format_columns({"age": "({value})"}).to_string()
    _, rows = _parse(out)
    assert rows[0]["age"] == "(30)"
    assert rows[1]["age"] == "(25)"


def test_format_columns_cross_column():
    out = pipeline().format_columns({"first": "{first} {last}"}).to_string()
    _, rows = _parse(out)
    assert rows[0]["first"] == "Alice Smith"


def test_format_columns_chainable():
    out = (
        pipeline()
        .format_columns({"age": "[{value}]"})
        .format_columns({"first": "{value}!"})
        .to_string()
    )
    _, rows = _parse(out)
    assert rows[0]["age"] == "[30]"
    assert rows[0]["first"] == "Alice!"
