"""Integration tests for encoder via Pipeline."""
import base64
import io
import urllib.parse
import pytest
from csvwrangler.pipeline import Pipeline
from csvwrangler.reader import CSVReader


_CSV = "id,name,city\n1,Alice,New York\n2,Bob,Los Angeles\n"


def pipeline():
    return Pipeline(CSVReader(io.StringIO(_CSV)))


def _parse(text: str) -> list[dict]:
    import csv
    return list(csv.DictReader(io.StringIO(text)))


def test_encode_base64_pipeline():
    out = pipeline().encode(["name"], mode="encode", encoding="base64").to_string()
    rows = _parse(out)
    assert rows[0]["name"] == base64.b64encode(b"Alice").decode()


def test_encode_url_pipeline():
    out = pipeline().encode(["city"], mode="encode", encoding="url").to_string()
    rows = _parse(out)
    assert rows[0]["city"] == urllib.parse.quote("New York")


def test_decode_roundtrip_pipeline():
    encoded = pipeline().encode(["name"], mode="encode", encoding="base64").to_string()
    decoded = Pipeline(CSVReader(io.StringIO(encoded))).encode(["name"], mode="decode", encoding="base64").to_string()
    rows = _parse(decoded)
    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "Bob"


def test_encode_headers_unchanged():
    out = pipeline().encode(["name"], mode="encode", encoding="base64").to_string()
    rows = _parse(out)
    assert set(rows[0].keys()) == {"id", "name", "city"}
