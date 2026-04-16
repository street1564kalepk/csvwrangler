"""Tests for CSVTyper."""
import pytest
from csvwrangler.typer import CSVTyper


class _FakeSource:
    def __init__(self, hdrs, data):
        self._headers = hdrs
        self._data = data

    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))


def _source(hdrs, data):
    return _FakeSource(hdrs, data)


# --- detected_types ---

def test_detect_int_column():
    src = _source(["id", "name"], [("1", "Alice"), ("2", "Bob")])
    t = CSVTyper(src)
    assert t.detected_types()["id"] == "int"

def test_detect_float_column():
    src = _source(["score"], [("1.5",), ("2.3",)])
    t = CSVTyper(src)
    assert t.detected_types()["score"] == "float"

def test_detect_str_column():
    src = _source(["name"], [("Alice",), ("Bob",)])
    t = CSVTyper(src)
    assert t.detected_types()["name"] == "str"

def test_detect_mixed_falls_back_to_str():
    src = _source(["val"], [("1",), ("abc",)])
    t = CSVTyper(src)
    assert t.detected_types()["val"] == "str"

# --- headers ---

def test_headers_unchanged():
    src = _source(["id", "name", "score"], [("1", "Alice", "9.5")])
    t = CSVTyper(src)
    assert t.headers() == ["id", "name", "score"]

# --- rows casting ---

def test_rows_cast_int():
    src = _source(["id"], [("10",), ("20",)])
    t = CSVTyper(src)
    result = list(t.rows())
    assert result[0]["id"] == 10
    assert isinstance(result[0]["id"], int)

def test_rows_cast_float():
    src = _source(["val"], [("3.14",), ("2.71",)])
    t = CSVTyper(src)
    result = list(t.rows())
    assert result[0]["val"] == pytest.approx(3.14)
    assert isinstance(result[0]["val"], float)

def test_rows_str_unchanged():
    src = _source(["name"], [("Alice",), ("Bob",)])
    t = CSVTyper(src)
    result = list(t.rows())
    assert result[0]["name"] == "Alice"

def test_rows_empty_string_preserved():
    src = _source(["id"], [("1",), ("",)])
    t = CSVTyper(src)
    result = list(t.rows())
    assert result[1]["id"] == ""

def test_row_count():
    src = _source(["id"], [("1",), ("2",), ("3",)])
    t = CSVTyper(src)
    assert t.row_count() == 3

def test_empty_source_defaults_to_str():
    src = _source(["id", "name"], [])
    t = CSVTyper(src)
    assert t.detected_types() == {"id": "str", "name": "str"}
