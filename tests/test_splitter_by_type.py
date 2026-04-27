import pytest
from csvwrangler.splitter_by_type import CSVTypeSplitter, _infer_type


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    def rows(self):
        return iter(self._rows)


def _source():
    return _FakeSource(
        headers=["id", "value"],
        rows=[
            {"id": "1", "value": "42"},
            {"id": "2", "value": "3.14"},
            {"id": "3", "value": "hello"},
            {"id": "4", "value": ""},
            {"id": "5", "value": "true"},
            {"id": "6", "value": "100"},
            {"id": "7", "value": "world"},
        ],
    )


# ---------------------------------------------------------------------------
# _infer_type unit tests
# ---------------------------------------------------------------------------

def test_infer_int():
    assert _infer_type("42") == "int"

def test_infer_negative_int():
    assert _infer_type("-7") == "int"

def test_infer_float():
    assert _infer_type("3.14") == "float"

def test_infer_bool_true():
    assert _infer_type("true") == "bool"

def test_infer_bool_false():
    assert _infer_type("False") == "bool"

def test_infer_empty():
    assert _infer_type("") == "empty"

def test_infer_whitespace_is_empty():
    assert _infer_type("   ") == "empty"

def test_infer_string():
    assert _infer_type("hello") == "string"


# ---------------------------------------------------------------------------
# CSVTypeSplitter tests
# ---------------------------------------------------------------------------

def test_headers_unchanged():
    sp = CSVTypeSplitter(_source(), "value")
    assert sp.headers == ["id", "value"]

def test_group_keys_present():
    sp = CSVTypeSplitter(_source(), "value")
    keys = sp.group_keys
    assert set(keys) == {"int", "float", "string", "empty", "bool"}

def test_int_group_row_count():
    sp = CSVTypeSplitter(_source(), "value")
    assert sp.row_count("int") == 2

def test_float_group_rows():
    sp = CSVTypeSplitter(_source(), "value")
    rows = list(sp.rows("float"))
    assert len(rows) == 1
    assert rows[0]["value"] == "3.14"

def test_string_group_rows():
    sp = CSVTypeSplitter(_source(), "value")
    values = [r["value"] for r in sp.rows("string")]
    assert set(values) == {"hello", "world"}

def test_empty_group():
    sp = CSVTypeSplitter(_source(), "value")
    rows = list(sp.rows("empty"))
    assert len(rows) == 1
    assert rows[0]["id"] == "4"

def test_bool_group():
    sp = CSVTypeSplitter(_source(), "value")
    rows = list(sp.rows("bool"))
    assert len(rows) == 1
    assert rows[0]["value"] == "true"

def test_missing_group_returns_empty():
    sp = CSVTypeSplitter(_source(), "value")
    assert list(sp.rows("nonexistent")) == []

def test_unknown_column_raises():
    sp = CSVTypeSplitter(_source(), "missing_col")
    with pytest.raises(KeyError):
        _ = sp.group_keys
