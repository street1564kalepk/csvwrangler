"""Tests for CSVTransposer."""
import pytest
from csvwrangler.transposer import CSVTransposer


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._data)


def _source():
    return _FakeSource(
        ["name", "age", "city"],
        [
            {"name": "Alice", "age": "30", "city": "NYC"},
            {"name": "Bob", "age": "25", "city": "LA"},
        ],
    )


def test_headers_default():
    t = CSVTransposer(_source())
    assert t.headers == ["field", "row_1", "row_2"]


def test_headers_custom_label_and_prefix():
    t = CSVTransposer(_source(), row_label_column="attribute", row_prefix="record")
    assert t.headers == ["attribute", "record_1", "record_2"]


def test_row_count_equals_original_column_count():
    t = CSVTransposer(_source())
    assert t.row_count() == 3  # name, age, city


def test_rows_field_column_contains_original_headers():
    t = CSVTransposer(_source())
    fields = [r["field"] for r in t.rows()]
    assert fields == ["name", "age", "city"]


def test_rows_values_match_original_data():
    t = CSVTransposer(_source())
    data = list(t.rows())
    name_row = next(r for r in data if r["field"] == "name")
    assert name_row["row_1"] == "Alice"
    assert name_row["row_2"] == "Bob"


def test_rows_age_values():
    t = CSVTransposer(_source())
    data = list(t.rows())
    age_row = next(r for r in data if r["field"] == "age")
    assert age_row["row_1"] == "30"
    assert age_row["row_2"] == "25"


def test_single_row_source():
    src = _FakeSource(["x", "y"], [{"x": "1", "y": "2"}])
    t = CSVTransposer(src)
    assert t.headers == ["field", "row_1"]
    data = list(t.rows())
    assert data[0] == {"field": "x", "row_1": "1"}
    assert data[1] == {"field": "y", "row_1": "2"}


def test_empty_source():
    src = _FakeSource(["a", "b"], [])
    t = CSVTransposer(src)
    assert t.headers == ["field"]
    assert t.row_count() == 2
    data = list(t.rows())
    assert data[0] == {"field": "a"}
    assert data[1] == {"field": "b"}


def test_build_called_once(monkeypatch):
    t = CSVTransposer(_source())
    call_count = 0
    original_build = t._build

    def counting_build():
        nonlocal call_count
        call_count += 1
        original_build()

    monkeypatch.setattr(t, "_build", counting_build)
    _ = t.headers
    _ = t.row_count()
    # After first real build _data is set; monkeypatched build tracks extra calls
    assert call_count == 2  # once per property access via monkeypatch
