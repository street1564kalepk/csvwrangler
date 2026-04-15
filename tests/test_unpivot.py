"""Tests for CSVUnpivot."""
import pytest
from csvwrangler.unpivot import CSVUnpivot


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        for row in self._data:
            yield dict(zip(self._headers, row))

    @property
    def row_count(self):
        return len(self._data)


def _source():
    return _FakeSource(
        headers=["id", "name", "jan", "feb", "mar"],
        data=[
            ["1", "Alice", "100", "200", "150"],
            ["2", "Bob",   "300", "400", "350"],
        ],
    )


def make_unpivot(**kwargs):
    defaults = dict(
        id_cols=["id", "name"],
        value_cols=["jan", "feb", "mar"],
        var_name="month",
        value_name="sales",
    )
    defaults.update(kwargs)
    return CSVUnpivot(_source(), **defaults)


# --- headers ---

def test_headers_contain_id_and_var_value_cols():
    up = make_unpivot()
    assert up.headers == ["id", "name", "month", "sales"]


def test_headers_default_var_value_names():
    up = CSVUnpivot(_source(), id_cols=["id"], value_cols=["jan", "feb"])
    assert up.headers == ["id", "variable", "value"]


# --- row_count ---

def test_row_count_multiplied_by_value_cols():
    up = make_unpivot()
    assert up.row_count == 6  # 2 rows * 3 value cols


def test_row_count_none_when_source_lacks_it():
    class _NoCount(_FakeSource):
        @property
        def row_count(self):
            raise AttributeError
    src = _NoCount(["id", "jan"], [["1", "10"]])
    up = CSVUnpivot(src, id_cols=["id"], value_cols=["jan"])
    assert up.row_count is None


# --- rows ---

def test_rows_count():
    rows = list(make_unpivot().rows())
    assert len(rows) == 6


def test_rows_keys_match_headers():
    up = make_unpivot()
    for row in up.rows():
        assert set(row.keys()) == set(up.headers)


def test_rows_variable_values():
    rows = list(make_unpivot().rows())
    months = [r["month"] for r in rows]
    assert months == ["jan", "feb", "mar", "jan", "feb", "mar"]


def test_rows_values_correct():
    rows = list(make_unpivot().rows())
    assert rows[0] == {"id": "1", "name": "Alice", "month": "jan", "sales": "100"}
    assert rows[4] == {"id": "2", "name": "Bob",   "month": "feb", "sales": "400"}


def test_rows_id_cols_preserved():
    rows = list(make_unpivot().rows())
    alice_rows = [r for r in rows if r["name"] == "Alice"]
    assert all(r["id"] == "1" for r in alice_rows)


# --- validation ---

def test_raises_on_missing_id_col():
    with pytest.raises(ValueError, match="not found"):
        CSVUnpivot(_source(), id_cols=["missing"], value_cols=["jan"])


def test_raises_on_missing_value_col():
    with pytest.raises(ValueError, match="not found"):
        CSVUnpivot(_source(), id_cols=["id"], value_cols=["dec"])


def test_raises_when_var_name_clashes_with_id_col():
    with pytest.raises(ValueError, match="clash"):
        CSVUnpivot(
            _source(),
            id_cols=["id", "name"],
            value_cols=["jan"],
            var_name="id",
        )
