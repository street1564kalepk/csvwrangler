"""Tests for CSVCaster."""

import pytest

from csvwrangler.caster import CSVCaster


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        for row in self._data:
            yield dict(row)


def _source():
    return _FakeSource(
        ["name", "age", "score", "active"],
        [
            {"name": "Alice", "age": "30", "score": "9.5", "active": "true"},
            {"name": "Bob", "age": "25", "score": "7.0", "active": "false"},
            {"name": "Carol", "age": "40", "score": "8.8", "active": "yes"},
        ],
    )


def test_headers_unchanged():
    caster = CSVCaster(_source(), {"age": "int"})
    assert caster.headers == ["name", "age", "score", "active"]


def test_cast_int():
    caster = CSVCaster(_source(), {"age": "int"})
    rows = list(caster.rows())
    assert rows[0]["age"] == 30
    assert rows[1]["age"] == 25
    assert isinstance(rows[0]["age"], int)


def test_cast_float():
    caster = CSVCaster(_source(), {"score": "float"})
    rows = list(caster.rows())
    assert rows[0]["score"] == pytest.approx(9.5)
    assert isinstance(rows[0]["score"], float)


def test_cast_bool_true_values():
    caster = CSVCaster(_source(), {"active": "bool"})
    rows = list(caster.rows())
    assert rows[0]["active"] is True   # "true"
    assert rows[1]["active"] is False  # "false"
    assert rows[2]["active"] is True   # "yes"


def test_cast_str_is_identity():
    caster = CSVCaster(_source(), {"age": "str"})
    rows = list(caster.rows())
    assert rows[0]["age"] == "30"


def test_cast_multiple_columns():
    caster = CSVCaster(_source(), {"age": "int", "score": "float"})
    rows = list(caster.rows())
    assert isinstance(rows[0]["age"], int)
    assert isinstance(rows[0]["score"], float)


def test_unknown_column_is_silently_skipped():
    caster = CSVCaster(_source(), {"nonexistent": "int"})
    rows = list(caster.rows())
    assert len(rows) == 3  # no error, just skipped


def test_errors_raise_on_bad_value():
    src = _FakeSource(["val"], [{"val": "not_a_number"}])
    caster = CSVCaster(src, {"val": "int"}, errors="raise")
    with pytest.raises(ValueError):
        list(caster.rows())


def test_errors_ignore_keeps_original():
    src = _FakeSource(["val"], [{"val": "not_a_number"}])
    caster = CSVCaster(src, {"val": "int"}, errors="ignore")
    rows = list(caster.rows())
    assert rows[0]["val"] == "not_a_number"


def test_errors_null_sets_none():
    src = _FakeSource(["val"], [{"val": "not_a_number"}])
    caster = CSVCaster(src, {"val": "int"}, errors="null")
    rows = list(caster.rows())
    assert rows[0]["val"] is None


def test_invalid_type_name_raises():
    with pytest.raises(ValueError, match="Unknown cast type"):
        CSVCaster(_source(), {"age": "datetime"})


def test_invalid_errors_mode_raises():
    with pytest.raises(ValueError, match="errors must be"):
        CSVCaster(_source(), {"age": "int"}, errors="skip")


def test_row_count():
    caster = CSVCaster(_source(), {"age": "int"})
    assert caster.row_count() == 3
