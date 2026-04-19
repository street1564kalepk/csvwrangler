"""Tests for CSVExpresser."""

import pytest
from csvwrangler.expresser import CSVExpresser


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return list(self._headers)

    def rows(self):
        yield from (dict(r) for r in self._rows)


def _source():
    return _FakeSource(
        ["name", "score"],
        [
            {"name": "Alice", "score": "10"},
            {"name": "Bob", "score": "20"},
            {"name": "Carol", "score": "5"},
        ],
    )


def test_headers_adds_new_column():
    expr = CSVExpresser(_source(), column="label", expression="name + '!'")
    assert expr.headers == ["name", "score", "label"]


def test_headers_does_not_duplicate_existing_column():
    expr = CSVExpresser(_source(), column="score", expression="score")
    assert expr.headers == ["name", "score"]


def test_new_column_values():
    expr = CSVExpresser(_source(), column="label", expression="name + '!'")
    result = list(expr.rows())
    assert result[0]["label"] == "Alice!"
    assert result[1]["label"] == "Bob!"
    assert result[2]["label"] == "Carol!"


def test_overwrite_existing_column():
    expr = CSVExpresser(_source(), column="score", expression="str(int(score) * 2)")
    result = list(expr.rows())
    assert result[0]["score"] == "20"
    assert result[1]["score"] == "40"


def test_row_count():
    expr = CSVExpresser(_source(), column="label", expression="name")
    assert expr.row_count == 3


def test_original_columns_preserved():
    expr = CSVExpresser(_source(), column="label", expression="name")
    result = list(expr.rows())
    assert result[0]["name"] == "Alice"
    assert result[0]["score"] == "10"


def test_empty_column_raises():
    with pytest.raises(ValueError, match="column"):
        CSVExpresser(_source(), column="", expression="name")


def test_empty_expression_raises():
    with pytest.raises(ValueError, match="expression"):
        CSVExpresser(_source(), column="label", expression="")
