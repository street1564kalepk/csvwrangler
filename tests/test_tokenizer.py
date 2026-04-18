"""Tests for CSVTokenizer."""
import pytest
from csvwrangler.tokenizer import CSVTokenizer


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
        ["id", "text"],
        [
            {"id": "1", "text": "hello world"},
            {"id": "2", "text": "foo  bar baz"},
            {"id": "3", "text": ""},
            {"id": "4", "text": "single"},
        ],
    )


def test_headers_include_count_col():
    t = CSVTokenizer(_source(), "text", count_col="token_count", tokens_col=None)
    assert t.headers == ["id", "text", "token_count"]


def test_headers_include_tokens_col():
    t = CSVTokenizer(_source(), "text", count_col=None, tokens_col="tokens")
    assert t.headers == ["id", "text", "tokens"]


def test_headers_include_both():
    t = CSVTokenizer(_source(), "text", count_col="cnt", tokens_col="toks")
    assert t.headers == ["id", "text", "cnt", "toks"]


def test_count_col_values():
    t = CSVTokenizer(_source(), "text", count_col="n", tokens_col=None)
    results = list(t.rows())
    assert results[0]["n"] == 2
    assert results[1]["n"] == 3
    assert results[2]["n"] == 0
    assert results[3]["n"] == 1


def test_tokens_col_values():
    t = CSVTokenizer(_source(), "text", count_col=None, tokens_col="toks")
    results = list(t.rows())
    assert results[0]["toks"] == "hello world"
    assert results[1]["toks"] == "foo bar baz"
    assert results[2]["toks"] == ""


def test_original_columns_preserved():
    t = CSVTokenizer(_source(), "text", count_col="n")
    for row in t.rows():
        assert "id" in row
        assert "text" in row


def test_invalid_column_raises():
    with pytest.raises(ValueError, match="not found"):
        CSVTokenizer(_source(), "missing", count_col="n")


def test_both_none_raises():
    with pytest.raises(ValueError, match="At least one"):
        CSVTokenizer(_source(), "text", count_col=None, tokens_col=None)


def test_row_count():
    t = CSVTokenizer(_source(), "text", count_col="n")
    assert t.row_count == 4


def test_custom_pattern():
    src = _FakeSource(["id", "text"], [{"id": "1", "text": "a,b,c"}])
    t = CSVTokenizer(src, "text", count_col="n", pattern=r",")
    results = list(t.rows())
    assert results[0]["n"] == 3
