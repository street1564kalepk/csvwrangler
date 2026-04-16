"""Tests for CSVEncoder."""
import base64
import urllib.parse
import pytest
from csvwrangler.encoder import CSVEncoder


class _FakeSource:
    def __init__(self, hdrs, data):
        self._hdrs = hdrs
        self._data = data

    def headers(self):
        return list(self._hdrs)

    def rows(self):
        for r in self._data:
            yield dict(r)


def _source():
    return _FakeSource(
        ["id", "name", "city"],
        [
            {"id": "1", "name": "Alice", "city": "New York"},
            {"id": "2", "name": "Bob", "city": "Los Angeles"},
        ],
    )


def test_headers_unchanged():
    enc = CSVEncoder(_source(), ["name"])
    assert enc.headers() == ["id", "name", "city"]


def test_encode_base64():
    enc = CSVEncoder(_source(), ["name"], mode="encode", encoding="base64")
    rows = list(enc.rows())
    assert rows[0]["name"] == base64.b64encode(b"Alice").decode()
    assert rows[1]["name"] == base64.b64encode(b"Bob").decode()


def test_decode_base64():
    encoded = _FakeSource(
        ["id", "name"],
        [{"id": "1", "name": base64.b64encode(b"Alice").decode()}],
    )
    dec = CSVEncoder(encoded, ["name"], mode="decode", encoding="base64")
    rows = list(dec.rows())
    assert rows[0]["name"] == "Alice"


def test_encode_url():
    enc = CSVEncoder(_source(), ["city"], mode="encode", encoding="url")
    rows = list(enc.rows())
    assert rows[0]["city"] == urllib.parse.quote("New York")
    assert rows[1]["city"] == urllib.parse.quote("Los Angeles")


def test_decode_url():
    src = _FakeSource(
        ["id", "city"],
        [{"id": "1", "city": urllib.parse.quote("New York")}],
    )
    dec = CSVEncoder(src, ["city"], mode="decode", encoding="url")
    rows = list(dec.rows())
    assert rows[0]["city"] == "New York"


def test_non_target_columns_unchanged():
    enc = CSVEncoder(_source(), ["name"], mode="encode", encoding="base64")
    rows = list(enc.rows())
    assert rows[0]["city"] == "New York"
    assert rows[0]["id"] == "1"


def test_invalid_encoding_raises():
    with pytest.raises(ValueError, match="Unsupported encoding"):
        CSVEncoder(_source(), ["name"], encoding="rot13")


def test_invalid_mode_raises():
    with pytest.raises(ValueError, match="mode must be"):
        CSVEncoder(_source(), ["name"], mode="scramble")


def test_row_count():
    enc = CSVEncoder(_source(), ["name"])
    assert enc.row_count() == 2
