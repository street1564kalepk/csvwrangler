"""Tests for CSVChunker."""
import pytest
from csvwrangler.chunker import CSVChunker


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._data


def _source(n=10):
    hdrs = ["id", "value"]
    data = [{"id": str(i), "value": str(i * 2)} for i in range(1, n + 1)]
    return _FakeSource(hdrs, data)


def test_headers_unchanged():
    c = CSVChunker(_source(), size=3)
    assert c.headers == ["id", "value"]


def test_chunk_size_property():
    assert CSVChunker(_source(), size=4).size == 4


def test_invalid_size_raises():
    with pytest.raises(ValueError):
        CSVChunker(_source(), size=0)


def test_chunk_count_exact_multiple():
    # 10 rows / 5 = 2 chunks
    c = CSVChunker(_source(10), size=5)
    assert c.chunk_count() == 2


def test_chunk_count_with_remainder():
    # 10 rows / 3 = 3 full + 1 partial = 4 chunks
    c = CSVChunker(_source(10), size=3)
    assert c.chunk_count() == 4


def test_chunk_sizes():
    chunks = list(CSVChunker(_source(7), size=3).chunks())
    assert [len(ch) for ch in chunks] == [3, 3, 1]


def test_chunks_contain_correct_rows():
    chunks = list(CSVChunker(_source(5), size=2).chunks())
    assert chunks[0][0]["id"] == "1"
    assert chunks[0][1]["id"] == "2"
    assert chunks[1][0]["id"] == "3"
    assert chunks[2][0]["id"] == "5"


def test_rows_passthrough():
    c = CSVChunker(_source(6), size=4)
    ids = [r["id"] for r in c.rows()]
    assert ids == [str(i) for i in range(1, 7)]


def test_row_count():
    assert CSVChunker(_source(9), size=4).row_count() == 9


def test_single_chunk_when_size_exceeds_rows():
    chunks = list(CSVChunker(_source(3), size=100).chunks())
    assert len(chunks) == 1
    assert len(chunks[0]) == 3


def test_empty_source_yields_no_chunks():
    chunks = list(CSVChunker(_FakeSource(["a"], []), size=5).chunks())
    assert chunks == []
