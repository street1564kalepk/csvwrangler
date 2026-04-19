"""Tests for CSVRowSplitter."""
import pytest
from csvwrangler.splitter_by_row import CSVRowSplitter


class _FakeSource:
    def __init__(self, headers, data):
        self._headers = headers
        self._data = data

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._data


def _source(n: int = 10):
    hdrs = ["id", "val"]
    data = [{"id": str(i), "val": str(i * 2)} for i in range(1, n + 1)]
    return _FakeSource(hdrs, data)


def test_headers_unchanged():
    sp = CSVRowSplitter(_source(6), 2)
    assert sp.headers == ["id", "val"]


def test_chunk_count_exact_division():
    sp = CSVRowSplitter(_source(6), 2)
    assert sp.chunk_count == 3


def test_chunk_count_with_remainder():
    sp = CSVRowSplitter(_source(7), 3)
    assert sp.chunk_count == 3  # 3+3+1


def test_chunk_size():
    sp = CSVRowSplitter(_source(7), 3)
    assert len(sp.chunk(0)) == 3
    assert len(sp.chunk(1)) == 3
    assert len(sp.chunk(2)) == 1


def test_chunk_contents():
    sp = CSVRowSplitter(_source(4), 2)
    first = sp.chunk(0)
    assert first[0]["id"] == "1"
    assert first[1]["id"] == "2"


def test_chunks_iterator_count():
    sp = CSVRowSplitter(_source(5), 2)
    result = list(sp.chunks())
    assert len(result) == 3


def test_row_count():
    sp = CSVRowSplitter(_source(9), 4)
    assert sp.row_count == 9


def test_rows_flat():
    sp = CSVRowSplitter(_source(4), 2)
    rows = list(sp.rows())
    assert len(rows) == 4
    assert rows[0]["id"] == "1"
    assert rows[-1]["id"] == "4"


def test_invalid_chunk_size():
    with pytest.raises(ValueError):
        CSVRowSplitter(_source(4), 0)


def test_single_chunk_when_size_exceeds_rows():
    sp = CSVRowSplitter(_source(3), 100)
    assert sp.chunk_count == 1
    assert len(sp.chunk(0)) == 3


def test_chunk_size_one():
    sp = CSVRowSplitter(_source(5), 1)
    assert sp.chunk_count == 5
    for i, chunk in enumerate(sp.chunks()):
        assert len(chunk) == 1
        assert chunk[0]["id"] == str(i + 1)
