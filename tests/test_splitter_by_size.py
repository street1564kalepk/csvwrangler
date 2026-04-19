"""Tests for CSVSizeSplitter."""
import pytest
from csvwrangler.splitter_by_size import CSVSizeSplitter


class _FakeSource:
    def __init__(self, headers, rows):
        self._headers = headers
        self._rows = rows

    @property
    def headers(self):
        return self._headers

    def rows(self):
        yield from self._rows


def _source():
    # Each row: name(~3-5 chars) + score(1-2 chars) = ~4-7 chars per row
    return _FakeSource(
        ["name", "score"],
        [
            {"name": "Ali", "score": "9"},   # size 4
            {"name": "Bob", "score": "10"},  # size 5
            {"name": "Cat", "score": "8"},   # size 4
            {"name": "Dan", "score": "7"},   # size 4
        ],
    )


def test_headers_unchanged():
    s = CSVSizeSplitter(_source(), max_bytes=100)
    assert s.headers == ["name", "score"]


def test_invalid_max_bytes_raises():
    with pytest.raises(ValueError):
        CSVSizeSplitter(_source(), max_bytes=0)


def test_all_rows_in_one_chunk_when_budget_large():
    s = CSVSizeSplitter(_source(), max_bytes=1000)
    assert s.chunk_count == 1
    assert len(s.rows_for_chunk(0)) == 4


def test_splits_into_multiple_chunks():
    # budget=9 → chunk1: Ali(4)+Bob(5)=9, chunk2: Cat(4)+Dan(4)=8
    s = CSVSizeSplitter(_source(), max_bytes=9)
    assert s.chunk_count == 2
    assert len(s.rows_for_chunk(0)) == 2
    assert len(s.rows_for_chunk(1)) == 2


def test_each_row_alone_when_budget_tiny():
    # budget=4 → each row gets its own chunk (Bob spills but starts fresh)
    s = CSVSizeSplitter(_source(), max_bytes=4)
    # Ali=4 fits alone, Bob=5 > 4 so alone, Cat=4 alone, Dan=4 alone
    assert s.chunk_count == 4


def test_chunks_iterator_yields_all_rows():
    s = CSVSizeSplitter(_source(), max_bytes=9)
    all_rows = [row for chunk in s.chunks() for row in chunk]
    assert len(all_rows) == 4


def test_chunk_content_correct():
    s = CSVSizeSplitter(_source(), max_bytes=9)
    first = s.rows_for_chunk(0)
    assert first[0]["name"] == "Ali"
    assert first[1]["name"] == "Bob"


def test_single_oversized_row_starts_new_chunk():
    src = _FakeSource(
        ["name", "score"],
        [
            {"name": "Ali", "score": "9"},
            {"name": "VeryLongName", "score": "100"},
            {"name": "Bob", "score": "1"},
        ],
    )
    s = CSVSizeSplitter(src, max_bytes=6)
    # Ali=4 fits; VeryLongName+100=15 > 6, new chunk alone; Bob=4 new chunk
    assert s.chunk_count == 3
