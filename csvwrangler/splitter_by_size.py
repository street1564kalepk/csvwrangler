"""CSVSizeSplitter – split rows into chunks where each chunk's total
cell-character count does not exceed a given byte budget."""
from __future__ import annotations
from typing import List, Iterator


class CSVSizeSplitter:
    """Split a CSV source into variable-length chunks capped by *max_bytes*.

    Each chunk is a list of row-dicts whose combined cell content (summed
    character lengths across all values) stays within *max_bytes*.
    """

    def __init__(self, source, max_bytes: int) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        self._source = source
        self._max_bytes = max_bytes
        self._chunks: List[List[dict]] | None = None

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def _row_size(self, row: dict) -> int:
        return sum(len(str(v)) for v in row.values())

    def _ensure_built(self) -> None:
        if self._chunks is not None:
            return
        self._chunks = []
        current: List[dict] = []
        current_size = 0
        for row in self._source.rows():
            rs = self._row_size(row)
            if current and current_size + rs > self._max_bytes:
                self._chunks.append(current)
                current = []
                current_size = 0
            current.append(row)
            current_size += rs
        if current:
            self._chunks.append(current)

    @property
    def chunk_count(self) -> int:
        self._ensure_built()
        return len(self._chunks)  # type: ignore[index]

    def chunks(self) -> Iterator[List[dict]]:
        self._ensure_built()
        yield from self._chunks  # type: ignore[union-attr]

    def rows_for_chunk(self, index: int) -> List[dict]:
        self._ensure_built()
        return self._chunks[index]  # type: ignore[index]
