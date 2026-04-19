"""CSVRowSplitter – split a source into chunks of N rows each."""
from __future__ import annotations
from typing import List, Iterator


class CSVRowSplitter:
    """Yield successive fixed-size row chunks from a source."""

    def __init__(self, source, chunk_size: int):
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        self._source = source
        self._chunk_size = chunk_size
        self._chunks: List[List[dict]] | None = None

    # ------------------------------------------------------------------
    @property
    def headers(self) -> List[str]:
        return self._source.headers

    # ------------------------------------------------------------------
    def _ensure_built(self) -> None:
        if self._chunks is not None:
            return
        self._chunks = []
        bucket: List[dict] = []
        for row in self._source.rows():
            bucket.append(row)
            if len(bucket) == self._chunk_size:
                self._chunks.append(bucket)
                bucket = []
        if bucket:
            self._chunks.append(bucket)

    # ------------------------------------------------------------------
    @property
    def chunk_count(self) -> int:
        self._ensure_built()
        return len(self._chunks)  # type: ignore[arg-type]

    def chunk(self, index: int) -> List[dict]:
        """Return the chunk at *index* (0-based)."""
        self._ensure_built()
        return self._chunks[index]  # type: ignore[index]

    def chunks(self) -> Iterator[List[dict]]:
        """Iterate over all chunks."""
        self._ensure_built()
        yield from self._chunks  # type: ignore[union-attr]

    def rows(self) -> Iterator[dict]:
        """Flat iteration – same as the source (convenience)."""
        for chunk in self.chunks():
            yield from chunk

    @property
    def row_count(self) -> int:
        return sum(len(c) for c in self.chunks())
