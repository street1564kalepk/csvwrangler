"""CSVChunker – split a source into fixed-size row chunks."""
from typing import Iterator, List, Dict, Any


class CSVChunker:
    """Yield successive chunks of *size* rows from *source*."""

    def __init__(self, source, size: int):
        if size < 1:
            raise ValueError("chunk size must be >= 1")
        self._source = source
        self._size = size

    # ------------------------------------------------------------------
    @property
    def headers(self) -> List[str]:
        return self._source.headers

    @property
    def size(self) -> int:
        return self._size

    # ------------------------------------------------------------------
    def chunks(self) -> Iterator[List[Dict[str, Any]]]:
        """Yield lists of row-dicts, each at most *size* rows long."""
        bucket: List[Dict[str, Any]] = []
        for row in self._source.rows():
            bucket.append(row)
            if len(bucket) == self._size:
                yield bucket
                bucket = []
        if bucket:
            yield bucket

    def rows(self) -> Iterator[Dict[str, Any]]:
        """Flat row iterator – re-yields every row (passthrough)."""
        for chunk in self.chunks():
            yield from chunk

    def chunk_count(self) -> int:
        """Return the number of chunks (consumes the source)."""
        return sum(1 for _ in self.chunks())

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVChunker(size={self._size})"
