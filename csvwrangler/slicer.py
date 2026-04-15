"""CSVSlicer — skip/limit rows by offset and count."""
from __future__ import annotations

from typing import Iterator


class CSVSlicer:
    """Wrap a source and yield only rows in [offset, offset+limit).

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows()``.
    offset:
        Number of data rows to skip before yielding (default 0).
    limit:
        Maximum number of rows to yield.  ``None`` means no upper bound.
    """

    def __init__(self, source, offset: int = 0, limit: int | None = None) -> None:
        if offset < 0:
            raise ValueError(f"offset must be >= 0, got {offset}")
        if limit is not None and limit < 0:
            raise ValueError(f"limit must be >= 0 or None, got {limit}")
        self._source = source
        self._offset = offset
        self._limit = limit

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return self._source.headers

    def rows(self) -> Iterator[dict]:
        emitted = 0
        for idx, row in enumerate(self._source.rows()):
            if idx < self._offset:
                continue
            if self._limit is not None and emitted >= self._limit:
                break
            yield row
            emitted += 1

    def row_count(self) -> int:
        """Return the number of rows that *will* be yielded (materialises rows)."""
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVSlicer(offset={self._offset}, limit={self._limit}, "
            f"source={self._source!r})"
        )
