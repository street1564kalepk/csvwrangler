"""CSVLimiter — limits and offsets rows from a CSV source."""

from typing import Iterator


class CSVLimiter:
    """Wraps a CSV source and yields at most *limit* rows, skipping *offset* rows first."""

    def __init__(self, source, limit: int = None, offset: int = 0):
        """
        Parameters
        ----------
        source  : any object exposing .headers and .rows()
        limit   : maximum number of rows to yield (None means no limit)
        offset  : number of rows to skip before yielding
        """
        if limit is not None and limit < 0:
            raise ValueError("limit must be a non-negative integer or None")
        if offset < 0:
            raise ValueError("offset must be a non-negative integer")

        self._source = source
        self._limit = limit
        self._offset = offset

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        """Pass headers through unchanged."""
        return self._source.headers

    def rows(self) -> Iterator[dict]:
        """Yield rows after applying offset and limit."""
        emitted = 0
        for index, row in enumerate(self._source.rows()):
            if index < self._offset:
                continue
            if self._limit is not None and emitted >= self._limit:
                break
            yield row
            emitted += 1

    def row_count(self) -> int:
        """Return the number of rows that would be yielded."""
        return sum(1 for _ in self.rows())

    def skip(self, n: int) -> "CSVLimiter":
        """Return a new CSVLimiter with *n* additional rows skipped."""
        return CSVLimiter(self._source, limit=self._limit, offset=self._offset + n)

    def take(self, n: int) -> "CSVLimiter":
        """Return a new CSVLimiter capped at *n* rows (from current offset)."""
        return CSVLimiter(self._source, limit=n, offset=self._offset)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVLimiter(limit={self._limit!r}, offset={self._offset!r}, "
            f"source={self._source!r})"
        )
