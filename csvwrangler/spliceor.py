"""CSVSpliceor – insert rows from a second source at a given position."""
from typing import Iterable


class CSVSpliceor:
    """Insert rows from *other* after *after_row* (0-based index, -1 = append)."""

    def __init__(self, source, other, after_row: int = -1):
        self._source = source
        self._other = other
        self._after = after_row

    # ------------------------------------------------------------------
    def headers(self) -> list[str]:
        return self._source.headers()

    # ------------------------------------------------------------------
    def rows(self) -> Iterable[dict]:
        src_headers = self._source.headers()
        other_headers = self._other.headers()

        def _normalise(row: dict) -> dict:
            """Return row with only src_headers keys; missing keys become ''."""
            return {h: row.get(h, "") for h in src_headers}

        after = self._after
        emitted = 0
        inserted = False

        for row in self._source.rows():
            yield row
            emitted += 1
            if not inserted and emitted == after + 1 and after >= 0:
                for other_row in self._other.rows():
                    yield _normalise(other_row)
                inserted = True

        if not inserted:
            for other_row in self._other.rows():
                yield _normalise(other_row)

    # ------------------------------------------------------------------
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVSpliceor(after_row={self._after})"
