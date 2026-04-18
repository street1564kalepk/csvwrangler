"""CSVReorderer – reorder columns in a CSV source."""
from __future__ import annotations
from typing import Iterable, Iterator


class CSVReorderer:
    """Reorder columns, optionally dropping unlisted ones."""

    def __init__(self, source, order: list[str], drop_rest: bool = False):
        src_headers = list(source.headers)
        unknown = [c for c in order if c not in src_headers]
        if unknown:
            raise ValueError(f"Unknown columns: {unknown}")
        self._source = source
        self._order = order
        self._drop_rest = drop_rest
        if drop_rest:
            self._headers = order
        else:
            rest = [h for h in src_headers if h not in order]
            self._headers = order + rest

    @property
    def headers(self) -> list[str]:
        return list(self._headers)

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            yield {col: row.get(col, "") for col in self._headers}

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVReorderer(order={self._order}, drop_rest={self._drop_rest})"
