"""CSVSwapper – swap the values of two columns row-by-row."""

from __future__ import annotations
from typing import Iterator


class CSVSwapper:
    """Swap the contents of two named columns."""

    def __init__(self, source, col_a: str, col_b: str) -> None:
        self._source = source
        self._col_a = col_a
        self._col_b = col_b
        self._validate()

    def _validate(self) -> None:
        hdrs = self._source.headers
        if self._col_a not in hdrs:
            raise ValueError(f"Column '{self._col_a}' not found in headers.")
        if self._col_b not in hdrs:
            raise ValueError(f"Column '{self._col_b}' not found in headers.")
        if self._col_a == self._col_b:
            raise ValueError("col_a and col_b must be different columns.")

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers)

    def rows(self) -> Iterator[dict]:
        a, b = self._col_a, self._col_b
        for row in self._source.rows():
            new_row = dict(row)
            new_row[a], new_row[b] = row[b], row[a]
            yield new_row

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
