"""CSVLengthClipper – keep only rows where a column value's length satisfies a condition."""

from __future__ import annotations

from typing import Iterable, Iterator

_OPS = {
    "eq": lambda a, b: a == b,
    "ne": lambda a, b: a != b,
    "lt": lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
    "gt": lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
}


class CSVLengthClipper:
    """Filter rows by the character length of a given column's value.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows``.
    column:
        Name of the column to measure.
    op:
        Comparison operator string: 'eq', 'ne', 'lt', 'lte', 'gt', 'gte'.
    length:
        Integer length to compare against.
    """

    def __init__(self, source, column: str, op: str, length: int) -> None:
        if op not in _OPS:
            raise ValueError(f"Unknown op {op!r}. Choose from {sorted(_OPS)}.")
        self._source = source
        self._column = column
        self._op = op
        self._length = length
        self._cmp = _OPS[op]

    @property
    def headers(self) -> list[str]:
        return self._source.headers

    def rows(self) -> Iterator[dict]:
        col = self._column
        cmp = self._cmp
        length = self._length
        for row in self._source.rows():
            val = row.get(col, "")
            if cmp(len(str(val)), length):
                yield row

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
