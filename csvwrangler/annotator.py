"""CSVAnnotator – add a literal constant column to every row."""
from __future__ import annotations
from typing import Iterator


class CSVAnnotator:
    """Append a fixed-value column to each row.

    Parameters
    ----------
    source      : any object exposing .headers and .rows()
    column      : name of the new column
    value       : constant value (stored as a string)
    overwrite   : if True, replace an existing column; default False
    """

    def __init__(self, source, column: str, value: str, overwrite: bool = False):
        if not column:
            raise ValueError("column name must not be empty")
        if not overwrite and column in source.headers:
            raise ValueError(f"column '{column}' already exists; pass overwrite=True to replace it")
        self._source = source
        self._column = column
        self._value = str(value)
        self._overwrite = overwrite

    # ------------------------------------------------------------------
    @property
    def headers(self) -> list[str]:
        hdrs = list(self._source.headers)
        if self._overwrite and self._column in hdrs:
            return hdrs          # column already present – keep order
        return hdrs + [self._column]

    def rows(self) -> Iterator[dict]:
        col = self._column
        val = self._value
        for row in self._source.rows():
            out = dict(row)
            out[col] = val
            yield out

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVAnnotator(column={self._column!r}, value={self._value!r})"
