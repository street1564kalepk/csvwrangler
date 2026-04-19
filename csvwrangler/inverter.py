"""CSVInverter – negate a boolean-like column (or all columns).

Truthy values ('1', 'true', 'yes') become '0' / falsy become '1'.
"""

from __future__ import annotations
from typing import Iterable, Iterator

_TRUTHY = {"1", "true", "yes", "y", "on"}


class CSVInverter:
    """Invert boolean-like string values in the specified columns."""

    def __init__(self, source, columns: list[str]) -> None:
        if not columns:
            raise ValueError("columns must not be empty")
        self._source = source
        self._columns = columns

    # ------------------------------------------------------------------
    def headers(self) -> list[str]:
        return self._source.headers()

    def rows(self) -> Iterator[dict]:
        cols = set(self._columns)
        hdrs = self.headers()
        unknown = cols - set(hdrs)
        if unknown:
            raise ValueError(f"Unknown columns: {unknown}")
        for row in self._source.rows():
            out = dict(row)
            for col in cols:
                val = out.get(col, "")
                out[col] = "0" if val.strip().lower() in _TRUTHY else "1"
            yield out

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
