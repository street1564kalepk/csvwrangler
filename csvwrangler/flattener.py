"""CSVFlattener – expand a delimited multi-value column into one row per value."""
from __future__ import annotations

from typing import Iterable, Iterator


class CSVFlattener:
    """Expand rows by splitting a multi-value column on a delimiter.

    Each original row is replaced by N rows (one per split value).  All other
    columns are copied verbatim.  The target column contains the individual
    split value.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` (list[str]) and ``.rows()``
        (Iterable[dict]).
    column:
        Name of the column to flatten.
    delimiter:
        String used to split the column value (default ``"|"``).
    strip:
        Whether to strip whitespace from each split part (default ``True``).
    """

    def __init__(
        self,
        source,
        column: str,
        delimiter: str = "|",
        strip: bool = True,
    ) -> None:
        if column not in source.headers:
            raise ValueError(
                f"Column '{column}' not found. Available: {source.headers}"
            )
        self._source = source
        self._column = column
        self._delimiter = delimiter
        self._strip = strip

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers)

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            raw = row.get(self._column, "")
            parts = raw.split(self._delimiter)
            for part in parts:
                value = part.strip() if self._strip else part
                yield {**row, self._column: value}

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVFlattener(column={self._column!r}, "
            f"delimiter={self._delimiter!r})"
        )
