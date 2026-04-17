"""CSVRounder – round numeric columns to a given number of decimal places."""
from __future__ import annotations
from typing import Iterable


class CSVRounder:
    """Round specified numeric columns to *decimals* places.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows()``.
    columns:
        Mapping of column name -> number of decimal places.
        Unknown columns are silently ignored.
    """

    def __init__(self, source, columns: dict[str, int]) -> None:
        if not columns:
            raise ValueError("columns mapping must not be empty")
        unknown = set(columns) - set(source.headers)
        if unknown:
            raise ValueError(f"Unknown columns: {sorted(unknown)}")
        self._source = source
        self._columns = columns

    # ------------------------------------------------------------------
    @property
    def headers(self) -> list[str]:
        return self._source.headers

    def rows(self) -> Iterable[dict]:
        for row in self._source.rows():
            out = dict(row)
            for col, places in self._columns.items():
                raw = out.get(col)
                if raw is None or raw == "":
                    continue
                try:
                    out[col] = round(float(raw), places)
                except (ValueError, TypeError):
                    pass  # leave non-numeric values untouched
            yield out

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVRounder(columns={self._columns!r})"
