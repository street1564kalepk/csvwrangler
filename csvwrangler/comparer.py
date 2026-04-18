"""CSVComparer – side-by-side column comparison between two sources."""
from __future__ import annotations
from typing import Iterable


class CSVComparer:
    """Compare values of matching columns across two CSV sources row-by-row.

    Produces rows with: row_index, column, left_value, right_value, match.
    """

    def __init__(self, left, right, columns: list[str] | None = None):
        self._left = left
        self._right = right
        self._columns = columns  # None means all common columns

    def headers(self) -> list[str]:
        return ["row_index", "column", "left_value", "right_value", "match"]

    def _resolve_columns(self) -> list[str]:
        if self._columns is not None:
            return self._columns
        left_h = set(self._left.headers())
        right_h = set(self._right.headers())
        common = left_h & right_h
        # preserve left order
        return [h for h in self._left.headers() if h in common]

    def rows(self) -> Iterable[dict]:
        cols = self._resolve_columns()
        for idx, (lrow, rrow) in enumerate(zip(self._left.rows(), self._right.rows()), start=1):
            for col in cols:
                lv = lrow.get(col, "")
                rv = rrow.get(col, "")
                yield {
                    "row_index": str(idx),
                    "column": col,
                    "left_value": lv,
                    "right_value": rv,
                    "match": "true" if lv == rv else "false",
                }

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def mismatches(self) -> Iterable[dict]:
        return (r for r in self.rows() if r["match"] == "false")

    def mismatch_count(self) -> int:
        return sum(1 for _ in self.mismatches())
