"""CSVCrosser – produce a cross (cartesian) product of two sources."""
from __future__ import annotations
from typing import Iterator


class CSVCrosser:
    """Yield every combination of rows from *left* and *right* sources."""

    def __init__(self, left, right, left_prefix: str = "left_", right_prefix: str = "right_"):
        self._left = left
        self._right = right
        self._left_prefix = left_prefix
        self._right_prefix = right_prefix

    # ------------------------------------------------------------------
    def headers(self) -> list[str]:
        lh = [f"{self._left_prefix}{h}" for h in self._left.headers()]
        rh = [f"{self._right_prefix}{h}" for h in self._right.headers()]
        return lh + rh

    def rows(self) -> Iterator[dict]:
        right_rows = list(self._right.rows())
        lh = self._left.headers()
        rh = self._right.headers()
        for lrow in self._left.rows():
            for rrow in right_rows:
                combined = {}
                for h in lh:
                    combined[f"{self._left_prefix}{h}"] = lrow[h]
                for h in rh:
                    combined[f"{self._right_prefix}{h}"] = rrow[h]
                yield combined

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
