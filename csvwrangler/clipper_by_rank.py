"""CSVRankClipper – keep only the top-N or bottom-N rows by a numeric column."""
from __future__ import annotations
from typing import Iterable


class CSVRankClipper:
    """Return the top or bottom *n* rows ranked by *column*.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows()``.
    column:
        Name of the numeric column to rank by.
    n:
        How many rows to keep.
    direction:
        ``'top'`` (default) keeps the highest values;
        ``'bottom'`` keeps the lowest.
    """

    def __init__(
        self,
        source,
        column: str,
        n: int,
        direction: str = "top",
    ) -> None:
        if direction not in ("top", "bottom"):
            raise ValueError("direction must be 'top' or 'bottom'")
        if n < 0:
            raise ValueError("n must be a non-negative integer")
        self._source = source
        self._column = column
        self._n = n
        self._direction = direction

    @property
    def headers(self) -> list[str]:
        return self._source.headers

    def rows(self) -> Iterable[dict]:
        all_rows = list(self._source.rows())
        reverse = self._direction == "top"
        try:
            ranked = sorted(
                all_rows,
                key=lambda r: float(r.get(self._column, 0)),
                reverse=reverse,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Column '{self._column}' contains non-numeric data: {exc}"
            ) from exc
        yield from ranked[: self._n]

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
