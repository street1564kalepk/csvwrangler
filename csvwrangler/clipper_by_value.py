"""CSVValueClipper – clamp numeric column values to [min, max] bounds."""

from __future__ import annotations

from typing import Iterable, Iterator


class CSVValueClipper:
    """Clamp values in specified columns to a [min_val, max_val] range.

    Values that cannot be coerced to float are left unchanged.
    Either *min_val* or *max_val* may be ``None`` to indicate no lower / upper
    bound respectively.
    """

    def __init__(
        self,
        source,
        columns: list[str],
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> None:
        if not columns:
            raise ValueError("columns must not be empty")
        if min_val is not None and max_val is not None and min_val > max_val:
            raise ValueError(
                f"min_val ({min_val}) must be <= max_val ({max_val})"
            )
        self._source = source
        self._columns = columns
        self._min = min_val
        self._max = max_val

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return self._source.headers

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows)

    def rows(self) -> Iterator[dict]:
        col_set = set(self._columns)
        for row in self._source.rows():
            yield {
                k: self._clip(v) if k in col_set else v
                for k, v in row.items()
            }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _clip(self, value: str) -> str:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return value
        if self._min is not None and num < self._min:
            num = self._min
        if self._max is not None and num > self._max:
            num = self._max
        # Preserve int-like appearance when possible
        if num == int(num) and "." not in str(value):
            return str(int(num))
        return str(num)
