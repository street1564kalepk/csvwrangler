"""CSVClamper – clamp numeric column values to a [min, max] range."""

from __future__ import annotations

from typing import Iterable, Iterator


class CSVClamper:
    """Clamp one or more numeric columns so every value stays within
    [lower, upper].  Non-numeric cells are left untouched."""

    def __init__(
        self,
        source,
        columns: list[str],
        lower: float | None = None,
        upper: float | None = None,
    ) -> None:
        if lower is None and upper is None:
            raise ValueError("At least one of 'lower' or 'upper' must be set.")
        if lower is not None and upper is not None and lower > upper:
            raise ValueError(f"lower ({lower}) must be <= upper ({upper}).")
        self._source = source
        self._columns = columns
        self._lower = lower
        self._upper = upper

    # ------------------------------------------------------------------
    # public interface
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return self._source.headers

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows)

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            yield self._clamp_row(row)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _clamp_row(self, row: dict) -> dict:
        out = dict(row)
        for col in self._columns:
            if col not in out:
                continue
            try:
                value = float(out[col])
            except (TypeError, ValueError):
                continue
            if self._lower is not None:
                value = max(self._lower, value)
            if self._upper is not None:
                value = min(self._upper, value)
            # Preserve int-like appearance when original was integer
            out[col] = int(value) if value == int(value) else value
        return out
