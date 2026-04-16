"""CSVWindow – compute rolling/window aggregates over a sorted source."""
from __future__ import annotations
from collections import deque
from typing import Iterable

_SUPPORTED = {"sum", "mean", "min", "max", "count"}


class CSVWindow:
    """Adds a new column containing a rolling aggregate over *column*."""

    def __init__(self, source, column: str, size: int, func: str, output: str | None = None):
        if func not in _SUPPORTED:
            raise ValueError(f"func must be one of {_SUPPORTED}, got {func!r}")
        if size < 1:
            raise ValueError("size must be >= 1")
        self._source = source
        self._column = column
        self._size = size
        self._func = func
        self._output = output or f"{column}_{func}_{size}"

    @property
    def headers(self) -> list[str]:
        return list(self._source.headers) + [self._output]

    def _apply(self, window: deque) -> str:
        vals = [float(v) for v in window if v not in ("", None)]
        if not vals:
            return ""
        if self._func == "sum":
            return str(sum(vals))
        if self._func == "mean":
            return str(sum(vals) / len(vals))
        if self._func == "min":
            return str(min(vals))
        if self._func == "max":
            return str(max(vals))
        if self._func == "count":
            return str(len(vals))

    def rows(self) -> Iterable[dict]:
        buf: deque = deque(maxlen=self._size)
        for row in self._source.rows():
            buf.append(row.get(self._column, ""))
            yield {**row, self._output: self._apply(buf)}

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
