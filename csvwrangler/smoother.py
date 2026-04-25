"""CSVSmoother – apply a moving-average (or median) smoothing to numeric columns."""
from __future__ import annotations

from collections import deque
from typing import Iterable, List, Optional


class CSVSmoother:
    """Smooth numeric columns using a rolling window average or median.

    Parameters
    ----------
    source:
        Any object exposing ``headers`` (list[str]) and ``rows`` (iterable of dicts).
    columns:
        Column names to smooth.  Must contain numeric-parseable values.
    window:
        Size of the rolling window (must be >= 1).
    method:
        ``'mean'`` (default) or ``'median'``.
    target_suffix:
        If given, smoothed values are written to ``<col><target_suffix>`` instead
        of overwriting the original column.
    """

    def __init__(
        self,
        source,
        columns: List[str],
        window: int = 3,
        method: str = "mean",
        target_suffix: Optional[str] = None,
    ) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        if method not in ("mean", "median"):
            raise ValueError("method must be 'mean' or 'median'")
        self._source = source
        self._columns = columns
        self._window = window
        self._method = method
        self._suffix = target_suffix

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def headers(self) -> List[str]:
        hdrs = list(self._source.headers)
        if self._suffix:
            for col in self._columns:
                target = f"{col}{self._suffix}"
                if target not in hdrs:
                    hdrs.append(target)
        return hdrs

    def rows(self) -> Iterable[dict]:
        buffers: dict[str, deque] = {col: deque(maxlen=self._window) for col in self._columns}
        for row in self._source.rows():
            out = dict(row)
            for col in self._columns:
                try:
                    val = float(row.get(col, ""))
                except (ValueError, TypeError):
                    val = None
                buf = buffers[col]
                if val is not None:
                    buf.append(val)
                smoothed = self._aggregate(list(buf)) if buf else ""
                target = f"{col}{self._suffix}" if self._suffix else col
                out[target] = "" if smoothed == "" else str(round(smoothed, 10)).rstrip("0").rstrip(".")
            yield out

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _aggregate(self, values: list) -> float:
        if self._method == "mean":
            return sum(values) / len(values)
        # median
        s = sorted(values)
        n = len(s)
        mid = n // 2
        return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2
