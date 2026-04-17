"""CSVClipper — clamp numeric column values to [min, max] bounds."""
from __future__ import annotations
from typing import Iterable, Iterator


class CSVClipper:
    """Clamp one or more numeric columns to specified min/max bounds.

    Parameters
    ----------
    source:
        Any object exposing ``.headers`` and ``.rows()``.
    rules:
        Mapping of ``column -> (min_val, max_val)``.  Either bound may be
        ``None`` to leave that side unconstrained.
    """

    def __init__(self, source, rules: dict[str, tuple[float | None, float | None]]) -> None:
        unknown = [c for c in rules if c not in source.headers]
        if unknown:
            raise ValueError(f"CSVClipper: unknown columns {unknown}")
        self._source = source
        self._rules = rules

    # ------------------------------------------------------------------
    @property
    def headers(self) -> list[str]:
        return self._source.headers

    # ------------------------------------------------------------------
    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            yield self._clip_row(row)

    # ------------------------------------------------------------------
    def _clip_row(self, row: dict) -> dict:
        out = dict(row)
        for col, (lo, hi) in self._rules.items():
            raw = out.get(col, "")
            try:
                val = float(raw)
            except (TypeError, ValueError):
                continue  # leave non-numeric values untouched
            if lo is not None and val < lo:
                val = lo
            if hi is not None and val > hi:
                val = hi
            # preserve int-like appearance when possible
            out[col] = int(val) if float(val) == int(val) else val
        return out

    # ------------------------------------------------------------------
    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return f"CSVClipper(rules={self._rules})"
