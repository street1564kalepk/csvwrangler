"""CSVCorrelator – compute pairwise Pearson correlation between numeric columns."""
from __future__ import annotations
from typing import List, Dict, Sequence
import math


class CSVCorrelator:
    """Compute pairwise Pearson correlation coefficients for selected columns."""

    def __init__(self, source, columns: Sequence[str] | None = None):
        self._source = source
        self._columns = list(columns) if columns else []
        self._result: Dict[str, Dict[str, float]] | None = None

    # ------------------------------------------------------------------
    def _coerce(self, value: str) -> float | None:
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _build(self) -> None:
        cols = self._columns or self._source.headers
        data: Dict[str, List[float]] = {c: [] for c in cols}

        for row in self._source.rows():
            for c in cols:
                v = self._coerce(row.get(c, ""))
                if v is not None:
                    data[c].append(v)

        def pearson(xs: List[float], ys: List[float]) -> float:
            n = min(len(xs), len(ys))
            if n < 2:
                return float("nan")
            xs, ys = xs[:n], ys[:n]
            mx = sum(xs) / n
            my = sum(ys) / n
            num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
            dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
            dy = math.sqrt(sum((y - my) ** 2 for y in ys))
            if dx == 0 or dy == 0:
                return float("nan")
            return num / (dx * dy)

        self._result = {}
        for a in cols:
            self._result[a] = {}
            for b in cols:
                self._result[a][b] = pearson(data[a], data[b])

    # ------------------------------------------------------------------
    @property
    def headers(self) -> List[str]:
        return self._source.headers

    @property
    def matrix(self) -> Dict[str, Dict[str, float]]:
        if self._result is None:
            self._build()
        return self._result  # type: ignore[return-value]

    def rows(self):
        """Yield original rows unchanged (correlator is a side-effect op)."""
        yield from self._source.rows()

    def __repr__(self) -> str:  # pragma: no cover
        cols = self._columns or "(all)"
        return f"CSVCorrelator(columns={cols})"
