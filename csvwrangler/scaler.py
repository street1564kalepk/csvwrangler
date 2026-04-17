"""CSVScaler – min-max or z-score scale numeric columns."""
from __future__ import annotations
from typing import Iterable, List, Dict


class CSVScaler:
    """Scale numeric columns using min-max or z-score normalisation."""

    METHODS = ("minmax", "zscore")

    def __init__(self, source, columns: List[str], method: str = "minmax"):
        if method not in self.METHODS:
            raise ValueError(f"method must be one of {self.METHODS}, got {method!r}")
        self._source = source
        self._columns = columns
        self._method = method
        self._stats: Dict[str, Dict] = {}
        self._data: List[Dict] = []
        self._built = False

    def _build(self):
        if self._built:
            return
        self._data = list(self._source.rows())
        for col in self._columns:
            vals = []
            for row in self._data:
                try:
                    vals.append(float(row[col]))
                except (ValueError, KeyError):
                    pass
            if not vals:
                self._stats[col] = {"skip": True}
                continue
            mn, mx = min(vals), max(vals)
            mean = sum(vals) / len(vals)
            variance = sum((v - mean) ** 2 for v in vals) / len(vals)
            std = variance ** 0.5
            self._stats[col] = {"min": mn, "max": mx, "mean": mean, "std": std, "skip": False}
        self._built = True

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def _scale(self, col: str, value: str) -> str:
        try:
            v = float(value)
        except (ValueError, TypeError):
            return value
        s = self._stats.get(col, {"skip": True})
        if s.get("skip"):
            return value
        if self._method == "minmax":
            denom = s["max"] - s["min"]
            scaled = 0.0 if denom == 0 else (v - s["min"]) / denom
        else:  # zscore
            scaled = 0.0 if s["std"] == 0 else (v - s["mean"]) / s["std"]
        return str(round(scaled, 6))

    def rows(self) -> Iterable[Dict]:
        self._build()
        for row in self._data:
            out = dict(row)
            for col in self._columns:
                if col in out:
                    out[col] = self._scale(col, out[col])
            yield out

    @property
    def row_count(self) -> int:
        self._build()
        return len(self._data)
