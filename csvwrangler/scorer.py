"""CSVScorer – score rows using a weighted expression map."""
from __future__ import annotations
from typing import Iterable


class CSVScorer:
    """Add a score column computed as a weighted sum of numeric columns."""

    def __init__(self, source, weights: dict, score_col: str = "score", default: float = 0.0):
        """
        Parameters
        ----------
        source   : any object exposing .headers and .rows
        weights  : {column: weight} mapping
        score_col: name of the new column
        default  : value used when a cell cannot be converted to float
        """
        self._source = source
        self._weights = weights
        self._score_col = score_col
        self._default = default
        unknown = [c for c in weights if c not in source.headers]
        if unknown:
            raise ValueError(f"Unknown columns: {unknown}")
        if score_col in source.headers:
            raise ValueError(f"Column '{score_col}' already exists")

    @property
    def headers(self) -> list[str]:
        return self._source.headers + [self._score_col]

    def _score_row(self, row: dict) -> dict:
        total = 0.0
        for col, weight in self._weights.items():
            try:
                total += float(row[col]) * weight
            except (ValueError, TypeError):
                total += self._default * weight
        return {**row, self._score_col: round(total, 6)}

    def rows(self) -> Iterable[dict]:
        for row in self._source.rows():
            yield self._score_row(row)

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
