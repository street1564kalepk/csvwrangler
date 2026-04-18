"""CSVBinner – bucket numeric column values into labelled bins."""

from __future__ import annotations

from typing import Iterable, Iterator, List, Optional, Tuple


class CSVBinner:
    """Adds a new column that assigns each row to a named bin."""

    def __init__(
        self,
        source,
        column: str,
        bins: List[Tuple[float, float, str]],
        output_column: str = "bin",
        default: str = "other",
    ) -> None:
        """
        Parameters
        ----------
        source       : any object with .headers and .rows
        column       : numeric column to inspect
        bins         : list of (low, high, label) – low inclusive, high exclusive
        output_column: name of the new column appended to each row
        default      : label used when no bin matches
        """
        if not bins:
            raise ValueError("bins must not be empty")
        if not column:
            raise ValueError("column must be a non-empty string")
        self._source = source
        self._column = column
        self._bins = bins
        self._output_column = output_column
        self._default = default

    # ------------------------------------------------------------------
    def headers(self) -> List[str]:
        return list(self._source.headers()) + [self._output_column]

    def _label(self, value: str) -> str:
        try:
            v = float(value)
        except (ValueError, TypeError):
            return self._default
        for low, high, label in self._bins:
            if low <= v < high:
                return label
        return self._default

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            label = self._label(row.get(self._column, ""))
            yield {**row, self._output_column: label}

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVBinner(column={self._column!r}, "
            f"bins={len(self._bins)}, output={self._output_column!r})"
        )
