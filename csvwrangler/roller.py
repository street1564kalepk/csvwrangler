"""CSVRoller – compute rolling/moving aggregates over a numeric column."""

from collections import deque


class CSVRoller:
    """Attach a rolling aggregate column to a source.

    Parameters
    ----------
    source   : any object with .headers and .rows
    column   : name of the numeric column to aggregate
    window   : number of rows in the rolling window
    agg      : one of 'mean', 'sum', 'min', 'max'
    out_col  : name of the new output column (default: '<column>_rolling_<agg>')
    """

    _SUPPORTED = frozenset({"mean", "sum", "min", "max"})

    def __init__(self, source, column: str, window: int, agg: str = "mean", out_col: str = ""):
        if column not in source.headers:
            raise ValueError(f"Column '{column}' not found in source headers.")
        if window < 1:
            raise ValueError("window must be >= 1.")
        if agg not in self._SUPPORTED:
            raise ValueError(f"agg must be one of {sorted(self._SUPPORTED)}.")

        self._source = source
        self._column = column
        self._window = window
        self._agg = agg
        self._out_col = out_col or f"{column}_rolling_{agg}"

    @property
    def headers(self):
        return self._source.headers + [self._out_col]

    def _compute(self, buf):
        vals = list(buf)
        if self._agg == "mean":
            return round(sum(vals) / len(vals), 10)
        if self._agg == "sum":
            return sum(vals)
        if self._agg == "min":
            return min(vals)
        return max(vals)

    def rows(self):
        buf = deque(maxlen=self._window)
        for row in self._source.rows():
            try:
                val = float(row[self._column])
            except (ValueError, TypeError):
                val = None

            if val is not None:
                buf.append(val)

            result = self._compute(buf) if buf else ""
            yield {**row, self._out_col: result}

    @property
    def row_count(self):
        return sum(1 for _ in self.rows())
