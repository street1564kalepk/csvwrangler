"""CSVGrouper – group rows by a column and emit aggregate rows."""
from collections import defaultdict
from typing import Iterator


class CSVGrouper:
    """Group source rows by *group_col* and compute per-group aggregates.

    Parameters
    ----------
    source      : any object with .headers and .rows
    group_col   : column to group by
    agg_col     : column to aggregate
    agg_func    : one of 'sum', 'count', 'min', 'max', 'mean'
    out_col     : name for the result column (default: '<agg_func>_<agg_col>')
    """

    _SUPPORTED = {"sum", "count", "min", "max", "mean"}

    def __init__(self, source, group_col: str, agg_col: str,
                 agg_func: str = "count", out_col: str | None = None):
        if agg_func not in self._SUPPORTED:
            raise ValueError(f"agg_func must be one of {self._SUPPORTED}")
        if group_col not in source.headers:
            raise ValueError(f"group_col '{group_col}' not in headers")
        if agg_col not in source.headers:
            raise ValueError(f"agg_col '{agg_col}' not in headers")
        self._source = source
        self._group_col = group_col
        self._agg_col = agg_col
        self._agg_func = agg_func
        self._out_col = out_col or f"{agg_func}_{agg_col}"

    @property
    def headers(self) -> list[str]:
        return [self._group_col, self._out_col]

    def _compute(self, buckets: dict) -> dict:
        result = {}
        for key, vals in buckets.items():
            nums = []
            for v in vals:
                try:
                    nums.append(float(v))
                except (ValueError, TypeError):
                    pass
            if self._agg_func == "count":
                result[key] = len(vals)
            elif self._agg_func == "sum":
                result[key] = sum(nums)
            elif self._agg_func == "min":
                result[key] = min(nums) if nums else ""
            elif self._agg_func == "max":
                result[key] = max(nums) if nums else ""
            elif self._agg_func == "mean":
                result[key] = (sum(nums) / len(nums)) if nums else ""
        return result

    def rows(self) -> Iterator[dict]:
        buckets: dict = defaultdict(list)
        for row in self._source.rows():
            buckets[row[self._group_col]].append(row[self._agg_col])
        computed = self._compute(buckets)
        for key, val in computed.items():
            yield {self._group_col: key, self._out_col: val}

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
