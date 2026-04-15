from typing import Iterable, Dict, Any, List, Optional
from collections import defaultdict


class CSVAggregator:
    """
    Groups rows by one or more columns and computes aggregate statistics
    (count, sum, min, max, mean) over numeric columns.
    """

    SUPPORTED_OPS = {"count", "sum", "min", "max", "mean"}

    def __init__(self, source, group_by: List[str], aggregations: Dict[str, str]):
        """
        :param source: any object with .headers and .rows
        :param group_by: list of column names to group by
        :param aggregations: dict mapping output_column -> "op:source_column"
                             e.g. {"total_sales": "sum:amount", "num_orders": "count:id"}
        """
        self._source = source
        self._group_by = group_by
        self._aggregations = aggregations
        self._validate()

    def _validate(self):
        src_headers = set(self._source.headers)
        for col in self._group_by:
            if col not in src_headers:
                raise ValueError(f"Group-by column '{col}' not in source headers")
        for out_col, spec in self._aggregations.items():
            parts = spec.split(":", 1)
            if len(parts) != 2:
                raise ValueError(f"Aggregation spec '{spec}' must be 'op:column'")
            op, col = parts
            if op not in self.SUPPORTED_OPS:
                raise ValueError(f"Unsupported aggregation op '{op}'")
            if col not in src_headers:
                raise ValueError(f"Aggregation column '{col}' not in source headers")

    @property
    def headers(self) -> List[str]:
        agg_cols = list(self._aggregations.keys())
        return self._group_by + agg_cols

    def rows(self) -> Iterable[Dict[str, Any]]:
        buckets: Dict[tuple, List[Dict]] = defaultdict(list)
        for row in self._source.rows():
            key = tuple(row[c] for c in self._group_by)
            buckets[key].append(row)

        for key, group_rows in buckets.items():
            result = {col: val for col, val in zip(self._group_by, key)}
            for out_col, spec in self._aggregations.items():
                op, src_col = spec.split(":", 1)
                values = []
                for r in group_rows:
                    try:
                        values.append(float(r[src_col]))
                    except (ValueError, TypeError):
                        pass
                if op == "count":
                    result[out_col] = len(group_rows)
                elif op == "sum":
                    result[out_col] = sum(values)
                elif op == "min":
                    result[out_col] = min(values) if values else None
                elif op == "max":
                    result[out_col] = max(values) if values else None
                elif op == "mean":
                    result[out_col] = sum(values) / len(values) if values else None
            yield result

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
