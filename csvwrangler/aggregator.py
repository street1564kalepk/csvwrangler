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

    def _compute_aggregate(self, op: str, src_col: str, group_rows: List[Dict]) -> Any:
        """Compute a single aggregate value for a group of rows.

        :param op: the aggregation operation (count, sum, min, max, mean)
        :param src_col: the source column name to aggregate over
        :param group_rows: the list of row dicts in this group
        :return: the computed aggregate value, or None if not applicable
        """
        if op == "count":
            return len(group_rows)
        values = []
        for r in group_rows:
            try:
                values.append(float(r[src_col]))
            except (ValueError, TypeError):
                pass
        if op == "sum":
            return sum(values)
        elif op == "min":
            return min(values) if values else None
        elif op == "max":
            return max(values) if values else None
        elif op == "mean":
            return sum(values) / len(values) if values else None

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
                result[out_col] = self._compute_aggregate(op, src_col, group_rows)
            yield result

    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
