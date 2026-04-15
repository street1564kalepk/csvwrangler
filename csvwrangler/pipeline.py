"""Pipeline: chainable CSV processing."""
from __future__ import annotations

import csv
import io
from typing import List, Optional

from csvwrangler.reader import CSVReader
from csvwrangler.writer import CSVWriter
from csvwrangler.filter import CSVFilter
from csvwrangler.transform import CSVTransform
from csvwrangler.sorter import CSVSorter
from csvwrangler.deduplicator import CSVDeduplicator
from csvwrangler.aggregator import CSVAggregator
from csvwrangler.limiter import CSVLimiter
from csvwrangler.joiner import CSVJoiner
from csvwrangler.slicer import CSVSlicer
from csvwrangler.sampler import CSVSampler
from csvwrangler.unpivot import CSVUnpivot


class Pipeline:
    """Chainable wrapper around CSV source + processing stages."""

    def __init__(self, source) -> None:
        self._source = source

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, path: str, encoding: str = "utf-8") -> "Pipeline":
        return cls(CSVReader(path, encoding=encoding))

    # ------------------------------------------------------------------
    # Transformation stages
    # ------------------------------------------------------------------

    def select(self, columns: List[str]) -> "Pipeline":
        return Pipeline(CSVTransform(self._source).select(columns))

    def where(self, column: str, op: str, value: str) -> "Pipeline":
        return Pipeline(CSVFilter(self._source, column, op, value))

    def rename(self, old: str, new: str) -> "Pipeline":
        return Pipeline(CSVTransform(self._source).rename(old, new))

    def add_column(self, name: str, expr) -> "Pipeline":
        return Pipeline(CSVTransform(self._source).add_column(name, expr))

    def sort(self, column: str, ascending: bool = True) -> "Pipeline":
        return Pipeline(CSVSorter(self._source, column, ascending=ascending))

    def deduplicate(self, columns: Optional[List[str]] = None) -> "Pipeline":
        return Pipeline(CSVDeduplicator(self._source, columns=columns))

    def aggregate(self, group_by: List[str], aggregations: dict) -> "Pipeline":
        return Pipeline(CSVAggregator(self._source, group_by=group_by, aggregations=aggregations))

    def limit(self, n: int) -> "Pipeline":
        return Pipeline(CSVLimiter(self._source, n))

    def slice(self, start: int, stop: int) -> "Pipeline":
        return Pipeline(CSVSlicer(self._source, start, stop))

    def sample(self, n: int, seed: Optional[int] = None) -> "Pipeline":
        return Pipeline(CSVSampler(self._source, n, seed=seed))

    def join(
        self,
        right_source,
        on: str,
        how: str = "inner",
        right_on: Optional[str] = None,
    ) -> "Pipeline":
        return Pipeline(CSVJoiner(self._source, right_source, on=on, how=how, right_on=right_on))

    def unpivot(
        self,
        id_cols: List[str],
        value_cols: List[str],
        var_name: str = "variable",
        value_name: str = "value",
    ) -> "Pipeline":
        return Pipeline(
            CSVUnpivot(
                self._source,
                id_cols=id_cols,
                value_cols=value_cols,
                var_name=var_name,
                value_name=value_name,
            )
        )

    # ------------------------------------------------------------------
    # Terminal operations
    # ------------------------------------------------------------------

    def to_file(self, path: str, encoding: str = "utf-8") -> None:
        writer = CSVWriter(self._source)
        writer.write(path, encoding=encoding)

    def to_string(self, buf: Optional[io.StringIO] = None) -> str:
        buf = buf or io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self._source.headers)
        writer.writeheader()
        for row in self._source.rows():
            writer.writerow(row)
        return buf.getvalue()

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def __repr__(self) -> str:  # pragma: no cover
        return f"Pipeline(source={self._source!r})"
