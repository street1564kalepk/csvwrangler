"""Pipeline – chainable builder that wires CSV processing stages together."""
from __future__ import annotations

import csv
import io
from typing import Callable

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
from csvwrangler.pivotter import CSVPivotter
from csvwrangler.renamer import CSVRenamer
from csvwrangler.flattener import CSVFlattener


class _SelectSource:
    """Thin wrapper that projects a subset of columns."""

    def __init__(self, source, columns: list[str]) -> None:
        missing = [c for c in columns if c not in source.headers]
        if missing:
            raise ValueError(f"Columns not found: {missing}")
        self._source = source
        self._columns = columns

    @property
    def headers(self) -> list[str]:
        return list(self._columns)

    def rows(self):
        for row in self._source.rows():
            yield {c: row[c] for c in self._columns}


class Pipeline:
    """Chainable pipeline for CSV transformations."""

    def __init__(self, source) -> None:
        self._source = source

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, file_or_path) -> "Pipeline":
        return cls(CSVReader(file_or_path))

    # ------------------------------------------------------------------
    # Transformation steps
    # ------------------------------------------------------------------

    def select(self, *columns: str) -> "Pipeline":
        return Pipeline(_SelectSource(self._source, list(columns)))

    def where(self, column: str, op: str, value: str) -> "Pipeline":
        return Pipeline(CSVFilter(self._source, column, op, value))

    def rename(self, old: str, new: str) -> "Pipeline":
        return Pipeline(CSVTransform(self._source).rename(old, new))

    def add_column(self, name: str, fn: Callable[[dict], str]) -> "Pipeline":
        return Pipeline(CSVTransform(self._source).add_column(name, fn))

    def sort(self, column: str, descending: bool = False) -> "Pipeline":
        return Pipeline(CSVSorter(self._source, column, descending=descending))

    def deduplicate(self, *columns: str) -> "Pipeline":
        cols = list(columns) if columns else None
        return Pipeline(CSVDeduplicator(self._source, columns=cols))

    def aggregate(self, group_by: list[str], aggregations: list[tuple]) -> "Pipeline":
        return Pipeline(CSVAggregator(self._source, group_by=group_by, aggregations=aggregations))

    def limit(self, n: int) -> "Pipeline":
        return Pipeline(CSVLimiter(self._source, n))

    def join(self, right, on: str, how: str = "inner") -> "Pipeline":
        return Pipeline(CSVJoiner(self._source, right, on=on, how=how))

    def slice(self, start: int = 0, stop: int | None = None) -> "Pipeline":
        return Pipeline(CSVSlicer(self._source, start=start, stop=stop))

    def sample(self, n: int, seed: int | None = None) -> "Pipeline":
        return Pipeline(CSVSampler(self._source, n=n, seed=seed))

    def unpivot(self, id_columns: list[str], value_columns: list[str],
                variable_name: str = "variable", value_name: str = "value") -> "Pipeline":
        return Pipeline(CSVUnpivot(
            self._source,
            id_columns=id_columns,
            value_columns=value_columns,
            variable_name=variable_name,
            value_name=value_name,
        ))

    def pivot(self, index: str, column: str, value: str,
              agg: str = "first") -> "Pipeline":
        return Pipeline(CSVPivotter(self._source, index=index, column=column,
                                    value=value, agg=agg))

    def rename_columns(self, mapping: dict[str, str]) -> "Pipeline":
        return Pipeline(CSVRenamer(self._source, mapping=mapping))

    def flatten(self, column: str, delimiter: str = "|", strip: bool = True) -> "Pipeline":
        """Expand a delimited multi-value column into one row per value."""
        return Pipeline(CSVFlattener(self._source, column=column,
                                     delimiter=delimiter, strip=strip))

    # ------------------------------------------------------------------
    # Terminal step
    # ------------------------------------------------------------------

    def to_file(self, file_or_path) -> None:
        writer = CSVWriter(self._source)
        writer.write(file_or_path)

    def to_string(self) -> str:
        buf = io.StringIO()
        self.to_file(buf)
        buf.seek(0)
        return buf.read()

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def headers(self) -> list[str]:
        return self._source.headers

    def __repr__(self) -> str:  # pragma: no cover
        return f"Pipeline(source={self._source!r})"
