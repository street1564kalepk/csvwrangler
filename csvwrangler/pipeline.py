"""Pipeline: chainable builder for CSV transformations."""

from __future__ import annotations

import csv
import io
from typing import Dict, List, Optional

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
from csvwrangler.caster import CSVCaster


class _SelectSource:
    """Thin wrapper that projects a subset of columns."""

    def __init__(self, source, columns: List[str]) -> None:
        missing = [c for c in columns if c not in source.headers]
        if missing:
            raise ValueError(f"Columns not found in source: {missing}")
        self._source = source
        self._columns = columns

    @property
    def headers(self) -> List[str]:
        return list(self._columns)

    def rows(self):
        for row in self._source.rows():
            yield {c: row[c] for c in self._columns}


class Pipeline:
    """Chainable pipeline over a CSV source."""

    def __init__(self, source) -> None:
        self._source = source

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, path: str, encoding: str = "utf-8") -> "Pipeline":
        return cls(CSVReader(path, encoding=encoding))

    @classmethod
    def from_string(cls, text: str) -> "Pipeline":
        reader = csv.DictReader(io.StringIO(text))
        rows_list = list(reader)
        headers = reader.fieldnames or []

        class _InMemory:
            @property
            def headers(self_):
                return list(headers)

            def rows(self_):
                yield from (dict(r) for r in rows_list)

        return cls(_InMemory())

    # ------------------------------------------------------------------
    # Transformation steps
    # ------------------------------------------------------------------

    def select(self, columns: List[str]) -> "Pipeline":
        return Pipeline(_SelectSource(self._source, columns))

    def where(self, column: str, op: str, value: str) -> "Pipeline":
        return Pipeline(CSVFilter(self._source, column, op, value))

    def rename(self, mapping: Dict[str, str]) -> "Pipeline":
        src = self._source
        for old, new in mapping.items():
            src = CSVTransform(src).rename(old, new)
        return Pipeline(src)

    def add_column(self, name: str, expression: str) -> "Pipeline":
        return Pipeline(CSVTransform(self._source).add_column(name, expression))

    def sort(self, column: str, descending: bool = False) -> "Pipeline":
        return Pipeline(CSVSorter(self._source, column, descending=descending))

    def deduplicate(self, columns: Optional[List[str]] = None) -> "Pipeline":
        return Pipeline(CSVDeduplicator(self._source, columns=columns))

    def aggregate(self, group_by: List[str], aggregations: Dict[str, str]) -> "Pipeline":
        return Pipeline(CSVAggregator(self._source, group_by=group_by, aggregations=aggregations))

    def limit(self, n: int) -> "Pipeline":
        return Pipeline(CSVLimiter(self._source, n))

    def slice(self, start: int, stop: int) -> "Pipeline":
        return Pipeline(CSVSlicer(self._source, start, stop))

    def sample(self, n: int, seed: Optional[int] = None) -> "Pipeline":
        return Pipeline(CSVSampler(self._source, n, seed=seed))

    def join(
        self,
        right_path: str,
        on: str,
        how: str = "inner",
        encoding: str = "utf-8",
    ) -> "Pipeline":
        right = CSVReader(right_path, encoding=encoding)
        return Pipeline(CSVJoiner(self._source, right, on=on, how=how))

    def unpivot(
        self,
        id_columns: List[str],
        value_column: str = "value",
        variable_column: str = "variable",
    ) -> "Pipeline":
        return Pipeline(
            CSVUnpivot(
                self._source,
                id_columns=id_columns,
                value_column=value_column,
                variable_column=variable_column,
            )
        )

    def pivot(
        self,
        index: str,
        columns: str,
        values: str,
        aggfunc: str = "first",
    ) -> "Pipeline":
        return Pipeline(
            CSVPivotter(self._source, index=index, columns=columns, values=values, aggfunc=aggfunc)
        )

    def rename_columns(self, mapping: Dict[str, str]) -> "Pipeline":
        return Pipeline(CSVRenamer(self._source, mapping))

    def flatten(self, column: str, separator: str = ",") -> "Pipeline":
        return Pipeline(CSVFlattener(self._source, column=column, separator=separator))

    def cast(self, casts: Dict[str, str], errors: str = "raise") -> "Pipeline":
        """Cast column values to target types.

        Parameters
        ----------
        casts:
            Mapping of column name -> type name (``'int'``, ``'float'``,
            ``'str'``, ``'bool'``).
        errors:
            How to handle conversion failures: ``'raise'`` (default),
            ``'ignore'``, or ``'null'``.
        """
        return Pipeline(CSVCaster(self._source, casts, errors=errors))

    # ------------------------------------------------------------------
    # Terminal steps
    # ------------------------------------------------------------------

    def to_file(self, path: str, encoding: str = "utf-8") -> None:
        CSVWriter(self._source).write(path, encoding=encoding)

    def to_string(self) -> str:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self._source.headers)
        writer.writeheader()
        writer.writerows(self._source.rows())
        return buf.getvalue()

    def collect(self) -> List[dict]:
        return list(self._source.rows())

    @property
    def headers(self) -> List[str]:
        return self._source.headers

    def __repr__(self) -> str:  # pragma: no cover
        return f"Pipeline(headers={self.headers})"
