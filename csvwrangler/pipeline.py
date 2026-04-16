"""Pipeline — chainable builder for CSV transformations."""
from __future__ import annotations
from csvwrangler.writer import CSVWriter


class Pipeline:
    def __init__(self, source):
        self._source = source

    # ------------------------------------------------------------------ output
    def to_file(self, path: str) -> None:
        CSVWriter(self._source).write(path)

    def to_string(self) -> str:
        import io
        buf = io.StringIO()
        CSVWriter(self._source)._write_to(buf)
        return buf.getvalue()

    # ----------------------------------------------------------------- helpers
    def _wrap(self, source) -> "Pipeline":
        return Pipeline(source)

    # ------------------------------------------------------------------ stages
    def filter(self, column: str, op: str, value: str) -> "Pipeline":
        from csvwrangler.filter import CSVFilter
        return self._wrap(CSVFilter(self._source, column, op, value))

    def select(self, columns: list[str]) -> "Pipeline":
        from csvwrangler.transform import CSVTransform
        return self._wrap(CSVTransform(self._source, select=columns))

    def rename(self, old: str, new: str) -> "Pipeline":
        from csvwrangler.transform import CSVTransform
        return self._wrap(CSVTransform(self._source, rename={old: new}))

    def add_column(self, name: str, expr: str) -> "Pipeline":
        from csvwrangler.transform import CSVTransform
        return self._wrap(CSVTransform(self._source, add_column=(name, expr)))

    def sort(self, column: str, descending: bool = False) -> "Pipeline":
        from csvwrangler.sorter import CSVSorter
        return self._wrap(CSVSorter(self._source, column, descending=descending))

    def dedup(self, columns: list[str] | None = None) -> "Pipeline":
        from csvwrangler.deduplicator import CSVDeduplicator
        return self._wrap(CSVDeduplicator(self._source, columns=columns))

    def aggregate(self, group_by: list[str], aggregations: list[tuple]) -> "Pipeline":
        from csvwrangler.aggregator import CSVAggregator
        return self._wrap(CSVAggregator(self._source, group_by=group_by, aggregations=aggregations))

    def limit(self, n: int) -> "Pipeline":
        from csvwrangler.limiter import CSVLimiter
        return self._wrap(CSVLimiter(self._source, n))

    def join(self, right, on: str, how: str = "inner") -> "Pipeline":
        from csvwrangler.joiner import CSVJoiner
        return self._wrap(CSVJoiner(self._source, right, on=on, how=how))

    def slice(self, start: int, end: int) -> "Pipeline":
        from csvwrangler.slicer import CSVSlicer
        return self._wrap(CSVSlicer(self._source, start, end))

    def sample(self, n: int, seed: int | None = None) -> "Pipeline":
        from csvwrangler.sampler import CSVSampler
        return self._wrap(CSVSampler(self._source, n, seed=seed))

    def unpivot(self, id_cols: list[str], value_col: str = "value", var_col: str = "variable") -> "Pipeline":
        from csvwrangler.unpivot import CSVUnpivot
        return self._wrap(CSVUnpivot(self._source, id_cols=id_cols, value_col=value_col, var_col=var_col))

    def pivot(self, index: str, columns: str, values: str) -> "Pipeline":
        from csvwrangler.pivotter import CSVPivotter
        return self._wrap(CSVPivotter(self._source, index=index, columns=columns, values=values))

    def flatten(self, column: str, delimiter: str = "|") -> "Pipeline":
        from csvwrangler.flattener import CSVFlattener
        return self._wrap(CSVFlattener(self._source, column=column, delimiter=delimiter))

    def cast(self, castings: dict) -> "Pipeline":
        from csvwrangler.caster import CSVCaster
        return self._wrap(CSVCaster(self._source, castings=castings))

    def split(self, column: str) -> "Pipeline":
        from csvwrangler.splitter import CSVSplitter
        return self._wrap(CSVSplitter(self._source, column=column))

    def fill(self, fills: dict) -> "Pipeline":
        from csvwrangler.filler import CSVFiller
        return self._wrap(CSVFiller(self._source, fills=fills))

    def validate(self, rules: list[tuple], mode: str = "drop") -> "Pipeline":
        from csvwrangler.pipeline_validate_patch import _validate
        return self._wrap(_validate(self._source, rules=rules, mode=mode))

    def stringops(self, ops: list[tuple]) -> "Pipeline":
        from csvwrangler.stringops import CSVStringOps
        return self._wrap(CSVStringOps(self._source, ops=ops))

    def diff(self, right, key: str, mode: str = "all") -> "Pipeline":
        from csvwrangler.differ import CSVDiffer
        return self._wrap(CSVDiffer(self._source, right, key=key, mode=mode))
