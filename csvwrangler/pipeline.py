"""Pipeline – chainable CSV processing."""
from __future__ import annotations

import csv
import io
from typing import Optional

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
from csvwrangler.splitter import CSVSplitter
from csvwrangler.filler import CSVFiller
from csvwrangler.validator import CSVValidator
from csvwrangler.typer import CSVTyper
from csvwrangler.stacker import CSVStacker
from csvwrangler.stringops import CSVStringOps
from csvwrangler.counter import CSVCounter
from csvwrangler.differ import CSVDiffer
from csvwrangler.zipper import CSVZipper
from csvwrangler.transposer import CSVTransposer
from csvwrangler.encoder import CSVEncoder
from csvwrangler.normalizer import CSVNormalizer
from csvwrangler.chunker import CSVChunker
from csvwrangler.interpolator import CSVInterpolator
from csvwrangler.window import CSVWindow
from csvwrangler.profiler import CSVProfiler
from csvwrangler.formatter import CSVFormatter
from csvwrangler.tagger import CSVTagger
from csvwrangler.rounder import CSVRounder
from csvwrangler.clipper import CSVClipper
from csvwrangler.condenser import CSVCondenser
from csvwrangler.scaler import CSVScaler
from csvwrangler.ranker import CSVRanker
from csvwrangler.correlator import CSVCorrelator
from csvwrangler.binner import CSVBinner
from csvwrangler.outlier import CSVOutlier
from csvwrangler.roller import CSVRoller
from csvwrangler.scorer import CSVScorer


class Pipeline:
    def __init__(self, source):
        self._source = source

    # ------------------------------------------------------------------ #
    # Constructors
    # ------------------------------------------------------------------ #
    @classmethod
    def from_file(cls, path: str, encoding: str = "utf-8") -> "Pipeline":
        return cls(CSVReader(path, encoding=encoding))

    @classmethod
    def from_string(cls, text: str) -> "Pipeline":
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        headers = reader.fieldnames or []

        class _Src:
            @property
            def headers(self_):
                return list(headers)

            @property
            def rows(self_):
                return iter(rows)

            @property
            def row_count(self_):
                return len(rows)

        return cls(_Src())

    # ------------------------------------------------------------------ #
    # Terminal operations
    # ------------------------------------------------------------------ #
    def to_file(self, path: str, encoding: str = "utf-8") -> None:
        CSVWriter(self._source).write(path, encoding=encoding)

    def to_string(self) -> str:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self._source.headers)
        writer.writeheader()
        for row in self._source.rows:
            writer.writerow(row)
        return buf.getvalue()

    # ------------------------------------------------------------------ #
    # Internal helper
    # ------------------------------------------------------------------ #
    def _wrap(self, new_source) -> "Pipeline":
        return Pipeline(new_source)

    # ------------------------------------------------------------------ #
    # Transformation methods
    # ------------------------------------------------------------------ #
    def where(self, column: str, operator: str, value: str) -> "Pipeline":
        return self._wrap(CSVFilter(self._source, column, operator, value))

    def select(self, columns: list) -> "Pipeline":
        return self._wrap(CSVTransform(self._source).select(columns))

    def rename(self, mapping: dict) -> "Pipeline":
        t = CSVTransform(self._source)
        for old, new in mapping.items():
            t = t.rename(old, new)
        return self._wrap(t)

    def add_column(self, name: str, expr) -> "Pipeline":
        return self._wrap(CSVTransform(self._source).add_column(name, expr))

    def sort_by(self, column: str, descending: bool = False) -> "Pipeline":
        return self._wrap(CSVSorter(self._source, column, descending=descending))

    def deduplicate(self, columns: Optional[list] = None) -> "Pipeline":
        return self._wrap(CSVDeduplicator(self._source, columns=columns))

    def aggregate(self, group_by: list, aggregations: dict) -> "Pipeline":
        return self._wrap(CSVAggregator(self._source, group_by, aggregations))

    def limit(self, n: int) -> "Pipeline":
        return self._wrap(CSVLimiter(self._source, n))

    def join(self, right, on: str, how: str = "inner") -> "Pipeline":
        return self._wrap(CSVJoiner(self._source, right._source, on=on, how=how))

    def slice(self, start: int, stop: int) -> "Pipeline":
        return self._wrap(CSVSlicer(self._source, start, stop))

    def sample(self, n: int, seed: Optional[int] = None) -> "Pipeline":
        return self._wrap(CSVSampler(self._source, n, seed=seed))

    def unpivot(self, id_columns: list, value_columns: list,
                key_col: str = "key", value_col: str = "value") -> "Pipeline":
        return self._wrap(CSVUnpivot(self._source, id_columns, value_columns,
                                     key_col=key_col, value_col=value_col))

    def pivot(self, index: str, columns: str, values: str,
              agg: str = "first") -> "Pipeline":
        return self._wrap(CSVPivotter(self._source, index, columns, values, agg=agg))

    def rename_columns(self, mapping: dict) -> "Pipeline":
        return self._wrap(CSVRenamer(self._source, mapping))

    def flatten(self, column: str, separator: str = ",") -> "Pipeline":
        return self._wrap(CSVFlattener(self._source, column, separator=separator))

    def cast(self, casts: dict) -> "Pipeline":
        return self._wrap(CSVCaster(self._source, casts))

    def split_by(self, column: str):
        splitter = CSVSplitter(self._source, column)
        return {key: Pipeline(grp) for key, grp in splitter.groups()}

    def fill(self, fills: dict) -> "Pipeline":
        return self._wrap(CSVFiller(self._source, fills))

    def validate(self, rules: dict, mode: str = "drop") -> "Pipeline":
        from csvwrangler.pipeline_validate_patch import _validate
        return _validate(self, rules, mode)

    def detect_types(self) -> dict:
        return CSVTyper(self._source).detected_types

    def stack(self, other: "Pipeline") -> "Pipeline":
        return self._wrap(CSVStacker(self._source, other._source))

    def string_ops(self, ops: dict) -> "Pipeline":
        return self._wrap(CSVStringOps(self._source, ops))

    def count_values(self, column: str, count_col: str = "count") -> "Pipeline":
        return self._wrap(CSVCounter(self._source, column, count_col=count_col))

    def diff(self, other: "Pipeline", key: str, mode: str = "all") -> "Pipeline":
        return self._wrap(CSVDiffer(self._source, other._source, key=key, mode=mode))

    def zip_with(self, other: "Pipeline", suffixes=None) -> "Pipeline":
        return self._wrap(CSVZipper(self._source, other._source, suffixes=suffixes))

    def transpose(self, key_col: str = "field", value_col: str = "value") -> "Pipeline":
        return self._wrap(CSVTransposer(self._source, key_col=key_col, value_col=value_col))

    def encode(self, ops: dict) -> "Pipeline":
        from csvwrangler.pipeline_encode_patch import _patch
        _patch(self.__class__)
        return self._wrap(CSVEncoder(self._source, ops))

    def normalize(self, ops: dict) -> "Pipeline":
        return self._wrap(CSVNormalizer(self._source, ops))

    def chunk(self, size: int) -> list:
        chunker = CSVChunker(self._source, size)
        results = []
        for chunk in chunker.chunks():
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=chunk[0].keys() if chunk else self._source.headers)
            w.writeheader()
            for row in chunk:
                w.writerow(row)
            results.append(buf.getvalue())
        return results

    def interpolate(self, columns: list, method: str = "linear") -> "Pipeline":
        return self._wrap(CSVInterpolator(self._source, columns, method=method))

    def window(self, ops: dict, window_size: int = 3) -> "Pipeline":
        return self._wrap(CSVWindow(self._source, ops, window_size=window_size))

    def profile(self) -> dict:
        return CSVProfiler(self._source).profile

    def format_columns(self, templates: dict) -> "Pipeline":
        return self._wrap(CSVFormatter(self._source, templates))

    def tag_column(self, column: str, name: str, rules: list) -> "Pipeline":
        return self._wrap(CSVTagger(self._source, column, name, rules))

    def round_columns(self, cols: dict) -> "Pipeline":
        return self._wrap(CSVRounder(self._source, cols))

    def clip(self, bounds: dict) -> "Pipeline":
        return self._wrap(CSVClipper(self._source, bounds))

    def condense(self, others: list, separator: str = " ") -> "Pipeline":
        return self._wrap(CSVCondenser(self._source,
                                       [o._source for o in others],
                                       separator=separator))

    def scale(self, columns: list, method: str = "minmax") -> "Pipeline":
        return self._wrap(CSVScaler(self._source, columns, method=method))

    def rank(self, column: str, name: str = "rank", descending: bool = False) -> "Pipeline":
        return self._wrap(CSVRanker(self._source, column, name=name, descending=descending))

    def correlate(self, columns: list) -> dict:
        return CSVCorrelator(self._source, columns).matrix

    def bin(self, column: str, bins: list, labels: list, name: str = "bin") -> "Pipeline":
        return self._wrap(CSVBinner(self._source, column, bins, labels, name=name))

    def flag_outliers(self, columns: list, method: str = "iqr",
                      name: str = "outlier") -> "Pipeline":
        return self._wrap(CSVOutlier(self._source, columns, method=method, name=name))

    def rolling(self, ops: dict, window: int = 3) -> "Pipeline":
        return self._wrap(CSVRoller(self._source, ops, window=window))

    def score(self, weights: dict, name: str = "score") -> "Pipeline":
        from csvwrangler.pipeline_scorer_patch import _patch
        _patch(self.__class__)
        return self._wrap(CSVScorer(self._source, weights, name=name))

    def shift_columns(self, shifts: dict) -> "Pipeline":
        from csvwrangler.pipeline_shifter_patch import _patch
        _patch(self.__class__)
        from csvwrangler.shifter import CSVShifter
        return self._wrap(CSVShifter(self._source, shifts))
