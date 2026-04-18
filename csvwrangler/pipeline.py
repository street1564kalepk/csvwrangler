"""Pipeline – chainable CSV processing."""
from __future__ import annotations
import csv
import io
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

    # ------------------------------------------------------------------ output
    def to_file(self, path: str) -> None:
        from csvwrangler.writer import CSVWriter
        CSVWriter(self._source).write(path)

    def to_string(self) -> str:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self._source.headers, lineterminator="\n")
        writer.writeheader()
        for row in self._source.rows():
            writer.writerow(row)
        return buf.getvalue()

    def _wrap(self, new_source):
        return Pipeline(new_source)

    # ------------------------------------------------------------------ ops
    def where(self, column, op, value):
        return self._wrap(CSVFilter(self._source, column, op, value))

    def select(self, columns):
        return self._wrap(CSVTransform(self._source).select(columns))

    def rename(self, old, new):
        return self._wrap(CSVTransform(self._source).rename(old, new))

    def add_column(self, name, expr):
        return self._wrap(CSVTransform(self._source).add_column(name, expr))

    def sort(self, column, descending=False):
        return self._wrap(CSVSorter(self._source, column, descending=descending))

    def deduplicate(self, columns=None):
        return self._wrap(CSVDeduplicator(self._source, columns=columns))

    def aggregate(self, group_by, aggregations):
        return self._wrap(CSVAggregator(self._source, group_by, aggregations))

    def limit(self, n):
        return self._wrap(CSVLimiter(self._source, n))

    def join(self, right, on, how="inner"):
        return self._wrap(CSVJoiner(self._source, right, on, how=how))

    def slice(self, start, stop):
        return self._wrap(CSVSlicer(self._source, start, stop))

    def sample(self, n, seed=None):
        return self._wrap(CSVSampler(self._source, n, seed=seed))

    def unpivot(self, id_cols, value_col="value", variable_col="variable"):
        return self._wrap(CSVUnpivot(self._source, id_cols, value_col=value_col, variable_col=variable_col))

    def pivot(self, index, column, value):
        return self._wrap(CSVPivotter(self._source, index, column, value))

    def rename_columns(self, mapping):
        return self._wrap(CSVRenamer(self._source, mapping))

    def flatten(self, column, delimiter=","):
        return self._wrap(CSVFlattener(self._source, column, delimiter=delimiter))

    def cast(self, casts):
        return self._wrap(CSVCaster(self._source, casts))

    def split_by(self, column):
        splitter = CSVSplitter(self._source, column)
        return {key: Pipeline(grp) for key, grp in splitter.groups()}

    def fill(self, fills):
        return self._wrap(CSVFiller(self._source, fills))

    def validate(self, rules, mode="drop"):
        from csvwrangler.pipeline_validate_patch import _validate
        return _validate(self, rules, mode)

    def detect_types(self):
        return CSVTyper(self._source).detected_types

    def stack(self, other):
        return self._wrap(CSVStacker(self._source, other))

    def string_ops(self, ops):
        return self._wrap(CSVStringOps(self._source, ops))

    def count_values(self, column):
        return self._wrap(CSVCounter(self._source, column))

    def diff(self, other, key):
        return self._wrap(CSVDiffer(self._source, other, key))

    def zip_with(self, other):
        return self._wrap(CSVZipper(self._source, other))

    def transpose(self):
        return self._wrap(CSVTransposer(self._source))

    def encode(self, column, encoding="base64"):
        from csvwrangler.pipeline_encode_patch import _patch, encode
        _patch(Pipeline)
        return self.encode(column, encoding)

    def normalize(self, ops):
        return self._wrap(CSVNormalizer(self._source, ops))

    def chunk(self, size):
        chunker = CSVChunker(self._source, size)
        results = []
        for chunk in chunker.chunks():
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=chunk[0].keys(), lineterminator="\n")
            w.writeheader()
            w.writerows(chunk)
            results.append(buf.getvalue())
        return results

    def interpolate(self, column, method="linear"):
        return self._wrap(CSVInterpolator(self._source, column, method=method))

    def window(self, column, func, window_size, out_col=None):
        return self._wrap(CSVWindow(self._source, column, func, window_size, out_col=out_col))

    def format_columns(self, templates):
        return self._wrap(CSVFormatter(self._source, templates))

    def tag_column(self, column, conditions, out_col="tag"):
        return self._wrap(CSVTagger(self._source, column, conditions, out_col=out_col))

    def round_columns(self, columns, decimals=2):
        return self._wrap(CSVRounder(self._source, columns, decimals=decimals))

    def clip(self, column, lower=None, upper=None):
        return self._wrap(CSVClipper(self._source, column, lower=lower, upper=upper))

    def condense(self, others):
        return self._wrap(CSVCondenser(self._source, others))

    def scale(self, columns, method="minmax"):
        return self._wrap(CSVScaler(self._source, columns, method=method))

    def rank(self, column, out_col="rank", descending=False):
        return self._wrap(CSVRanker(self._source, column, out_col=out_col, descending=descending))

    def correlate(self, columns=None):
        return CSVCorrelator(self._source, columns=columns).matrix()

    def bin(self, column, bins, labels=None, out_col=None):
        return self._wrap(CSVBinner(self._source, column, bins, labels=labels, out_col=out_col))

    def flag_outliers(self, column, method="iqr", out_col=None):
        return self._wrap(CSVOutlier(self._source, column, method=method, out_col=out_col))

    def rolling(self, column, func, window, out_col=None):
        return self._wrap(CSVRoller(self._source, column, func, window, out_col=out_col))

    def score(self, weights: dict, score_col: str = "score", default: float = 0.0):
        return self._wrap(CSVScorer(self._source, weights, score_col=score_col, default=default))
