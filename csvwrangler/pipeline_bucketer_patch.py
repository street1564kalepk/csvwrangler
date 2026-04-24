"""Patch Pipeline with a .bucket_column() method."""
from __future__ import annotations

from typing import List, Tuple


def bucket_column(
    self,
    column: str,
    buckets: List[Tuple[str, float]],
    target_column: str = "bucket",
    default: str = "other",
):
    """Append a bucket label column derived from *column*.

    Parameters
    ----------
    column:
        Source column containing numeric values.
    buckets:
        Ordered list of ``(label, upper_bound)`` pairs.
    target_column:
        Name of the new label column (default ``"bucket"``).
    default:
        Fallback label when no bucket matches (default ``"other"``).
    """
    from csvwrangler.bucketer import CSVBucketer
    from csvwrangler.pipeline import Pipeline

    src = self._Src(self._source.headers, list(self._source.rows()))
    bucketed = CSVBucketer(src, column, buckets, target_column, default)
    new_src = self._Src(bucketed.headers, list(bucketed.rows()))
    p = Pipeline.__new__(Pipeline)
    p._source = new_src
    return p


def _patch(pipeline_cls):
    pipeline_cls.bucket_column = bucket_column
