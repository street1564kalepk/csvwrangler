"""Patch Pipeline with a .partition() method."""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:  # pragma: no cover
    from csvwrangler.pipeline import Pipeline


def partition(self, n_parts: int) -> "Dict[int, Pipeline]":
    """Split the pipeline into *n_parts* equal-ish partitions.

    Returns a dict mapping partition index -> new Pipeline wrapping that slice.
    """
    from csvwrangler.partitioner import CSVPartitioner
    from csvwrangler.pipeline import Pipeline

    partitioner = CSVPartitioner(self._source, n_parts=n_parts)

    result: Dict[int, Pipeline] = {}
    for idx in range(n_parts):
        rows_snapshot = partitioner.partition(idx)
        headers_snapshot = partitioner.headers

        class _PartSrc:
            def __init__(self, hdrs, data):
                self._hdrs = hdrs
                self._data = data

            @property
            def headers(self):
                return self._hdrs

            def rows(self):
                yield from self._data

        src = _PartSrc(headers_snapshot, rows_snapshot)
        p = Pipeline.__new__(Pipeline)
        p._source = src  # type: ignore[attr-defined]
        result[idx] = p

    return result


def _patch() -> None:
    from csvwrangler.pipeline import Pipeline
    if not hasattr(Pipeline, "partition"):
        Pipeline.partition = partition  # type: ignore[attr-defined]


_patch()
