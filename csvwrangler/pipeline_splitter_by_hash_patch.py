"""Patch Pipeline with a split_by_hash() method."""
from __future__ import annotations

from typing import Dict


class _GroupSrc:
    def __init__(self, headers, row_list):
        self._headers = headers
        self._rows = row_list

    @property
    def headers(self):
        return self._headers

    def rows(self):
        return iter(self._rows)


def split_by_hash(self, column: str, n_buckets: int = 2) -> Dict[int, "Pipeline"]:
    """Return a dict mapping bucket index → Pipeline for each hash bucket."""
    from csvwrangler.splitter_by_hash import CSVHashSplitter
    from csvwrangler.pipeline import Pipeline

    splitter = CSVHashSplitter(self._source, column, n_buckets=n_buckets)
    result: Dict[int, Pipeline] = {}
    for key in splitter.group_keys:
        src = _GroupSrc(splitter.headers, splitter.bucket_rows(key))
        p = Pipeline.__new__(Pipeline)
        p._source = src
        result[key] = p
    return result


def _patch(pipeline_cls):
    pipeline_cls.split_by_hash = split_by_hash
