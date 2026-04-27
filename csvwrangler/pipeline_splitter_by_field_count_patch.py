"""Patch Pipeline with a split_by_field_count method."""

from __future__ import annotations
from typing import Dict
from csvwrangler.splitter_by_field_count import CSVFieldCountSplitter


def split_by_field_count(self) -> Dict[int, "Pipeline"]:
    """Split the current pipeline into sub-pipelines keyed by non-empty field count.

    Returns a dict mapping integer field-count to a Pipeline whose source
    contains only rows with that many non-empty fields.
    """
    from csvwrangler.pipeline import Pipeline

    splitter = CSVFieldCountSplitter(self._source)
    result: Dict[int, Pipeline] = {}

    for key in splitter.group_keys:
        rows_snapshot = splitter.rows_for(key)
        headers_snapshot = list(splitter.headers)

        class _GroupSrc:
            def __init__(self, hdrs, data):
                self._hdrs = hdrs
                self._data = data

            @property
            def headers(self):
                return self._hdrs

            def rows(self):
                return iter(self._data)

        p = Pipeline.__new__(Pipeline)
        p._source = _GroupSrc(headers_snapshot, rows_snapshot)
        result[key] = p

    return result


def _patch(pipeline_cls):
    pipeline_cls.split_by_field_count = split_by_field_count
