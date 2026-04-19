"""Patch Pipeline with a .split_by_columns() method."""
from __future__ import annotations
from typing import List
from csvwrangler.splitter_by_column import CSVColumnSplitter


def split_by_columns(self, groups: List[List[str]]):
    """Split the pipeline source into multiple column-group sub-sources.

    Returns a list of new Pipeline instances, one per group.
    """
    from csvwrangler.pipeline import Pipeline

    splitter = CSVColumnSplitter(self._source, groups)

    pipelines = []
    for idx in range(splitter.group_count):

        class _ColSrc:
            def __init__(self, sp, i):
                self._sp = sp
                self._i = i

            @property
            def headers(self):
                return self._sp.headers_for(self._i)

            def rows(self):
                return self._sp.rows_for(self._i)

            @property
            def row_count(self):
                return self._sp.row_count_for(self._i)

        p = Pipeline.__new__(Pipeline)
        p._source = _ColSrc(splitter, idx)
        pipelines.append(p)

    return pipelines


def _patch(pipeline_cls):
    pipeline_cls.split_by_columns = split_by_columns
