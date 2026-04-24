"""Patch Pipeline with a split_by_date() method."""
from __future__ import annotations

from typing import Dict


def split_by_date(self, column: str, period: str = "month") -> Dict[str, "Pipeline"]:
    """Split the pipeline into a dict of sub-Pipelines keyed by date period.

    Parameters
    ----------
    column:
        Name of the column containing ISO-8601 date values.
    period:
        One of ``'year'``, ``'month'``, ``'week'``, ``'day'``.

    Returns
    -------
    dict mapping period key -> Pipeline whose source is the rows for that key.
    """
    from csvwrangler.splitter_by_date import CSVDateSplitter
    from csvwrangler.pipeline import Pipeline

    splitter = CSVDateSplitter(self._source, column, period)
    hdrs = splitter.headers()

    result: Dict[str, Pipeline] = {}
    for key in splitter.group_keys():
        rows = list(splitter.rows_for(key))

        class _GroupSrc:
            def __init__(self, h, r):
                self._h = h
                self._r = r

            def headers(self):
                return list(self._h)

            def rows(self):
                yield from self._r

        result[key] = Pipeline(_GroupSrc(hdrs, rows))

    return result


def _patch():
    from csvwrangler.pipeline import Pipeline
    if not hasattr(Pipeline, "split_by_date"):
        Pipeline.split_by_date = split_by_date


_patch()
