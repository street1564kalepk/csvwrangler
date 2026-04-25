"""Patch Pipeline with a split_by_weekday() method."""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from csvwrangler.pipeline import Pipeline


def split_by_weekday(self, column: str) -> Dict[str, "Pipeline"]:
    """Return a dict mapping weekday names to Pipeline instances.

    Each Pipeline wraps the rows that fall on that weekday.  Only
    non-empty groups are included in the returned mapping.
    """
    from csvwrangler.splitter_by_weekday import CSVWeekdaySplitter
    from csvwrangler.pipeline import Pipeline

    splitter = CSVWeekdaySplitter(self._source, column)

    class _GroupSrc:
        def __init__(self, hdrs, data):
            self._hdrs = hdrs
            self._data = data

        @property
        def headers(self):
            return self._hdrs

        def rows(self):
            return iter(self._data)

    result: Dict[str, Pipeline] = {}
    for key in splitter.group_keys:
        src = _GroupSrc(splitter.headers, splitter.group(key))
        p = Pipeline.__new__(Pipeline)
        p._source = src
        result[key] = p
    return result


def _patch(pipeline_cls):
    pipeline_cls.split_by_weekday = split_by_weekday
