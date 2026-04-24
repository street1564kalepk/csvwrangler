"""Patch Pipeline with a ``split_by_regex`` method."""
from __future__ import annotations

from typing import Dict

from csvwrangler.splitter_by_regex import CSVRegexSplitter


def split_by_regex(self, column: str, patterns: Dict[str, str]) -> Dict[str, "Pipeline"]:
    """Return a dict mapping each group name to a new Pipeline.

    Parameters
    ----------
    column:
        Column whose value is matched against each pattern.
    patterns:
        Ordered mapping of *group_name* -> *regex_string*.

    Returns
    -------
    dict[str, Pipeline]
        Keys are the group names (plus ``"__unmatched__"`` for rows that
        matched nothing).  Each value is a fresh ``Pipeline`` wrapping the
        rows for that group.
    """
    from csvwrangler.pipeline import Pipeline  # local import to avoid cycles

    splitter = CSVRegexSplitter(self._source, column, patterns)

    result: Dict[str, Pipeline] = {}
    for key in splitter.group_keys:
        rows_snapshot = list(splitter.rows(key))
        hdrs = splitter.headers

        class _GroupSrc:
            def __init__(self, headers, data):
                self._headers = headers
                self._data = data

            @property
            def headers(self):
                return list(self._headers)

            def rows(self):
                yield from self._data

        result[key] = Pipeline._from_source(_GroupSrc(hdrs, rows_snapshot))
    return result


def _patch(pipeline_cls):
    pipeline_cls.split_by_regex = split_by_regex
