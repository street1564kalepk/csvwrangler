"""Patch Pipeline with a .split_by_parity() method."""

from __future__ import annotations
from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from csvwrangler.pipeline import Pipeline

from csvwrangler.splitter_by_parity import CSVParitySplitter


def split_by_parity(self: "Pipeline") -> Dict[str, "Pipeline"]:
    """Split the pipeline into two pipelines: 'even' and 'odd' row groups.

    Returns a dict with keys 'even' and 'odd', each being a new Pipeline
    wrapping the corresponding rows.
    """
    from csvwrangler.pipeline import Pipeline

    splitter = CSVParitySplitter(self._source)
    groups = splitter.groups()
    hdrs = splitter.headers

    result: Dict[str, Pipeline] = {}
    for key, row_list in groups.items():
        class _GroupSrc:
            def __init__(self, headers, rows):
                self._headers = headers
                self._rows = rows

            @property
            def headers(self):
                return self._headers

            def rows(self):
                return iter(self._rows)

        src = _GroupSrc(list(hdrs), list(row_list))
        p = Pipeline.__new__(Pipeline)
        p._source = src
        result[key] = p
    return result


def _patch():
    from csvwrangler.pipeline import Pipeline
    Pipeline.split_by_parity = split_by_parity


_patch()
