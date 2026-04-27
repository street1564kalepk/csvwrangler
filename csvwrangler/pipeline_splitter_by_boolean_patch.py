"""Patch Pipeline with a split_by_boolean() method."""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from csvwrangler.pipeline import Pipeline


def split_by_boolean(
    self: "Pipeline",
    column: str,
    *,
    strict: bool = False,
) -> dict[str, "Pipeline"]:
    """Split the pipeline into two child pipelines keyed 'true' and 'false'.

    Args:
        column: Name of the column containing boolean-like values.
        strict: If True, raise on unrecognised values instead of treating them
                as falsy.

    Returns:
        A dict with keys ``'true'`` and ``'false'``, each being a new
        :class:`Pipeline` wrapping the corresponding row subset.
    """
    from csvwrangler.splitter_by_boolean import CSVBooleanSplitter
    from csvwrangler.pipeline import Pipeline

    splitter = CSVBooleanSplitter(self._source, column, strict=strict)

    class _GroupSrc:
        def __init__(self, headers, rows_list):
            self._headers = headers
            self._rows = rows_list

        @property
        def headers(self):
            return self._headers

        @property
        def rows(self):
            return iter(self._rows)

    hdrs = splitter.headers
    return {
        "true": Pipeline._from_source(_GroupSrc(hdrs, splitter.true_rows)),
        "false": Pipeline._from_source(_GroupSrc(hdrs, splitter.false_rows)),
    }


def _patch(pipeline_cls):
    pipeline_cls.split_by_boolean = split_by_boolean
