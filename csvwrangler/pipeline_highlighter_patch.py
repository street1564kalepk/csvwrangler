"""Patch Pipeline with a .highlight() method."""
from __future__ import annotations

from typing import Callable


def highlight(
    self,
    predicate: Callable[[dict], bool],
    flag_column: str = "highlighted",
    true_value: str = "true",
    false_value: str = "false",
):
    """Add a flag column marking rows where *predicate* returns truthy.

    Parameters
    ----------
    predicate:
        Callable ``(row: dict) -> bool``.
    flag_column:
        Name of the new column (default ``"highlighted"``).
    true_value / false_value:
        Strings written for matching / non-matching rows.
    """
    from csvwrangler.highlighter import CSVHighlighter

    self._src = CSVHighlighter(
        self._src, predicate, flag_column, true_value, false_value
    )
    return self


def _patch(pipeline_cls):
    pipeline_cls.highlight = highlight
