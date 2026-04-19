"""Patch Pipeline with a .replace_values() method."""
from __future__ import annotations
from typing import Dict


def replace_values(
    self,
    column_replacements: Dict[str, Dict[str, str]],
    *,
    substring: bool = False,
):
    """Replace values in one or more columns.

    Parameters
    ----------
    column_replacements:
        ``{column: {old: new, ...}, ...}``
    substring:
        If *True*, perform substring replacement rather than exact match.
    """
    from csvwrangler.replacer import CSVReplacer

    self._source = CSVReplacer(self._source, column_replacements, substring=substring)
    return self


def _patch(pipeline_cls):
    pipeline_cls.replace_values = replace_values
