"""Patch Pipeline with a ``split_by_prefix`` method."""

from __future__ import annotations

from typing import Dict, List, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from csvwrangler.pipeline import Pipeline

from csvwrangler.splitter_by_prefix import CSVPrefixSplitter


def split_by_prefix(
    self: "Pipeline",
    column: str,
    prefixes: List[str],
    *,
    case_sensitive: bool = True,
) -> Dict[str, "Pipeline"]:
    """Split the pipeline into sub-pipelines keyed by column value prefix.

    Parameters
    ----------
    column:
        The column whose values are inspected.
    prefixes:
        List of prefix strings to match against.  The first matching prefix
        wins.  Rows that do not match any prefix are placed in
        ``"__other__"``.
    case_sensitive:
        When *False* both the cell value and each prefix are lowercased
        before comparison.

    Returns
    -------
    dict
        A mapping of ``prefix -> Pipeline`` (plus ``"__other__"`` key).
    """
    from csvwrangler.pipeline import Pipeline  # local import to avoid circularity

    splitter = CSVPrefixSplitter(
        self._source,
        column,
        prefixes,
        case_sensitive=case_sensitive,
    )

    result: Dict[str, Pipeline] = {}
    for key in splitter.group_keys:
        rows = splitter.group(key)
        headers = splitter.headers

        class _GroupSrc:
            def __init__(self, h, r):
                self._h = h
                self._r = r

            @property
            def headers(self):
                return list(self._h)

            @property
            def rows(self):
                yield from self._r

        result[key] = Pipeline.from_source(_GroupSrc(headers, rows))
    return result


def _patch(pipeline_cls) -> None:
    pipeline_cls.split_by_prefix = split_by_prefix
