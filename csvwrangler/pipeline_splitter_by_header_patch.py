"""Patch Pipeline with a ``split_by_header`` method."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from csvwrangler.pipeline import Pipeline

from csvwrangler.splitter_by_header import CSVHeaderSplitter


def split_by_header(
    self: "Pipeline",
    separator: str = "_",
) -> Dict[str, "Pipeline"]:
    """Split the pipeline into sub-pipelines keyed by column-name prefix.

    Parameters
    ----------
    separator:
        Character(s) used to detect the prefix boundary in column names.
        Defaults to ``'_'``.

    Returns
    -------
    dict
        Mapping of *prefix* -> :class:`Pipeline` containing only the columns
        whose names start with that prefix.
    """
    from csvwrangler.pipeline import Pipeline  # local import to avoid circulars

    splitter = CSVHeaderSplitter(self._source, separator=separator)

    class _GroupSrc:
        def __init__(self, key: str, parent_splitter: CSVHeaderSplitter) -> None:
            self._key = key
            self._splitter = parent_splitter

        @property
        def headers(self):
            return self._splitter.group_headers(self._key)

        def rows(self):
            yield from self._splitter.rows(self._key)

    result: Dict[str, Pipeline] = {}
    for key in splitter.group_keys:
        p = Pipeline.__new__(Pipeline)
        p._source = _GroupSrc(key, splitter)  # type: ignore[attr-defined]
        result[key] = p
    return result


def _patch(pipeline_cls) -> None:
    pipeline_cls.split_by_header = split_by_header
