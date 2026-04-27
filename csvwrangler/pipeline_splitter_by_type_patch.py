"""Patch Pipeline with a split_by_type() method."""

from __future__ import annotations

from csvwrangler.splitter_by_type import CSVTypeSplitter


class _GroupSrc:
    """Thin wrapper so a single type-bucket looks like a pipeline source."""

    def __init__(self, splitter: CSVTypeSplitter, group: str) -> None:
        self._splitter = splitter
        self._group = group

    @property
    def headers(self) -> list[str]:
        return self._splitter.headers

    def rows(self):
        return self._splitter.rows(self._group)


def split_by_type(self, column: str) -> dict:
    """Return a dict mapping inferred-type label -> Pipeline for that group.

    Args:
        column: Name of the column whose values determine the type group.

    Returns:
        A dict whose keys are type labels (``'int'``, ``'float'``,
        ``'bool'``, ``'string'``, ``'empty'``) and whose values are
        new :class:`Pipeline` instances containing the matching rows.
    """
    from csvwrangler.pipeline import Pipeline  # local import to avoid cycles

    splitter = CSVTypeSplitter(self._source, column)
    return {
        key: Pipeline._from_source(_GroupSrc(splitter, key))
        for key in splitter.group_keys
    }


def _patch(pipeline_cls) -> None:
    pipeline_cls.split_by_type = split_by_type
