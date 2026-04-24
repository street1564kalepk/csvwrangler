"""Patch Pipeline with a .clip_by_length() method."""

from __future__ import annotations

from csvwrangler.clipper_by_length import CSVLengthClipper


def clip_by_length(self, column: str, op: str, length: int):
    """Keep only rows where ``len(column_value) <op> length``.

    Parameters
    ----------
    column:
        Column whose value length is tested.
    op:
        One of ``'eq'``, ``'ne'``, ``'lt'``, ``'lte'``, ``'gt'``, ``'gte'``.
    length:
        Integer to compare against.

    Returns
    -------
    Pipeline
        A new pipeline stage wrapping a :class:`CSVLengthClipper`.
    """
    from csvwrangler.pipeline import Pipeline

    clipped = CSVLengthClipper(self._source, column, op, length)
    return Pipeline._from_source(clipped)


def _patch():
    from csvwrangler.pipeline import Pipeline

    if not hasattr(Pipeline, "clip_by_length"):
        Pipeline.clip_by_length = clip_by_length


_patch()
