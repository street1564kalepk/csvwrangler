"""Patch Pipeline with a .cross() method."""
from __future__ import annotations
from csvwrangler.crosser import CSVCrosser


def cross(
    self,
    other,
    left_prefix: str = "left_",
    right_prefix: str = "right_",
):
    """Return a new Pipeline that is the cartesian product of *self* and *other*."""
    from csvwrangler.pipeline import Pipeline

    left_src = self._source   # noqa: SLF001
    right_src = other._source  # noqa: SLF001
    new_src = CSVCrosser(left_src, right_src, left_prefix, right_prefix)
    p = Pipeline.__new__(Pipeline)
    p._source = new_src       # noqa: SLF001
    return p


def _patch(pipeline_cls):
    pipeline_cls.cross = cross
