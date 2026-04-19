"""Patch Pipeline with a .sequence() method."""

from __future__ import annotations
from csvwrangler.sequencer import CSVSequencer


def sequence(
    self,
    column: str = "seq",
    start: int = 1,
    step: int = 1,
    position: str = "first",
):
    """Add an auto-incrementing sequence column.

    Args:
        column:   Name of the new column (default ``"seq"``).
        start:    First sequence value (default ``1``).
        step:     Increment between values (default ``1``).
        position: ``"first"`` (default) or ``"last"``.

    Returns:
        A new Pipeline wrapping a :class:`CSVSequencer`.
    """
    from csvwrangler.pipeline import Pipeline

    new = Pipeline.__new__(Pipeline)
    new._source = CSVSequencer(
        self._source,
        column=column,
        start=start,
        step=step,
        position=position,
    )
    return new


def _patch():
    from csvwrangler.pipeline import Pipeline

    Pipeline.sequence = sequence
