"""Patch Pipeline to expose shift_columns()."""
from __future__ import annotations
from csvwrangler.shifter import CSVShifter


def shift_columns(self, shifts: dict) -> "Pipeline":
    """Shift numeric or date column values by a fixed amount.

    Parameters
    ----------
    shifts : dict  {column: offset}  where offset is int/float or
             a timedelta-kwargs dict e.g. {"days": 7}.
    """
    return self._wrap(CSVShifter(self._source, shifts))


def _patch(pipeline_cls):
    pipeline_cls.shift_columns = shift_columns
