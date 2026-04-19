"""Patch Pipeline with an .express() method."""

from __future__ import annotations
from csvwrangler.expresser import CSVExpresser


def express(self, column: str, expression: str):
    """Add or overwrite *column* using a Python *expression* string.

    Column names in the current schema can be referenced directly inside
    the expression.

    Example::

        pipeline.express("full_name", "first + ' ' + last")
    """
    from csvwrangler.pipeline import Pipeline

    src = self._build_source()
    expresser = CSVExpresser(src, column=column, expression=expression)
    return Pipeline._from_source(expresser)


def _patch(pipeline_cls):
    pipeline_cls.express = express
