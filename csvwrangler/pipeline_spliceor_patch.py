"""Patch Pipeline with a .splice() method."""
from csvwrangler.spliceor import CSVSpliceor


def splice(self, other_pipeline, after_row: int = -1):
    """Insert rows from *other_pipeline* after *after_row* (default: append).

    Parameters
    ----------
    other_pipeline:
        Another Pipeline whose rows are spliced in.
    after_row:
        Zero-based row index after which to insert.  ``-1`` appends.
    """
    from csvwrangler.pipeline import Pipeline

    other_src = other_pipeline._source  # noqa: SLF001
    new_src = CSVSpliceor(self._source, other_src, after_row=after_row)
    p = Pipeline.__new__(Pipeline)
    p._source = new_src  # noqa: SLF001
    return p


def _patch():
    from csvwrangler.pipeline import Pipeline

    if not hasattr(Pipeline, "splice"):
        Pipeline.splice = splice


_patch()
