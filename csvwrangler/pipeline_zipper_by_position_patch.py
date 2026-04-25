"""Patch Pipeline with a .zip_by_position(other, mode) method."""
from csvwrangler.zipper_by_position import CSVPositionZipper


def zip_by_position(self, other, mode: str = "append"):
    """Zip this pipeline with *other* pipeline column-by-column.

    Parameters
    ----------
    other : Pipeline
        The right-hand pipeline whose columns are merged in.
    mode : str
        ``'append'`` (default) – all left columns then all right columns.
        ``'interleave'`` / ``'alternate'`` – columns interleaved l, r, l, r …

    Returns
    -------
    Pipeline
        A new pipeline wrapping the zipped source.
    """
    from csvwrangler.pipeline import Pipeline

    class _Src:
        def __init__(self, left_src, right_src, _mode):
            self._z = CSVPositionZipper(left_src, right_src, mode=_mode)

        @property
        def headers(self):
            return self._z.headers

        @property
        def row_count(self):
            return self._z.row_count

        def rows(self):
            return self._z.rows()

    left_src = self._source
    right_src = other._source
    new_src = _Src(left_src, right_src, mode)
    p = Pipeline.__new__(Pipeline)
    p._source = new_src
    return p


def _patch():
    from csvwrangler.pipeline import Pipeline
    if not hasattr(Pipeline, "zip_by_position"):
        Pipeline.zip_by_position = zip_by_position


_patch()
