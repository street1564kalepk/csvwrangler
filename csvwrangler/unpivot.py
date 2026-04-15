"""CSVUnpivot: melt wide CSV data into long format."""
from typing import Iterator, List, Optional


class CSVUnpivot:
    """
    Transform wide-format rows into long-format rows by unpivoting
    value columns into (variable, value) pairs.

    Example:
        Input columns : id, name, jan, feb, mar
        id_cols       : ["id", "name"]
        value_cols    : ["jan", "feb", "mar"]
        var_name      : "month"
        value_name    : "sales"

        Output columns: id, name, month, sales
    """

    def __init__(
        self,
        source,
        id_cols: List[str],
        value_cols: List[str],
        var_name: str = "variable",
        value_name: str = "value",
    ) -> None:
        self._source = source
        self._id_cols = id_cols
        self._value_cols = value_cols
        self._var_name = var_name
        self._value_name = value_name
        self._validate()

    def _validate(self) -> None:
        src_headers = set(self._source.headers)
        for col in self._id_cols + self._value_cols:
            if col not in src_headers:
                raise ValueError(f"Column '{col}' not found in source headers.")
        if self._var_name in self._id_cols or self._value_name in self._id_cols:
            raise ValueError(
                "var_name / value_name must not clash with id_cols."
            )

    @property
    def headers(self) -> List[str]:
        return self._id_cols + [self._var_name, self._value_name]

    def rows(self) -> Iterator[dict]:
        out_headers = self.headers
        for row in self._source.rows():
            id_part = {col: row[col] for col in self._id_cols}
            for vcol in self._value_cols:
                record = dict(id_part)
                record[self._var_name] = vcol
                record[self._value_name] = row[vcol]
                yield record

    @property
    def row_count(self) -> Optional[int]:
        src_count = getattr(self._source, "row_count", None)
        if src_count is not None:
            return src_count * len(self._value_cols)
        return None

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CSVUnpivot(id_cols={self._id_cols}, "
            f"value_cols={self._value_cols}, "
            f"var_name={self._var_name!r}, value_name={self._value_name!r})"
        )
