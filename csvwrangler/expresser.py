"""CSVExpresser – add a computed column via a Python expression string."""

from __future__ import annotations
from typing import Iterable, Iterator


class CSVExpresser:
    """Evaluate a Python expression against each row and store the result
    in a new (or existing) column.

    The expression is evaluated with the current row dict as the local
    namespace, so column names can be referenced directly.

    Example::

        expr = CSVExpresser(source, column="full_name",
                            expression="first + ' ' + last")
    """

    def __init__(self, source, column: str, expression: str) -> None:
        if not column:
            raise ValueError("column must be a non-empty string")
        if not expression:
            raise ValueError("expression must be a non-empty string")
        self._source = source
        self._column = column
        self._expression = expression
        self._code = compile(expression, "<expression>", "eval")
        src_headers: list[str] = list(source.headers)
        if column not in src_headers:
            src_headers.append(column)
        self._headers: list[str] = src_headers

    @property
    def headers(self) -> list[str]:
        return list(self._headers)

    def rows(self) -> Iterator[dict]:
        for row in self._source.rows():
            try:
                value = eval(self._code, {"__builtins__": {}}, dict(row))  # noqa: S307
            except Exception as exc:  # pragma: no cover
                raise RuntimeError(
                    f"Expression '{self._expression}' failed on row {row}: {exc}"
                ) from exc
            result = dict(row)
            result[self._column] = str(value)
            yield {h: result.get(h, "") for h in self._headers}

    @property
    def row_count(self) -> int:
        return sum(1 for _ in self.rows())
