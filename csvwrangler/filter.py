from typing import Callable, Iterator, Dict, Any, List, Optional


class CSVFilter:
    """
    Provides chainable filtering operations on CSV row iterators.
    """

    def __init__(self, rows: Iterator[Dict[str, Any]], headers: List[str]):
        self._rows = rows
        self._headers = headers
        self._predicates: List[Callable[[Dict[str, Any]], bool]] = []

    @property
    def headers(self) -> List[str]:
        return self._headers

    def where(self, column: str, op: str, value: Any) -> "CSVFilter":
        """Add a filter condition. Supported ops: ==, !=, <, <=, >, >=, contains, startswith, endswith."""
        if column not in self._headers:
            raise ValueError(f"Column '{column}' not found in headers: {self._headers}")

        supported_ops = {"==", "!=", "<", "<=", ">", ">=", "contains", "startswith", "endswith"}
        if op not in supported_ops:
            raise ValueError(f"Unsupported operator '{op}'. Must be one of: {sorted(supported_ops)}")

        def predicate(row: Dict[str, Any]) -> bool:
            cell = row.get(column, "")
            try:
                numeric_cell = float(cell)
                numeric_value = float(value)
            except (ValueError, TypeError):
                numeric_cell = None
                numeric_value = None

            if op == "==":
                return cell == str(value)
            elif op == "!=":
                return cell != str(value)
            elif op == "contains":
                return str(value) in cell
            elif op == "startswith":
                return cell.startswith(str(value))
            elif op == "endswith":
                return cell.endswith(str(value))
            elif numeric_cell is not None and numeric_value is not None:
                if op == "<":
                    return numeric_cell < numeric_value
                elif op == "<=":
                    return numeric_cell <= numeric_value
                elif op == ">":
                    return numeric_cell > numeric_value
                elif op == ">=":
                    return numeric_cell >= numeric_value
            raise ValueError(f"Operator '{op}' requires numeric values, but could not convert '{cell}' or '{value}' to float.")

        self._predicates.append(predicate)
        return self

    def custom(self, func: Callable[[Dict[str, Any]], bool]) -> "CSVFilter":
        """Add a custom predicate function."""
        self._predicates.append(func)
        return self

    def apply(self) -> Iterator[Dict[str, Any]]:
        """Yield rows that match all predicates."""
        for row in self._rows:
            if all(pred(row) for pred in self._predicates):
                yield row

    def __repr__(self) -> str:
        return f"CSVFilter(predicates={len(self._predicates)})"
