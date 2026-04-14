"""Tests for CSVReader and CSVWriter."""

import csv
import io
import sys
from pathlib import Path

import pytest

from csvwrangler.reader import CSVReader
from csvwrangler.writer import CSVWriter


SAMPLE_ROWS = [
    {"id": "1", "name": "Alice", "age": "30"},
    {"id": "2", "name": "Bob", "age": "25"},
    {"id": "3", "name": "Charlie", "age": "35"},
]


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create a temporary CSV file with sample data."""
    filepath = tmp_path / "sample.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "age"])
        writer.writeheader()
        writer.writerows(SAMPLE_ROWS)
    return filepath


class TestCSVReader:
    def test_headers(self, sample_csv):
        reader = CSVReader(str(sample_csv))
        assert reader.headers == ["id", "name", "age"]

    def test_rows_yields_dicts(self, sample_csv):
        reader = CSVReader(str(sample_csv))
        rows = list(reader.rows())
        assert rows == SAMPLE_ROWS

    def test_row_count(self, sample_csv):
        reader = CSVReader(str(sample_csv))
        assert reader.row_count() == 3

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            CSVReader("/nonexistent/path/file.csv")

    def test_custom_delimiter(self, tmp_path):
        filepath = tmp_path / "pipe.csv"
        filepath.write_text("a|b|c\n1|2|3\n")
        reader = CSVReader(str(filepath), delimiter="|")
        assert reader.headers == ["a", "b", "c"]
        assert list(reader.rows()) == [{"a": "1", "b": "2", "c": "3"}]


class TestCSVWriter:
    def test_write_to_file(self, tmp_path):
        out = tmp_path / "out.csv"
        writer = CSVWriter(str(out))
        count = writer.write(iter(SAMPLE_ROWS))
        assert count == 3
        reader = CSVReader(str(out))
        assert list(reader.rows()) == SAMPLE_ROWS

    def test_write_empty_rows(self, tmp_path):
        out = tmp_path / "empty.csv"
        writer = CSVWriter(str(out))
        count = writer.write(iter([]))
        assert count == 0
        assert not out.exists()

    def test_write_to_stdout(self, sample_csv, capsys):
        reader = CSVReader(str(sample_csv))
        writer = CSVWriter()  # no filepath => stdout
        count = writer.write(reader.rows())
        captured = capsys.readouterr()
        assert count == 3
        assert "Alice" in captured.out
        assert "id,name,age" in captured.out
