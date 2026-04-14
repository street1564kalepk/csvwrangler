# csvwrangler

> A CLI tool for fast filtering, joining, and transforming large CSV files with a chainable syntax.

---

## Installation

```bash
pip install csvwrangler
```

Or install from source:

```bash
git clone https://github.com/yourname/csvwrangler.git
cd csvwrangler
pip install -e .
```

---

## Usage

`csvwrangler` uses a chainable command syntax to build up transformations on CSV files.

**Basic example — filter rows, select columns, and save output:**

```bash
csvwrangler load data.csv \
  filter "age > 30" \
  select name,age,email \
  sort age \
  save output.csv
```

**Join two files on a shared key:**

```bash
csvwrangler load users.csv \
  join orders.csv --on user_id \
  filter "total > 100" \
  save result.csv
```

**Available commands:**

| Command  | Description                          |
|----------|--------------------------------------|
| `load`   | Load a CSV file                      |
| `filter` | Filter rows by expression            |
| `select` | Keep only specified columns          |
| `drop`   | Remove specified columns             |
| `sort`   | Sort rows by column                  |
| `join`   | Join with another CSV file           |
| `rename` | Rename columns                       |
| `save`   | Write result to a CSV file           |

For full documentation, run:

```bash
csvwrangler --help
```

---

## License

This project is licensed under the [MIT License](LICENSE).