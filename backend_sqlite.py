"""SQLite database backend — mount a .db file as a read-only filesystem.

Structure:
    /
      table_name/
        _schema.sql       — CREATE TABLE statement
        row_0/
          column_name     — file containing the cell value as text
        row_1/
          ...
"""

import sqlite3
from backend import Backend, ResourceInfo, NotFoundError, BackendError


def _parse_row_name(name: str) -> int | None:
    """Parse 'row_N' and return N, or None if not a valid row name."""
    if not name.startswith("row_"):
        return None
    try:
        return int(name[4:])
    except ValueError:
        return None


class SqliteBackend(Backend):
    """Expose a SQLite database as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            self._conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            self._conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            raise BackendError(f"Cannot open SQLite database: {e}") from e

        try:
            cur = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            self._tables = [row[0] for row in cur.fetchall()]
        except sqlite3.Error as e:
            raise BackendError(f"Cannot read database schema: {e}") from e

        self._columns: dict[str, list[str]] = {}
        for table in self._tables:
            cur = self._conn.execute(f'PRAGMA table_info("{table}")')
            self._columns[table] = [row[1] for row in cur.fetchall()]

    def _get_schema(self, table: str) -> bytes:
        cur = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"Table not found: {table}")
        return (row[0] + ";\n").encode("utf-8")

    def _get_row_count(self, table: str) -> int:
        cur = self._conn.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cur.fetchone()[0]

    def _get_cell(self, table: str, row_index: int, column: str) -> bytes:
        if column not in self._columns.get(table, []):
            raise NotFoundError(f"Column not found: {column}")
        cur = self._conn.execute(
            f'SELECT "{column}" FROM "{table}" LIMIT 1 OFFSET ?', (row_index,)
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"Row {row_index} not found in {table}")
        value = row[0]
        return b"" if value is None else str(value).encode("utf-8")

    def info(self, path: list[str]) -> ResourceInfo:
        if len(path) == 0:
            return ResourceInfo(is_dir=True)

        if path[0] not in self._tables:
            raise NotFoundError(f"Not found: {path}")
        table = path[0]

        if len(path) == 1:
            return ResourceInfo(is_dir=True)

        if len(path) == 2:
            if path[1] == "_schema.sql":
                data = self._get_schema(table)
                return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")
            row_idx = _parse_row_name(path[1])
            if row_idx is not None and row_idx < self._get_row_count(table):
                return ResourceInfo(is_dir=True)
            raise NotFoundError(f"Not found: {path}")

        if len(path) == 3:
            row_idx = _parse_row_name(path[1])
            if row_idx is None:
                raise NotFoundError(f"Not found: {path}")
            data = self._get_cell(table, row_idx, path[2])
            return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: list[str]) -> list[str]:
        if len(path) == 0:
            return sorted(self._tables)

        if path[0] not in self._tables:
            raise NotFoundError(f"Not a directory: {path}")
        table = path[0]

        if len(path) == 1:
            try:
                count = self._get_row_count(table)
            except sqlite3.Error as e:
                raise BackendError(f"Error reading table {table}: {e}") from e
            entries = ["_schema.sql"]
            entries.extend(f"row_{i}" for i in range(count))
            return entries

        if len(path) == 2:
            row_idx = _parse_row_name(path[1])
            if row_idx is None or row_idx >= self._get_row_count(table):
                raise NotFoundError(f"Not a directory: {path}")
            return list(self._columns[table])

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: list[str]) -> bytes:
        if len(path) < 2 or path[0] not in self._tables:
            raise NotFoundError(f"Not found: {path}")
        table = path[0]

        if len(path) == 2 and path[1] == "_schema.sql":
            try:
                return self._get_schema(table)
            except sqlite3.Error as e:
                raise BackendError(f"Error reading from database: {e}") from e

        if len(path) == 3:
            row_idx = _parse_row_name(path[1])
            if row_idx is None:
                raise NotFoundError(f"Not found: {path}")
            try:
                return self._get_cell(table, row_idx, path[2])
            except sqlite3.Error as e:
                raise BackendError(f"Error reading from database: {e}") from e

        raise NotFoundError(f"Not found: {path}")
