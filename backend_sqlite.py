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
from backend import Backend, ResourceInfo, NotFoundError, BackendError, _normalize


class SqliteBackend(Backend):
    """Expose a SQLite database as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            # Open read-only
            self._conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            self._conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            raise BackendError(f"Cannot open SQLite database: {e}") from e

        # Cache table names and column names
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

    def _get_schema(self, table: str) -> str:
        cur = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"Table not found: {table}")
        return row[0] + ";\n"

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
        if value is None:
            return b""
        return str(value).encode("utf-8")

    def _parse_row_name(self, name: str) -> int | None:
        """Parse 'row_N' and return N, or None if not a valid row name."""
        if not name.startswith("row_"):
            return None
        try:
            return int(name[4:])
        except ValueError:
            return None

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)

        if path == "/":
            return ResourceInfo(is_dir=True)

        parts = path.strip("/").split("/")
        if len(parts) == 1:
            if parts[0] in self._tables:
                return ResourceInfo(is_dir=True)
            raise NotFoundError(f"Not found: {path}")

        if len(parts) == 2:
            table, name = parts
            if table not in self._tables:
                raise NotFoundError(f"Not found: {path}")
            if name == "_schema.sql":
                data = self._get_schema(table).encode("utf-8")
                return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")
            row_idx = self._parse_row_name(name)
            if row_idx is not None:
                count = self._get_row_count(table)
                if row_idx < count:
                    return ResourceInfo(is_dir=True)
            raise NotFoundError(f"Not found: {path}")

        if len(parts) == 3:
            table, row_name, column = parts
            if table not in self._tables:
                raise NotFoundError(f"Not found: {path}")
            row_idx = self._parse_row_name(row_name)
            if row_idx is None:
                raise NotFoundError(f"Not found: {path}")
            data = self._get_cell(table, row_idx, column)
            return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: str) -> list[str]:
        path = _normalize(path)

        if path == "/":
            return sorted(self._tables)

        parts = path.strip("/").split("/")
        if len(parts) == 1:
            table = parts[0]
            if table not in self._tables:
                raise NotFoundError(f"Not a directory: {path}")
            try:
                count = self._get_row_count(table)
            except sqlite3.Error as e:
                raise BackendError(f"Error reading table {table}: {e}") from e
            entries = ["_schema.sql"]
            entries.extend(f"row_{i}" for i in range(count))
            return entries

        if len(parts) == 2:
            table, row_name = parts
            if table not in self._tables:
                raise NotFoundError(f"Not a directory: {path}")
            row_idx = self._parse_row_name(row_name)
            if row_idx is None:
                raise NotFoundError(f"Not a directory: {path}")
            count = self._get_row_count(table)
            if row_idx >= count:
                raise NotFoundError(f"Not a directory: {path}")
            return list(self._columns[table])

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        parts = path.strip("/").split("/")

        if len(parts) == 2:
            table, name = parts
            if table not in self._tables:
                raise NotFoundError(f"Not found: {path}")
            if name == "_schema.sql":
                try:
                    return self._get_schema(table).encode("utf-8")
                except sqlite3.Error as e:
                    raise BackendError(f"Error reading from database: {e}") from e

        if len(parts) == 3:
            table, row_name, column = parts
            if table not in self._tables:
                raise NotFoundError(f"Not found: {path}")
            row_idx = self._parse_row_name(row_name)
            if row_idx is None:
                raise NotFoundError(f"Not found: {path}")
            try:
                return self._get_cell(table, row_idx, column)
            except sqlite3.Error as e:
                raise BackendError(f"Error reading from database: {e}") from e

        raise NotFoundError(f"Not found: {path}")
