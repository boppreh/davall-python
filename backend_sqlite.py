"""SQLite database backend — mount a .db file as a read-only filesystem.

Structure:
    /
      table_name/
        _schema.sql       — CREATE TABLE statement
        row_<rowid>.json  — each row as a JSON object
"""

import json
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

        # Cache table names
        try:
            cur = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            self._tables = [row[0] for row in cur.fetchall()]
        except sqlite3.Error as e:
            raise BackendError(f"Cannot read database schema: {e}") from e

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

    def _get_row(self, table: str, index: int) -> bytes:
        cur = self._conn.execute(f'SELECT rowid, * FROM "{table}" LIMIT 1 OFFSET ?', (index,))
        row = cur.fetchone()
        if row is None:
            raise NotFoundError(f"Row {index} not found in {table}")
        return json.dumps(dict(row), indent=2, default=str).encode("utf-8")

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)

        if path == "/":
            return ResourceInfo(is_dir=True)

        parts = path.strip("/").split("/")
        if len(parts) == 1:
            # Table directory
            if parts[0] in self._tables:
                return ResourceInfo(is_dir=True)
            raise NotFoundError(f"Not found: {path}")

        if len(parts) == 2:
            table, filename = parts
            if table not in self._tables:
                raise NotFoundError(f"Not found: {path}")
            if filename == "_schema.sql":
                data = self._get_schema(table).encode("utf-8")
                return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")
            if filename.startswith("row_") and filename.endswith(".json"):
                try:
                    index = int(filename[4:-5])
                except ValueError:
                    raise NotFoundError(f"Not found: {path}")
                data = self._get_row(table, index)
                return ResourceInfo(is_dir=False, size=len(data), content_type="application/json")

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
            entries.extend(f"row_{i}.json" for i in range(count))
            return entries

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        parts = path.strip("/").split("/")

        if len(parts) == 2:
            table, filename = parts
            if table not in self._tables:
                raise NotFoundError(f"Not found: {path}")
            try:
                if filename == "_schema.sql":
                    return self._get_schema(table).encode("utf-8")
                if filename.startswith("row_") and filename.endswith(".json"):
                    try:
                        index = int(filename[4:-5])
                    except ValueError:
                        raise NotFoundError(f"Not found: {path}")
                    return self._get_row(table, index)
            except sqlite3.Error as e:
                raise BackendError(f"Error reading from database: {e}") from e

        raise NotFoundError(f"Not found: {path}")
