"""CSV file backend — mount a .csv file as a read-only filesystem.

Structure:
    /
      _headers.txt       — column names, one per line
      row_0000/
        column_name      — file containing the cell value
      row_0001/
        ...
"""

import csv
from backend import Backend, ResourceInfo, NotFoundError, BackendError, _normalize


class CsvBackend(Backend):
    """Expose a CSV file as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                self._headers = reader.fieldnames or []
                self._rows = list(reader)
        except (FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot read CSV file: {e}") from e

        self._headers_bytes = "\n".join(self._headers).encode("utf-8")
        self._width = max(4, len(str(len(self._rows))))

    def _row_dirname(self, index: int) -> str:
        return f"row_{index:0{self._width}d}"

    def _parse_row_name(self, name: str) -> int | None:
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
            name = parts[0]
            if name == "_headers.txt":
                return ResourceInfo(is_dir=False, size=len(self._headers_bytes), content_type="text/plain")
            row_idx = self._parse_row_name(name)
            if row_idx is not None and 0 <= row_idx < len(self._rows):
                return ResourceInfo(is_dir=True)
            raise NotFoundError(f"Not found: {path}")

        if len(parts) == 2:
            row_name, column = parts
            row_idx = self._parse_row_name(row_name)
            if row_idx is None or row_idx < 0 or row_idx >= len(self._rows):
                raise NotFoundError(f"Not found: {path}")
            if column not in self._headers:
                raise NotFoundError(f"Not found: {path}")
            value = (self._rows[row_idx].get(column) or "").encode("utf-8")
            return ResourceInfo(is_dir=False, size=len(value), content_type="text/plain")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: str) -> list[str]:
        path = _normalize(path)
        if path == "/":
            entries = ["_headers.txt"]
            entries.extend(self._row_dirname(i) for i in range(len(self._rows)))
            return entries

        parts = path.strip("/").split("/")
        if len(parts) == 1:
            row_idx = self._parse_row_name(parts[0])
            if row_idx is not None and 0 <= row_idx < len(self._rows):
                return list(self._headers)
            raise NotFoundError(f"Not a directory: {path}")

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        parts = path.strip("/").split("/")

        if len(parts) == 1:
            if parts[0] == "_headers.txt":
                return self._headers_bytes
            raise NotFoundError(f"Not found: {path}")

        if len(parts) == 2:
            row_name, column = parts
            row_idx = self._parse_row_name(row_name)
            if row_idx is None or row_idx < 0 or row_idx >= len(self._rows):
                raise NotFoundError(f"Not found: {path}")
            if column not in self._headers:
                raise NotFoundError(f"Not found: {path}")
            return (self._rows[row_idx].get(column) or "").encode("utf-8")

        raise NotFoundError(f"Not found: {path}")
