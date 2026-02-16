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
from backend import Backend, ResourceInfo, NotFoundError, BackendError


def _parse_row_name(name: str) -> int | None:
    if not name.startswith("row_"):
        return None
    try:
        return int(name[4:])
    except ValueError:
        return None


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

    def info(self, path: list[str]) -> ResourceInfo:
        if len(path) == 0:
            return ResourceInfo(is_dir=True)

        if len(path) == 1:
            if path[0] == "_headers.txt":
                return ResourceInfo(is_dir=False, size=len(self._headers_bytes), content_type="text/plain")
            row_idx = _parse_row_name(path[0])
            if row_idx is not None and 0 <= row_idx < len(self._rows):
                return ResourceInfo(is_dir=True)
            raise NotFoundError(f"Not found: {path}")

        if len(path) == 2:
            row_idx = _parse_row_name(path[0])
            if row_idx is None or row_idx < 0 or row_idx >= len(self._rows):
                raise NotFoundError(f"Not found: {path}")
            if path[1] not in self._headers:
                raise NotFoundError(f"Not found: {path}")
            value = (self._rows[row_idx].get(path[1]) or "").encode("utf-8")
            return ResourceInfo(is_dir=False, size=len(value), content_type="text/plain")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: list[str]) -> list[str]:
        if len(path) == 0:
            entries = ["_headers.txt"]
            entries.extend(self._row_dirname(i) for i in range(len(self._rows)))
            return entries

        if len(path) == 1:
            row_idx = _parse_row_name(path[0])
            if row_idx is not None and 0 <= row_idx < len(self._rows):
                return list(self._headers)
            raise NotFoundError(f"Not a directory: {path}")

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: list[str]) -> bytes:
        if len(path) == 1 and path[0] == "_headers.txt":
            return self._headers_bytes

        if len(path) == 2:
            row_idx = _parse_row_name(path[0])
            if row_idx is None or row_idx < 0 or row_idx >= len(self._rows):
                raise NotFoundError(f"Not found: {path}")
            if path[1] not in self._headers:
                raise NotFoundError(f"Not found: {path}")
            return (self._rows[row_idx].get(path[1]) or "").encode("utf-8")

        raise NotFoundError(f"Not found: {path}")
