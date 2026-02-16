"""CSV file backend — mount a .csv file as a read-only filesystem.

Structure:
    /
      _headers.txt       — column names, one per line
      row_0000.json      — {"col1": "val1", "col2": "val2"}
      row_0001.json
      ...
"""

import csv
import json
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
        # Pre-format row width for filenames
        self._width = max(4, len(str(len(self._rows))))

    def _row_filename(self, index: int) -> str:
        return f"row_{index:0{self._width}d}.json"

    def _row_bytes(self, index: int) -> bytes:
        if index < 0 or index >= len(self._rows):
            raise NotFoundError(f"Row {index} not found")
        return json.dumps(self._rows[index], indent=2).encode("utf-8")

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)
        if path == "/":
            return ResourceInfo(is_dir=True)

        name = path.strip("/")
        if "/" in name:
            raise NotFoundError(f"Not found: {path}")

        if name == "_headers.txt":
            return ResourceInfo(is_dir=False, size=len(self._headers_bytes), content_type="text/plain")

        if name.startswith("row_") and name.endswith(".json"):
            try:
                index = int(name[4:-5])
            except ValueError:
                raise NotFoundError(f"Not found: {path}")
            data = self._row_bytes(index)
            return ResourceInfo(is_dir=False, size=len(data), content_type="application/json")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: str) -> list[str]:
        path = _normalize(path)
        if path != "/":
            raise NotFoundError(f"Not a directory: {path}")
        entries = ["_headers.txt"]
        entries.extend(self._row_filename(i) for i in range(len(self._rows)))
        return entries

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        name = path.strip("/")

        if name == "_headers.txt":
            return self._headers_bytes

        if name.startswith("row_") and name.endswith(".json"):
            try:
                index = int(name[4:-5])
            except ValueError:
                raise NotFoundError(f"Not found: {path}")
            return self._row_bytes(index)

        raise NotFoundError(f"Not found: {path}")
