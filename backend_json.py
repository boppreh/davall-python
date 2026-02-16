"""JSON file backend — mount a .json file as a read-only filesystem.

Structure:
    dict keys   -> directories (if value is dict/list) or files (if scalar)
    list indices -> directory entries named "0", "1", ...
    scalars     -> files containing the string representation
"""

import json
from backend import Backend, ResourceInfo, NotFoundError, BackendError


class JsonBackend(Backend):
    """Expose a JSON file as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                self._root = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot read JSON file: {e}") from e

        if not isinstance(self._root, (dict, list)):
            raise BackendError("JSON root must be a dict or list")

    def _resolve(self, path: list[str]):
        node = self._root
        for part in path:
            if isinstance(node, dict):
                if part not in node:
                    raise NotFoundError(f"Not found: {path}")
                node = node[part]
            elif isinstance(node, list):
                try:
                    index = int(part)
                except ValueError:
                    raise NotFoundError(f"Not found: {path}")
                if index < 0 or index >= len(node):
                    raise NotFoundError(f"Not found: {path}")
                node = node[index]
            else:
                raise NotFoundError(f"Not found: {path}")
        return node

    def _to_bytes(self, node) -> bytes:
        if isinstance(node, (dict, list)):
            raise NotFoundError("Not a file")
        if node is None:
            return b"null"
        if isinstance(node, bool):
            return b"true" if node else b"false"
        return str(node).encode("utf-8")

    def info(self, path: list[str]) -> ResourceInfo:
        node = self._resolve(path)
        if isinstance(node, (dict, list)):
            return ResourceInfo(is_dir=True)
        data = self._to_bytes(node)
        return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")

    def list(self, path: list[str]) -> list[str]:
        node = self._resolve(path)
        if isinstance(node, dict):
            return sorted(node.keys())
        if isinstance(node, list):
            return [str(i) for i in range(len(node))]
        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: list[str]) -> bytes:
        node = self._resolve(path)
        return self._to_bytes(node)
