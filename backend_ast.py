"""Python AST backend — mount a .py file exposing its structure.

Structure:
    /
      ClassName/
        method_name.py     — source code of the method
      function_name.py     — source code of the function
"""

import ast
import textwrap
from backend import Backend, ResourceInfo, NotFoundError, BackendError


class AstBackend(Backend):
    """Expose a Python file's classes and functions as a filesystem."""

    def __init__(self, path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                self._source = f.read()
                self._lines = self._source.splitlines(keepends=True)
        except (FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot read Python file: {e}") from e

        try:
            self._tree = ast.parse(self._source)
        except SyntaxError as e:
            raise BackendError(f"Cannot parse Python file: {e}") from e

        # name -> bytes (function) or dict of name -> bytes (class methods)
        self._entries: dict[str, dict | bytes] = {}

        for node in ast.iter_child_nodes(self._tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                self._entries[node.name + ".py"] = self._extract_source(node)
            elif isinstance(node, ast.ClassDef):
                methods = {}
                for item in ast.iter_child_nodes(node):
                    if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                        methods[item.name + ".py"] = self._extract_source(item)
                self._entries[node.name] = methods

    def _extract_source(self, node: ast.AST) -> bytes:
        start = node.lineno - 1
        end = node.end_lineno if node.end_lineno else start + 1
        lines = self._lines[start:end]
        return textwrap.dedent("".join(lines)).encode("utf-8")

    def info(self, path: list[str]) -> ResourceInfo:
        if len(path) == 0:
            return ResourceInfo(is_dir=True)

        if path[0] not in self._entries:
            raise NotFoundError(f"Not found: {path}")

        if len(path) == 1:
            val = self._entries[path[0]]
            if isinstance(val, dict):
                return ResourceInfo(is_dir=True)
            return ResourceInfo(is_dir=False, size=len(val), content_type="text/x-python")

        if len(path) == 2:
            val = self._entries[path[0]]
            if isinstance(val, dict) and path[1] in val:
                return ResourceInfo(is_dir=False, size=len(val[path[1]]), content_type="text/x-python")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: list[str]) -> list[str]:
        if len(path) == 0:
            return sorted(self._entries.keys())

        if len(path) == 1:
            if path[0] in self._entries and isinstance(self._entries[path[0]], dict):
                return sorted(self._entries[path[0]].keys())
            raise NotFoundError(f"Not a directory: {path}")

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: list[str]) -> bytes:
        if len(path) == 1 and path[0] in self._entries:
            val = self._entries[path[0]]
            if isinstance(val, bytes):
                return val
            raise NotFoundError(f"Not a file: {path}")

        if len(path) == 2:
            val = self._entries.get(path[0])
            if isinstance(val, dict) and path[1] in val:
                return val[path[1]]

        raise NotFoundError(f"Not found: {path}")
