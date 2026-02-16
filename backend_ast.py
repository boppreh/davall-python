"""Python AST backend — mount a .py file exposing its structure.

Structure:
    /
      ClassName/
        method_name.py     — source code of the method
      function_name.py     — source code of the function
"""

import ast
import textwrap
from backend import Backend, ResourceInfo, NotFoundError, BackendError, _normalize


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

        # Build structure: top-level functions and classes
        self._entries: dict[str, dict | bytes] = {}  # name -> bytes (function) or dict (class)

        for node in ast.iter_child_nodes(self._tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                name = node.name + ".py"
                self._entries[name] = self._extract_source(node)
            elif isinstance(node, ast.ClassDef):
                methods = {}
                for item in ast.iter_child_nodes(node):
                    if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef):
                        mname = item.name + ".py"
                        methods[mname] = self._extract_source(item)
                self._entries[node.name] = methods

    def _extract_source(self, node: ast.AST) -> bytes:
        """Extract source code for an AST node."""
        start = node.lineno - 1
        end = node.end_lineno if node.end_lineno else start + 1
        lines = self._lines[start:end]
        source = textwrap.dedent("".join(lines))
        return source.encode("utf-8")

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)
        if path == "/":
            return ResourceInfo(is_dir=True)

        parts = path.strip("/").split("/")
        if len(parts) == 1:
            name = parts[0]
            if name in self._entries:
                val = self._entries[name]
                if isinstance(val, dict):
                    return ResourceInfo(is_dir=True)
                return ResourceInfo(is_dir=False, size=len(val), content_type="text/x-python")
            raise NotFoundError(f"Not found: {path}")

        if len(parts) == 2:
            cls_name, method_name = parts
            if cls_name in self._entries and isinstance(self._entries[cls_name], dict):
                methods = self._entries[cls_name]
                if method_name in methods:
                    data = methods[method_name]
                    return ResourceInfo(is_dir=False, size=len(data), content_type="text/x-python")
            raise NotFoundError(f"Not found: {path}")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: str) -> list[str]:
        path = _normalize(path)
        if path == "/":
            return sorted(self._entries.keys())

        parts = path.strip("/").split("/")
        if len(parts) == 1:
            name = parts[0]
            if name in self._entries and isinstance(self._entries[name], dict):
                return sorted(self._entries[name].keys())
            raise NotFoundError(f"Not a directory: {path}")

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        parts = path.strip("/").split("/")

        if len(parts) == 1:
            name = parts[0]
            if name in self._entries:
                val = self._entries[name]
                if isinstance(val, bytes):
                    return val
                raise NotFoundError(f"Not a file: {path}")

        if len(parts) == 2:
            cls_name, method_name = parts
            if cls_name in self._entries and isinstance(self._entries[cls_name], dict):
                methods = self._entries[cls_name]
                if method_name in methods:
                    return methods[method_name]

        raise NotFoundError(f"Not found: {path}")
