"""Base backend interface and in-memory reference implementation."""

from dataclasses import dataclass


@dataclass
class ResourceInfo:
    """Metadata about a resource (file or directory)."""
    is_dir: bool
    size: int = 0
    content_type: str = "application/octet-stream"


class BackendError(Exception):
    """Base error for backend operations."""
    pass


class NotFoundError(BackendError):
    """Resource does not exist."""
    pass


class Backend:
    """Abstract read-only filesystem interface.

    Paths are '/'-separated, always starting with '/'.
    The root '/' is always a directory.
    """

    def info(self, path: str) -> ResourceInfo:
        """Return metadata for the resource at path."""
        raise NotImplementedError

    def list(self, path: str) -> list[str]:
        """Return child names for a directory. Raises NotFoundError if not a directory."""
        raise NotImplementedError

    def get(self, path: str) -> bytes:
        """Return the content of a file. Raises NotFoundError if not a file."""
        raise NotImplementedError


def _normalize(path: str) -> str:
    """Normalize a path: ensure leading /, remove trailing /, collapse doubles."""
    if not path.startswith("/"):
        path = "/" + path
    # Collapse repeated slashes
    while "//" in path:
        path = path.replace("//", "/")
    # Remove trailing slash (except for root)
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return path


class MemoryBackend(Backend):
    """In-memory backend backed by a nested dict.

    Structure: nested dicts are directories, bytes/str values are files.
    String values are encoded to UTF-8 bytes on read.

    Example:
        MemoryBackend({
            "readme.txt": "Hello, world!",
            "docs": {
                "guide.txt": "A guide",
            }
        })
    """

    def __init__(self, tree: dict):
        self._tree = tree

    def _resolve(self, path: str):
        """Walk the tree to find the node at path. Returns the node or raises NotFoundError."""
        path = _normalize(path)
        if path == "/":
            return self._tree

        parts = path.strip("/").split("/")
        node = self._tree
        for part in parts:
            if not isinstance(node, dict) or part not in node:
                raise NotFoundError(f"Not found: {path}")
            node = node[part]
        return node

    def info(self, path: str) -> ResourceInfo:
        node = self._resolve(path)
        if isinstance(node, dict):
            return ResourceInfo(is_dir=True)
        data = node.encode("utf-8") if isinstance(node, str) else node
        return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")

    def list(self, path: str) -> list[str]:
        node = self._resolve(path)
        if not isinstance(node, dict):
            raise NotFoundError(f"Not a directory: {path}")
        return sorted(node.keys())

    def get(self, path: str) -> bytes:
        node = self._resolve(path)
        if isinstance(node, dict):
            raise NotFoundError(f"Not a file: {path}")
        if isinstance(node, str):
            return node.encode("utf-8")
        return node
