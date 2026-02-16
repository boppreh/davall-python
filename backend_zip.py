"""ZIP archive backend — mount a .zip file as a read-only filesystem."""

import zipfile
import mimetypes
from backend import Backend, ResourceInfo, NotFoundError, BackendError


class ZipBackend(Backend):
    """Expose the contents of a ZIP archive as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            self._zf = zipfile.ZipFile(path, "r")
        except (zipfile.BadZipFile, FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot open ZIP file: {e}") from e

        # Build a set of directory tuples and a dict of file tuples for fast lookup.
        # ZIP files don't always have explicit directory entries, so we infer
        # directories from file paths.
        self._files: dict[tuple, zipfile.ZipInfo] = {}  # path tuple -> ZipInfo
        self._dirs: set[tuple] = {()}  # all known directories (empty tuple = root)

        for zi in self._zf.infolist():
            parts = tuple(p for p in zi.filename.split("/") if p)
            if zi.is_dir():
                self._dirs.add(parts)
            else:
                self._files[parts] = zi
                # Ensure all parent directories exist
                for i in range(1, len(parts)):
                    self._dirs.add(parts[:i])

    def info(self, path: list[str]) -> ResourceInfo:
        key = tuple(path)
        if key in self._dirs:
            return ResourceInfo(is_dir=True)
        if key in self._files:
            zi = self._files[key]
            ctype = mimetypes.guess_type(zi.filename)[0] or "application/octet-stream"
            return ResourceInfo(is_dir=False, size=zi.file_size, content_type=ctype)
        raise NotFoundError(f"Not found: {path}")

    def list(self, path: list[str]) -> list[str]:
        key = tuple(path)
        if key not in self._dirs:
            raise NotFoundError(f"Not a directory: {path}")

        depth = len(key)
        children = set()
        for fpath in self._files:
            if fpath[:depth] == key and len(fpath) == depth + 1:
                children.add(fpath[depth])
        for dpath in self._dirs:
            if dpath[:depth] == key and len(dpath) == depth + 1:
                children.add(dpath[depth])
        return sorted(children)

    def get(self, path: list[str]) -> bytes:
        key = tuple(path)
        if key in self._dirs:
            raise NotFoundError(f"Not a file: {path}")
        if key not in self._files:
            raise NotFoundError(f"Not found: {path}")
        try:
            return self._zf.read(self._files[key].filename)
        except Exception as e:
            raise BackendError(f"Error reading from ZIP: {e}") from e
