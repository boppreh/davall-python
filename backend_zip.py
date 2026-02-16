"""ZIP archive backend — mount a .zip file as a read-only filesystem."""

import zipfile
import mimetypes
from backend import Backend, ResourceInfo, NotFoundError, BackendError, _normalize


class ZipBackend(Backend):
    """Expose the contents of a ZIP archive as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            self._zf = zipfile.ZipFile(path, "r")
        except (zipfile.BadZipFile, FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot open ZIP file: {e}") from e

        # Build a set of directories and a dict of file infos for fast lookup.
        # ZIP files don't always have explicit directory entries, so we infer
        # directories from file paths.
        self._files: dict[str, zipfile.ZipInfo] = {}  # normalized path -> ZipInfo
        self._dirs: set[str] = {"/"}  # all known directories

        for zi in self._zf.infolist():
            # Normalize the path
            name = "/" + zi.filename
            if zi.is_dir():
                self._dirs.add(_normalize(name))
            else:
                norm = _normalize(name)
                self._files[norm] = zi
                # Ensure all parent directories exist
                parts = norm.strip("/").split("/")
                for i in range(1, len(parts)):
                    self._dirs.add("/" + "/".join(parts[:i]))

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)
        if path in self._dirs:
            return ResourceInfo(is_dir=True)
        if path in self._files:
            zi = self._files[path]
            ctype = mimetypes.guess_type(zi.filename)[0] or "application/octet-stream"
            return ResourceInfo(is_dir=False, size=zi.file_size, content_type=ctype)
        raise NotFoundError(f"Not found: {path}")

    def list(self, path: str) -> list[str]:
        path = _normalize(path)
        if path not in self._dirs:
            raise NotFoundError(f"Not a directory: {path}")

        prefix = path if path == "/" else path + "/"
        children = set()
        # Check files
        for fpath in self._files:
            if fpath.startswith(prefix):
                rest = fpath[len(prefix):]
                if "/" not in rest:
                    children.add(rest)
        # Check directories
        for dpath in self._dirs:
            if dpath.startswith(prefix) and dpath != path:
                rest = dpath[len(prefix):]
                if "/" not in rest and rest:
                    children.add(rest)
        return sorted(children)

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        if path in self._dirs:
            raise NotFoundError(f"Not a file: {path}")
        if path not in self._files:
            raise NotFoundError(f"Not found: {path}")
        try:
            return self._zf.read(self._files[path].filename)
        except Exception as e:
            raise BackendError(f"Error reading from ZIP: {e}") from e
