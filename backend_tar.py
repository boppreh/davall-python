"""TAR archive backend — mount .tar, .tar.gz, .tar.bz2, .tar.xz files."""

import tarfile
import mimetypes
from backend import Backend, ResourceInfo, NotFoundError, BackendError


class TarBackend(Backend):
    """Expose the contents of a TAR archive as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            self._tf = tarfile.open(path, "r:*")
        except (tarfile.TarError, FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot open TAR file: {e}") from e

        self._files: dict[tuple, tarfile.TarInfo] = {}
        self._dirs: set[tuple] = {()}

        for member in self._tf.getmembers():
            parts = tuple(p for p in member.name.split("/") if p)
            if member.isdir():
                self._dirs.add(parts)
            else:
                self._files[parts] = member
                for i in range(1, len(parts)):
                    self._dirs.add(parts[:i])

    def info(self, path: list[str]) -> ResourceInfo:
        key = tuple(path)
        if key in self._dirs:
            return ResourceInfo(is_dir=True)
        if key in self._files:
            member = self._files[key]
            ctype = mimetypes.guess_type(member.name)[0] or "application/octet-stream"
            return ResourceInfo(is_dir=False, size=member.size, content_type=ctype)
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
            f = self._tf.extractfile(self._files[key])
            if f is None:
                raise BackendError(f"Cannot read {path} (may be a link or special file)")
            return f.read()
        except Exception as e:
            raise BackendError(f"Error reading from TAR: {e}") from e
