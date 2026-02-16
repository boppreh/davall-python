"""TAR archive backend — mount .tar, .tar.gz, .tar.bz2, .tar.xz files."""

import tarfile
import mimetypes
from backend import Backend, ResourceInfo, NotFoundError, BackendError, _normalize


class TarBackend(Backend):
    """Expose the contents of a TAR archive as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            self._tf = tarfile.open(path, "r:*")
        except (tarfile.TarError, FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot open TAR file: {e}") from e

        self._files: dict[str, tarfile.TarInfo] = {}
        self._dirs: set[str] = {"/"}

        for member in self._tf.getmembers():
            name = "/" + member.name
            norm = _normalize(name)
            if member.isdir():
                self._dirs.add(norm)
            else:
                self._files[norm] = member
                # Ensure parent directories exist
                parts = norm.strip("/").split("/")
                for i in range(1, len(parts)):
                    self._dirs.add("/" + "/".join(parts[:i]))

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)
        if path in self._dirs:
            return ResourceInfo(is_dir=True)
        if path in self._files:
            member = self._files[path]
            ctype = mimetypes.guess_type(member.name)[0] or "application/octet-stream"
            return ResourceInfo(is_dir=False, size=member.size, content_type=ctype)
        raise NotFoundError(f"Not found: {path}")

    def list(self, path: str) -> list[str]:
        path = _normalize(path)
        if path not in self._dirs:
            raise NotFoundError(f"Not a directory: {path}")

        prefix = path if path == "/" else path + "/"
        children = set()
        for fpath in self._files:
            if fpath.startswith(prefix):
                rest = fpath[len(prefix):]
                if "/" not in rest:
                    children.add(rest)
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
            f = self._tf.extractfile(self._files[path])
            if f is None:
                raise BackendError(f"Cannot read {path} (may be a link or special file)")
            return f.read()
        except Exception as e:
            raise BackendError(f"Error reading from TAR: {e}") from e
