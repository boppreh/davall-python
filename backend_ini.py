"""INI/Config file backend — mount a .ini/.cfg file as a read-only filesystem.

Structure:
    /
      section_name/
        key1        — file containing the value
        key2
"""

import configparser
from backend import Backend, ResourceInfo, NotFoundError, BackendError, _normalize


class IniBackend(Backend):
    """Expose an INI/config file as a read-only filesystem."""

    def __init__(self, path: str):
        self._config = configparser.ConfigParser()
        try:
            with open(path, "r", encoding="utf-8") as f:
                self._config.read_file(f)
        except (FileNotFoundError, OSError, configparser.Error) as e:
            raise BackendError(f"Cannot read INI file: {e}") from e

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)
        if path == "/":
            return ResourceInfo(is_dir=True)

        parts = path.strip("/").split("/")
        if len(parts) == 1:
            section = parts[0]
            if self._config.has_section(section):
                return ResourceInfo(is_dir=True)
            raise NotFoundError(f"Not found: {path}")

        if len(parts) == 2:
            section, key = parts
            if self._config.has_section(section) and self._config.has_option(section, key):
                value = self._config.get(section, key).encode("utf-8")
                return ResourceInfo(is_dir=False, size=len(value), content_type="text/plain")
            raise NotFoundError(f"Not found: {path}")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: str) -> list[str]:
        path = _normalize(path)
        if path == "/":
            return sorted(self._config.sections())

        parts = path.strip("/").split("/")
        if len(parts) == 1:
            section = parts[0]
            if not self._config.has_section(section):
                raise NotFoundError(f"Not a directory: {path}")
            return sorted(self._config.options(section))

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        parts = path.strip("/").split("/")

        if len(parts) == 2:
            section, key = parts
            if self._config.has_section(section) and self._config.has_option(section, key):
                return self._config.get(section, key).encode("utf-8")

        raise NotFoundError(f"Not found: {path}")
