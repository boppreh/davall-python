"""INI/Config file backend — mount a .ini/.cfg file as a read-only filesystem.

Structure:
    /
      section_name/
        key1        — file containing the value
        key2
"""

import configparser
from backend import Backend, ResourceInfo, NotFoundError, BackendError


class IniBackend(Backend):
    """Expose an INI/config file as a read-only filesystem."""

    def __init__(self, path: str):
        self._config = configparser.ConfigParser()
        try:
            with open(path, "r", encoding="utf-8") as f:
                self._config.read_file(f)
        except (FileNotFoundError, OSError, configparser.Error) as e:
            raise BackendError(f"Cannot read INI file: {e}") from e

    def info(self, path: list[str]) -> ResourceInfo:
        if len(path) == 0:
            return ResourceInfo(is_dir=True)

        if len(path) == 1:
            if self._config.has_section(path[0]):
                return ResourceInfo(is_dir=True)
            raise NotFoundError(f"Not found: {path}")

        if len(path) == 2:
            section, key = path
            if self._config.has_section(section) and self._config.has_option(section, key):
                value = self._config.get(section, key).encode("utf-8")
                return ResourceInfo(is_dir=False, size=len(value), content_type="text/plain")
            raise NotFoundError(f"Not found: {path}")

        raise NotFoundError(f"Not found: {path}")

    def list(self, path: list[str]) -> list[str]:
        if len(path) == 0:
            return sorted(self._config.sections())

        if len(path) == 1:
            if not self._config.has_section(path[0]):
                raise NotFoundError(f"Not a directory: {path}")
            return sorted(self._config.options(path[0]))

        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: list[str]) -> bytes:
        if len(path) == 2:
            section, key = path
            if self._config.has_section(section) and self._config.has_option(section, key):
                return self._config.get(section, key).encode("utf-8")

        raise NotFoundError(f"Not found: {path}")
