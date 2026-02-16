"""OS information backend — expose system information as a read-only filesystem.

Structure:
    /
      platform/
        system           — e.g. "Linux"
        release          — e.g. "6.17.7"
        version          — full version string
        machine          — e.g. "x86_64"
        processor        — processor name
        python_version   — e.g. "3.14.2"
        node             — hostname
      env/
        HOME             — environment variable values
        PATH
        ...
      cpu/
        count            — number of CPUs
      cwd                — current working directory
      pid                — current process ID
      uid                — current user ID (Unix only)
"""

import os
import platform
from backend import Backend, ResourceInfo, NotFoundError


def _build_tree() -> dict[str, dict | str]:
    """Build the info tree from current system state."""
    tree: dict[str, dict | str] = {}

    tree["platform"] = {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "processor": platform.processor() or "unknown",
        "python_version": platform.python_version(),
        "node": platform.node(),
    }

    tree["env"] = dict(os.environ)

    cpu_info: dict[str, str] = {}
    count = os.cpu_count()
    if count is not None:
        cpu_info["count"] = str(count)
    if cpu_info:
        tree["cpu"] = cpu_info

    tree["cwd"] = os.getcwd()
    tree["pid"] = str(os.getpid())

    if hasattr(os, "getuid"):
        tree["uid"] = str(os.getuid())

    return tree


class OsInfoBackend(Backend):
    """Expose OS and system information as a read-only filesystem."""

    def __init__(self):
        self._tree = _build_tree()

    def _resolve(self, path: list[str]):
        node = self._tree
        for part in path:
            if not isinstance(node, dict) or part not in node:
                raise NotFoundError(f"Not found: {path}")
            node = node[part]
        return node

    def info(self, path: list[str]) -> ResourceInfo:
        node = self._resolve(path)
        if isinstance(node, dict):
            return ResourceInfo(is_dir=True)
        data = str(node).encode("utf-8")
        return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")

    def list(self, path: list[str]) -> list[str]:
        node = self._resolve(path)
        if isinstance(node, dict):
            return sorted(node.keys())
        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: list[str]) -> bytes:
        node = self._resolve(path)
        if isinstance(node, dict):
            raise NotFoundError(f"Not a file: {path}")
        return str(node).encode("utf-8")
