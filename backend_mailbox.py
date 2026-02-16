"""Mailbox backend — mount an mbox file as a read-only filesystem.

Structure:
    /
      0001_Subject_Line.eml   — full RFC 822 message
      0002_Another_Subject.eml
"""

import mailbox
import re
from backend import Backend, ResourceInfo, NotFoundError, BackendError


def _safe_filename(s: str, max_len: int = 60) -> str:
    """Sanitize a string for use as a filename."""
    s = re.sub(r'[^\w\s\-.]', '', s)
    s = re.sub(r'\s+', '_', s.strip())
    return s[:max_len] if s else "no_subject"


class MailboxBackend(Backend):
    """Expose an mbox file as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            self._mbox = mailbox.mbox(path)
        except (FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot open mailbox: {e}") from e

        self._files: dict[str, int] = {}
        self._order: list[str] = []
        width = max(4, len(str(len(self._mbox))))

        for i, message in enumerate(self._mbox):
            subject = message.get("Subject", "no_subject")
            safe = _safe_filename(subject)
            name = f"{i:0{width}d}_{safe}.eml"
            self._files[name] = i
            self._order.append(name)

    def close(self):
        self._mbox.close()

    def _get_message_bytes(self, index: int) -> bytes:
        return self._mbox[index].as_bytes()

    def info(self, path: list[str]) -> ResourceInfo:
        if len(path) == 0:
            return ResourceInfo(is_dir=True)
        if len(path) == 1 and path[0] in self._files:
            data = self._get_message_bytes(self._files[path[0]])
            return ResourceInfo(is_dir=False, size=len(data), content_type="message/rfc822")
        raise NotFoundError(f"Not found: {path}")

    def list(self, path: list[str]) -> list[str]:
        if len(path) == 0:
            return list(self._order)
        raise NotFoundError(f"Not a directory: {path}")

    def get(self, path: list[str]) -> bytes:
        if len(path) == 1 and path[0] in self._files:
            return self._get_message_bytes(self._files[path[0]])
        raise NotFoundError(f"Not found: {path}")
