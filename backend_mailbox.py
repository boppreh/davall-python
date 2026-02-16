"""Mailbox backend — mount an mbox file as a read-only filesystem.

Structure:
    /
      0001_Subject_Line.eml   — full RFC 822 message
      0002_Another_Subject.eml
"""

import mailbox
import re
from backend import Backend, ResourceInfo, NotFoundError, BackendError, _normalize


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

        # Build index: filename -> (index, message bytes)
        self._files: dict[str, int] = {}
        self._order: list[str] = []
        width = max(4, len(str(len(self._mbox))))

        for i, message in enumerate(self._mbox):
            subject = message.get("Subject", "no_subject")
            safe = _safe_filename(subject)
            name = f"{i:0{width}d}_{safe}.eml"
            self._files[name] = i
            self._order.append(name)

    def _get_message_bytes(self, index: int) -> bytes:
        msg = self._mbox[index]
        return msg.as_bytes()

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)
        if path == "/":
            return ResourceInfo(is_dir=True)
        name = path.strip("/")
        if "/" in name:
            raise NotFoundError(f"Not found: {path}")
        if name not in self._files:
            raise NotFoundError(f"Not found: {path}")
        data = self._get_message_bytes(self._files[name])
        return ResourceInfo(is_dir=False, size=len(data), content_type="message/rfc822")

    def list(self, path: str) -> list[str]:
        path = _normalize(path)
        if path != "/":
            raise NotFoundError(f"Not a directory: {path}")
        return list(self._order)

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        name = path.strip("/")
        if name not in self._files:
            raise NotFoundError(f"Not found: {path}")
        return self._get_message_bytes(self._files[name])
