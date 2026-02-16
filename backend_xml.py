"""XML file backend — mount an .xml file as a read-only filesystem.

Structure:
    /
      tag_name/
        _text            — text content (if any)
        _attribs/        — directory of attributes (if any)
          attr_name      — file containing attribute value
        child_tag/       — child elements
        child_tag_1/     — duplicate tags get numeric suffixes
"""

import xml.etree.ElementTree as ET
from backend import Backend, ResourceInfo, NotFoundError, BackendError, _normalize


def _strip_ns(tag: str) -> str:
    """Remove namespace from a tag like {http://...}name → name."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


class _XmlNode:
    """Intermediate representation of an XML element as a virtual directory."""

    def __init__(self, element: ET.Element):
        self.tag = _strip_ns(element.tag)
        self.text = (element.text or "").strip() or None
        self.attribs = dict(element.attrib) if element.attrib else None

        # Build children with disambiguated names
        self.children: dict[str, "_XmlNode"] = {}
        name_counts: dict[str, int] = {}
        for child_el in element:
            base = _strip_ns(child_el.tag)
            count = name_counts.get(base, 0)
            name_counts[base] = count + 1

        # Second pass: assign names
        name_used: dict[str, int] = {}
        needs_suffix = {name for name, count in name_counts.items() if count > 1}
        for child_el in element:
            base = _strip_ns(child_el.tag)
            if base in needs_suffix:
                idx = name_used.get(base, 0)
                name_used[base] = idx + 1
                name = f"{base}_{idx}"
            else:
                name = base
            self.children[name] = _XmlNode(child_el)


_SPECIAL_FILES = ("_text",)
_SPECIAL_DIRS = ("_attribs",)


class XmlBackend(Backend):
    """Expose an XML file as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            tree = ET.parse(path)
        except (ET.ParseError, FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot read XML file: {e}") from e
        self._root = _XmlNode(tree.getroot())

    def _resolve(self, path: str) -> _XmlNode:
        path = _normalize(path)
        if path == "/":
            return self._root
        parts = path.strip("/").split("/")
        node = self._root
        for part in parts:
            if part in _SPECIAL_FILES or part in _SPECIAL_DIRS:
                raise NotFoundError("Special entries are not traversable as nodes")
            if part not in node.children:
                raise NotFoundError(f"Not found: {path}")
            node = node.children[part]
        return node

    def info(self, path: str) -> ResourceInfo:
        path = _normalize(path)
        parts = path.strip("/").split("/")

        # Check for _text file
        if parts[-1] == "_text":
            parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
            parent = self._resolve(parent_path)
            if parent.text is None:
                raise NotFoundError(f"Not found: {path}")
            data = parent.text.encode("utf-8")
            return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")

        # Check for _attribs directory
        if parts[-1] == "_attribs":
            parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
            parent = self._resolve(parent_path)
            if not parent.attribs:
                raise NotFoundError(f"Not found: {path}")
            return ResourceInfo(is_dir=True)

        # Check for _attribs/attr_name file
        if len(parts) >= 2 and parts[-2] == "_attribs":
            grandparent_path = "/" + "/".join(parts[:-2]) if len(parts) > 2 else "/"
            grandparent = self._resolve(grandparent_path)
            attr_name = parts[-1]
            if not grandparent.attribs or attr_name not in grandparent.attribs:
                raise NotFoundError(f"Not found: {path}")
            data = grandparent.attribs[attr_name].encode("utf-8")
            return ResourceInfo(is_dir=False, size=len(data), content_type="text/plain")

        node = self._resolve(path)
        return ResourceInfo(is_dir=True)

    def list(self, path: str) -> list[str]:
        path = _normalize(path)
        parts = path.strip("/").split("/")

        # Check for _attribs directory listing
        if parts[-1] == "_attribs":
            parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
            parent = self._resolve(parent_path)
            if not parent.attribs:
                raise NotFoundError(f"Not a directory: {path}")
            return sorted(parent.attribs.keys())

        node = self._resolve(path)
        entries = []
        if node.text is not None:
            entries.append("_text")
        if node.attribs:
            entries.append("_attribs")
        entries.extend(sorted(node.children.keys()))
        return entries

    def get(self, path: str) -> bytes:
        path = _normalize(path)
        parts = path.strip("/").split("/")

        if parts[-1] == "_text":
            parent_path = "/" + "/".join(parts[:-1]) if len(parts) > 1 else "/"
            parent = self._resolve(parent_path)
            if parent.text is None:
                raise NotFoundError(f"Not found: {path}")
            return parent.text.encode("utf-8")

        # _attribs/attr_name
        if len(parts) >= 2 and parts[-2] == "_attribs":
            grandparent_path = "/" + "/".join(parts[:-2]) if len(parts) > 2 else "/"
            grandparent = self._resolve(grandparent_path)
            attr_name = parts[-1]
            if not grandparent.attribs or attr_name not in grandparent.attribs:
                raise NotFoundError(f"Not found: {path}")
            return grandparent.attribs[attr_name].encode("utf-8")

        raise NotFoundError(f"Not a file: {path}")
