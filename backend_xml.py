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
from backend import Backend, ResourceInfo, NotFoundError, BackendError


def _strip_ns(tag: str) -> str:
    """Remove namespace from a tag like {http://...}name -> name."""
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
            name_counts[base] = name_counts.get(base, 0) + 1

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


class XmlBackend(Backend):
    """Expose an XML file as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            tree = ET.parse(path)
        except (ET.ParseError, FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot read XML file: {e}") from e
        self._root = _XmlNode(tree.getroot())

    def _resolve_node(self, path: list[str]) -> _XmlNode:
        """Resolve a path to an _XmlNode, skipping _text and _attribs segments."""
        node = self._root
        for part in path:
            if part in ("_text", "_attribs"):
                raise NotFoundError("Special entries are not traversable as nodes")
            if part not in node.children:
                raise NotFoundError(f"Not found: {path}")
            node = node.children[part]
        return node

    def info(self, path: list[str]) -> ResourceInfo:
        if len(path) == 0:
            return ResourceInfo(is_dir=True)

        last = path[-1]
        parent_path = path[:-1]

        if last == "_text":
            parent = self._resolve_node(parent_path)
            if parent.text is None:
                raise NotFoundError(f"Not found: {path}")
            return ResourceInfo(is_dir=False, size=len(parent.text.encode("utf-8")), content_type="text/plain")

        if last == "_attribs":
            parent = self._resolve_node(parent_path)
            if not parent.attribs:
                raise NotFoundError(f"Not found: {path}")
            return ResourceInfo(is_dir=True)

        if len(path) >= 2 and path[-2] == "_attribs":
            node_path = path[:-2]
            node = self._resolve_node(node_path)
            attr_name = last
            if not node.attribs or attr_name not in node.attribs:
                raise NotFoundError(f"Not found: {path}")
            return ResourceInfo(is_dir=False, size=len(node.attribs[attr_name].encode("utf-8")), content_type="text/plain")

        node = self._resolve_node(path)
        return ResourceInfo(is_dir=True)

    def list(self, path: list[str]) -> list[str]:
        if len(path) > 0 and path[-1] == "_attribs":
            parent = self._resolve_node(path[:-1])
            if not parent.attribs:
                raise NotFoundError(f"Not a directory: {path}")
            return sorted(parent.attribs.keys())

        node = self._resolve_node(path)
        entries = []
        if node.text is not None:
            entries.append("_text")
        if node.attribs:
            entries.append("_attribs")
        entries.extend(sorted(node.children.keys()))
        return entries

    def get(self, path: list[str]) -> bytes:
        if len(path) == 0:
            raise NotFoundError(f"Not a file: {path}")

        last = path[-1]

        if last == "_text":
            parent = self._resolve_node(path[:-1])
            if parent.text is None:
                raise NotFoundError(f"Not found: {path}")
            return parent.text.encode("utf-8")

        if len(path) >= 2 and path[-2] == "_attribs":
            node = self._resolve_node(path[:-2])
            if not node.attribs or last not in node.attribs:
                raise NotFoundError(f"Not found: {path}")
            return node.attribs[last].encode("utf-8")

        raise NotFoundError(f"Not a file: {path}")
