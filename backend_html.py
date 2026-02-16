"""HTML file backend — mount an .html file as a read-only filesystem.

Structure:
    /
      head/
        title/
          _text
        meta_0/
          _attribs/
            charset
      body/
        div/
          _text
          _attribs/
            class
          p/
            _text

Similar to the XML backend: elements become directories, text content
is in _text files, attributes are under _attribs/ directories.
Duplicate tags get numeric suffixes.
"""

from html.parser import HTMLParser
from backend import Backend, ResourceInfo, NotFoundError, BackendError


class _HtmlNode:
    """Intermediate representation of an HTML element."""

    def __init__(self, tag: str):
        self.tag = tag
        self.text: str | None = None
        self.attribs: dict[str, str] | None = None
        self.children: dict[str, "_HtmlNode"] = {}
        self._name_counts: dict[str, int] = {}

    def add_child(self, node: "_HtmlNode") -> str:
        base = node.tag
        count = self._name_counts.get(base, 0)
        self._name_counts[base] = count + 1
        return base, count

    def finalize_names(self):
        """Rename children: add suffixes only for tags that appeared more than once."""
        new_children: dict[str, _HtmlNode] = {}
        name_used: dict[str, int] = {}
        needs_suffix = {name for name, count in self._name_counts.items() if count > 1}

        # Rebuild in insertion order
        for _old_name, child in self.children.items():
            base = child.tag
            if base in needs_suffix:
                idx = name_used.get(base, 0)
                name_used[base] = idx + 1
                name = f"{base}_{idx}"
            else:
                name = base
            new_children[name] = child
            child.finalize_names()

        self.children = new_children


class _TreeBuilder(HTMLParser):
    """Build an _HtmlNode tree from HTML."""

    def __init__(self):
        super().__init__()
        self.root = _HtmlNode("document")
        self._stack: list[_HtmlNode] = [self.root]

    def handle_starttag(self, tag, attrs):
        node = _HtmlNode(tag)
        if attrs:
            node.attribs = dict(attrs)
        parent = self._stack[-1]
        parent.add_child(node)
        parent.children[f"_tmp_{id(node)}"] = node
        self._stack.append(node)

    def handle_endtag(self, tag):
        # Pop back to matching tag, tolerating mismatches
        for i in range(len(self._stack) - 1, 0, -1):
            if self._stack[i].tag == tag:
                self._stack = self._stack[:i]
                return

    def handle_data(self, data):
        text = data.strip()
        if text and self._stack:
            node = self._stack[-1]
            if node.text:
                node.text += " " + text
            else:
                node.text = text

    def handle_startendtag(self, tag, attrs):
        node = _HtmlNode(tag)
        if attrs:
            node.attribs = dict(attrs)
        parent = self._stack[-1]
        parent.add_child(node)
        parent.children[f"_tmp_{id(node)}"] = node


class HtmlBackend(Backend):
    """Expose an HTML file as a read-only filesystem."""

    def __init__(self, path: str):
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
        except (FileNotFoundError, OSError) as e:
            raise BackendError(f"Cannot read HTML file: {e}") from e

        builder = _TreeBuilder()
        builder.feed(content)
        self._root = builder.root
        self._root.finalize_names()

    def _resolve_node(self, path: list[str]) -> _HtmlNode:
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
            node = self._resolve_node(path[:-2])
            if not node.attribs or last not in node.attribs:
                raise NotFoundError(f"Not found: {path}")
            return ResourceInfo(is_dir=False, size=len(node.attribs[last].encode("utf-8")), content_type="text/plain")

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
