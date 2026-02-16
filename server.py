"""Read-only WebDAV server built on http.server."""

import io
import json
import zipfile
import xml.etree.ElementTree as ET
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import unquote, quote, urlparse, parse_qs

from backend import Backend, NotFoundError, BackendError


DAV_NS = "DAV:"
SUPPORTED_PROPS = [
    "displayname",
    "getcontentlength",
    "getcontenttype",
    "resourcetype",
    "getlastmodified",
]


def _parse_path(raw: str) -> list[str]:
    """Parse a URL path into a list of segments. Handles decoding, slashes, dots."""
    decoded = unquote(raw)
    return [p for p in decoded.split("/") if p]


def _to_href(path: list[str], is_dir: bool) -> str:
    """Convert a path list back to a URL-safe href string."""
    href = "/" + "/".join(quote(p, safe="") for p in path)
    if is_dir and not href.endswith("/"):
        href += "/"
    return href


def _build_response_element(href: str, info, include_props: list[str] | None = None) -> ET.Element:
    """Build a DAV:response element for a resource."""
    response = ET.Element(f"{{{DAV_NS}}}response")

    href_el = ET.SubElement(response, f"{{{DAV_NS}}}href")
    href_el.text = href

    propstat = ET.SubElement(response, f"{{{DAV_NS}}}propstat")
    prop = ET.SubElement(propstat, f"{{{DAV_NS}}}prop")

    props_to_report = include_props if include_props is not None else SUPPORTED_PROPS

    for pname in props_to_report:
        if pname == "displayname":
            el = ET.SubElement(prop, f"{{{DAV_NS}}}displayname")
            name = href.rstrip("/").rsplit("/", 1)[-1] or "/"
            el.text = unquote(name)
        elif pname == "getcontentlength":
            if not info.is_dir:
                el = ET.SubElement(prop, f"{{{DAV_NS}}}getcontentlength")
                el.text = str(info.size)
        elif pname == "getcontenttype":
            if not info.is_dir:
                el = ET.SubElement(prop, f"{{{DAV_NS}}}getcontenttype")
                el.text = info.content_type
        elif pname == "resourcetype":
            rt = ET.SubElement(prop, f"{{{DAV_NS}}}resourcetype")
            if info.is_dir:
                ET.SubElement(rt, f"{{{DAV_NS}}}collection")
        elif pname == "getlastmodified":
            el = ET.SubElement(prop, f"{{{DAV_NS}}}getlastmodified")
            el.text = "Thu, 01 Jan 1970 00:00:00 GMT"

    status = ET.SubElement(propstat, f"{{{DAV_NS}}}status")
    status.text = "HTTP/1.1 200 OK"

    return response


def _multistatus_xml(responses: list[ET.Element]) -> bytes:
    """Wrap response elements in a multistatus document and serialize."""
    ET.register_namespace("D", DAV_NS)
    ms = ET.Element(f"{{{DAV_NS}}}multistatus")
    for r in responses:
        ms.append(r)

    buf = io.BytesIO()
    tree = ET.ElementTree(ms)
    tree.write(buf, xml_declaration=True, encoding="utf-8")
    return buf.getvalue()


def _parse_propfind_body(body: bytes) -> list[str] | None:
    """Parse a PROPFIND request body to determine requested properties.

    Returns None for allprop (or empty body), or a list of property local names.
    """
    if not body or not body.strip():
        return None

    root = ET.fromstring(body)
    if root.find(f"{{{DAV_NS}}}allprop") is not None:
        return None

    prop_el = root.find(f"{{{DAV_NS}}}prop")
    if prop_el is None:
        return None

    props = []
    for child in prop_el:
        tag = child.tag
        if tag.startswith(f"{{{DAV_NS}}}"):
            tag = tag[len(f"{{{DAV_NS}}}"):]
        props.append(tag)
    return props


class WebDAVHandler(BaseHTTPRequestHandler):
    """HTTP request handler for read-only WebDAV."""

    backend: Backend

    def log_message(self, format, *args):
        pass

    def _send(self, status: int, body: bytes, content_type: str, include_body: bool = True):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def _parse_request_path(self) -> tuple[list[str], str | None]:
        """Parse the request URL into (path_segments, dump_format).

        dump_format is "json", "zip", or None.
        """
        parsed = urlparse(self.path)
        path = _parse_path(parsed.path)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        if "json" in qs:
            return path, "json"
        if "zip" in qs:
            return path, "zip"
        return path, None

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Allow", "OPTIONS, GET, HEAD, PROPFIND")
        self.send_header("DAV", "1")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        self._handle_get(include_body=True)

    def do_HEAD(self):
        self._handle_get(include_body=False)

    def _build_json_subtree(self, path: list[str]):
        """Recursively build a JSON-serializable subtree from a path."""
        info = self.backend.info(path)
        if not info.is_dir:
            data = self.backend.get(path)
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                return None

        children = self.backend.list(path)
        result = {}
        for name in children:
            result[name] = self._build_json_subtree(path + [name])
        return result

    def _build_zip_subtree(self, path: list[str]) -> bytes:
        """Recursively build a ZIP archive from a path."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            info = self.backend.info(path)
            if not info.is_dir:
                data = self.backend.get(path)
                zf.writestr(path[-1] if path else "data", data)
            else:
                self._zip_recurse(zf, path)
        return buf.getvalue()

    def _zip_recurse(self, zf: zipfile.ZipFile, base: list[str], rel: list[str] = []):
        """Add all files under base to the ZIP archive with rel as prefix."""
        for name in self.backend.list(base):
            child = base + [name]
            child_rel = rel + [name]
            info = self.backend.info(child)
            if info.is_dir:
                self._zip_recurse(zf, child, child_rel)
            else:
                zf.writestr("/".join(child_rel), self.backend.get(child))

    def _try(self, fn, include_body: bool = True):
        """Call fn(), returning its result. On backend errors, send an error response and return None."""
        try:
            return fn()
        except NotFoundError:
            self._send(404, b"Not Found", "text/plain", include_body)
            return None
        except BackendError as e:
            self._send(500, str(e).encode(), "text/plain", include_body)
            return None

    def _handle_get(self, include_body: bool):
        path, dump_format = self._parse_request_path()

        if dump_format == "json":
            tree = self._try(lambda: self._build_json_subtree(path), include_body)
            if tree is None:
                return
            body = json.dumps(tree, indent=2, ensure_ascii=False).encode("utf-8")
            return self._send(200, body, "application/json; charset=utf-8", include_body)

        if dump_format == "zip":
            body = self._try(lambda: self._build_zip_subtree(path), include_body)
            if body is None:
                return
            return self._send(200, body, "application/zip", include_body)

        info = self._try(lambda: self.backend.info(path), include_body)
        if info is None:
            return

        if info.is_dir:
            children = self._try(lambda: self.backend.list(path), include_body)
            if children is None:
                return
            dir_name = "/" + "/".join(path) if path else "/"
            lines = [f"<html><head><title>{dir_name}</title></head><body>"]
            lines.append(f"<h1>{dir_name}</h1><ul>")
            if path:
                lines.append('<li><a href="../">..</a></li>')
            for name in children:
                try:
                    child_info = self.backend.info(path + [name])
                    href = quote(name, safe="") + ("/" if child_info.is_dir else "")
                except BackendError:
                    href = quote(name, safe="")
                lines.append(f'<li><a href="{href}">{name}</a></li>')
            lines.append("</ul></body></html>")
            body = "\n".join(lines).encode("utf-8")
            return self._send(200, body, "text/html; charset=utf-8", include_body)
        else:
            data = self._try(lambda: self.backend.get(path), include_body)
            if data is None:
                return
            return self._send(200, data, info.content_type, include_body)

    def do_PROPFIND(self):
        path, _ = self._parse_request_path()
        depth = self.headers.get("Depth", "1")

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b""
        requested_props = _parse_propfind_body(body)

        info = self._try(lambda: self.backend.info(path))
        if info is None:
            return

        responses = []
        responses.append(_build_response_element(_to_href(path, info.is_dir), info, requested_props))

        if info.is_dir and depth != "0":
            children = self._try(lambda: self.backend.list(path))
            if children is None:
                return

            for name in children:
                child_path = path + [name]
                try:
                    child_info = self.backend.info(child_path)
                except BackendError:
                    continue

                responses.append(_build_response_element(
                    _to_href(child_path, child_info.is_dir), child_info, requested_props
                ))

                if depth == "infinity" and child_info.is_dir:
                    self._propfind_recurse(child_path, responses, requested_props)

        xml_bytes = _multistatus_xml(responses)
        self._send(207, xml_bytes, "application/xml; charset=utf-8")

    def _propfind_recurse(self, dir_path: list[str], responses: list, requested_props):
        """Recursively add PROPFIND responses for all descendants."""
        try:
            children = self.backend.list(dir_path)
        except BackendError:
            return
        for name in children:
            child_path = dir_path + [name]
            try:
                child_info = self.backend.info(child_path)
            except BackendError:
                continue
            responses.append(_build_response_element(
                _to_href(child_path, child_info.is_dir), child_info, requested_props
            ))
            if child_info.is_dir:
                self._propfind_recurse(child_path, responses, requested_props)

    def _method_not_allowed(self):
        self.send_response(405)
        self.send_header("Allow", "OPTIONS, GET, HEAD, PROPFIND")
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_PUT = lambda self: self._method_not_allowed()
    do_DELETE = lambda self: self._method_not_allowed()
    do_MKCOL = lambda self: self._method_not_allowed()
    do_PROPPATCH = lambda self: self._method_not_allowed()
    do_MOVE = lambda self: self._method_not_allowed()
    do_COPY = lambda self: self._method_not_allowed()
    do_LOCK = lambda self: self._method_not_allowed()
    do_UNLOCK = lambda self: self._method_not_allowed()
    do_POST = lambda self: self._method_not_allowed()
    do_PATCH = lambda self: self._method_not_allowed()


def make_server(backend: Backend, host: str = "localhost", port: int = 8080) -> HTTPServer:
    """Create a WebDAV server for the given backend."""
    handler_class = type("Handler", (WebDAVHandler,), {"backend": backend})
    return HTTPServer((host, port), handler_class)
