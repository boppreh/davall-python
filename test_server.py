"""Tests for the WebDAV server and MemoryBackend."""

import io
import json
import threading
import unittest
import xml.etree.ElementTree as ET
import zipfile
from http.client import HTTPConnection

from backend import MemoryBackend, NotFoundError, ResourceInfo
from server import make_server, DAV_NS


SAMPLE_TREE = {
    "hello.txt": "Hello, world!",
    "empty.txt": "",
    "binary.bin": b"\x00\x01\x02\x03",
    "docs": {
        "guide.txt": "A guide to things",
        "nested": {
            "deep.txt": "Deep content",
        },
    },
}


class TestMemoryBackend(unittest.TestCase):
    def setUp(self):
        self.backend = MemoryBackend(SAMPLE_TREE)

    def test_root_is_dir(self):
        info = self.backend.info([])
        self.assertTrue(info.is_dir)

    def test_list_root(self):
        children = self.backend.list([])
        self.assertEqual(children, ["binary.bin", "docs", "empty.txt", "hello.txt"])

    def test_get_file(self):
        self.assertEqual(self.backend.get(["hello.txt"]), b"Hello, world!")

    def test_get_binary(self):
        self.assertEqual(self.backend.get(["binary.bin"]), b"\x00\x01\x02\x03")

    def test_get_empty(self):
        self.assertEqual(self.backend.get(["empty.txt"]), b"")

    def test_info_file(self):
        info = self.backend.info(["hello.txt"])
        self.assertFalse(info.is_dir)
        self.assertEqual(info.size, 13)

    def test_list_subdir(self):
        children = self.backend.list(["docs"])
        self.assertEqual(children, ["guide.txt", "nested"])

    def test_nested_file(self):
        self.assertEqual(self.backend.get(["docs", "guide.txt"]), b"A guide to things")

    def test_deep_nested(self):
        self.assertEqual(self.backend.get(["docs", "nested", "deep.txt"]), b"Deep content")

    def test_not_found(self):
        with self.assertRaises(NotFoundError):
            self.backend.info(["nonexistent"])

    def test_get_dir_raises(self):
        with self.assertRaises(NotFoundError):
            self.backend.get(["docs"])

    def test_list_file_raises(self):
        with self.assertRaises(NotFoundError):
            self.backend.list(["hello.txt"])


class TestWebDAVServer(unittest.TestCase):
    """Integration tests against a live WebDAV server."""

    @classmethod
    def setUpClass(cls):
        cls.backend = MemoryBackend(SAMPLE_TREE)
        cls.server = make_server(cls.backend, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever)
        cls.thread.daemon = True
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def _conn(self) -> HTTPConnection:
        return HTTPConnection("127.0.0.1", self.port)

    def _propfind(self, path: str, depth: str = "1", body: bytes = b"") -> tuple:
        conn = self._conn()
        headers = {"Depth": depth}
        if body:
            headers["Content-Type"] = "application/xml"
            headers["Content-Length"] = str(len(body))
        conn.request("PROPFIND", path, body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        if resp.status == 207:
            return resp.status, ET.fromstring(data)
        return resp.status, data

    # --- OPTIONS ---

    def test_options(self):
        conn = self._conn()
        conn.request("OPTIONS", "/")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertIn("PROPFIND", resp.getheader("Allow"))
        self.assertEqual(resp.getheader("DAV"), "1")

    # --- GET ---

    def test_get_file(self):
        conn = self._conn()
        conn.request("GET", "/hello.txt")
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertEqual(data, b"Hello, world!")

    def test_get_dir_listing(self):
        conn = self._conn()
        conn.request("GET", "/docs")
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertIn(b"guide.txt", data)

    def test_get_not_found(self):
        conn = self._conn()
        conn.request("GET", "/nonexistent")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        self.assertEqual(resp.status, 404)

    # --- Path normalization at server level ---

    def test_trailing_slash(self):
        conn = self._conn()
        conn.request("GET", "/docs/")
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertIn(b"guide.txt", data)

    def test_double_slash(self):
        conn = self._conn()
        conn.request("GET", "//hello.txt")
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertEqual(data, b"Hello, world!")

    # --- HEAD ---

    def test_head_file(self):
        conn = self._conn()
        conn.request("HEAD", "/hello.txt")
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader("Content-Length"), "13")
        self.assertEqual(data, b"")

    # --- PROPFIND ---

    def test_propfind_root_depth0(self):
        status, xml = self._propfind("/", depth="0")
        self.assertEqual(status, 207)
        responses = xml.findall(f"{{{DAV_NS}}}response")
        self.assertEqual(len(responses), 1)
        rt = responses[0].find(f".//{{{DAV_NS}}}resourcetype")
        self.assertIsNotNone(rt.find(f"{{{DAV_NS}}}collection"))

    def test_propfind_root_depth1(self):
        status, xml = self._propfind("/", depth="1")
        self.assertEqual(status, 207)
        responses = xml.findall(f"{{{DAV_NS}}}response")
        self.assertEqual(len(responses), 5)

    def test_propfind_file(self):
        status, xml = self._propfind("/hello.txt", depth="0")
        self.assertEqual(status, 207)
        responses = xml.findall(f"{{{DAV_NS}}}response")
        self.assertEqual(len(responses), 1)
        cl = responses[0].find(f".//{{{DAV_NS}}}getcontentlength")
        self.assertIsNotNone(cl)
        self.assertEqual(cl.text, "13")
        rt = responses[0].find(f".//{{{DAV_NS}}}resourcetype")
        self.assertIsNone(rt.find(f"{{{DAV_NS}}}collection"))

    def test_propfind_subdir(self):
        status, xml = self._propfind("/docs", depth="1")
        self.assertEqual(status, 207)
        responses = xml.findall(f"{{{DAV_NS}}}response")
        self.assertEqual(len(responses), 3)

    def test_propfind_depth_infinity(self):
        status, xml = self._propfind("/", depth="infinity")
        self.assertEqual(status, 207)
        responses = xml.findall(f"{{{DAV_NS}}}response")
        self.assertEqual(len(responses), 8)

    def test_propfind_not_found(self):
        status, _ = self._propfind("/nonexistent", depth="0")
        self.assertEqual(status, 404)

    def test_propfind_allprop(self):
        body = b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:allprop/></D:propfind>'
        status, xml = self._propfind("/hello.txt", depth="0", body=body)
        self.assertEqual(status, 207)

    def test_propfind_specific_props(self):
        body = b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:displayname/><D:getcontentlength/></D:prop></D:propfind>'
        status, xml = self._propfind("/hello.txt", depth="0", body=body)
        self.assertEqual(status, 207)
        responses = xml.findall(f"{{{DAV_NS}}}response")
        prop = responses[0].find(f".//{{{DAV_NS}}}prop")
        self.assertIsNotNone(prop.find(f"{{{DAV_NS}}}displayname"))
        self.assertIsNotNone(prop.find(f"{{{DAV_NS}}}getcontentlength"))
        self.assertIsNone(prop.find(f"{{{DAV_NS}}}resourcetype"))

    # --- Write methods should be rejected ---

    def test_put_rejected(self):
        conn = self._conn()
        conn.request("PUT", "/new.txt", body=b"data")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        self.assertEqual(resp.status, 405)

    def test_delete_rejected(self):
        conn = self._conn()
        conn.request("DELETE", "/hello.txt")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        self.assertEqual(resp.status, 405)

    def test_mkcol_rejected(self):
        conn = self._conn()
        conn.request("MKCOL", "/newdir")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        self.assertEqual(resp.status, 405)

    def test_lock_rejected(self):
        conn = self._conn()
        conn.request("LOCK", "/hello.txt")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        self.assertEqual(resp.status, 405)

    # --- ?json subtree ---

    def _get_json(self, path: str) -> dict | str:
        conn = self._conn()
        conn.request("GET", path)
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertIn("application/json", resp.getheader("Content-Type"))
        return json.loads(data)

    def test_json_file(self):
        result = self._get_json("/hello.txt?json")
        self.assertEqual(result, "Hello, world!")

    def test_json_root(self):
        result = self._get_json("/?json")
        self.assertIsInstance(result, dict)
        self.assertEqual(result["hello.txt"], "Hello, world!")
        self.assertEqual(result["empty.txt"], "")
        self.assertIsInstance(result["docs"], dict)
        self.assertEqual(result["docs"]["guide.txt"], "A guide to things")

    def test_json_subdir(self):
        result = self._get_json("/docs?json")
        self.assertIsInstance(result, dict)
        self.assertEqual(result["guide.txt"], "A guide to things")
        self.assertIsInstance(result["nested"], dict)
        self.assertEqual(result["nested"]["deep.txt"], "Deep content")

    def test_json_nested_subdir(self):
        result = self._get_json("/docs/nested?json")
        self.assertEqual(result, {"deep.txt": "Deep content"})

    def test_json_not_found(self):
        conn = self._conn()
        conn.request("GET", "/nonexistent?json")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        self.assertEqual(resp.status, 404)

    def test_json_binary_file(self):
        result = self._get_json("/binary.bin?json")
        self.assertIsInstance(result, str)

    def test_json_head(self):
        conn = self._conn()
        conn.request("HEAD", "/docs?json")
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertIn("application/json", resp.getheader("Content-Type"))
        self.assertEqual(data, b"")
        self.assertGreater(int(resp.getheader("Content-Length")), 0)


    # --- ?zip subtree ---

    def _get_zip(self, path: str) -> zipfile.ZipFile:
        conn = self._conn()
        conn.request("GET", path)
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader("Content-Type"), "application/zip")
        return zipfile.ZipFile(io.BytesIO(data))

    def test_zip_root(self):
        with self._get_zip("/?zip") as zf:
            names = sorted(zf.namelist())
            self.assertIn("hello.txt", names)
            self.assertIn("docs/guide.txt", names)
            self.assertIn("docs/nested/deep.txt", names)
            self.assertEqual(zf.read("hello.txt"), b"Hello, world!")
            self.assertEqual(zf.read("docs/guide.txt"), b"A guide to things")

    def test_zip_subdir(self):
        with self._get_zip("/docs?zip") as zf:
            names = sorted(zf.namelist())
            self.assertIn("guide.txt", names)
            self.assertIn("nested/deep.txt", names)
            self.assertEqual(zf.read("guide.txt"), b"A guide to things")

    def test_zip_file(self):
        with self._get_zip("/hello.txt?zip") as zf:
            self.assertEqual(zf.namelist(), ["hello.txt"])
            self.assertEqual(zf.read("hello.txt"), b"Hello, world!")

    def test_zip_binary(self):
        with self._get_zip("/binary.bin?zip") as zf:
            self.assertEqual(zf.read("binary.bin"), b"\x00\x01\x02\x03")

    def test_zip_not_found(self):
        conn = self._conn()
        conn.request("GET", "/nonexistent?zip")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        self.assertEqual(resp.status, 404)

    def test_zip_head(self):
        conn = self._conn()
        conn.request("HEAD", "/docs?zip")
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader("Content-Type"), "application/zip")
        self.assertEqual(data, b"")
        self.assertGreater(int(resp.getheader("Content-Length")), 0)


if __name__ == "__main__":
    unittest.main()
