"""Tests for all backends."""

import io
import json
import os
import tempfile
import unittest
import zipfile

from backend import NotFoundError, BackendError


class TestZipBackend(unittest.TestCase):
    def _make_zip(self, files: dict[str, bytes]) -> str:
        """Create a temporary ZIP file with the given contents. Returns path."""
        f = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        with zipfile.ZipFile(f, "w") as zf:
            for name, data in files.items():
                zf.writestr(name, data)
        f.close()
        self._tmpfiles.append(f.name)
        return f.name

    def setUp(self):
        self._tmpfiles = []

    def tearDown(self):
        for f in self._tmpfiles:
            os.unlink(f)

    def test_basic(self):
        from backend_zip import ZipBackend
        path = self._make_zip({
            "hello.txt": b"Hello!",
            "dir/nested.txt": b"Nested content",
        })
        b = ZipBackend(path)

        # Root
        info = b.info("/")
        self.assertTrue(info.is_dir)
        self.assertEqual(b.list("/"), ["dir", "hello.txt"])

        # File
        info = b.info("/hello.txt")
        self.assertFalse(info.is_dir)
        self.assertEqual(info.size, 6)
        self.assertEqual(b.get("/hello.txt"), b"Hello!")

        # Nested dir
        info = b.info("/dir")
        self.assertTrue(info.is_dir)
        self.assertEqual(b.list("/dir"), ["nested.txt"])

        # Nested file
        self.assertEqual(b.get("/dir/nested.txt"), b"Nested content")

    def test_not_found(self):
        from backend_zip import ZipBackend
        path = self._make_zip({"a.txt": b"data"})
        b = ZipBackend(path)
        with self.assertRaises(NotFoundError):
            b.info("/nonexistent")
        with self.assertRaises(NotFoundError):
            b.get("/nonexistent")

    def test_get_dir_raises(self):
        from backend_zip import ZipBackend
        path = self._make_zip({"dir/file.txt": b"data"})
        b = ZipBackend(path)
        with self.assertRaises(NotFoundError):
            b.get("/dir")

    def test_list_file_raises(self):
        from backend_zip import ZipBackend
        path = self._make_zip({"file.txt": b"data"})
        b = ZipBackend(path)
        with self.assertRaises(NotFoundError):
            b.list("/file.txt")

    def test_empty_zip(self):
        from backend_zip import ZipBackend
        path = self._make_zip({})
        b = ZipBackend(path)
        self.assertTrue(b.info("/").is_dir)
        self.assertEqual(b.list("/"), [])

    def test_bad_zip(self):
        from backend_zip import ZipBackend
        f = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        f.write(b"not a zip file")
        f.close()
        self._tmpfiles.append(f.name)
        with self.assertRaises(BackendError):
            ZipBackend(f.name)

    def test_content_type(self):
        from backend_zip import ZipBackend
        path = self._make_zip({"image.png": b"\x89PNG", "data.json": b"{}"})
        b = ZipBackend(path)
        info = b.info("/image.png")
        self.assertIn("png", info.content_type)
        info = b.info("/data.json")
        self.assertIn("json", info.content_type)


if __name__ == "__main__":
    unittest.main()
