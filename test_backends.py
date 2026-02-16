"""Tests for all backends."""

import io
import json
import os
import sqlite3
import tarfile
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


class TestTarBackend(unittest.TestCase):
    def _make_tar(self, files: dict[str, bytes], compression: str = "") -> str:
        """Create a temporary TAR file. Returns path."""
        suffix = ".tar" + (f".{compression}" if compression else "")
        mode = "w:" + compression
        f = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        f.close()
        self._tmpfiles.append(f.name)
        with tarfile.open(f.name, mode) as tf:
            for name, data in files.items():
                ti = tarfile.TarInfo(name=name)
                ti.size = len(data)
                tf.addfile(ti, io.BytesIO(data))
        return f.name

    def setUp(self):
        self._tmpfiles = []

    def tearDown(self):
        for f in self._tmpfiles:
            os.unlink(f)

    def test_basic(self):
        from backend_tar import TarBackend
        path = self._make_tar({
            "hello.txt": b"Hello!",
            "dir/nested.txt": b"Nested content",
        })
        b = TarBackend(path)
        self.assertTrue(b.info("/").is_dir)
        self.assertEqual(b.list("/"), ["dir", "hello.txt"])
        self.assertEqual(b.get("/hello.txt"), b"Hello!")
        self.assertTrue(b.info("/dir").is_dir)
        self.assertEqual(b.get("/dir/nested.txt"), b"Nested content")

    def test_gzip(self):
        from backend_tar import TarBackend
        path = self._make_tar({"file.txt": b"compressed"}, compression="gz")
        b = TarBackend(path)
        self.assertEqual(b.get("/file.txt"), b"compressed")

    def test_bz2(self):
        from backend_tar import TarBackend
        path = self._make_tar({"file.txt": b"bz2 data"}, compression="bz2")
        b = TarBackend(path)
        self.assertEqual(b.get("/file.txt"), b"bz2 data")

    def test_not_found(self):
        from backend_tar import TarBackend
        path = self._make_tar({"a.txt": b"data"})
        b = TarBackend(path)
        with self.assertRaises(NotFoundError):
            b.info("/nonexistent")

    def test_empty_tar(self):
        from backend_tar import TarBackend
        path = self._make_tar({})
        b = TarBackend(path)
        self.assertEqual(b.list("/"), [])

    def test_bad_tar(self):
        from backend_tar import TarBackend
        f = tempfile.NamedTemporaryFile(suffix=".tar", delete=False)
        f.write(b"not a tar file")
        f.close()
        self._tmpfiles.append(f.name)
        with self.assertRaises(BackendError):
            TarBackend(f.name)


class TestSqliteBackend(unittest.TestCase):
    def _make_db(self, setup_sql: list[str]) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        f.close()
        self._tmpfiles.append(f.name)
        conn = sqlite3.connect(f.name)
        for sql in setup_sql:
            conn.execute(sql)
        conn.commit()
        conn.close()
        return f.name

    def setUp(self):
        self._tmpfiles = []

    def tearDown(self):
        for f in self._tmpfiles:
            os.unlink(f)

    def test_basic(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db([
            "CREATE TABLE users (name TEXT, age INTEGER)",
            "INSERT INTO users VALUES ('Alice', 30)",
            "INSERT INTO users VALUES ('Bob', 25)",
        ])
        b = SqliteBackend(path)

        # Root lists tables
        self.assertTrue(b.info("/").is_dir)
        self.assertEqual(b.list("/"), ["users"])

        # Table directory
        self.assertTrue(b.info("/users").is_dir)
        entries = b.list("/users")
        self.assertEqual(entries, ["_schema.sql", "row_0.json", "row_1.json"])

        # Schema file
        schema = b.get("/users/_schema.sql")
        self.assertIn(b"CREATE TABLE", schema)
        self.assertIn(b"users", schema)

        # Row files
        row0 = json.loads(b.get("/users/row_0.json"))
        self.assertEqual(row0["name"], "Alice")
        self.assertEqual(row0["age"], 30)

        row1 = json.loads(b.get("/users/row_1.json"))
        self.assertEqual(row1["name"], "Bob")

    def test_multiple_tables(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db([
            "CREATE TABLE a (x INTEGER)",
            "CREATE TABLE b (y TEXT)",
        ])
        b = SqliteBackend(path)
        self.assertEqual(b.list("/"), ["a", "b"])

    def test_empty_table(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db(["CREATE TABLE empty (col TEXT)"])
        b = SqliteBackend(path)
        self.assertEqual(b.list("/empty"), ["_schema.sql"])

    def test_not_found(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db(["CREATE TABLE t (x INTEGER)"])
        b = SqliteBackend(path)
        with self.assertRaises(NotFoundError):
            b.info("/nonexistent")
        with self.assertRaises(NotFoundError):
            b.get("/t/row_999.json")

    def test_info_sizes(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db([
            "CREATE TABLE t (val TEXT)",
            "INSERT INTO t VALUES ('hello')",
        ])
        b = SqliteBackend(path)
        info = b.info("/t/_schema.sql")
        self.assertFalse(info.is_dir)
        self.assertGreater(info.size, 0)
        info = b.info("/t/row_0.json")
        self.assertFalse(info.is_dir)
        self.assertGreater(info.size, 0)


class TestJsonBackend(unittest.TestCase):
    def _make_json(self, data) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        json.dump(data, f)
        f.close()
        self._tmpfiles.append(f.name)
        return f.name

    def setUp(self):
        self._tmpfiles = []

    def tearDown(self):
        for f in self._tmpfiles:
            os.unlink(f)

    def test_dict(self):
        from backend_json import JsonBackend
        path = self._make_json({"name": "Alice", "age": 30})
        b = JsonBackend(path)
        self.assertTrue(b.info("/").is_dir)
        self.assertEqual(b.list("/"), ["age", "name"])
        self.assertEqual(b.get("/name"), b"Alice")
        self.assertEqual(b.get("/age"), b"30")

    def test_nested_dict(self):
        from backend_json import JsonBackend
        path = self._make_json({"a": {"b": "value"}})
        b = JsonBackend(path)
        self.assertTrue(b.info("/a").is_dir)
        self.assertEqual(b.list("/a"), ["b"])
        self.assertEqual(b.get("/a/b"), b"value")

    def test_list(self):
        from backend_json import JsonBackend
        path = self._make_json({"items": [10, 20, 30]})
        b = JsonBackend(path)
        self.assertTrue(b.info("/items").is_dir)
        self.assertEqual(b.list("/items"), ["0", "1", "2"])
        self.assertEqual(b.get("/items/0"), b"10")
        self.assertEqual(b.get("/items/2"), b"30")

    def test_root_list(self):
        from backend_json import JsonBackend
        path = self._make_json(["a", "b", "c"])
        b = JsonBackend(path)
        self.assertEqual(b.list("/"), ["0", "1", "2"])
        self.assertEqual(b.get("/0"), b"a")

    def test_null_bool(self):
        from backend_json import JsonBackend
        path = self._make_json({"n": None, "t": True, "f": False})
        b = JsonBackend(path)
        self.assertEqual(b.get("/n"), b"null")
        self.assertEqual(b.get("/t"), b"true")
        self.assertEqual(b.get("/f"), b"false")

    def test_not_found(self):
        from backend_json import JsonBackend
        path = self._make_json({"a": 1})
        b = JsonBackend(path)
        with self.assertRaises(NotFoundError):
            b.info("/nonexistent")

    def test_bad_json(self):
        from backend_json import JsonBackend
        f = tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w")
        f.write("not json{{{")
        f.close()
        self._tmpfiles.append(f.name)
        with self.assertRaises(BackendError):
            JsonBackend(f.name)

    def test_scalar_root_rejected(self):
        from backend_json import JsonBackend
        path = self._make_json("just a string")
        with self.assertRaises(BackendError):
            JsonBackend(path)


class TestCsvBackend(unittest.TestCase):
    def _make_csv(self, content: str) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", newline="")
        f.write(content)
        f.close()
        self._tmpfiles.append(f.name)
        return f.name

    def setUp(self):
        self._tmpfiles = []

    def tearDown(self):
        for f in self._tmpfiles:
            os.unlink(f)

    def test_basic(self):
        from backend_csv import CsvBackend
        path = self._make_csv("name,age\nAlice,30\nBob,25\n")
        b = CsvBackend(path)
        self.assertTrue(b.info("/").is_dir)
        entries = b.list("/")
        self.assertEqual(entries[0], "_headers.txt")
        self.assertIn("row_0000.json", entries)
        self.assertIn("row_0001.json", entries)

        self.assertEqual(b.get("/_headers.txt"), b"name\nage")

        row0 = json.loads(b.get("/row_0000.json"))
        self.assertEqual(row0["name"], "Alice")
        self.assertEqual(row0["age"], "30")

    def test_empty_csv(self):
        from backend_csv import CsvBackend
        path = self._make_csv("col1,col2\n")
        b = CsvBackend(path)
        self.assertEqual(b.list("/"), ["_headers.txt"])

    def test_not_found(self):
        from backend_csv import CsvBackend
        path = self._make_csv("x\n1\n")
        b = CsvBackend(path)
        with self.assertRaises(NotFoundError):
            b.get("/row_999.json")
        with self.assertRaises(NotFoundError):
            b.info("/nonexistent")

    def test_info_sizes(self):
        from backend_csv import CsvBackend
        path = self._make_csv("a,b\n1,2\n")
        b = CsvBackend(path)
        info = b.info("/_headers.txt")
        self.assertFalse(info.is_dir)
        self.assertGreater(info.size, 0)


if __name__ == "__main__":
    unittest.main()
