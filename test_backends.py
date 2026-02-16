"""Tests for all backends."""

import io
import json
import mailbox
import os
import sqlite3
import tarfile
import tempfile
import unittest
import zipfile

from backend import NotFoundError, BackendError


class TestZipBackend(unittest.TestCase):
    def _make_zip(self, files: dict[str, bytes]) -> str:
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
        self.assertTrue(b.info([]).is_dir)
        self.assertEqual(b.list([]), ["dir", "hello.txt"])
        info = b.info(["hello.txt"])
        self.assertFalse(info.is_dir)
        self.assertEqual(info.size, 6)
        self.assertEqual(b.get(["hello.txt"]), b"Hello!")
        self.assertTrue(b.info(["dir"]).is_dir)
        self.assertEqual(b.list(["dir"]), ["nested.txt"])
        self.assertEqual(b.get(["dir", "nested.txt"]), b"Nested content")

    def test_not_found(self):
        from backend_zip import ZipBackend
        path = self._make_zip({"a.txt": b"data"})
        b = ZipBackend(path)
        with self.assertRaises(NotFoundError):
            b.info(["nonexistent"])
        with self.assertRaises(NotFoundError):
            b.get(["nonexistent"])

    def test_get_dir_raises(self):
        from backend_zip import ZipBackend
        path = self._make_zip({"dir/file.txt": b"data"})
        b = ZipBackend(path)
        with self.assertRaises(NotFoundError):
            b.get(["dir"])

    def test_list_file_raises(self):
        from backend_zip import ZipBackend
        path = self._make_zip({"file.txt": b"data"})
        b = ZipBackend(path)
        with self.assertRaises(NotFoundError):
            b.list(["file.txt"])

    def test_empty_zip(self):
        from backend_zip import ZipBackend
        path = self._make_zip({})
        b = ZipBackend(path)
        self.assertTrue(b.info([]).is_dir)
        self.assertEqual(b.list([]), [])

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
        self.assertIn("png", b.info(["image.png"]).content_type)
        self.assertIn("json", b.info(["data.json"]).content_type)


class TestTarBackend(unittest.TestCase):
    def _make_tar(self, files: dict[str, bytes], compression: str = "") -> str:
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
        self.assertTrue(b.info([]).is_dir)
        self.assertEqual(b.list([]), ["dir", "hello.txt"])
        self.assertEqual(b.get(["hello.txt"]), b"Hello!")
        self.assertTrue(b.info(["dir"]).is_dir)
        self.assertEqual(b.get(["dir", "nested.txt"]), b"Nested content")

    def test_gzip(self):
        from backend_tar import TarBackend
        path = self._make_tar({"file.txt": b"compressed"}, compression="gz")
        b = TarBackend(path)
        self.assertEqual(b.get(["file.txt"]), b"compressed")

    def test_bz2(self):
        from backend_tar import TarBackend
        path = self._make_tar({"file.txt": b"bz2 data"}, compression="bz2")
        b = TarBackend(path)
        self.assertEqual(b.get(["file.txt"]), b"bz2 data")

    def test_not_found(self):
        from backend_tar import TarBackend
        path = self._make_tar({"a.txt": b"data"})
        b = TarBackend(path)
        with self.assertRaises(NotFoundError):
            b.info(["nonexistent"])

    def test_empty_tar(self):
        from backend_tar import TarBackend
        path = self._make_tar({})
        b = TarBackend(path)
        self.assertEqual(b.list([]), [])

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
        self.assertTrue(b.info([]).is_dir)
        self.assertEqual(b.list([]), ["users"])
        self.assertTrue(b.info(["users"]).is_dir)
        self.assertEqual(b.list(["users"]), ["_schema.sql", "row_0", "row_1"])
        self.assertIn(b"CREATE TABLE", b.get(["users", "_schema.sql"]))
        self.assertTrue(b.info(["users", "row_0"]).is_dir)
        self.assertEqual(b.list(["users", "row_0"]), ["name", "age"])
        self.assertEqual(b.get(["users", "row_0", "name"]), b"Alice")
        self.assertEqual(b.get(["users", "row_0", "age"]), b"30")
        self.assertEqual(b.get(["users", "row_1", "name"]), b"Bob")

    def test_multiple_tables(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db([
            "CREATE TABLE a (x INTEGER)",
            "CREATE TABLE b (y TEXT)",
        ])
        b = SqliteBackend(path)
        self.assertEqual(b.list([]), ["a", "b"])

    def test_empty_table(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db(["CREATE TABLE empty (col TEXT)"])
        b = SqliteBackend(path)
        self.assertEqual(b.list(["empty"]), ["_schema.sql"])

    def test_not_found(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db(["CREATE TABLE t (x INTEGER)"])
        b = SqliteBackend(path)
        with self.assertRaises(NotFoundError):
            b.info(["nonexistent"])
        with self.assertRaises(NotFoundError):
            b.info(["t", "row_999"])

    def test_info_sizes(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db([
            "CREATE TABLE t (val TEXT)",
            "INSERT INTO t VALUES ('hello')",
        ])
        b = SqliteBackend(path)
        info = b.info(["t", "_schema.sql"])
        self.assertFalse(info.is_dir)
        self.assertGreater(info.size, 0)
        info = b.info(["t", "row_0", "val"])
        self.assertFalse(info.is_dir)
        self.assertEqual(info.size, 5)

    def test_null_value(self):
        from backend_sqlite import SqliteBackend
        path = self._make_db([
            "CREATE TABLE t (val TEXT)",
            "INSERT INTO t VALUES (NULL)",
        ])
        b = SqliteBackend(path)
        self.assertEqual(b.get(["t", "row_0", "val"]), b"")


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
        self.assertTrue(b.info([]).is_dir)
        self.assertEqual(b.list([]), ["age", "name"])
        self.assertEqual(b.get(["name"]), b"Alice")
        self.assertEqual(b.get(["age"]), b"30")

    def test_nested_dict(self):
        from backend_json import JsonBackend
        path = self._make_json({"a": {"b": "value"}})
        b = JsonBackend(path)
        self.assertTrue(b.info(["a"]).is_dir)
        self.assertEqual(b.list(["a"]), ["b"])
        self.assertEqual(b.get(["a", "b"]), b"value")

    def test_list(self):
        from backend_json import JsonBackend
        path = self._make_json({"items": [10, 20, 30]})
        b = JsonBackend(path)
        self.assertTrue(b.info(["items"]).is_dir)
        self.assertEqual(b.list(["items"]), ["0", "1", "2"])
        self.assertEqual(b.get(["items", "0"]), b"10")
        self.assertEqual(b.get(["items", "2"]), b"30")

    def test_root_list(self):
        from backend_json import JsonBackend
        path = self._make_json(["a", "b", "c"])
        b = JsonBackend(path)
        self.assertEqual(b.list([]), ["0", "1", "2"])
        self.assertEqual(b.get(["0"]), b"a")

    def test_null_bool(self):
        from backend_json import JsonBackend
        path = self._make_json({"n": None, "t": True, "f": False})
        b = JsonBackend(path)
        self.assertEqual(b.get(["n"]), b"null")
        self.assertEqual(b.get(["t"]), b"true")
        self.assertEqual(b.get(["f"]), b"false")

    def test_not_found(self):
        from backend_json import JsonBackend
        path = self._make_json({"a": 1})
        b = JsonBackend(path)
        with self.assertRaises(NotFoundError):
            b.info(["nonexistent"])

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
        self.assertTrue(b.info([]).is_dir)
        entries = b.list([])
        self.assertEqual(entries[0], "_headers.txt")
        self.assertIn("row_0000", entries)
        self.assertIn("row_0001", entries)
        self.assertEqual(b.get(["_headers.txt"]), b"name\nage")
        self.assertTrue(b.info(["row_0000"]).is_dir)
        self.assertEqual(b.list(["row_0000"]), ["name", "age"])
        self.assertEqual(b.get(["row_0000", "name"]), b"Alice")
        self.assertEqual(b.get(["row_0000", "age"]), b"30")
        self.assertEqual(b.get(["row_0001", "name"]), b"Bob")

    def test_empty_csv(self):
        from backend_csv import CsvBackend
        path = self._make_csv("col1,col2\n")
        b = CsvBackend(path)
        self.assertEqual(b.list([]), ["_headers.txt"])

    def test_not_found(self):
        from backend_csv import CsvBackend
        path = self._make_csv("x\n1\n")
        b = CsvBackend(path)
        with self.assertRaises(NotFoundError):
            b.info(["row_999"])
        with self.assertRaises(NotFoundError):
            b.info(["nonexistent"])

    def test_info_sizes(self):
        from backend_csv import CsvBackend
        path = self._make_csv("a,b\n1,2\n")
        b = CsvBackend(path)
        info = b.info(["_headers.txt"])
        self.assertFalse(info.is_dir)
        self.assertGreater(info.size, 0)


class TestIniBackend(unittest.TestCase):
    def _make_ini(self, content: str) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".ini", delete=False, mode="w")
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
        from backend_ini import IniBackend
        path = self._make_ini("[server]\nhost = localhost\nport = 8080\n\n[database]\nurl = sqlite:///db\n")
        b = IniBackend(path)
        self.assertTrue(b.info([]).is_dir)
        self.assertEqual(b.list([]), ["database", "server"])
        self.assertTrue(b.info(["server"]).is_dir)
        self.assertEqual(sorted(b.list(["server"])), ["host", "port"])
        self.assertEqual(b.get(["server", "host"]), b"localhost")
        self.assertEqual(b.get(["server", "port"]), b"8080")
        self.assertEqual(b.get(["database", "url"]), b"sqlite:///db")

    def test_empty_section(self):
        from backend_ini import IniBackend
        path = self._make_ini("[empty]\n")
        b = IniBackend(path)
        self.assertEqual(b.list(["empty"]), [])

    def test_not_found(self):
        from backend_ini import IniBackend
        path = self._make_ini("[s]\nk = v\n")
        b = IniBackend(path)
        with self.assertRaises(NotFoundError):
            b.info(["nonexistent"])
        with self.assertRaises(NotFoundError):
            b.get(["s", "nonexistent"])


class TestXmlBackend(unittest.TestCase):
    def _make_xml(self, content: str) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".xml", delete=False, mode="w")
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
        from backend_xml import XmlBackend
        path = self._make_xml('<root><child>Hello</child></root>')
        b = XmlBackend(path)
        self.assertTrue(b.info([]).is_dir)
        self.assertIn("child", b.list([]))
        self.assertTrue(b.info(["child"]).is_dir)
        self.assertIn("_text", b.list(["child"]))
        self.assertEqual(b.get(["child", "_text"]), b"Hello")

    def test_attributes(self):
        from backend_xml import XmlBackend
        path = self._make_xml('<root><item id="1" name="test"/></root>')
        b = XmlBackend(path)
        self.assertIn("_attribs", b.list(["item"]))
        self.assertTrue(b.info(["item", "_attribs"]).is_dir)
        self.assertEqual(sorted(b.list(["item", "_attribs"])), ["id", "name"])
        self.assertEqual(b.get(["item", "_attribs", "id"]), b"1")
        self.assertEqual(b.get(["item", "_attribs", "name"]), b"test")

    def test_duplicate_tags(self):
        from backend_xml import XmlBackend
        path = self._make_xml('<root><item>A</item><item>B</item><item>C</item></root>')
        b = XmlBackend(path)
        entries = b.list([])
        self.assertIn("item_0", entries)
        self.assertIn("item_1", entries)
        self.assertIn("item_2", entries)
        self.assertEqual(b.get(["item_0", "_text"]), b"A")
        self.assertEqual(b.get(["item_2", "_text"]), b"C")

    def test_nested(self):
        from backend_xml import XmlBackend
        path = self._make_xml('<a><b><c>deep</c></b></a>')
        b = XmlBackend(path)
        self.assertEqual(b.get(["b", "c", "_text"]), b"deep")

    def test_no_text(self):
        from backend_xml import XmlBackend
        path = self._make_xml('<root><empty/></root>')
        b = XmlBackend(path)
        self.assertNotIn("_text", b.list(["empty"]))

    def test_not_found(self):
        from backend_xml import XmlBackend
        path = self._make_xml('<root/>')
        b = XmlBackend(path)
        with self.assertRaises(NotFoundError):
            b.info(["nonexistent"])

    def test_bad_xml(self):
        from backend_xml import XmlBackend
        path = self._make_xml('not xml at all <<<')
        with self.assertRaises(BackendError):
            XmlBackend(path)


class TestMailboxBackend(unittest.TestCase):
    def _make_mbox(self, messages: list[tuple[str, str]]) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".mbox", delete=False)
        f.close()
        self._tmpfiles.append(f.name)
        mbox = mailbox.mbox(f.name)
        for subject, body in messages:
            msg = mailbox.mboxMessage()
            msg["Subject"] = subject
            msg["From"] = "test@example.com"
            msg.set_payload(body)
            mbox.add(msg)
        mbox.flush()
        mbox.close()
        return f.name

    def setUp(self):
        self._tmpfiles = []

    def tearDown(self):
        for f in self._tmpfiles:
            os.unlink(f)

    def test_basic(self):
        from backend_mailbox import MailboxBackend
        path = self._make_mbox([
            ("Hello World", "First message body"),
            ("Test Email", "Second message"),
        ])
        b = MailboxBackend(path)
        self.assertTrue(b.info([]).is_dir)
        entries = b.list([])
        self.assertEqual(len(entries), 2)
        self.assertIn("Hello_World", entries[0])
        self.assertTrue(entries[0].endswith(".eml"))
        data = b.get([entries[0]])
        self.assertIn(b"Hello World", data)
        self.assertIn(b"First message body", data)

    def test_empty_mbox(self):
        from backend_mailbox import MailboxBackend
        path = self._make_mbox([])
        b = MailboxBackend(path)
        self.assertEqual(b.list([]), [])

    def test_not_found(self):
        from backend_mailbox import MailboxBackend
        path = self._make_mbox([("Test", "body")])
        b = MailboxBackend(path)
        with self.assertRaises(NotFoundError):
            b.get(["nonexistent.eml"])


class TestAstBackend(unittest.TestCase):
    def _make_py(self, content: str) -> str:
        f = tempfile.NamedTemporaryFile(suffix=".py", delete=False, mode="w")
        f.write(content)
        f.close()
        self._tmpfiles.append(f.name)
        return f.name

    def setUp(self):
        self._tmpfiles = []

    def tearDown(self):
        for f in self._tmpfiles:
            os.unlink(f)

    def test_functions(self):
        from backend_ast import AstBackend
        path = self._make_py("def hello():\n    return 'hi'\n\ndef goodbye():\n    return 'bye'\n")
        b = AstBackend(path)
        self.assertTrue(b.info([]).is_dir)
        entries = b.list([])
        self.assertIn("hello.py", entries)
        self.assertIn("goodbye.py", entries)
        src = b.get(["hello.py"])
        self.assertIn(b"def hello", src)
        self.assertIn(b"return 'hi'", src)

    def test_class_with_methods(self):
        from backend_ast import AstBackend
        path = self._make_py(
            "class Foo:\n"
            "    def bar(self):\n"
            "        pass\n"
            "    def baz(self):\n"
            "        return 1\n"
        )
        b = AstBackend(path)
        self.assertIn("Foo", b.list([]))
        self.assertTrue(b.info(["Foo"]).is_dir)
        methods = b.list(["Foo"])
        self.assertIn("bar.py", methods)
        self.assertIn("baz.py", methods)
        self.assertIn(b"def baz", b.get(["Foo", "baz.py"]))

    def test_empty_file(self):
        from backend_ast import AstBackend
        path = self._make_py("# just a comment\nx = 1\n")
        b = AstBackend(path)
        self.assertEqual(b.list([]), [])

    def test_not_found(self):
        from backend_ast import AstBackend
        path = self._make_py("def f(): pass\n")
        b = AstBackend(path)
        with self.assertRaises(NotFoundError):
            b.info(["nonexistent"])

    def test_syntax_error(self):
        from backend_ast import AstBackend
        path = self._make_py("def (broken syntax")
        with self.assertRaises(BackendError):
            AstBackend(path)


if __name__ == "__main__":
    unittest.main()
