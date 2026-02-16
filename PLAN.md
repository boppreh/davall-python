# davall - Read-only WebDAV interface to non-filesystem backends

## Goal

Mount structured data (databases, archives, config files, etc.) as read-only
WebDAV shares, browsable from any file manager.

Python 3, standard library only, no external dependencies.

## WebDAV Operations

Based on RFC 4918, a read-only server needs:

| Method   | Purpose                          | Needed? |
|----------|----------------------------------|---------|
| OPTIONS  | Capability discovery             | Yes     |
| GET      | Read file content                | Yes     |
| HEAD     | Read metadata without content    | Yes     |
| PROPFIND | List directories and properties  | Yes     |
| PUT      | Write file                       | No (405)|
| DELETE   | Delete resource                  | No (405)|
| MKCOL    | Create directory                 | No (405)|
| PROPPATCH| Modify properties                | No (405)|
| MOVE     | Move resource                    | No (405)|
| COPY     | Copy resource                    | No (405)|
| LOCK     | Lock resource                    | No (405)|
| UNLOCK   | Unlock resource                  | No (405)|

All write methods return 405 Method Not Allowed.

PROPFIND is the core: it returns XML multistatus responses with resource
properties (displayname, getcontentlength, getcontenttype, resourcetype,
getlastmodified). Supports Depth: 0, 1, and infinity headers.

## Backend Interface

Each backend exposes a simple read-only filesystem abstraction:

```python
class Backend:
    def list(self, path: str) -> list[str]      # child names in a directory
    def get(self, path: str) -> bytes            # file content
    def info(self, path: str) -> ResourceInfo    # metadata (size, is_dir, mtime, content_type)
```

Paths are `/`-separated, always starting with `/`. The root is always a directory.

Errors in backends (missing files, corrupt data, permission errors) propagate as
appropriate HTTP status codes (404, 500, etc.).

## Backends (in order of implementation)

### 1. Memory (dict-based) — for testing the WebDAV frontend
Simple nested dict structure. Used as the reference backend for WebDAV tests.

### 2. ZIP archives (`zipfile`)
Mount a .zip file. Internal directory structure maps directly to WebDAV paths.
Exposes file sizes, modification times, and content.

### 3. TAR archives (`tarfile`)
Mount .tar, .tar.gz, .tar.bz2, .tar.xz files. Same directory mapping as ZIP.

### 4. SQLite databases (`sqlite3`)
Structure:
```
/
  table_name/
    _schema.sql          — CREATE TABLE statement
    row_<rowid>.json     — each row as a JSON object
```
Tables are directories, rows are JSON files. Schema is exposed as a SQL file.

### 5. JSON files (`json`)
Structure:
```
/
  key1             — if value is scalar, it's a file containing the value
  key2/            — if value is dict, it's a subdirectory
    nested_key     — ...
  key3/            — if value is list, it's a directory with indexed entries
    0              — first element
    1              — second element
```

### 6. CSV files (`csv`)
Structure:
```
/
  _headers.txt         — column names, one per line
  row_0000.json        — {"col1": "val1", "col2": "val2"}
  row_0001.json
  ...
```

### 7. INI/Config files (`configparser`)
Structure:
```
/
  section_name/
    key1           — file containing the value
    key2
```

### 8. XML files (`xml.etree.ElementTree`)
Structure:
```
/
  root_tag/
    _text          — text content if any
    _attribs.json  — attributes if any
    child_tag/
      ...
```
Repeated tags get numeric suffixes: `item_0/`, `item_1/`.

### 9. Mailbox (`mailbox`)
Mount mbox/Maildir files:
```
/
  0001_Subject_Line.eml   — full RFC 822 message
  0002_Another_Subject.eml
```

### 10. Python AST (`ast`)
Mount a .py file, exposing its structure:
```
/
  ClassName/
    method_name.py     — source code of the method
  function_name.py     — source code of the function
```

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│ File Manager │────▶│ WebDAV Server│────▶│   Backend    │
│  (client)    │◀────│ (http.server)│◀────│ (zip/sql/...)│
└─────────────┘     └──────────────┘     └──────────────┘
                     Translates            Provides
                     HTTP ↔ XML            list/get/info
```

Single Python file per backend. The server auto-detects which backend to use
based on file extension, or accepts explicit selection via CLI argument.
