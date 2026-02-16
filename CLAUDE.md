# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

davall is a read-only WebDAV server that mounts structured data files (ZIP, TAR, SQLite, JSON, CSV, INI, XML, mbox, Python) as browsable filesystems. Python 3 standard library only — no external dependencies.

## Commands

```bash
# Run all tests
python -m unittest test_server test_backends -v

# Run tests for a specific backend
python -m unittest test_backends.TestZipBackend -v

# Run the server
python main.py <file> [-p PORT] [--host HOST] [-t TYPE]
```

## Architecture

```
Backend (backend.py)          — abstract interface: info(), list(), get()
  ├── MemoryBackend           — in-memory dict, used for WebDAV server tests
  ├── ZipBackend              — backend_zip.py
  ├── TarBackend              — backend_tar.py
  ├── SqliteBackend           — backend_sqlite.py
  ├── JsonBackend             — backend_json.py
  ├── CsvBackend              — backend_csv.py
  ├── IniBackend              — backend_ini.py
  ├── XmlBackend              — backend_xml.py
  ├── MailboxBackend          — backend_mailbox.py
  └── AstBackend              — backend_ast.py

WebDAV Server (server.py)     — http.server-based, translates HTTP to backend calls
CLI (main.py)                 — auto-detects backend from file extension
```

All backends implement three methods on top of `Backend`: `info(path) -> ResourceInfo`, `list(path) -> list[str]`, `get(path) -> bytes`. Paths are `/`-separated, starting with `/`.

The WebDAV server supports OPTIONS, GET, HEAD, PROPFIND. All write methods return 405.

## Key conventions

- `NotFoundError` for missing resources, `BackendError` for internal errors
- `_normalize(path)` in `backend.py` handles path normalization (leading `/`, collapsing `//`, stripping trailing `/`)
- Backend constructors raise `BackendError` for invalid/corrupt input files
- Server tests use a live HTTP server on a random port (port 0)
- This is read-only — no write operations should ever be added to backends
