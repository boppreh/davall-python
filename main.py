"""CLI entry point for davall — read-only WebDAV server for structured data."""

import argparse
import os
import sys

from backend import Backend, BackendError
from server import make_server

BACKENDS = {
    ".zip": ("backend_zip", "ZipBackend"),
    ".tar": ("backend_tar", "TarBackend"),
    ".tar.gz": ("backend_tar", "TarBackend"),
    ".tgz": ("backend_tar", "TarBackend"),
    ".tar.bz2": ("backend_tar", "TarBackend"),
    ".tar.xz": ("backend_tar", "TarBackend"),
    ".db": ("backend_sqlite", "SqliteBackend"),
    ".sqlite": ("backend_sqlite", "SqliteBackend"),
    ".sqlite3": ("backend_sqlite", "SqliteBackend"),
    ".json": ("backend_json", "JsonBackend"),
    ".csv": ("backend_csv", "CsvBackend"),
    ".ini": ("backend_ini", "IniBackend"),
    ".cfg": ("backend_ini", "IniBackend"),
    ".xml": ("backend_xml", "XmlBackend"),
    ".mbox": ("backend_mailbox", "MailboxBackend"),
    ".py": ("backend_ast", "AstBackend"),
    ".toml": ("backend_toml", "TomlBackend"),
}


def detect_backend(path: str) -> tuple[str, str]:
    """Detect backend from file extension. Returns (module_name, class_name)."""
    lower = path.lower()
    # Check compound extensions first
    for ext in sorted(BACKENDS, key=len, reverse=True):
        if lower.endswith(ext):
            return BACKENDS[ext]
    raise ValueError(
        f"Cannot detect backend for '{path}'. "
        f"Supported extensions: {', '.join(sorted(BACKENDS))}"
    )


def load_backend(path: str, backend_type: str | None = None) -> Backend:
    """Load a backend for the given file."""
    if backend_type:
        # Find by name
        for ext, (mod, cls) in BACKENDS.items():
            if cls.lower().replace("backend", "") == backend_type.lower().replace("backend", ""):
                module = __import__(mod)
                return getattr(module, cls)(path)
        raise ValueError(f"Unknown backend type: {backend_type}")

    mod_name, cls_name = detect_backend(path)
    module = __import__(mod_name)
    return getattr(module, cls_name)(path)


def main():
    parser = argparse.ArgumentParser(
        description="davall — read-only WebDAV server for structured data"
    )
    parser.add_argument("file", help="File to mount")
    parser.add_argument("-p", "--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--host", default="localhost", help="Host to bind to")
    parser.add_argument("-t", "--type", dest="backend_type", help="Force backend type")
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: {args.file} not found", file=sys.stderr)
        sys.exit(1)

    try:
        backend = load_backend(args.file, args.backend_type)
    except (BackendError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    server = make_server(backend, args.host, args.port)
    print(f"Serving {args.file} on http://{args.host}:{args.port}/")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
