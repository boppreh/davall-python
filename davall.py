"""CLI entry point for davall — read-only WebDAV server for structured data."""

import argparse
import os
import sys

from backend import Backend, BackendError
from server import make_server

# Maps subcommand name -> (module, class, takes_file)
SUBCOMMANDS = {
    "zip": ("backend_zip", "ZipBackend"),
    "tar": ("backend_tar", "TarBackend"),
    "sqlite": ("backend_sqlite", "SqliteBackend"),
    "json": ("backend_json", "JsonBackend"),
    "csv": ("backend_csv", "CsvBackend"),
    "ini": ("backend_ini", "IniBackend"),
    "xml": ("backend_xml", "XmlBackend"),
    "mbox": ("backend_mailbox", "MailboxBackend"),
    "ast": ("backend_ast", "AstBackend"),
    "toml": ("backend_toml", "TomlBackend"),
    "html": ("backend_html", "HtmlBackend"),
}

# Extension -> subcommand name for auto-detection
EXT_MAP = {
    ".zip": "zip",
    ".tar": "tar", ".tar.gz": "tar", ".tgz": "tar",
    ".tar.bz2": "tar", ".tar.xz": "tar",
    ".db": "sqlite", ".sqlite": "sqlite", ".sqlite3": "sqlite",
    ".json": "json",
    ".csv": "csv",
    ".ini": "ini", ".cfg": "ini",
    ".xml": "xml",
    ".mbox": "mbox",
    ".py": "ast",
    ".toml": "toml",
    ".html": "html", ".htm": "html",
}


def detect_subcommand(path: str) -> str:
    """Detect subcommand from file extension."""
    lower = path.lower()
    for ext in sorted(EXT_MAP, key=len, reverse=True):
        if lower.endswith(ext):
            return EXT_MAP[ext]
    raise ValueError(
        f"Cannot detect backend for '{path}'. "
        f"Supported extensions: {', '.join(sorted(EXT_MAP))}"
    )


def load_backend(path: str, name: str) -> Backend:
    """Load a backend by subcommand name for the given file."""
    mod_name, cls_name = SUBCOMMANDS[name]
    module = __import__(mod_name)
    return getattr(module, cls_name)(path)


def main():
    parser = argparse.ArgumentParser(
        description="davall — read-only WebDAV server for structured data"
    )
    parser.add_argument("-p", "--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument("--host", default="localhost", help="Host to bind to")

    sub = parser.add_subparsers(dest="command")

    # File-based backends
    for name in SUBCOMMANDS:
        p = sub.add_parser(name, help=f"Mount a {name} file")
        p.add_argument("file", help="File to mount")

    # Auto-detect backend
    p = sub.add_parser("auto", help="Auto-detect backend from file extension")
    p.add_argument("file", help="File to mount")

    # OS info (no file)
    sub.add_parser("osinfo", help="Mount OS/platform information")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "osinfo":
        from backend_osinfo import OsInfoBackend
        backend = OsInfoBackend()
        label = "OS info"
    else:
        if not os.path.exists(args.file):
            print(f"Error: {args.file} not found", file=sys.stderr)
            sys.exit(1)

        if args.command == "auto":
            try:
                name = detect_subcommand(args.file)
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)
        else:
            name = args.command

        try:
            backend = load_backend(args.file, name)
        except (BackendError, ValueError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        label = args.file

    server = make_server(backend, args.host, args.port)
    print(f"Serving {label} on http://{args.host}:{args.port}/")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
