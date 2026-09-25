"""Serve the static PLY viewer on port 8080 with the latest (or given) point cloud."""

from __future__ import annotations

import argparse
import http.server
import mimetypes
import socketserver
import sys
from functools import partial
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VIEWER = ROOT / "viewer"


def find_latest_ply(search_root: Path) -> Path:
    plies = sorted(search_root.rglob("*.ply"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not plies:
        raise FileNotFoundError(f"No .ply files under {search_root}")
    return plies[0]


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, ply_path: Path, **kwargs):
        self.ply_path = ply_path
        super().__init__(*args, directory=str(VIEWER), **kwargs)

    def do_GET(self):
        if self._is_ply_path():
            return self._send_file(self.ply_path, "application/octet-stream")
        return super().do_GET()

    def do_HEAD(self):
        if self._is_ply_path():
            return self._send_file(self.ply_path, "application/octet-stream", body=False)
        return super().do_HEAD()

    def _is_ply_path(self) -> bool:
        return self.path.split("?", 1)[0] in ("/pointcloud.ply", "/ply")

    def _send_file(self, path: Path, content_type: str, body: bool = True):
        if not path.is_file():
            self.send_error(404, f"Missing {path}")
            return
        size = path.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(size))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if body:
            with path.open("rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


def main() -> int:
    mimetypes.add_type("model/ply", ".ply")
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--ply", type=Path, default=None, help="PLY to serve (default: newest under repo)")
    p.add_argument("--search-root", type=Path, default=ROOT / "outputs")
    args = p.parse_args()

    ply = args.ply.resolve() if args.ply else find_latest_ply(args.search_root.resolve())
    if not ply.is_file():
        raise SystemExit(f"PLY not found: {ply}")
    if not (VIEWER / "index.html").is_file():
        raise SystemExit(f"Missing viewer: {VIEWER / 'index.html'}")

    print(f"Serving viewer from {VIEWER}")
    print(f"PLY: {ply} ({ply.stat().st_size / 1e6:.1f} MB)")
    print(f"Open http://127.0.0.1:{args.port}/")

    handler = partial(Handler, ply_path=ply)
    with socketserver.ThreadingTCPServer(("0.0.0.0", args.port), handler) as httpd:
        httpd.allow_reuse_address = True
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
