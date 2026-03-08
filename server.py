import json
import os
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HOST = "127.0.0.1"
PORT = 8080
DATA_FILE = "records.json"


def _load() -> dict:
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            records = json.load(f)
        return {r["id"]: r for r in records}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _persist(data: dict) -> None:
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(list(data.values()), f)
    os.replace(tmp, DATA_FILE)


storage: dict = _load()
lock = threading.RLock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"{self.address_string()} - {format % args}")

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b"{}"
        return json.loads(body)

    def _send_json(self, code: int, data):
        payload = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _send_error_json(self, code: int, message: str):
        self._send_json(code, {"error": message})

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/records":
            params = parse_qs(parsed.query)
            items = list(storage.values())
            try:
                offset = int(params.get("offset", [0])[0])
                limit = int(params.get("limit", [len(items)])[0])
            except ValueError:
                self._send_error_json(400, "limit and offset must be integers")
                return
            self._send_json(200, items[offset: offset + limit])
            return

        if parsed.path.startswith("/records/"):
            record_id = parsed.path[len("/records/"):]
            record = storage.get(record_id)
            if record is None:
                self._send_error_json(404, "not found")
                return
            self._send_json(200, record)
            return

        self._send_error_json(404, "not found")

    def do_POST(self):
        if self.path != "/records":
            self._send_error_json(404, "not found")
            return

        try:
            body = self._read_json()
        except (json.JSONDecodeError, ValueError):
            self._send_error_json(400, "invalid JSON")
            return

        data = body.get("data", body)
        record_id = str(uuid.uuid4())
        record = {"id": record_id, "data": data}
        with lock:
            storage[record_id] = record
            _persist(storage)
        self._send_json(201, record)


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Listening on http://{HOST}:{PORT}")
    server.serve_forever()
