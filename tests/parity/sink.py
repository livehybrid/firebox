"""A Stoker-protocol sink: listens on a unix socket, accepts every
connection, parses NDJSON envelopes and records them. Stdlib only."""
from __future__ import annotations

import json
import os
import socket
import threading
from typing import Dict, List


class Sink:
    def __init__(self, path: str):
        self.path = path
        if os.path.exists(path):
            os.unlink(path)
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(path)
        self.listener.listen(64)
        self.listener.settimeout(0.2)
        self.events: List[Dict] = []
        self.malformed = 0
        self.connections = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._accept = threading.Thread(target=self._run, daemon=True)
        self._accept.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                conn, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self._lock:
                self.connections += 1
            t = threading.Thread(target=self._serve, args=(conn,), daemon=True)
            t.start()
            self._threads.append(t)

    def _serve(self, conn):
        buf = b""
        conn.settimeout(0.5)
        while not self._stop.is_set():
            try:
                chunk = conn.recv(1 << 16)
            except socket.timeout:
                continue
            except OSError:
                break
            if not chunk:
                break
            buf += chunk
            while True:
                i = buf.find(b"\n")
                if i < 0:
                    break
                line, buf = buf[:i], buf[i + 1:]
                self._handle(line)
        if buf.strip():
            self._handle(buf)
        conn.close()

    def _handle(self, line: bytes):
        try:
            doc = json.loads(line.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            with self._lock:
                self.malformed += 1
            return
        if not isinstance(doc, dict) or doc.get("event") is None:
            with self._lock:
                self.malformed += 1
            return
        with self._lock:
            self.events.append(doc)

    def count(self) -> int:
        with self._lock:
            return len(self.events)

    def close(self):
        self._stop.set()
        try:
            self.listener.close()
        except OSError:
            pass
        for t in self._threads:
            t.join(2)
        if os.path.exists(self.path):
            os.unlink(self.path)
