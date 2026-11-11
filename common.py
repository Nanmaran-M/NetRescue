"""
common.py
Shared helpers for NetRescue.

Every message sent over a socket is one JSON object followed by a newline.
This keeps client/server code simple: read a line, parse JSON, done.
"""

import json

HOST = "127.0.0.1"   # localhost - no physical server needed
PORT = 6000           # central NetRescue server listens here

TIMEOUT_SECONDS = 5   # if a server misses heartbeats for this long, it's "failed"
HEARTBEAT_INTERVAL = 2


def send_msg(sock, obj: dict):
    """Send one JSON message, newline-terminated."""
    line = json.dumps(obj) + "\n"
    sock.sendall(line.encode("utf-8"))


def make_reader(sock):
    """Wrap a socket so we can read it line by line (one JSON message per line)."""
    return sock.makefile("r", encoding="utf-8")


def read_msg(reader):
    """Read and parse one JSON message. Returns None if the connection closed."""
    line = reader.readline()
    if not line:
        return None
    line = line.strip()
    if not line:
        return None
    return json.loads(line)
