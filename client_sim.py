"""
client_sim.py
Simulates ONE client (e.g. "Client 1"). Run several of these, one per terminal.

Run directly:
    python client_sim.py C1
"""

import socket
import sys

from common import HOST, PORT, make_reader, read_msg, send_msg


def run_client(name: str):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    reader = make_reader(sock)

    send_msg(sock, {"type": "register_client", "name": name})
    print(f"[{name}] connected. Waiting for assignment...")

    try:
        while True:
            msg = read_msg(reader)
            if msg is None:
                print(f"[{name}] disconnected from server.")
                break
            if msg["type"] == "assigned":
                print(f"[{name}] assigned to server {msg['server']}")
            elif msg["type"] == "reassigned":
                print(f"[{name}] MOVED to server {msg['server']} — reason: {msg['reason']}")
            elif msg["type"] == "error":
                print(f"[{name}] error: {msg['message']}")
    except (KeyboardInterrupt, ConnectionResetError):
        print(f"[{name}] stopped.")
    finally:
        sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python client_sim.py <name>")
        print("Example: python client_sim.py C1")
        sys.exit(1)

    run_client(sys.argv[1])
