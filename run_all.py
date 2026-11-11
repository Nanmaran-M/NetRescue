"""
run_all.py
----------
One-click launcher for the NetRescue immersive theater.

Usage:
    python run_all.py
    python run_all.py --servers 5 --clients 10

The GUI can also launch this file automatically from its NETWORK CONFIGURATION
console when the user changes the topology size.
"""

from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
PYTHON = sys.executable
PROCESSES = []

CAPACITIES = [10, 8, 12, 14, 9, 16, 11, 13]
RELIABILITIES = [0.90, 0.80, 0.95, 0.92, 0.88, 0.97, 0.91, 0.94]


def server_name(index: int) -> str:
    return chr(ord("A") + index)


def build_servers(count: int):
    result = []
    for i in range(count):
        capacity = CAPACITIES[i % len(CAPACITIES)]
        reliability = RELIABILITIES[i % len(RELIABILITIES)]
        initial_load = max(1, int(capacity * (0.20 + (i % 4) * 0.05)))
        result.append((server_name(i), capacity, reliability, initial_load))
    return result


def build_clients(count: int):
    return [f"C{i}" for i in range(1, count + 1)]


def port_busy(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(0.15)
    try:
        return sock.connect_ex(("127.0.0.1", port)) == 0
    finally:
        sock.close()


def wait_for_port_free(port: int, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not port_busy(port):
            return True
        time.sleep(0.25)
    return False


def wait_for_server(port: int, timeout: float = 12.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if port_busy(port):
            return True
        time.sleep(0.2)
    return False


def start_process(*args):
    process = subprocess.Popen(
        [PYTHON, *args],
        cwd=BASE,
    )
    PROCESSES.append(process)
    return process


def cleanup():
    for process in PROCESSES:
        try:
            if process.poll() is None:
                process.terminate()
        except OSError:
            pass


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--servers", type=int, default=3)
    parser.add_argument("--clients", type=int, default=3)
    return parser.parse_args()


def main():
    args = parse_args()
    server_count = max(2, min(8, args.servers))
    client_count = max(1, min(20, args.clients))

    # The central server uses the fixed TCP port from common.py.
    if port_busy(6000):
        if not wait_for_port_free(6000):
            print("Port 6000 is still busy. Close an older NetRescue instance and try again.")
            return 1

    print(f"Starting NetRescue with {server_count} servers and {client_count} clients...")

    try:
        central = start_process("netrescue_server.py")

        if not wait_for_server(6000):
            print("Central NetRescue server did not start.")
            return 1

        for name, capacity, reliability, load in build_servers(server_count):
            start_process(
                "virtual_server.py",
                name,
                str(capacity),
                f"{reliability:.2f}",
                str(load),
            )
            time.sleep(0.30)

        time.sleep(1.0)

        for name in build_clients(client_count):
            start_process("client_sim.py", name)
            time.sleep(0.20)

        print("NetRescue is running.")
        print("Close the theater window to stop this demo.")
        return_code = central.wait()
        return return_code
    finally:
        cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
