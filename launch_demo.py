"""
launch_demo.py
--------------
Multiprocessing launcher for the NetRescue demo.

Usage:
    python launch_demo.py --servers 3 --clients 3
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import time

from virtual_server import run_virtual_server
from client_sim import run_client

CAPACITIES = [10, 8, 12, 14, 9, 16, 11, 13]
RELIABILITIES = [0.90, 0.80, 0.95, 0.92, 0.88, 0.97, 0.91, 0.94]


def build_servers(count):
    items = []
    for i in range(count):
        name = chr(ord("A") + i)
        cap = CAPACITIES[i % len(CAPACITIES)]
        rel = RELIABILITIES[i % len(RELIABILITIES)]
        load = max(1, int(cap * (0.20 + (i % 4) * 0.05)))
        items.append((name, cap, rel, load))
    return items


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--servers", type=int, default=3)
    parser.add_argument("--clients", type=int, default=3)
    args = parser.parse_args()

    server_count = max(2, min(8, args.servers))
    client_count = max(1, min(20, args.clients))

    processes = []
    try:
        for name, cap, rel, load in build_servers(server_count):
            p = mp.Process(
                target=run_virtual_server,
                args=(name, cap, rel, load),
                daemon=True,
            )
            p.start()
            processes.append(p)
            time.sleep(0.30)

        time.sleep(1.0)

        for i in range(1, client_count + 1):
            p = mp.Process(
                target=run_client,
                args=(f"C{i}",),
                daemon=True,
            )
            p.start()
            processes.append(p)
            time.sleep(0.20)

        print(f"Launched {server_count} servers and {client_count} clients.")
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Stopping demo...")
    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()


if __name__ == "__main__":
    main()
