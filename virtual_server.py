import random
import socket
import sys
import threading
import time

from common import HOST, PORT, HEARTBEAT_INTERVAL, make_reader, read_msg, send_msg


def _command_listener(sock, name, state):
    reader = make_reader(sock)
    try:
        while state['running']:
            msg = read_msg(reader)
            if msg is None:
                break
            if msg.get('type') == 'simulate_overload':
                state['load'] = state['capacity']
                print(f'[{name}] Overload simulated by dashboard -- load set to {state["capacity"]}')
            elif msg.get('type') == 'set_load':
                state['load'] = min(state['capacity'], max(0.0, float(msg.get('load', 0))))
                print(f'[{name}] Dashboard set load to {state["load"]:.1f}/{state["capacity"]:.1f}')
            elif msg.get('type') == 'stop_heartbeats':
                state['heartbeats_enabled'] = False
                print(f'[{name}] Paused heartbeats (simulated failure).')
            elif msg.get('type') == 'resume_heartbeats':
                state['heartbeats_enabled'] = True
                print(f'[{name}] Resumed heartbeats (restored).')
            elif msg.get('type') == 'ping':
                send_msg(sock, {'type': 'ping_ack', 'name': name})
    except Exception:
        pass


def run_virtual_server(name: str, capacity: float, reliability: float, load: float):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    send_msg(sock, {
        'type': 'register_server',
        'name': name,
        'capacity': capacity,
        'reliability': reliability,
        'load': load,
    })
    print(f'[{name}] registered with central server. Sending heartbeats every '
          f'{HEARTBEAT_INTERVAL}s. Ctrl+C to stop (simulates a crash).')

    state = {'running': True, 'heartbeats_enabled': True, 'load': float(load), 'capacity': float(capacity)}

    listener = threading.Thread(
        target=_command_listener, args=(sock, name, state), daemon=True
    )
    listener.start()

    try:
        while state['running']:
            if state.get('heartbeats_enabled', True):
                state['load'] = min(state['capacity'], max(0, state['load'] + random.choice([-1, 0, 0, 1, 1])))
                send_msg(sock, {'type': 'heartbeat', 'name': name, 'load': state['load']})
            time.sleep(HEARTBEAT_INTERVAL)
    except (KeyboardInterrupt, BrokenPipeError, ConnectionResetError):
        print(f'[{name}] stopped sending heartbeats (simulated crash).')
    finally:
        state['running'] = False
        sock.close()


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print('Usage: python virtual_server.py <name> <capacity> <reliability> <load>')
        print('Example: python virtual_server.py A 10 0.9 2')
        sys.exit(1)

    name = sys.argv[1]
    capacity = float(sys.argv[2])
    reliability = float(sys.argv[3])
    load = float(sys.argv[4])
    run_virtual_server(name, capacity, reliability, load)
