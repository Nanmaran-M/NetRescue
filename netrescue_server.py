"""
netrescue_server.py
The CENTRAL NetRescue server.

Run this FIRST, in its own terminal (or use run_all.py to do everything at once):
    python netrescue_server.py

Responsibilities:
  - Accept socket connections from virtual servers and clients   (Socket Programming)
  - Track heartbeats, detect failures                            (Failure Detection)
  - Pick the best replacement server using a SymPy formula        (SymPy)
  - Use filter()/map() to pick and score healthy servers          (Functional Programming)
  - Show everything live in a Tkinter dashboard                   (Tkinter GUI)

Extra features in the full implementation:
  - Multi-factor health/recovery score with SymPy
  - Predictive overload using linear trend extrapolation
  - Predictive preventive client rebalancing
  - Failure-reason classification
  - Failure history and recovery-time metrics
  - Network Resilience Index (project metric)
  - Explainable recovery decision and factor breakdown
"""

import math
import socket
import threading
import os
import subprocess
import statistics
import time
import sys
import ctypes
from collections import deque
import tkinter as tk
from tkinter import ttk

from sympy import symbols, lambdify

from common import HOST, PORT, TIMEOUT_SECONDS, HEARTBEAT_INTERVAL, make_reader, read_msg, send_msg

# ---------------------------------------------------------------------------
# SHARED STATE  (protected by `lock` since multiple threads touch it)
# ---------------------------------------------------------------------------
lock = threading.RLock()

servers = {}   # name -> {conn, capacity, reliability, load, status, last_heartbeat,
               #          load_history, heartbeat_intervals, failure_count, overloaded}
clients = {}   # name -> {conn, server}
event_log = []  # list of (stamp, kind, text), most recent last
last_recovery_incident = {}  # stores detailed incident data including recovery switch time and reasons
metrics = {
    "failures": 0,
    "recoveries": 0,
    "rebalances": 0,
    "total_recovery_time": 0.0,
    "recovery_times": [],
}

# Project-tuned thresholds. These are intentionally visible so the design can
# be explained in a viva instead of hiding magic numbers in the code.
OVERLOAD_RATIO = 0.75  # load/capacity at or above this is overloaded
PREDICTION_HORIZON = 60  # seconds — only surface predictions within this window
PREVENTIVE_ACTION_WINDOW = 15  # seconds — act automatically when risk is close
PREVENTIVE_ACTION_COOLDOWN = 15  # seconds per server between preventive moves
PREVENTIVE_SCORE_MARGIN = 1.05  # alternative should be at least 5% better
last_preventive_action = {}
AUTO_RECOVERY_ENABLED = True



def log_event(text: str, kind: str = "INFO"):
    stamp = time.strftime("%H:%M:%S")
    upper_text = text.upper()
    if kind == "INFO":
        if upper_text.startswith("FAILURE") or "CRITICAL INCIDENT" in upper_text or "FAILED" in upper_text:
            kind = "FAILURE"
        elif upper_text.startswith("RECOVERY") or upper_text.startswith("RESTORE") or upper_text.startswith("REBALANCE") or "PREVENTIVE" in upper_text or "AUTO-RESCUE" in upper_text or "MOVED CLIENT" in upper_text:
            kind = "RECOVERY"
        elif upper_text.startswith("OVERLOAD") or upper_text.startswith("WARNING"):
            kind = "OVERLOAD"
        elif upper_text.startswith("PREDICTION"):
            kind = "PREDICTION"

    with lock:
        event_log.append((stamp, kind, text))
        if len(event_log) > 300:
            event_log.pop(0)
    print(f"[{stamp}] [{kind:<10}] {text}")


# ---------------------------------------------------------------------------
# SYMPY MULTI-FACTOR HEALTH / RECOVERY MODEL
# ---------------------------------------------------------------------------
# The original formula only used capacity, reliability and load.
# The upgraded model considers five factors so the replacement decision can
# be explained instead of looking like a random choice:
#
#   A = available-capacity ratio
#   R = server reliability
#   H = heartbeat stability
#   B = client-balance factor
#   F = failure-history factor
#
# Health Score = 100 * (0.30A + 0.20R + 0.15H + 0.25B + 0.10F)
#
# This is a project-specific weighted heuristic, not a standard industry score.
_A, _R, _H, _B, _F = symbols("A R H B F")
_score_expr = 100 * (
    0.30 * _A
    + 0.20 * _R
    + 0.15 * _H
    + 0.25 * _B
    + 0.10 * _F
)
_score_func = lambdify((_A, _R, _H, _B, _F), _score_expr, "math")


def heartbeat_stability(name: str) -> float:
    """Return a 0..1 stability factor from heartbeat timing jitter."""
    with lock:
        if name not in servers:
            return 1.0
        intervals = list(servers[name].get("heartbeat_intervals", []))
    if len(intervals) < 2:
        return 1.0
    avg = statistics.fmean(intervals)
    if avg <= 0:
        return 0.0
    jitter = statistics.fmean(abs(x - HEARTBEAT_INTERVAL) for x in intervals)
    return max(0.0, min(1.0, 1.0 - jitter / HEARTBEAT_INTERVAL))


def score_breakdown(name: str) -> dict:
    """Return the five normalized factors and final 0..100 score."""
    with lock:
        if name not in servers:
            return {
                "available_capacity": 1.0,
                "reliability": 0.9,
                "heartbeat_stability": 1.0,
                "client_balance": 1.0,
                "failure_history": 1.0,
                "score": 85.0,
            }
        s = dict(servers[name])
        client_count = sum(1 for info in list(clients.values()) if info.get("server") == name)

    capacity = max(float(s.get("capacity", 10.0)), 1.0)
    load = max(float(s.get("load", 0.0)), 0.0)
    available_ratio = max(0.0, min(1.0, (capacity - load) / capacity))
    client_balance = max(0.0, min(1.0, 1.0 - (client_count / capacity)))
    failure_factor = 1.0 / (1.0 + float(s.get("failure_count", 0)))
    heartbeat_factor = heartbeat_stability(name)

    score = _score_func(
        available_ratio,
        float(s.get("reliability", 0.9)),
        heartbeat_factor,
        client_balance,
        failure_factor,
    )

    return {
        "available_capacity": available_ratio,
        "reliability": float(s.get("reliability", 0.9)),
        "heartbeat_stability": heartbeat_factor,
        "client_balance": client_balance,
        "failure_history": failure_factor,
        "score": max(0.0, min(100.0, float(score))),
    }


def recovery_score(name: str) -> float:
    """Return the multi-factor health/recovery score used for server selection."""
    try:
        return score_breakdown(name)["score"]
    except Exception:
        return 50.0


def _pick_best_server_name(exclude: str):
    """Filter healthy servers, score them, and select the highest score."""
    with lock:
        healthy_names = [
            n for n, s in list(servers.items())
            if n != exclude and s.get("status") == "healthy"
        ]
    if not healthy_names:
        return None
    scored = [(n, recovery_score(n)) for n in healthy_names]
    best_name, _ = max(scored, key=lambda pair: pair[1])
    return best_name


def pick_best_server(exclude: str):
    """Return (best_server, score) for the immersive GUI."""
    best_name = _pick_best_server_name(exclude)
    if not best_name:
        return None, None
    return best_name, recovery_score(best_name)


def format_score_explanation(name: str) -> str:
    """Create a human-readable reason for why this server scored as it did."""
    b = score_breakdown(name)
    return (
        f"Available capacity: {b['available_capacity'] * 100:.0f}%\n"
        f"Reliability: {b['reliability'] * 100:.0f}%\n"
        f"Heartbeat stability: {b['heartbeat_stability'] * 100:.0f}%\n"
        f"Client balance: {b['client_balance'] * 100:.0f}%\n"
        f"Failure-history factor: {b['failure_history'] * 100:.0f}%\n"
        f"Final health score: {b['score']:.1f}/100"
    )


# ---------------------------------------------------------------------------
# FEATURE: FAILURE-REASON CLASSIFICATION
# Looks at recent load trend + heartbeat regularity to guess *why* a server
# failed, instead of just saying "timeout". Simple rule-based logic - not a
# trained model, but a genuine diagnosis step, not decoration.
# ---------------------------------------------------------------------------
def classify_failure(name: str, trigger_reason: str) -> str:
    if trigger_reason == "manually simulated":
        return "Manual Trigger"

    s = servers[name]
    if trigger_reason == "heartbeat timeout":
        ratio = (s["load"] / s["capacity"]) if s["capacity"] else 0.0
        if ratio >= OVERLOAD_RATIO and len(s["load_history"]) >= 3:
            return "Gradual Overload"
        intervals = list(s["heartbeat_intervals"])
        if len(intervals) >= 3:
            avg_interval = statistics.fmean(intervals)
            jitter = statistics.fmean(abs(x - HEARTBEAT_INTERVAL) for x in intervals)
            if jitter > HEARTBEAT_INTERVAL * 0.35 or avg_interval > HEARTBEAT_INTERVAL * 1.35:
                return "Unstable Connection"
        return "Sudden Crash / Heartbeat Loss"
    loads = [l for _, l in s["load_history"]]
    intervals = list(s["heartbeat_intervals"])

    if len(loads) >= 3:
        recent_loads = loads[-6:] if len(loads) > 6 else loads
        increasing = all(recent_loads[i] <= recent_loads[i + 1] for i in range(len(recent_loads) - 1))
        if increasing and (recent_loads[-1] - recent_loads[0]) >= 2:
            return "Gradual Overload"

    if len(intervals) >= 2:
        avg = sum(intervals) / len(intervals)
        variance = sum((x - avg) ** 2 for x in intervals) / len(intervals)
        if variance > (avg if avg > 0 else 1) * 2:
            return "Unstable Connection"

    return "Sudden Crash"


# ---------------------------------------------------------------------------
# FEATURE: FAILURE PREDICTION
# Uses least-squares linear regression on the server's recent load_history to
# fit a trend line and extrapolate *when* the load will cross the overload
# threshold.  If that moment is within PREDICTION_HORIZON seconds we surface:
#   - the ETA in seconds
#   - the best replacement server (highest recovery score)
#   - how many clients to move (always 1 — we move gradually, same as rebalancing)
#
# Must be called WITHOUT holding `lock` — it acquires the lock itself for the
# snapshot, then releases before calling pick_best_server / recovery_score.
# ---------------------------------------------------------------------------
def predict_overload(name: str):
    """Return (eta_seconds, best_server, best_score) or None."""
    with lock:
        if name not in servers or servers[name]["status"] != "healthy":
            return None
        s = servers[name]
        history = list(s["load_history"])   # [(timestamp, load), ...]
        current_load = s["load"]
        capacity     = s["capacity"]

    if len(history) > 8:
        history = history[-8:]

    if len(history) < 3:
        return None   # not enough data for a meaningful trend

    overload_threshold = OVERLOAD_RATIO * capacity
    if current_load >= overload_threshold:
        return None   # already flagged overloaded — no need to predict

    # --- Least-squares linear regression: load = slope*t + intercept ----------
    times = [t for t, _ in history]
    loads = [l for _, l in history]
    t0    = times[0]
    ts    = [t - t0 for t in times]   # shift origin to avoid large numbers

    n      = len(ts)
    sum_t  = sum(ts)
    sum_l  = sum(loads)
    sum_tl = sum(t * l for t, l in zip(ts, loads))
    sum_t2 = sum(t * t for t in ts)

    denom = n * sum_t2 - sum_t ** 2
    if abs(denom) < 1e-9:
        return None   # all timestamps identical — can't fit a line

    slope = (n * sum_tl - sum_t * sum_l) / denom   # load-units per second

    if slope <= 0.05:
        return None   # trending flat or downward — no imminent overload

    # eta = how many seconds until load reaches the overload threshold
    eta = (overload_threshold - current_load) / slope

    if eta < 0 or eta > PREDICTION_HORIZON:
        return None   # already past threshold or too far away to matter

    best = _pick_best_server_name(exclude=name)
    if best is None:
        return None

    with lock:
        best_score = recovery_score(best) if best in servers else None

    return (round(eta), best, best_score)


# ---------------------------------------------------------------------------
# RECOVERY LOGIC
# ---------------------------------------------------------------------------
def _complete_recovery_metrics(start_time: float, reference_time: float | None = None):
    """Record total failure-to-recovery duration for dashboard metrics."""
    handling_time = max(0.0, time.time() - start_time)
    total_time = handling_time
    if reference_time is not None:
        total_time = max(0.0, time.time() - reference_time)
    with lock:
        metrics["recovery_times"].append(total_time)
        if len(metrics["recovery_times"]) > 100:
            metrics["recovery_times"].pop(0)
        metrics["total_recovery_time"] += total_time
    return total_time, handling_time


def trigger_recovery(failed_name: str, trigger_reason: str, failure_reference_time: float | None = None):
    recovery_start = time.time()
    with lock:
        if failed_name not in servers or servers[failed_name]["status"] == "failed":
            # If server is already failed, check if stranded clients still need rescue
            stranded = [c for c, info in clients.items() if info["server"] == failed_name]
            if not stranded:
                return  # already handled
            diagnosis = servers[failed_name].get("last_diagnosis", "Server Offline")
        else:
            diagnosis = classify_failure(failed_name, trigger_reason)
            servers[failed_name]["status"] = "failed"
            servers[failed_name]["overloaded"] = False
            servers[failed_name]["failure_count"] += 1
            servers[failed_name]["last_diagnosis"] = diagnosis
            servers[failed_name]["last_failure_time"] = time.time()
            metrics["failures"] += 1
            metrics["recoveries"] += 1

    log_event(f"FAILURE: Server {failed_name} ({trigger_reason}) -> Diagnosis: {diagnosis}", "FAILURE")

    with lock:
        best = _pick_best_server_name(exclude=failed_name)
        affected_clients = [c for c, info in clients.items() if info["server"] == failed_name]
        best_score = recovery_score(best) if best else None
        candidate_scores = {
            n: recovery_score(n)
            for n in servers.keys()
            if n != failed_name and servers[n]["status"] == "healthy"
        }

    if best is None:
        total_time, handling_time = _complete_recovery_metrics(recovery_start, failure_reference_time)
        log_event(
            f"RECOVERY FAILED: No healthy server available for recovery! "
            f"Total failure-to-decision: {total_time:.2f}s",
            "RECOVERY"
        )
        return

    # MEASURE EXACT CLIENT SWITCHING TIME:
    # "recovery time meant here the time of switching the clients to the other server when the server fails"
    switch_start = time.perf_counter()

    for cname in affected_clients:
        with lock:
            clients[cname]["server"] = best
            servers[best]["load"] += 1
            conn = clients[cname]["conn"]
        try:
            send_msg(conn, {
                "type": "reassigned",
                "server": best,
                "reason": f"{failed_name} failed; selected using multi-factor health score",
            })
        except OSError:
            pass
        log_event(f"  Moved client {cname} from {failed_name} -> {best}", "RECOVERY")

    switch_end = time.perf_counter()
    client_switch_time = max(0.0001, switch_end - switch_start)
    client_switch_time_ms = client_switch_time * 1000.0

    total_time, handling_time = _complete_recovery_metrics(recovery_start, failure_reference_time)

    # Immediately reflect load change in load history for live graphs
    now = time.time()
    with lock:
        servers[failed_name]["load"] = 0.0
        servers[failed_name]["load_history"].append((now, 0.0))
        servers[best]["load_history"].append((now, servers[best]["load"]))
        servers[failed_name]["last_recovery_time"] = client_switch_time
        servers[failed_name]["last_recovery_time_ms"] = client_switch_time_ms

    factors = score_breakdown(best)
    runner_ups = sorted(
        [(n, s) for n, s in candidate_scores.items() if n != best],
        key=lambda pair: pair[1],
        reverse=True
    )
    runner_up_name = f"Server {runner_ups[0][0]} (Score {runner_ups[0][1]:.1f}/100)" if runner_ups else "None (single candidate)"

    reasons = [
        f"1. Failure Trigger: Server {failed_name} failed ({diagnosis} via {trigger_reason}).",
        f"2. Failover Action: Connected client(s) {affected_clients or 'none'} rerouted to prevent service disruption.",
        f"3. Selection Rationale: Server {best} chosen with highest overall Health Score ({best_score:.1f}/100).",
        f"4. Available Headroom: {factors['available_capacity']*100:.0f}% free capacity (lowest active load).",
        f"5. Reliability & Stability: {factors['reliability']*100:.0f}% reliability, {factors['heartbeat_stability']*100:.0f}% heartbeat stability.",
        f"6. Load Distribution: {factors['client_balance']*100:.0f}% balance factor with {factors['failure_history']*100:.0f}% low-failure rating.",
        f"7. Competitive Margin: Outscored runner-up {runner_up_name}.",
    ]

    incident_record = {
        "failed_server": failed_name,
        "replacement_server": best,
        "trigger_reason": trigger_reason,
        "diagnosis": diagnosis,
        "switch_time": client_switch_time,
        "switch_time_ms": client_switch_time_ms,
        "total_time": total_time,
        "affected_clients": list(affected_clients),
        "best_score": best_score,
        "factors": factors,
        "reasons": reasons,
        "runner_ups": runner_ups,
        "timestamp": time.strftime("%H:%M:%S"),
    }

    with lock:
        last_recovery_incident[failed_name] = incident_record
        last_recovery_incident["latest"] = incident_record
        servers[failed_name]["last_incident"] = incident_record

    log_event(
        f"RECOVERY DECISION: {failed_name} -> {best} (health score={best_score:.1f}/100)",
        "RECOVERY"
    )
    log_event(
        f"RECOVERY COMPLETE: {failed_name} -> {best} in {client_switch_time_ms:.2f} ms ({client_switch_time:.4f}s) "
        f"[Client switch recovery time]",
        "RECOVERY"
    )


def rescue_stranded_clients(failed_name: str):
    """Rescue any clients stranded on a failed server without reviving the failed server."""
    with lock:
        if failed_name not in servers or servers[failed_name]["status"] != "failed":
            return
        affected_clients = [c for c, info in clients.items() if info["server"] == failed_name]
        best = _pick_best_server_name(exclude=failed_name)
    if not affected_clients:
        log_event(f"RESCUE: No clients stranded on Server {failed_name}", "INFO")
        return
    if best is None:
        log_event(f"RESCUE FAILED: No healthy server available to receive clients from Server {failed_name}!", "RECOVERY")
        return

    switch_start = time.perf_counter()
    for cname in affected_clients:
        with lock:
            clients[cname]["server"] = best
            servers[best]["load"] += 1
            conn = clients[cname]["conn"]
        try:
            send_msg(conn, {
                "type": "reassigned",
                "server": best,
                "reason": f"Manual rescue from failed server {failed_name}",
            })
        except OSError:
            pass
        log_event(f"  Moved client {cname} from {failed_name} -> {best}", "RECOVERY")
    switch_end = time.perf_counter()
    switch_time = max(0.0001, switch_end - switch_start)
    switch_time_ms = switch_time * 1000.0

    now = time.time()
    with lock:
        servers[failed_name]["load"] = 0.0
        servers[failed_name]["load_history"].append((now, 0.0))
        servers[best]["load_history"].append((now, servers[best]["load"]))
        servers[failed_name]["last_recovery_time"] = switch_time
        servers[failed_name]["last_recovery_time_ms"] = switch_time_ms

    log_event(f"RESCUE COMPLETE: Switched clients {affected_clients} to Server {best} in {switch_time_ms:.2f} ms", "RECOVERY")


def manual_fail(name: str):
    """Simulate a real failure by pausing the virtual server heartbeat stream."""
    with lock:
        if name not in servers or servers[name]["status"] == "failed":
            return
        conn = servers[name]["conn"]
    try:
        send_msg(conn, {"type": "stop_heartbeats"})
    except OSError:
        pass
    threading.Thread(
        target=trigger_recovery,
        args=(name, "manually simulated"),
        daemon=True,
    ).start()


def manual_overload(name: str):
    """Spike a server to full capacity and tell the virtual server process."""
    with lock:
        if name not in servers or servers[name]["status"] != "healthy":
            return
        servers[name]["load"] = servers[name]["capacity"]
        conn = servers[name]["conn"]
    try:
        send_msg(conn, {"type": "simulate_overload"})
    except OSError:
        pass
    log_event(f"OVERLOAD SIMULATED: {name} — load set to capacity")


def manual_set_load(name: str, requested_load: float):
    """Interactively set a simulated server load from the dashboard."""
    with lock:
        if name not in servers or servers[name]["status"] != "healthy":
            return
        capacity = float(servers[name]["capacity"])
        value = max(0.0, min(float(requested_load), capacity))
        servers[name]["load"] = value
        conn = servers[name]["conn"]
    try:
        send_msg(conn, {"type": "set_load", "load": value})
    except OSError:
        pass
    log_event(f"LOAD CONTROL: Server {name} set to {value:.1f}/{capacity:.1f}")


# ---------------------------------------------------------------------------
# FEATURE: PREDICTIVE PREVENTIVE REBALANCING
# If regression predicts overload very soon, move one client before the server
# reaches the threshold. This is the key step that makes the system proactive.
# ---------------------------------------------------------------------------
def preventive_rebalance(overloaded_name: str, best: str):
    now = time.time()
    last = last_preventive_action.get(overloaded_name, 0.0)
    if now - last < PREVENTIVE_ACTION_COOLDOWN:
        return False

    with lock:
        if overloaded_name not in servers or best not in servers:
            return False
        if servers[overloaded_name]["status"] != "healthy":
            return False
        if servers[best]["status"] != "healthy":
            return False

        candidates = [c for c, info in clients.items() if info["server"] == overloaded_name]
        if not candidates:
            return False

        source_score = recovery_score(overloaded_name)
        target_score = recovery_score(best)
        if target_score <= source_score * PREVENTIVE_SCORE_MARGIN:
            return False

        cname = candidates[0]
        clients[cname]["server"] = best
        servers[overloaded_name]["load"] = max(0, servers[overloaded_name]["load"] - 1)
        servers[best]["load"] += 1
        metrics["rebalances"] += 1
        conn = clients[cname]["conn"]
        last_preventive_action[overloaded_name] = now

    try:
        send_msg(
            conn,
            {
                "type": "reassigned",
                "server": best,
                "reason": f"Predictive prevention: {overloaded_name} was nearing overload",
            },
        )
    except OSError:
        pass

    log_event(
        f"PREVENTIVE ACTION: {overloaded_name} nearing overload -> "
        f"moved client {cname} to {best}"
    )
    return True


# ---------------------------------------------------------------------------
# FEATURE: PROACTIVE LOAD REBALANCING
# A server crossing the overload line hasn't FAILED - it's still healthy and
# still serving its clients. Without this, a client stuck on an overloaded
# server just sits there indefinitely, since trigger_recovery only ever runs
# for servers that actually go down. This moves ONE client off the
# overloaded server onto a genuinely better one, as soon as it starts
# struggling - not waiting for a full crash.
# ---------------------------------------------------------------------------
def rebalance_overloaded_server(overloaded_name: str):
    with lock:
        candidates = [c for c, info in clients.items() if info["server"] == overloaded_name]
        if not candidates:
            return
        best = _pick_best_server_name(exclude=overloaded_name)
        if best is None:
            return
        # only move if the alternative is actually meaningfully better -
        # otherwise we'd just be shuffling clients for no real gain
        if recovery_score(best) <= recovery_score(overloaded_name):
            return
        cname = candidates[0]  # move just one client at a time, gradually
        clients[cname]["server"] = best
        servers[overloaded_name]["load"] = max(0, servers[overloaded_name]["load"] - 1)
        servers[best]["load"] += 1
        metrics["rebalances"] += 1
        conn = clients[cname]["conn"]

    try:
        send_msg(conn, {"type": "reassigned", "server": best,
                         "reason": f"{overloaded_name} overloaded - proactive rebalance"})
    except OSError:
        pass
    log_event(f"REBALANCE: {overloaded_name} is overloaded -> moved client {cname} to {best}")


# ---------------------------------------------------------------------------
# BACKGROUND MONITOR THREAD  (heartbeat timeouts + overload warnings + rebalancing)
# Combined into one thread/one loop to keep things simple and reduce
# background thread count on Windows.
# ---------------------------------------------------------------------------
def monitor_loop():
    last_prediction_log = {}  # server_name -> timestamp of last logged prediction warning
    while True:
        time.sleep(1)
        now = time.time()
        warnings = []            # collect messages, log them AFTER releasing the lock
        failed_names = []
        failure_reference_times = {}
        newly_overloaded = []    # servers that JUST crossed the overload line this tick
        healthy_names = []

        with lock:
            for name, s in servers.items():
                if s["status"] != "healthy":
                    continue
                healthy_names.append(name)
                # heartbeat timeout check
                if now - s["last_heartbeat"] > TIMEOUT_SECONDS:
                    failed_names.append(name)
                    failure_reference_times[name] = s["last_heartbeat"]
                    continue
                # overload check
                ratio = s["load"] / s["capacity"] if s["capacity"] else 0
                was_overloaded = s["overloaded"]
                s["overloaded"] = ratio >= OVERLOAD_RATIO
                if s["overloaded"] and not was_overloaded:
                    warnings.append(f"WARNING: {name} approaching overload "
                                     f"(load {s['load']}/{s['capacity']})")
                    newly_overloaded.append(name)

        # Check for failure predictions only when automatic recovery is enabled.
        if AUTO_RECOVERY_ENABLED:
            for name in healthy_names:
                if name in failed_names or name in newly_overloaded:
                    continue
                pred = predict_overload(name)
                if pred:
                    eta, best_srv, _ = pred
                    last_time = last_prediction_log.get(name, 0)
                    if now - last_time >= 8:
                        last_prediction_log[name] = now
                        action = "Recommended action"
                        if eta <= PREVENTIVE_ACTION_WINDOW:
                            if preventive_rebalance(name, best_srv):
                                action = "Preventive action taken"
                        warnings.append(
                            f"PREDICTION: Server {name} is predicted to become overloaded in ~{eta} seconds. "
                            f"{action}: {best_srv}"
                        )

        # IMPORTANT: log_event() and rebalance_overloaded_server() both acquire
        # `lock` themselves, so none of this may run while `lock` is still
        # held above (threading.Lock is not reentrant - doing so would
        # deadlock this thread, and every other thread waiting on `lock`,
        # permanently).
        for w in warnings:
            log_event(w)
        if AUTO_RECOVERY_ENABLED:
            for name in failed_names:
                trigger_recovery(
                    name,
                    "heartbeat timeout",
                    failure_reference_time=failure_reference_times.get(name),
                )
            for name in newly_overloaded:
                rebalance_overloaded_server(name)
        elif failed_names or newly_overloaded:
            if failed_names:
                for name in failed_names:
                    log_event(f"AUTO RECOVERY PAUSED: failure detected on Server {name}")
            if newly_overloaded:
                for name in newly_overloaded:
                    log_event(f"AUTO REBALANCE PAUSED: overload detected on Server {name}")



# ---------------------------------------------------------------------------
# CONNECTION HANDLERS  (one thread per connected socket = real concurrency)
# ---------------------------------------------------------------------------
def handle_connection(conn, addr):
    reader = make_reader(conn)
    role = None
    name = None
    try:
        while True:
            msg = read_msg(reader)
            if msg is None:
                break

            mtype = msg.get("type")

            if mtype == "register_server":
                role, name = "server", msg["name"]
                with lock:
                    servers[name] = {
                        "conn": conn,
                        "capacity": msg["capacity"],
                        "reliability": msg["reliability"],
                        "load": msg["load"],
                        "status": "healthy",
                        "last_heartbeat": time.time(),
                        "load_history": deque([(time.time(), msg["load"])], maxlen=30),
                        "heartbeat_intervals": deque(maxlen=8),
                        "failure_count": 0,
                        "overloaded": False,
                        "last_diagnosis": "-",
                        "last_failure_time": None,
                        "last_recovery_time": None,
                        "recovery_cooldown_until": 0,  # epoch time; heartbeats cannot
                                                       # revive the server before this
                    }
                log_event(f"Server {name} registered "
                          f"(capacity={msg['capacity']}, reliability={msg['reliability']})")

            elif mtype == "heartbeat":
                name = msg["name"]
                with lock:
                    if name in servers:
                        s = servers[name]
                        now = time.time()
                        interval = now - s["last_heartbeat"]
                        s["heartbeat_intervals"].append(interval)
                        s["last_heartbeat"] = now
                        if s["status"] != "failed":
                            if "load" in msg:
                                s["load"] = msg["load"]
                                s["load_history"].append((now, msg["load"]))
                        # Removed auto-recovery of failed servers on heartbeat!
                        # Failed servers stay failed until explicitly restored via button or console.

            elif mtype == "ping_ack":
                name = msg.get("name", name or "?")
                log_event(f"PING ACK: Server {name} is responsive", "INFO")

            elif mtype == "register_client":
                role, name = "client", msg["name"]
                with lock:
                    target = _pick_best_server_name(exclude="")
                    if target is None:
                        send_msg(conn, {"type": "error", "message": "no servers available"})
                        continue
                    clients[name] = {"conn": conn, "server": target}
                    servers[target]["load"] += 1
                send_msg(conn, {"type": "assigned", "server": target})
                log_event(f"Client {name} connected -> assigned to {target}", "INFO")

    except (ConnectionResetError, BrokenPipeError, OSError):
        pass
    finally:
        if role == "server" and name:
            log_event(f"Server {name} disconnected", "INFO")
        elif role == "client" and name:
            with lock:
                clients.pop(name, None)
            log_event(f"Client {name} disconnected", "INFO")


def socket_server_thread():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen()
    log_event(f"NetRescue central server listening on {HOST}:{PORT}", "INFO")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()




# ---------------------------------------------------------------------------
# Immersive theater compatibility API
# ---------------------------------------------------------------------------
def load_ratio(server: dict) -> float:
    capacity = max(float(server.get("capacity", 1)), 1.0)
    return max(0.0, min(1.0, float(server.get("load", 0)) / capacity))

def trigger_failure(name: str, manual: bool = True):
    if manual:
        manual_fail(name)
    else:
        threading.Thread(
            target=trigger_recovery,
            args=(name, "heartbeat timeout"),
            daemon=True,
        ).start()

def set_server_load(name: str, load: float):
    manual_set_load(name, load)

def reset_server(name: str):
    with lock:
        if name not in servers:
            return
        conn = servers[name]["conn"]
        servers[name]["status"] = "healthy"
        servers[name]["overloaded"] = False
        servers[name]["recovery_cooldown_until"] = 0
        servers[name]["last_heartbeat"] = time.time()
        initial_load = max(1.0, float(servers[name]["capacity"]) * 0.20)
        servers[name]["load"] = initial_load
        servers[name]["load_history"].append((time.time(), initial_load))
    try:
        send_msg(conn, {"type": "resume_heartbeats"})
        send_msg(conn, {"type": "set_load", "load": initial_load})
    except OSError:
        pass
    log_event(f"RESTORE: Server {name} restored to healthy state by operator", "RECOVERY")

server_loop = socket_server_thread
events = event_log

def health_score(name: str) -> float:
    with lock:
        exists = name in servers
    return recovery_score(name) if exists else 0.0


def send_to_server(name: str, payload: dict) -> bool:
    """Send a control/ping JSON message to a connected virtual server."""
    with lock:
        state = servers.get(name)
        conn = state.get("conn") if state else None
    if conn is None:
        return False
    try:
        send_msg(conn, payload)
        return True
    except OSError:
        return False

def proactive_rebalance(name: str):
    """Compatibility wrapper for proactive overload rebalancing."""
    return rebalance_overloaded_server(name)

def predicted_eta(name: str):
    prediction = predict_overload(name)
    return prediction[0] if prediction else None


# ============================================================
# NETRESCUE "EMERGENCY OPERATIONS THEATER"
# ------------------------------------------------------------
# A deliberately different interface from a conventional dashboard.
#
# One dominant canvas:
#   • Rescue Core in the middle
#   • servers arranged as glowing command beacons
#   • clients orbiting their current server
#   • live heartbeat pulses
#   • failure blast / overload heat halo
#   • recovery route animation
#   • incident decision card appears only during a failure
#
# Tkinter Canvas only. No HTML / CSS / JavaScript.
# ============================================================

BG = "#050b14"
GRID = "#102338"
GRID_2 = "#16314a"
TEXT = "#eaf4ff"
MUTED = "#93acc4"
BLUE = "#3ea7ff"
CYAN = "#42e8ff"
GREEN = "#37e67b"
YELLOW = "#ffcc45"
ORANGE = "#ff8d3a"
RED = "#ff3e5d"
PURPLE = "#aa7cff"
WHITE = "#ffffff"


def enable_clear_dpi():
    """Enable native DPI rendering on Windows before creating Tk widgets."""
    if sys.platform != "win32":
        return
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
    except Exception:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
        except Exception:
            pass


class EmergencyOperationsTheater(tk.Tk):
    """Immersive Canvas-first NetRescue command theater."""

    def __init__(self):
        # Rendering-only improvement: native DPI + adaptive window sizing.
        enable_clear_dpi()
        super().__init__()

        self.title("NETRESCUE • Emergency Operations Theater")

        # Keep the same interface, but make sure the complete theater fits
        # inside the actual screen instead of being clipped on smaller laptops.
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        width = max(1000, min(1600, screen_w - 40))
        height = max(650, min(980, screen_h - 90))
        self.geometry(f"{width}x{height}+{max(10, (screen_w - width)//2)}+{max(10, (screen_h - height)//2)}")
        self.minsize(min(1200, width), min(760, height))
        self.configure(bg=BG)

        try:
            # 1.30 is intentionally conservative: text becomes clearer without
            # changing the existing theater composition or control rail.
            self.tk.call("tk", "scaling", 1.30)
        except tk.TclError:
            pass

        self.selected = "B"
        self.pulse = 0
        self.banner_until = 0.0
        self.banner_text = ""
        self.banner_color = RED
        self.last_failed = None
        self.zoom = 1.0
        self.drag_origin = None
        self.pan_x = 0
        self.pan_y = 0
        self.auto_mode = True

        self.server_points = {}
        self.client_points = {}
        self.hit_targets = []

        self._build()
        self._bind_keys()

        self.after(200, self.refresh)
        self.after(90, self.animate)

    # -------------------- window --------------------

    def _build(self):
        header = tk.Frame(self, bg="#06101c", height=64)
        header.pack(fill="x")
        header.pack_propagate(False)

        left = tk.Frame(header, bg="#06101c")
        left.pack(side="left", padx=22)

        tk.Label(
            left, text="NETRESCUE", bg="#06101c", fg=TEXT,
            font=("Segoe UI", 22, "bold")
        ).pack(side="left")
        tk.Label(
            left, text="  /  EMERGENCY OPERATIONS THEATER",
            bg="#06101c", fg="#7f9fbd", font=("Segoe UI", 10, "bold")
        ).pack(side="left", pady=6)

        self.header_state = tk.Label(
            header, text="● NETWORK STABLE", bg="#06101c", fg=GREEN,
            font=("Segoe UI", 10, "bold")
        )
        self.header_state.pack(side="right", padx=24)

        scene = tk.Frame(self, bg=BG)
        scene.pack(fill="both", expand=True, padx=10, pady=(10, 0))

        self.canvas = tk.Canvas(
            scene, bg=BG, highlightthickness=1,
            highlightbackground="#11283d", cursor="crosshair"
        )
        self.canvas.pack(fill="both", expand=True)
        self.canvas.bind("<Configure>", lambda _e: self.draw_scene())
        self.canvas.bind("<Button-1>", self.on_canvas_click)
        self.canvas.bind("<ButtonPress-2>", self.start_pan)
        self.canvas.bind("<B2-Motion>", self.pan_scene)
        self.canvas.bind("<MouseWheel>", self.on_zoom)

        # Compact command + console rail. The main window stays canvas-first.
        command = tk.Frame(self, bg="#071321", height=142)
        command.pack(fill="x")
        command.pack_propagate(False)

        action_row = tk.Frame(command, bg="#071321", height=62)
        action_row.pack(fill="x")
        action_row.pack_propagate(False)

        self.target_var = tk.StringVar(value="B")
        tk.Label(action_row, text="TARGET", bg="#071321", fg=MUTED,
                 font=("Segoe UI", 9, "bold")).pack(side="left", padx=(18, 6), pady=10)

        self.target_combo = ttk.Combobox(
            action_row, textvariable=self.target_var,
            values=("A", "B", "C"), state="readonly", width=5
        )
        self.target_combo.pack(side="left", padx=(0, 10), pady=10)

        self._action_button(action_row, "FAIL", self.do_fail, "#6e1525")
        self._action_button(action_row, "OVERLOAD", self.do_overload, "#6a4310")
        self._action_button(action_row, "AUTO RESCUE", self.do_auto_rescue, "#0f5a3c")
        self._action_button(action_row, "RESTORE", self.do_restore, "#123f67")
        self._action_button(action_row, "REBALANCE", self.do_rebalance, "#3f2f70")
        self._action_button(action_row, "RESET", self.do_reset, "#24364c")
        self._action_button(action_row, "PING", self.do_ping, "#155e75")

        self.auto_button = tk.Button(
            action_row, text="AUTO: ON", command=self.toggle_auto,
            bg="#14532d", fg="white", activebackground="#284b6b",
            activeforeground="white", relief="flat", bd=0, cursor="hand2",
            padx=12, pady=8, font=("Segoe UI", 9, "bold")
        )
        self.auto_button.pack(side="left", padx=4, pady=9)

        self.command_status = tk.Label(
            action_row,
            text="Interactive rescue controls",
            bg="#071321", fg="#6f89a6", font=("Consolas", 8)
        )
        self.command_status.pack(side="left", padx=14)

        console_row = tk.Frame(command, bg="#06111e", height=46)
        console_row.pack(fill="x")
        console_row.pack_propagate(False)

        tk.Label(
            console_row, text="CONSOLES", bg="#06111e", fg="#6f89a6",
            font=("Segoe UI", 9, "bold")
        ).pack(side="left", padx=(18, 8))

        self._console_button(console_row, "EVENT LOG", self.open_event_console, "#17314a")
        self._console_button(console_row, "DECISION FACTORS", self.open_decision_console, "#214532")
        self._console_button(console_row, "GRAPH HISTORY", self.open_graph_console, "#3b2d4f")
        self._console_button(console_row, "NETWORK CONFIG", self.open_config_console, "#3b3a26")

        tk.Label(
            console_row,
            text="Separate live consoles  •  history  •  recovery reasoning  •  topology size",
            bg="#06111e", fg="#7897b2", font=("Consolas", 8)
        ).pack(side="left", padx=16)

        self.ticker = tk.Label(
            self, text="", bg="#030811", fg="#89a4bd", anchor="w",
            padx=18, pady=6, font=("Consolas", 10)
        )
        self.ticker.pack(fill="x")

        self.console_windows = {}
        self.console_widgets = {}

    def _console_button(self, parent, text, command, color):
        tk.Button(
            parent, text=text, command=command,
            bg=color, fg="white", activebackground="#31597c",
            activeforeground="white", relief="flat", bd=0, cursor="hand2",
            padx=13, pady=6, font=("Segoe UI", 9, "bold")
        ).pack(side="left", padx=4, pady=6)

    def _action_button(self, parent, text, command, color):
        tk.Button(
            parent,
            text=text,
            command=command,
            bg=color,
            fg="white",
            activebackground="#284b6b",
            activeforeground="white",
            relief="flat",
            bd=0,
            cursor="hand2",
            padx=14,
            pady=9,
            font=("Segoe UI", 9, "bold"),
        ).pack(side="left", padx=4, pady=9)

    def _bind_keys(self):
        self.bind("<KeyPress-f>", lambda _e: self.do_fail())
        self.bind("<KeyPress-F>", lambda _e: self.do_fail())
        self.bind("<KeyPress-o>", lambda _e: self.do_overload())
        self.bind("<KeyPress-O>", lambda _e: self.do_overload())
        self.bind("<KeyPress-r>", lambda _e: self.do_restore())
        self.bind("<KeyPress-R>", lambda _e: self.do_restore())
        self.bind("<space>", lambda _e: self.do_auto_rescue())
        self.bind("<KeyPress-p>", lambda _e: self.do_ping())
        self.bind("<KeyPress-P>", lambda _e: self.do_ping())
        for key in "123456789":
            self.bind(f"<KeyPress-{key}>", lambda _e, k=key: self.select_server_index(int(k) - 1))

    # -------------------- controls --------------------

    def available_servers(self):
        with lock:
            return sorted(servers.keys())

    def target(self):
        value = self.target_var.get()
        names = self.available_servers()
        if value in names:
            return value
        return names[0] if names else "A"

    def select_server_index(self, index):
        names = self.available_servers()
        if 0 <= index < len(names):
            self.select_server(names[index])

    def select_server(self, name):
        self.selected = name
        self.target_var.set(name)
        self.command_status.config(
            text=f"SELECTED SERVER {name} • F=fail • O=overload • P=ping • Space=auto rescue • R=restore"
        )

    def _incident_banner(self, text, color=RED, seconds=4.0):
        self.banner_text = text
        self.banner_color = color
        self.banner_until = time.time() + seconds

    def do_fail(self):
        name = self.target()
        # Stop the real virtual-server heartbeat stream, then trigger recovery.
        send_to_server(name, {"type": "stop_heartbeats"})
        trigger_failure(name, manual=True)
        self.select_server(name)
        self.last_failed = name
        self._incident_banner(
            f"CRITICAL INCIDENT  //  SERVER {name} OFFLINE  //  RECOVERY ROUTE ENGAGED",
            RED,
            5.0,
        )

    def do_overload(self):
        name = self.target()
        with lock:
            s = servers.get(name)
            if not s or s["status"] != "healthy":
                return
            capacity = s["capacity"]

        set_server_load(name, capacity)
        log_event(f"Manual overload applied to {name}.", "OVERLOAD")
        self.select_server(name)
        self._incident_banner(
            f"PRESSURE EVENT  //  SERVER {name} AT {capacity}/{capacity}  //  LOAD SHEDDING",
            ORANGE,
            4.0,
        )

    def do_restore(self):
        name = self.target()
        reset_server(name)
        self.select_server(name)
        self._incident_banner(
            f"RESTORE  //  SERVER {name} HEARTBEAT STREAM RESUMED",
            GREEN,
            3.0,
        )

    def do_rebalance(self):
        name = self.target()
        proactive_rebalance(name)
        self.select_server(name)
        self._incident_banner(
            f"REBALANCE  //  CLIENT ROUTE OPTIMIZED FROM SERVER {name}",
            PURPLE,
            3.5,
        )

    def do_ping(self):
        name = self.target()
        if send_to_server(name, {"type": "ping"}):
            self._incident_banner(
                f"PING  //  SERVER {name} ACK CHANNEL TEST SENT",
                CYAN,
                2.5,
            )
        else:
            self._incident_banner(
                f"PING  //  SERVER {name} NOT CONNECTED",
                RED,
                3.0,
            )

    def toggle_auto(self):
        global AUTO_RECOVERY_ENABLED
        AUTO_RECOVERY_ENABLED = not AUTO_RECOVERY_ENABLED
        self.auto_mode = AUTO_RECOVERY_ENABLED
        if hasattr(self, "auto_button"):
            self.auto_button.config(
                text="AUTO: ON" if AUTO_RECOVERY_ENABLED else "AUTO: OFF",
                bg="#14532d" if AUTO_RECOVERY_ENABLED else "#5b2330",
            )
        self._incident_banner(
            "AUTOMATION  //  AUTO RECOVERY " + ("ENABLED" if AUTO_RECOVERY_ENABLED else "PAUSED"),
            GREEN if AUTO_RECOVERY_ENABLED else ORANGE,
            3.0,
        )

    def do_auto_rescue(self):
        failed = []
        overloaded = []

        with lock:
            for name, data in servers.items():
                if data["status"] == "failed":
                    failed.append(name)
                elif load_ratio(data) >= OVERLOAD_RATIO:
                    overloaded.append(name)

        for name in failed:
            rescue_stranded_clients(name)

        for name in overloaded:
            proactive_rebalance(name)

        self._incident_banner(
            "AUTO-RESCUE  //  SCANNING NETWORK  //  APPLYING BEST AVAILABLE ROUTES",
            GREEN,
            4.0,
        )

    def do_reset(self):
        # Restore every currently registered server without destroying the simulation.
        with lock:
            names = list(servers.keys())
        for name in names:
            reset_server(name)

        self.selected = "B"
        self.target_var.set("B")
        self._incident_banner(
            "NETWORK RESET  //  ALL REGISTERED SERVERS RETURNED TO MONITORED STATE",
            BLUE,
            3.5,
        )

    # -------------------- scene math --------------------

    def center(self):
        w = max(900, self.canvas.winfo_width())
        h = max(520, self.canvas.winfo_height())
        return w / 2 + self.pan_x, h / 2 + self.pan_y

    def point(self, cx, cy, radius, angle):
        return (
            cx + radius * math.cos(angle),
            cy + radius * math.sin(angle),
        )

    def server_layout(self):
        cx, cy = self.center()
        names = self.available_servers()
        n = len(names)
        if n == 0:
            return {}

        radius = min(
            self.canvas.winfo_width(),
            self.canvas.winfo_height(),
        ) * 0.27 * self.zoom

        result = {}
        start_angle = math.radians(-90)
        for index, name in enumerate(names):
            angle = start_angle + (2 * math.pi * index / max(1, n))
            result[name] = self.point(cx, cy, radius, angle)
        return result

    def client_layout(self, server_positions):
        positions = {}
        names = sorted(server_positions)
        if not names:
            return positions

        with lock:
            snapshot = {name: data.copy() for name, data in clients.items()}

        grouped = {name: [] for name in names}
        for client_name, data in sorted(snapshot.items()):
            destination = data.get("server")
            if destination in grouped:
                grouped[destination].append(client_name)
            else:
                # Show unassigned clients in the first server orbit until assignment arrives.
                grouped[names[0]].append(client_name)

        for server_index, server_name in enumerate(names):
            sx, sy = server_positions[server_name]
            client_names = grouped[server_name]
            orbit = max(105, 118 * self.zoom)
            count = len(client_names)
            for index, client_name in enumerate(client_names):
                angle = (2 * math.pi * index / max(1, count)) + (self.pulse * 0.012) + server_index * 0.35
                positions[client_name] = self.point(sx, sy, orbit, angle)

        return positions

    # -------------------- drawing --------------------

    def draw_scene(self):
        c = self.canvas
        c.delete("all")

        w = max(900, c.winfo_width())
        h = max(520, c.winfo_height())
        cx, cy = self.center()

        # Background grid.
        for x in range(-50, int(w) + 50, 42):
            c.create_line(
                x, 0, x, h,
                fill=GRID,
            )
        for y in range(-50, int(h) + 50, 42):
            c.create_line(
                0, y, w, y,
                fill=GRID,
            )

        # Huge atmospheric circles.
        for r in (
            0.45 * min(w, h),
            0.35 * min(w, h),
            0.25 * min(w, h),
        ):
            c.create_oval(
                cx - r, cy - r,
                cx + r, cy + r,
                outline="#0d263b",
                width=1,
            )

        self.draw_header_overlay(w, h)

        server_positions = self.server_layout()
        client_positions = self.client_layout(server_positions)

        self.server_points = server_positions
        self.client_points = client_positions
        self.hit_targets = []

        self.draw_routes(server_positions, client_positions)
        self.draw_core(cx, cy)
        self.draw_clients(client_positions)
        self.draw_servers(server_positions)
        self.draw_incident_layer(server_positions)
        self.draw_decision_card(server_positions)
        self.draw_prediction_overlay(server_positions)
        self.draw_metrics_overlay(w, h)
        self.draw_help_overlay(w, h)

    def draw_header_overlay(self, w, h):
        c = self.canvas

        with lock:
            total = len(servers)
            healthy = sum(
                1
                for s in servers.values()
                if s["status"] == "healthy"
            )
            active_clients = sum(
                1
                for info in clients.values()
                if info.get("server")
            )
            failure_count = metrics["failures"]
            recovery_count = metrics["recoveries"]
            rebalance_count = metrics.get("rebalances", 0)
            recovery_times = metrics.get("recovery_times", [])
            avg_recovery = (sum(recovery_times) / len(recovery_times)) if recovery_times else 0.0
            resilience_index = (100.0 * healthy / total) if total else 0.0

        c.create_text(
            24, 24,
            text="TACTICAL NETWORK VIEW",
            anchor="w",
            fill=TEXT,
            font=("Segoe UI", 14, "bold"),
        )
        c.create_text(
            24, 47,
            text="Heartbeat field  •  server state  •  automatic rescue routing",
            anchor="w",
            fill=MUTED,
            font=("Segoe UI", 10),
        )

        status = "STABLE" if total == healthy else "DEGRADED"
        status_color = GREEN if total == healthy else RED

        c.create_text(
            w - 24, 24,
            text=f"● {status}",
            anchor="e",
            fill=status_color,
            font=("Segoe UI", 13, "bold"),
        )
        c.create_text(
            w - 24, 47,
            text=(
                f"SERVERS {healthy}/{total}   "
                f"CLIENTS {active_clients}   "
                f"FAILURES {failure_count}   "
                f"RECOVERIES {recovery_count}   "
                f"REBALANCES {rebalance_count}   "
                f"NRI {resilience_index:.0f}%"
            ),
            anchor="e",
            fill="#89a6c1",
            font=("Consolas", 10),
        )

    def server_color(self, name):
        palette = [BLUE, RED, ORANGE, PURPLE, CYAN, YELLOW, GREEN, "#ff5c9b", "#7dd3fc", "#f472b6"]
        names = self.available_servers()
        try:
            return palette[names.index(name) % len(palette)]
        except ValueError:
            return BLUE

    def draw_routes(self, server_positions, client_positions):
        c = self.canvas
        cx, cy = self.center()

        # Server → central rescue core.
        for name, (sx, sy) in server_positions.items():
            color = self.server_color(name)

            with lock:
                state = servers.get(name, {})
                failed = state.get("status") == "failed"

            route_color = RED if failed else "#28506f"
            width = 4 if failed else 2

            c.create_line(
                sx, sy,
                cx, cy,
                fill=route_color,
                width=width,
                dash=(7, 6) if not failed else (2, 6),
            )

            # Heartbeat / data pulse along route.
            phase = (self.pulse % 70) / 70.0
            px = sx + (cx - sx) * phase
            py = sy + (cy - sy) * phase

            if failed:
                # failed link gets an outward warning marker
                c.create_oval(
                    px - 6, py - 6,
                    px + 6, py + 6,
                    outline=RED,
                    width=2,
                )
            else:
                c.create_oval(
                    px - 4, py - 4,
                    px + 4, py + 4,
                    fill=color,
                    outline="",
                )

        # Client → current server routes.
        with lock:
            snapshot = {
                name: data.copy()
                for name, data in clients.items()
            }

        for client_name, (cx1, cy1) in client_positions.items():
            destination = snapshot.get(client_name, {}).get("server")
            if destination not in server_positions:
                continue

            sx, sy = server_positions[destination]
            c.create_line(
                cx1, cy1,
                sx, sy,
                fill="#27445d",
                width=1,
                dash=(4, 7),
            )

            phase = ((self.pulse + len(client_name) * 17) % 90) / 90.0
            px = cx1 + (sx - cx1) * phase
            py = cy1 + (sy - cy1) * phase

            c.create_oval(
                px - 3, py - 3,
                px + 3, py + 3,
                fill=CYAN,
                outline="",
            )

        # Failed server → best replacement recovery route.
        failed_names = []
        with lock:
            failed_names = [
                name
                for name, state in servers.items()
                if state.get("status") == "failed"
            ]

        if failed_names:
            failed = failed_names[0]
            replacement, score = pick_best_server(failed)

            if replacement and replacement in server_positions:
                x1, y1 = server_positions[failed]
                x2, y2 = server_positions[replacement]

                c.create_line(
                    x1, y1, x2, y2,
                    fill=GREEN,
                    width=5,
                    dash=(14, 8),
                )

                phase = (self.pulse % 50) / 50.0
                rx = x1 + (x2 - x1) * phase
                ry = y1 + (y2 - y1) * phase

                c.create_oval(
                    rx - 7, ry - 7,
                    rx + 7, ry + 7,
                    fill=WHITE,
                    outline=GREEN,
                    width=2,
                )

                mx = (x1 + x2) / 2
                my = (y1 + y2) / 2 - 24

                c.create_text(
                    mx, my,
                    text=f"RESCUE ROUTE  →  {replacement}  •  SCORE {score:.1f}",
                    fill=GREEN,
                    font=("Segoe UI", 10, "bold"),
                )

    def draw_core(self, cx, cy):
        c = self.canvas

        # Layered core rings.
        for r, color, width in (
            (88, "#0b263b", 2),
            (70, "#16415b", 2),
            (53, CYAN, 2),
        ):
            c.create_oval(
                cx - r, cy - r,
                cx + r, cy + r,
                outline=color,
                width=width,
            )

        # Rotating tick ring.
        for i in range(18):
            angle = math.radians(i * 20 + self.pulse * 2)
            x1, y1 = self.point(cx, cy, 78, angle)
            x2, y2 = self.point(cx, cy, 88, angle)
            c.create_line(
                x1, y1, x2, y2,
                fill="#2a7899",
                width=2,
            )

        c.create_oval(
            cx - 42, cy - 42,
            cx + 42, cy + 42,
            fill="#071d2e",
            outline=CYAN,
            width=2,
        )

        c.create_text(
            cx, cy - 9,
            text="RESCUE",
            fill=TEXT,
            font=("Segoe UI", 13, "bold"),
        )
        c.create_text(
            cx, cy + 10,
            text="CORE",
            fill=CYAN,
            font=("Segoe UI", 10, "bold"),
        )

        with lock:
            if servers:
                resilience = 100 * sum(
                    1
                    for s in servers.values()
                    if s["status"] == "healthy"
                ) / len(servers)
            else:
                resilience = 0

        c.create_text(
            cx, cy + 114,
            text=f"RESILIENCE  {resilience:.0f}%",
            fill=GREEN if resilience == 100 else YELLOW,
            font=("Consolas", 9, "bold"),
        )

    def draw_clients(self, points):
        c = self.canvas

        for name, (x, y) in points.items():
            selected = self.selected == name
            ring = CYAN if selected else "#2d5877"

            c.create_oval(
                x - 24, y - 24,
                x + 24, y + 24,
                fill="#08192a",
                outline=ring,
                width=3 if selected else 1,
                tags=(f"client:{name}",),
            )

            c.create_oval(
                x - 10, y - 10,
                x + 10, y + 10,
                fill="#10314a",
                outline=CYAN,
                width=1,
                tags=(f"client:{name}",),
            )

            c.create_text(
                x, y,
                text=name,
                fill=TEXT,
                font=("Segoe UI", 10, "bold"),
                tags=(f"client:{name}",),
            )

            with lock:
                destination = clients.get(name, {}).get("server", "—")

            c.create_text(
                x, y + 34,
                text=f"→ {destination}",
                fill="#7e9db8",
                font=("Consolas", 10),
            )

            self.hit_targets.append(
                ("client", name, (x - 32, y - 32, x + 32, y + 32))
            )

    def draw_servers(self, points):
        c = self.canvas

        for name, (x, y) in points.items():
            with lock:
                s = servers.get(name)
                if s:
                    s = s.copy()
                else:
                    s = {
                        "capacity": 10,
                        "load": 0,
                        "reliability": 0,
                        "status": "offline",
                        "failure_count": 0,
                    }

            ratio = load_ratio(s)
            status = s.get("status", "offline")

            if status == "failed":
                primary = RED
                glow = "#411321"
            elif ratio >= OVERLOAD_RATIO:
                primary = YELLOW
                glow = "#3c2d10"
            else:
                primary = self.server_color(name)
                glow = "#0c2539"

            selected = self.selected == name

            # Animated halo.
            halo = 98 + int(8 * math.sin(self.pulse * 0.08 + ord(name)))
            c.create_oval(
                x - halo, y - halo,
                x + halo, y + halo,
                outline="#162f46" if status != "failed" else "#522033",
                width=2,
            )

            # Failure / overload visual effect.
            if status == "failed":
                pulse = 110 + (self.pulse % 18)
                c.create_oval(
                    x - pulse, y - pulse,
                    x + pulse, y + pulse,
                    outline=RED,
                    width=2,
                )
            elif ratio >= OVERLOAD_RATIO:
                pulse = 103 + (self.pulse % 12)
                c.create_oval(
                    x - pulse, y - pulse,
                    x + pulse, y + pulse,
                    outline="#664c17",
                    width=2,
                )

            # Tower body.
            body_w = 110
            body_h = 100

            c.create_polygon(
                x - 55, y + 40,
                x - 37, y - 34,
                x + 30, y - 44,
                x + 55, y + 40,
                fill="#071828",
                outline=primary if not selected else CYAN,
                width=3 if selected else 2,
                tags=(f"server:{name}",),
            )

            # Tower face.
            c.create_rectangle(
                x - 34, y - 28,
                x + 34, y + 30,
                fill=glow,
                outline=primary if not selected else CYAN,
                width=2,
                tags=(f"server:{name}",),
            )

            # Antenna.
            c.create_line(
                x, y - 28,
                x, y - 62,
                fill=primary,
                width=3,
                tags=(f"server:{name}",),
            )

            c.create_oval(
                x - 5, y - 67,
                x + 5, y - 57,
                fill=primary,
                outline="",
                tags=(f"server:{name}",),
            )

            c.create_text(
                x, y - 10,
                text=f"SERVER {name}",
                fill=TEXT,
                font=("Segoe UI", 13, "bold"),
                tags=(f"server:{name}",),
            )

            c.create_text(
                x, y + 10,
                text=f"{s.get('load', 0)} / {s.get('capacity', 0)}",
                fill="#b5cde2",
                font=("Consolas", 10),
                tags=(f"server:{name}",),
            )

            status_text = (
                "OFFLINE"
                if status == "failed"
                else "HIGH LOAD"
                if ratio >= OVERLOAD_RATIO
                else "HEALTHY"
            )

            c.create_text(
                x, y + 58,
                text=status_text,
                fill=primary,
                font=("Segoe UI", 10, "bold"),
            )

            # Tiny health bar.
            bar_w = 88
            c.create_rectangle(
                x - bar_w / 2, y + 75,
                x + bar_w / 2, y + 81,
                fill="#10263a",
                outline="",
            )
            score = health_score(name)
            c.create_rectangle(
                x - bar_w / 2, y + 75,
                x - bar_w / 2 + bar_w * score / 100,
                y + 81,
                fill=primary,
                outline="",
            )

            c.create_text(
                x, y + 94,
                text=f"HEALTH {score:.0f}",
                fill="#7897b2",
                font=("Consolas", 10),
            )

            self.hit_targets.append(
                ("server", name, (x - body_w, y - body_h, x + body_w, y + body_h))
            )

    def draw_incident_layer(self, server_positions):
        c = self.canvas
        now = time.time()

        if now < self.banner_until and self.banner_text:
            w = self.canvas.winfo_width()
            h = self.canvas.winfo_height()

            c.create_rectangle(
                0, h * 0.075,
                w, h * 0.075 + 48,
                fill="#0a121d",
                outline=self.banner_color,
                width=2,
            )

            c.create_text(
                w / 2, h * 0.075 + 24,
                text=self.banner_text,
                fill=self.banner_color,
                font=("Segoe UI", 13, "bold"),
            )

        # Dim failed area with an incident marker.
        failed_names = []
        with lock:
            failed_names = [
                name
                for name, s in servers.items()
                if s["status"] == "failed"
            ]

        for name in failed_names:
            if name not in server_positions:
                continue

            x, y = server_positions[name]

            # Concentric warning rings.
            for offset in (0, 16, 32):
                r = 118 + ((self.pulse * 2 + offset) % 55)
                c.create_oval(
                    x - r, y - r,
                    x + r, y + r,
                    outline="#6a1d2e",
                    width=1,
                )

            c.create_text(
                x, y - 115,
                text="⚠ FAILURE",
                fill=RED,
                font=("Segoe UI", 14, "bold"),
            )

    def draw_decision_card(self, server_positions):
        # Decision details are now available through the dedicated console button.
        # Keep the theater scene clean and show only a small incident hint.
        c = self.canvas
        failed = None
        with lock:
            for name, state in servers.items():
                if state.get("status") == "failed":
                    failed = name
                    break
        if failed is None:
            return

        w = self.canvas.winfo_width()
        h = self.canvas.winfo_height()
        x = w - 360
        y = 88
        c.create_rectangle(x, y, w - 24, y + 58, fill="#071321", outline=GREEN, width=1)
        c.create_text(
            x + 14, y + 15,
            text=f"RECOVERY DECISION READY  •  SERVER {failed}",
            anchor="w", fill=GREEN, font=("Segoe UI", 10, "bold")
        )
        c.create_text(
            x + 14, y + 38,
            text="Open DECISION FACTORS console for score, diagnosis and replacement ranking.",
            anchor="w", fill=MUTED, font=("Consolas", 8)
        )

    def draw_prediction_overlay(self, server_positions):
        c = self.canvas
        predictions = []
        with lock:
            names = list(servers.keys())
        for name in names:
            prediction = predict_overload(name)
            if prediction:
                eta, best, score = prediction
                predictions.append((name, eta, best, score))
        if not predictions:
            return
        w = self.canvas.winfo_width()
        x, y = 26, 88
        width = min(430, w * 0.32)
        height = 56 + 21 * min(4, len(predictions))
        c.create_rectangle(x, y, x + width, y + height, fill="#071321", outline=YELLOW, width=1)
        c.create_text(x + 12, y + 12, text="PREDICTIVE OVERLOAD WATCH", anchor="w", fill=YELLOW, font=("Segoe UI", 10, "bold"))
        yy = y + 34
        for name, eta, best, score in predictions[:4]:
            c.create_text(x + 12, yy, text=f"{name}: ~{eta}s to threshold  →  {best} ({score:.0f})", anchor="w", fill="#c8d8e8", font=("Consolas", 8))
            yy += 21

    def draw_metrics_overlay(self, w, h):
        c = self.canvas
        with lock:
            times = list(metrics.get("recovery_times", []))
            failures = metrics.get("failures", 0)
            recoveries = metrics.get("recoveries", 0)
            rebalances = metrics.get("rebalances", 0)
        avg = sum(times) / len(times) if times else 0.0
        c.create_text(
            w / 2, h - 42,
            text=f"RECOVERY {avg:.2f}s   •   FAILURES {failures}   •   RECOVERIES {recoveries}   •   REBALANCES {rebalances}",
            fill="#7d9bb5", font=("Consolas", 10),
        )

    def draw_help_overlay(self, w, h):
        c = self.canvas
        c.create_text(
            w / 2,
            h - 18,
            text=(
                "1–9 SELECT SERVER   •   F FAILURE   •   O OVERLOAD   •   P PING   •   "
                "R RESTORE   •   SPACE AUTO-RESCUE   •   CONSOLES FOR LOG / DECISION / GRAPH / CONFIG"
            ),
            fill="#6f8faa",
            font=("Consolas", 10),
        )

    # -------------------- interaction --------------------

    def on_canvas_click(self, event):
        for kind, name, box in reversed(self.hit_targets):
            x1, y1, x2, y2 = box
            if x1 <= event.x <= x2 and y1 <= event.y <= y2:
                if kind == "server":
                    self.select_server(name)
                else:
                    self.selected = name
                    self.command_status.config(
                        text=f"CLIENT {name} • click its server route to inspect • use target box for actions"
                    )
                return

    def start_pan(self, event):
        self.drag_origin = (event.x, event.y, self.pan_x, self.pan_y)

    def pan_scene(self, event):
        if not self.drag_origin:
            return

        x0, y0, px, py = self.drag_origin
        self.pan_x = px + event.x - x0
        self.pan_y = py + event.y - y0

    def on_zoom(self, event):
        if event.delta > 0:
            self.zoom = min(1.35, self.zoom + 0.05)
        else:
            self.zoom = max(0.80, self.zoom - 0.05)

    # -------------------- console windows --------------------

    def _open_console(self, key, title, geometry="850x620"):
        window = self.console_windows.get(key)
        try:
            if window is not None and window.winfo_exists():
                window.deiconify()
                window.lift()
                window.focus_force()
                return window
        except tk.TclError:
            pass

        window = tk.Toplevel(self)
        window.title(f"NETRESCUE • {title}")
        window.geometry(geometry)
        window.minsize(620, 420)
        window.configure(bg="#050b14")
        self.console_windows[key] = window

        def close_window():
            try:
                window.destroy()
            finally:
                self.console_windows.pop(key, None)
                self.console_widgets.pop(key, None)

        window.protocol("WM_DELETE_WINDOW", close_window)
        return window

    def _console_title(self, parent, title, subtitle):
        head = tk.Frame(parent, bg="#071321", height=64)
        head.pack(fill="x")
        head.pack_propagate(False)
        tk.Label(
            head, text=title, bg="#071321", fg=TEXT,
            font=("Segoe UI", 18, "bold")
        ).pack(anchor="w", padx=18, pady=(10, 0))
        tk.Label(
            head, text=subtitle, bg="#071321", fg=MUTED,
            font=("Consolas", 8)
        ).pack(anchor="w", padx=18)
        return head

    def open_event_console(self):
        win = self._open_console("events", "EVENT LOG CONSOLE", "1050x680")
        if "text" not in self.console_widgets.get("events", {}):
            head = self._console_title(
                win,
                "EVENT LOG CONSOLE",
                "Live timestamped system events • failure • recovery & client switching • overload • predictions",
            )
            toolbar = tk.Frame(win, bg="#050b14")
            toolbar.pack(fill="x", padx=14, pady=8)

            tk.Label(toolbar, text="FILTER BY TYPE:", bg="#050b14", fg=MUTED, font=("Segoe UI", 9, "bold")).pack(side="left", padx=(0, 6))
            var = tk.StringVar(value="ALL")
            combo = ttk.Combobox(
                toolbar, textvariable=var,
                values=("ALL", "FAILURE", "RECOVERY", "OVERLOAD", "PREDICTION", "INFO"),
                state="readonly", width=14,
            )
            combo.pack(side="left")
            combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_event_console(force=True))

            text_frame = tk.Frame(win, bg="#02070d")
            text_frame.pack(fill="both", expand=True, padx=14, pady=(0, 14))

            scrollbar = ttk.Scrollbar(text_frame)
            scrollbar.pack(side="right", fill="y")

            text = tk.Text(
                text_frame, bg="#02070d", fg="#d6e5f6", insertbackground=TEXT,
                relief="flat", font=("Consolas", 10), wrap="word", yscrollcommand=scrollbar.set
            )
            text.pack(fill="both", expand=True)
            scrollbar.config(command=text.yview)

            # Colored tags for high readability
            text.tag_config("FAILURE", foreground="#ff4d6d", font=("Consolas", 9, "bold"))
            text.tag_config("RECOVERY", foreground="#34d399", font=("Consolas", 9, "bold"))
            text.tag_config("OVERLOAD", foreground="#fbbf24", font=("Consolas", 9, "bold"))
            text.tag_config("PREDICTION", foreground="#c084fc", font=("Consolas", 9, "bold"))
            text.tag_config("INFO", foreground="#7dd3fc")
            text.tag_config("STAMP", foreground="#64748b")
            text.tag_config("MSG", foreground="#e2e8f0")

            self.console_widgets["events"] = {
                "text": text,
                "filter": var,
                "last_len": -1,
                "last_filt": ""
            }

        self._refresh_event_console(force=True)

    def _refresh_event_console(self, force=False):
        widgets = self.console_widgets.get("events")
        if not widgets:
            return
        try:
            text = widgets["text"]
            filt = widgets["filter"].get()
            with lock:
                data = list(events)

            if not force and len(data) == widgets.get("last_len", -1) and filt == widgets.get("last_filt", ""):
                win = self.console_windows.get("events")
                if win and win.winfo_exists():
                    win.after(500, self._refresh_event_console)
                return

            widgets["last_len"] = len(data)
            widgets["last_filt"] = filt

            text.configure(state="normal")
            text.delete("1.0", "end")

            for stamp, kind, message in reversed(data):
                if filt != "ALL" and kind != filt:
                    continue
                text.insert("end", f"[{stamp}] ", "STAMP")
                text.insert("end", f"{kind:<10} ", kind)
                text.insert("end", f"{message}\n", "MSG")

            text.configure(state="disabled")
            win = self.console_windows.get("events")
            if win and win.winfo_exists():
                win.after(500, self._refresh_event_console)
        except tk.TclError:
            return

    def open_decision_console(self):
        win = self._open_console("decision", "DECISION FACTORS CONSOLE", "980x780")
        if "canvas" not in self.console_widgets.get("decision", {}):
            self._console_title(
                win,
                "DECISION FACTORS CONSOLE",
                "Explainable recovery engine • client switching time • reasons for server switch • SymPy 5-factor scoring",
            )
            toolbar = tk.Frame(win, bg="#071321", height=42)
            toolbar.pack(fill="x", padx=14, pady=(6, 4))

            tk.Label(toolbar, text="INSPECT SERVER:", bg="#071321", fg=MUTED, font=("Segoe UI", 10, "bold")).pack(side="left", padx=(6, 8))
            target_var = tk.StringVar(value=self.selected)
            combo = ttk.Combobox(toolbar, textvariable=target_var, state="readonly", width=6)
            combo.pack(side="left", padx=(0, 14))

            def on_select(_e):
                self.selected = target_var.get()
                self._refresh_decision_console()

            combo.bind("<<ComboboxSelected>>", on_select)

            def restore_action():
                target = target_var.get()
                reset_server(target)
                self._incident_banner(f"RESTORE  //  SERVER {target} RESTORED", GREEN, 3.0)
                self._refresh_decision_console()

            restore_btn = tk.Button(
                toolbar, text="↺ RESTORE SELECTED SERVER", command=restore_action,
                bg="#0f5a3c", fg="white", activebackground="#166534", relief="flat", bd=0,
                padx=12, pady=4, font=("Segoe UI", 10, "bold"), cursor="hand2"
            )
            restore_btn.pack(side="left", padx=4)

            tk.Label(
                toolbar,
                text="  • Failed servers stay failed until restored using this button",
                bg="#071321", fg="#64748b", font=("Consolas", 8)
            ).pack(side="left", padx=10)

            canvas = tk.Canvas(win, bg="#050b14", highlightthickness=0)
            canvas.pack(fill="both", expand=True, padx=14, pady=(0, 14))

            self.console_widgets["decision"] = {
                "canvas": canvas,
                "target_var": target_var,
                "combo": combo,
                "restore_btn": restore_btn
            }
        self._refresh_decision_console()

    def _refresh_decision_console(self):
        widgets = self.console_widgets.get("decision")
        if not widgets:
            return
        try:
            c = widgets["canvas"]
            c.delete("all")
            w = max(c.winfo_width(), 920)

            with lock:
                names = sorted(servers.keys())
                failed_names = [name for name, s in servers.items() if s.get("status") == "failed"]
                selected = widgets["target_var"].get()
                if not selected or selected not in servers:
                    selected = failed_names[0] if failed_names else (names[0] if names else "A")
                    widgets["target_var"].set(selected)

                if names and tuple(widgets["combo"]["values"]) != tuple(names):
                    widgets["combo"]["values"] = tuple(names)

                target_state = servers.get(selected, {}).copy() if selected in servers else {}
                incident = last_recovery_incident.get(selected) or last_recovery_incident.get("latest")
                latest_inc = last_recovery_incident.get("latest")

            if not target_state:
                c.create_text(28, 30, text="Waiting for server telemetry...", anchor="w", fill=MUTED, font=("Consolas", 11))
                return

            status = target_state.get("status", "healthy")
            is_failed = status == "failed"

            # Update restore button style depending on status
            restore_btn = widgets.get("restore_btn")
            if restore_btn:
                if is_failed:
                    restore_btn.config(text=f"↺ RESTORE SERVER {selected} (OFFLINE)", bg="#b91c1c")
                else:
                    restore_btn.config(text=f"↺ RESTORE SERVER {selected} (HEALTHY)", bg="#0f5a3c")

            # 1. Target Header
            c.create_text(28, 28, text=f"ANALYSIS TARGET  •  SERVER {selected}", anchor="w", fill=TEXT, font=("Segoe UI", 18, "bold"))
            status_text = "● STATUS: OFFLINE / FAILED" if is_failed else "● STATUS: HEALTHY / MONITORED"
            status_color = RED if is_failed else GREEN
            c.create_text(420, 28, text=status_text, anchor="w", fill=status_color, font=("Segoe UI", 13, "bold"))

            diagnosis = target_state.get("last_diagnosis", "Normal Operation")
            c.create_text(28, 56, text=f"Diagnosis: {diagnosis}   |   Failure Count: {target_state.get('failure_count', 0)}   |   Load: {target_state.get('load', 0):.0f}/{target_state.get('capacity', 0):.0f}", anchor="w", fill="#8baac7", font=("Consolas", 10))

            # 2. PROMINENT RECOVERY TIME BOX (CLIENT SWITCH DURATION)
            # "recovery time meant here the time of switching the clients to the other server when the server fails"
            y_box = 80
            box_h = 82
            c.create_rectangle(24, y_box, w - 24, y_box + box_h, fill="#071d33", outline=CYAN, width=2)

            # Determine recovery time and replacement
            rel_incident = incident if (incident and (incident.get("failed_server") == selected or is_failed)) else latest_inc
            if rel_incident:
                sw_ms = rel_incident.get("switch_time_ms", 0.0)
                sw_sec = rel_incident.get("switch_time", 0.0)
                src = rel_incident.get("failed_server", selected)
                dest = rel_incident.get("replacement_server", "—")
                aff_clients = rel_incident.get("affected_clients", [])
                tot_time = rel_incident.get("total_time", 0.0)
                clients_display = ", ".join(aff_clients) if aff_clients else "None"

                c.create_text(
                    40, y_box + 22,
                    text=f"⏱  RECOVERY TIME (CLIENT SWITCH DURATION): {sw_ms:.2f} ms ({sw_sec:.4f} s)",
                    anchor="w", fill=CYAN, font=("Segoe UI", 13, "bold")
                )
                c.create_text(
                    40, y_box + 46,
                    text=f"Failover switch: All client socket sessions migrated from Server {src} ➜ Server {dest} in {sw_ms:.2f} ms.",
                    anchor="w", fill=TEXT, font=("Segoe UI", 9)
                )
                c.create_text(
                    40, y_box + 66,
                    text=f"Rescued Clients: {clients_display}   |   Detection-to-Recovery Latency: {tot_time:.2f}s   |   Target Score: {rel_incident.get('best_score', 0):.1f}/100",
                    anchor="w", fill="#82a5c5", font=("Consolas", 8)
                )
            else:
                c.create_text(
                    40, y_box + 24,
                    text="⏱  RECOVERY TIME (CLIENT SWITCH DURATION): STANDBY / NO ACTIVE INCIDENT",
                    anchor="w", fill="#5a7d9c", font=("Segoe UI", 14, "bold")
                )
                c.create_text(
                    40, y_box + 52,
                    text="When a server failure occurs, the exact client socket transfer duration (in ms) is measured and reported here.",
                    anchor="w", fill=MUTED, font=("Segoe UI", 9)
                )

            # 3. REASONS WHY SERVER WAS CHANGED
            y_reasons = y_box + box_h + 16
            best_srv, best_score = pick_best_server(selected if is_failed else "")
            eval_target = best_srv if (is_failed and best_srv) else selected

            c.create_text(
                28, y_reasons,
                text="EXACT REASONS WHY SERVER WAS CHANGED / RESCUE DECISION FACTORS:",
                anchor="w", fill=YELLOW, font=("Segoe UI", 10, "bold")
            )
            y_r = y_reasons + 22

            if rel_incident and rel_incident.get("reasons"):
                for reason_line in rel_incident["reasons"]:
                    c.create_text(34, y_r, text=f"•  {reason_line}", anchor="w", fill="#d2e3f4", font=("Segoe UI", 9))
                    y_r += 20
            else:
                # Provide live comparative reason
                factors = score_breakdown(eval_target) if eval_target in servers else None
                if is_failed:
                    c.create_text(34, y_r, text=f"•  1. Failure Cause: Server {selected} failed ({diagnosis}). Heartbeats ceased.", anchor="w", fill="#d2e3f4", font=("Segoe UI", 9))
                    y_r += 20
                    if best_srv:
                        c.create_text(34, y_r, text=f"•  2. Selected Replacement: Server {best_srv} achieved highest health score ({best_score:.1f}/100).", anchor="w", fill="#d2e3f4", font=("Segoe UI", 9))
                        y_r += 20
                        if factors:
                            c.create_text(34, y_r, text=f"•  3. Capacity Advantage: {factors['available_capacity']*100:.0f}% free capacity (lowest relative load).", anchor="w", fill="#d2e3f4", font=("Segoe UI", 9))
                            y_r += 20
                            c.create_text(34, y_r, text=f"•  4. Stability Factors: Reliability={factors['reliability']*100:.0f}%, Heartbeat Stability={factors['heartbeat_stability']*100:.0f}%.", anchor="w", fill="#d2e3f4", font=("Segoe UI", 9))
                            y_r += 20
                else:
                    c.create_text(34, y_r, text=f"•  Server {selected} is healthy and active. Ready as a candidate replacement with health score {health_score(selected):.1f}/100.", anchor="w", fill="#d2e3f4", font=("Segoe UI", 9))
                    y_r += 20

            # 4. FIVE-FACTOR MATHEMATICAL BREAKDOWN
            y_factors = max(y_r + 14, 340)
            c.create_text(
                28, y_factors,
                text=f"SYMPY FIVE-FACTOR HEALTH SCORE BREAKDOWN  (Evaluating Server {eval_target})",
                anchor="w", fill=TEXT, font=("Segoe UI", 10, "bold")
            )
            c.create_text(
                28, y_factors + 20,
                text="Health Score = 100 × (0.30A + 0.20R + 0.15H + 0.25B + 0.10F)",
                anchor="w", fill="#82a5c5", font=("Consolas", 10)
            )

            eval_factors = score_breakdown(eval_target) if eval_target in servers else None
            y_bar = y_factors + 48
            if eval_factors:
                factor_items = [
                    ("A  AVAILABLE CAPACITY", eval_factors["available_capacity"], CYAN),
                    ("R  RELIABILITY", eval_factors["reliability"], GREEN),
                    ("H  HEARTBEAT STABILITY", eval_factors["heartbeat_stability"], BLUE),
                    ("B  CLIENT BALANCE", eval_factors["client_balance"], YELLOW),
                    ("F  FAILURE HISTORY", eval_factors["failure_history"], ORANGE),
                ]
                bar_x = 240
                bar_max_w = min(360, w - bar_x - 140)
                for label, value, color in factor_items:
                    c.create_text(32, y_bar, text=label, anchor="w", fill="#a6bad0", font=("Consolas", 9, "bold"))
                    c.create_rectangle(bar_x, y_bar - 6, bar_x + bar_max_w, y_bar + 6, fill="#0e2032", outline="")
                    c.create_rectangle(bar_x, y_bar - 6, bar_x + int(bar_max_w * value), y_bar + 6, fill=color, outline="")
                    c.create_text(bar_x + bar_max_w + 14, y_bar, text=f"{value*100:.0f}%", anchor="w", fill=TEXT, font=("Consolas", 9, "bold"))
                    y_bar += 28

                final_sc = eval_factors["score"]
                c.create_text(32, y_bar + 4, text=f"FINAL COMPUTED HEALTH SCORE: {final_sc:.1f} / 100", anchor="w", fill=GREEN, font=("Segoe UI", 10, "bold"))
                y_bar += 32

            # 5. HEALTHY SERVER RANKING TABLE
            y_rank = y_bar + 10
            c.create_text(28, y_rank, text="CANDIDATE RECOVERY RANKING (ALL HEALTHY SERVERS):", anchor="w", fill=TEXT, font=("Segoe UI", 10, "bold"))
            y_rank += 24

            ranked = []
            for name in names:
                with lock:
                    st = servers[name]
                    if st.get("status") == "healthy":
                        ranked.append((name, recovery_score(name)))
            ranked.sort(key=lambda pair: pair[1], reverse=True)

            if ranked:
                for rank, (name, sc) in enumerate(ranked[:6], 1):
                    tag = " [TOP CHOICE]" if rank == 1 else ""
                    tag_col = GREEN if rank == 1 else "#a0bccc"
                    c.create_text(34, y_rank, text=f"#{rank}  Server {name}:", anchor="w", fill=self.server_color(name), font=("Consolas", 9, "bold"))
                    c.create_text(160, y_rank, text=f"Health Score {sc:.1f}/100{tag}", anchor="w", fill=tag_col, font=("Consolas", 10))
                    y_rank += 20
            else:
                c.create_text(34, y_rank, text="No healthy servers available for failover!", anchor="w", fill=RED, font=("Segoe UI", 10, "bold"))

            win = self.console_windows.get("decision")
            if win and win.winfo_exists():
                win.after(600, self._refresh_decision_console)
        except tk.TclError:
            return

    def open_graph_console(self):
        win = self._open_console("graph", "GRAPH HISTORY CONSOLE", "1080x700")
        if "canvas" not in self.console_widgets.get("graph", {}):
            self._console_title(
                win,
                "GRAPH HISTORY CONSOLE",
                "Live rolling load telemetry (last 60s) • true time synchronization • failure & load drops highlighted",
            )
            canvas = tk.Canvas(win, bg="#050b14", highlightthickness=0)
            canvas.pack(fill="both", expand=True, padx=14, pady=(0, 14))
            self.console_widgets["graph"] = {"canvas": canvas}
        self._refresh_graph_console()

    def _refresh_graph_console(self):
        widgets = self.console_widgets.get("graph")
        if not widgets:
            return
        try:
            c = widgets["canvas"]
            c.delete("all")
            w = max(c.winfo_width(), 980)
            h = max(c.winfo_height(), 570)
            left, right, top, bottom = 62, 35, 45, 55
            pw = w - left - right
            ph = h - top - bottom

            with lock:
                data = {name: dict(state) for name, state in servers.items()}

            if not data:
                c.create_text(w/2, h/2, text="Waiting for server telemetry...", fill=MUTED, font=("Segoe UI", 11))
                return

            max_cap = max(float(s.get("capacity", 10)) for s in data.values())
            max_y = max(10.0, max_cap * 1.15)

            # TRUE TIME-BASED MAPPING: Rolling 60-second window
            WINDOW = 60.0
            now = time.time()
            t_start = now - WINDOW

            # Horizontal load gridlines + labels
            for i in range(6):
                value = max_y * i / 5
                y = top + ph - (value / max_y) * ph
                c.create_line(left, y, left + pw, y, fill="#102338")
                c.create_text(left - 8, y, text=f"{value:.0f}", anchor="e", fill="#7895ae", font=("Consolas", 8))

            # Vertical time gridlines (-60s, -45s, -30s, -15s, Now)
            time_markers = [
                ("-60s", 0.0),
                ("-45s", 0.25),
                ("-30s", 0.50),
                ("-15s", 0.75),
                ("Now (Live)", 1.0)
            ]
            for label, fraction in time_markers:
                x = left + fraction * pw
                c.create_line(x, top, x, top + ph, fill="#102338", dash=(2, 4))
                c.create_text(x, top + ph + 16, text=label, anchor="center", fill="#7895ae", font=("Consolas", 8))

            # Overload threshold line
            threshold_ratio = OVERLOAD_RATIO
            threshold_y = top + ph - ((max_cap * threshold_ratio) / max_y) * ph
            c.create_line(left, threshold_y, left + pw, threshold_y, fill=YELLOW, dash=(8, 5), width=1)
            c.create_text(left + 8, threshold_y - 8, text=f"OVERLOAD THRESHOLD ({threshold_ratio*100:.0f}%)", anchor="w", fill=YELLOW, font=("Consolas", 9, "bold"))

            palette = {name: self.server_color(name) for name in data}

            # Legend at Top with live health status
            legend_x = left
            for name in sorted(data):
                st = data[name].get("status", "healthy")
                cap = float(data[name].get("capacity", 10))
                ld = float(data[name].get("load", 0))
                col = palette[name]

                if st == "failed":
                    c.create_line(legend_x, 20, legend_x + 18, 20, fill=RED, width=3, dash=(4, 2))
                    text_str = f"Server {name} [FAILED]"
                    text_col = RED
                else:
                    c.create_line(legend_x, 20, legend_x + 18, 20, fill=col, width=3)
                    text_str = f"Server {name}: {ld:.0f}/{cap:.0f}"
                    text_col = TEXT

                c.create_text(legend_x + 24, 20, text=text_str, anchor="w", fill=text_col, font=("Consolas", 9, "bold"))
                legend_x += 140

            # Plot each server using real timestamps
            for name in sorted(data):
                st = data[name].get("status", "healthy")
                history = list(data[name].get("load_history", []))
                if len(history) < 2:
                    continue

                col = palette[name] if st == "healthy" else RED
                pts = []

                for t, load in history:
                    if t < t_start - 2:
                        continue
                    clamped_t = max(t_start, min(now, t))
                    x = left + ((clamped_t - t_start) / WINDOW) * pw
                    y = top + ph - (min(float(load), max_y) / max_y) * ph
                    pts.extend([x, y])

                if len(pts) >= 4:
                    if st == "failed":
                        c.create_line(pts, fill=RED, width=2, dash=(6, 3))
                    else:
                        c.create_line(pts, fill=col, width=3, smooth=True)

                    # Draw point dots
                    for i in range(0, len(pts), 2):
                        px, py = pts[i], pts[i + 1]
                        c.create_oval(px - 2, py - 2, px + 2, py + 2, fill=col, outline="")

                    # Highlight latest point
                    last_x, last_y = pts[-2], pts[-1]
                    if st == "failed":
                        # Failure marker
                        c.create_oval(last_x - 6, last_y - 6, last_x + 6, last_y + 6, fill="#2b0a11", outline=RED, width=2)
                        c.create_text(last_x, last_y - 12, text=f"✕ {name} OFFLINE", fill=RED, font=("Segoe UI", 9, "bold"), anchor="center")
                    else:
                        c.create_oval(last_x - 4, last_y - 4, last_x + 4, last_y + 4, fill=col, outline="#050b14", width=2)

            win = self.console_windows.get("graph")
            if win and win.winfo_exists():
                win.after(650, self._refresh_graph_console)
        except tk.TclError:
            return

    def open_config_console(self):
        win = self._open_console("config", "NETWORK CONFIGURATION CONSOLE", "720x470")
        if "server_var" not in self.console_widgets.get("config", {}):
            self._console_title(
                win,
                "NETWORK CONFIGURATION",
                "Customize topology size • server/client count • new run starts automatically",
            )
            body = tk.Frame(win, bg="#050b14")
            body.pack(fill="both", expand=True, padx=18, pady=18)

            current_servers = max(2, len(self.available_servers()) or 3)
            with lock:
                current_clients = max(1, len(clients) or 3)

            server_var = tk.IntVar(value=min(current_servers, 8))
            client_var = tk.IntVar(value=min(current_clients, 20))

            tk.Label(body, text="NUMBER OF SERVERS", bg="#050b14", fg=MUTED, font=("Segoe UI", 10, "bold")).pack(anchor="w")
            server_spin = tk.Spinbox(body, from_=2, to=8, textvariable=server_var, width=8, font=("Segoe UI", 12))
            server_spin.pack(anchor="w", pady=(6, 18))

            tk.Label(body, text="NUMBER OF CLIENTS", bg="#050b14", fg=MUTED, font=("Segoe UI", 10, "bold")).pack(anchor="w")
            client_spin = tk.Spinbox(body, from_=1, to=20, textvariable=client_var, width=8, font=("Segoe UI", 12))
            client_spin.pack(anchor="w", pady=(6, 18))

            tk.Label(
                body,
                text="Server names are generated automatically: A, B, C, ...\nClients are generated automatically: C1, C2, C3, ...\nApplying the configuration restarts run_all.py so real server/client processes match the new topology.",
                justify="left", anchor="w", bg="#050b14", fg="#8ca4bc", font=("Consolas", 10)
            ).pack(fill="x", pady=(4, 22))

            apply_button = tk.Button(
                body, text="APPLY & RESTART NETWORK", command=lambda: self.apply_network_config(server_var.get(), client_var.get()),
                bg="#0f5a3c", fg="white", activebackground="#1f7a54", relief="flat", bd=0, cursor="hand2",
                padx=18, pady=11, font=("Segoe UI", 10, "bold")
            )
            apply_button.pack(anchor="w")

            self.console_widgets["config"] = {
                "server_var": server_var,
                "client_var": client_var,
                "apply_button": apply_button,
            }

    def apply_network_config(self, server_count, client_count):
        try:
            server_count = max(2, min(8, int(server_count)))
            client_count = max(1, min(20, int(client_count)))
        except (TypeError, ValueError):
            return

        base = os.path.dirname(os.path.abspath(__file__))
        launcher = os.path.join(base, "run_all.py")
        if not os.path.exists(launcher):
            self._incident_banner("CONFIG  //  run_all.py NOT FOUND", RED, 4.0)
            return

        try:
            subprocess.Popen(
                [sys.executable, launcher, "--servers", str(server_count), "--clients", str(client_count)],
                cwd=base,
            )
            self._incident_banner(
                f"NETWORK CONFIG  //  RESTARTING WITH {server_count} SERVERS / {client_count} CLIENTS",
                CYAN, 4.0,
            )
            self.after(900, self.destroy)
        except OSError as exc:
            self._incident_banner(f"CONFIG ERROR  //  {exc}", RED, 5.0)

    # -------------------- refresh --------------------

    def refresh(self):
        try:
            with lock:
                data = list(events)

            if data:
                stamp, kind, message = data[-1]
                self.ticker.config(
                    text=f"  [{stamp}] {kind:<10} {message}"
                )

            names = self.available_servers()
            if hasattr(self, "target_combo"):
                self.target_combo["values"] = tuple(names) if names else ("A", "B", "C")
                if self.target_var.get() not in names and names:
                    self.target_var.set(names[0])
                    self.selected = names[0]

            with lock:
                healthy = sum(
                    1
                    for s in servers.values()
                    if s["status"] == "healthy"
                )
                total = len(servers)

            if total and healthy == total:
                self.header_state.config(
                    text="● NETWORK STABLE",
                    fg=GREEN,
                )
            else:
                self.header_state.config(
                    text="● INCIDENT / DEGRADED",
                    fg=RED,
                )

            self.draw_scene()
        except tk.TclError:
            return

        self.after(650, self.refresh)

    def animate(self):
        self.pulse += 1
        try:
            self.draw_scene()
        except tk.TclError:
            return
        self.after(110, self.animate)


def main():
    # Network threads stay in the background; Tkinter remains on the main thread.
    threading.Thread(target=server_loop, daemon=True).start()
    threading.Thread(target=monitor_loop, daemon=True).start()
    EmergencyOperationsTheater().mainloop()


if __name__ == "__main__":
    main()
