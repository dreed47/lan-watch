#!/usr/bin/env python3
"""LAN watch: Frigate pull from the Beelink, plus ping loss for cameras and ESPs.

The bitrate is go2rtc's own byte counter on the Beelink. This app SSHes there
and asks for it. It does not sniff the network. Stream URLs are stripped on
the Beelink before anything is printed, because those URLs contain the camera
password.
"""

import json
import os
import sqlite3
import subprocess
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request

BEELINK_HOST = os.environ.get("BEELINK_HOST", "192.168.86.16")
BEELINK_USER = os.environ.get("BEELINK_USER", "david")
SSH_KEY = os.environ.get("SSH_KEY", "/run/ssh/id_ed25519")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
SAMPLE_SECONDS = int(os.environ.get("SAMPLE_SECONDS", "10"))
TZ = ZoneInfo(os.environ.get("TZ", "America/New_York"))
DATA = Path(os.environ.get("DATA_DIR", "/data"))
DB_PATH = DATA / "lan-watch.db"
DEVICES_PATH = Path(os.environ.get("DEVICES_FILE", "/app/devices.json"))
POLL_MIN = 30
POLL_MAX = 3600
PRESETS = (30, 60, 120, 300, 900)

# Runs on the Beelink. Prints one JSON object and nothing else.
REMOTE = r"""
import json, subprocess, time
SAMPLE = int(%(sample)d)

def grab():
    raw = subprocess.check_output(
        ["docker", "exec", "frigate", "wget", "-qO-", "http://127.0.0.1:1984/api/streams"],
        text=True, timeout=20,
    )
    data = json.loads(raw)
    out = {}
    for name, stream in data.items():
        if "://" in name or "@" in name:
            continue
        recv = 0
        for prod in stream.get("producers") or []:
            recv += prod.get("bytes_recv") or 0
        out[name] = {"bytes": recv, "consumers": len(stream.get("consumers") or [])}
    return out

def fps():
    raw = subprocess.check_output(
        ["docker", "exec", "frigate", "wget", "-qO-", "http://127.0.0.1:5000/api/stats"],
        text=True, timeout=20,
    )
    cams = json.loads(raw).get("cameras") or {}
    return {name: cams[name].get("camera_fps") for name in cams}

def health():
    raw = subprocess.check_output(
        ["docker", "inspect", "-f", "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}", "frigate"],
        text=True, timeout=15,
    ).strip()
    return raw

first = grab()
time.sleep(SAMPLE)
second = grab()
rows = []
total = 0.0
for name in sorted(second):
    delta = second[name]["bytes"] - first.get(name, {}).get("bytes", 0)
    mbps = max(delta, 0) * 8 / SAMPLE / 1e6
    if mbps > 0.005:
        total += mbps
    rows.append({
        "name": name,
        "mbps": round(mbps, 3),
        "consumers": second[name]["consumers"],
    })
print(json.dumps({
    "streams": rows,
    "total_mbps": round(total, 3),
    "fps": fps(),
    "frigate": health(),
}))
"""

app = Flask(__name__)
_lock = threading.Lock()
_wake = threading.Event()
_once = False
_state = {"checking": False, "next_check_at": None}
_latest = {
    "ok": False,
    "error": "Waiting for the first check.",
    "at": None,
    "at_iso": None,
    "failed_at": None,
    "total_mbps": None,
    "streams": [],
    "fps": {},
    "frigate": None,
    "pings": [],
}


def _now():
    return datetime.now(TZ)


def _iso(dt):
    return dt.isoformat(timespec="seconds")


def _clamp_poll(value):
    if isinstance(value, bool):
        raise ValueError("Enter a number of seconds from 30 to 3600.")
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        raise ValueError("Enter a number of seconds from 30 to 3600.") from None
    if seconds < POLL_MIN or seconds > POLL_MAX:
        raise ValueError("Enter a number of seconds from 30 to 3600.")
    return seconds


def _db():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY,
            at TEXT NOT NULL,
            ok INTEGER NOT NULL,
            total_mbps REAL,
            payload TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    return conn


def _ensure_settings():
    conn = _db()
    try:
        have = {row[0] for row in conn.execute("SELECT key FROM settings")}
        if "poll_seconds" not in have:
            try:
                default = _clamp_poll(POLL_SECONDS)
            except ValueError:
                default = 60
            conn.execute(
                "INSERT INTO settings (key, value) VALUES ('poll_seconds', ?)",
                (str(default),),
            )
        if "enabled" not in have:
            conn.execute(
                "INSERT INTO settings (key, value) VALUES ('enabled', '1')"
            )
        conn.commit()
    finally:
        conn.close()


def _load_settings():
    conn = _db()
    try:
        rows = dict(conn.execute("SELECT key, value FROM settings"))
    finally:
        conn.close()
    try:
        poll = _clamp_poll(rows.get("poll_seconds", POLL_SECONDS))
    except ValueError:
        poll = 60
    return {
        "enabled": rows.get("enabled", "1") == "1",
        "poll_seconds": poll,
        "sample_seconds": SAMPLE_SECONDS,
        "presets": list(PRESETS),
    }


def _update_settings(body):
    if not isinstance(body, dict):
        raise ValueError("Settings must be an object.")
    current = _load_settings()
    enabled = current["enabled"]
    poll = current["poll_seconds"]
    if "enabled" in body:
        if not isinstance(body["enabled"], bool):
            raise ValueError("Checks on or off must be true or false.")
        enabled = body["enabled"]
    if "poll_seconds" in body:
        poll = _clamp_poll(body["poll_seconds"])
    conn = _db()
    try:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES ('enabled', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            ("1" if enabled else "0",),
        )
        conn.execute(
            "INSERT INTO settings (key, value) VALUES ('poll_seconds', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(poll),),
        )
        conn.commit()
    finally:
        conn.close()
    return _load_settings()


def _load_devices():
    try:
        data = json.loads(DEVICES_PATH.read_text())
    except Exception as exc:
        return [], str(exc)
    devices = []
    for kind in ("esp", "cameras"):
        for item in data.get(kind, []):
            devices.append({"kind": kind, "name": item["name"], "ip": item["ip"]})
    return devices, None


def _ssh_sample():
    script = REMOTE % {"sample": SAMPLE_SECONDS}
    cmd = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "UserKnownHostsFile=/data/known_hosts",
        "-o", "ConnectTimeout=8",
        f"{BEELINK_USER}@{BEELINK_HOST}",
        "python3", "-",
    ]
    proc = subprocess.run(
        cmd,
        input=script,
        text=True,
        capture_output=True,
        timeout=SAMPLE_SECONDS + 40,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "ssh failed").strip()
        err = err.replace(BEELINK_HOST, "beelink")
        raise RuntimeError(err.splitlines()[-1][:300])
    line = proc.stdout.strip().splitlines()[-1]
    if "://" in line or ("@" in line and "rtsp" in line.lower()):
        raise RuntimeError("remote sample included a stream URL; discarded")
    return json.loads(line)


def _ping_one(device):
    proc = subprocess.run(
        ["ping", "-n", "-c", "5", "-W", "1", "-q", device["ip"]],
        text=True,
        capture_output=True,
        timeout=12,
    )
    text = (proc.stdout or "") + (proc.stderr or "")
    loss = None
    avg = None
    for line in text.splitlines():
        if "packet loss" in line:
            for part in line.split(","):
                part = part.strip()
                if part.endswith("packet loss"):
                    loss = float(part.split("%")[0])
        if line.startswith("rtt") or line.startswith("round-trip"):
            try:
                avg = float(line.split("=")[1].split("/")[1])
            except (IndexError, ValueError):
                avg = None
    if loss is None:
        loss = 100.0
    return {
        "kind": device["kind"],
        "name": device["name"],
        "ip": device["ip"],
        "loss": loss,
        "avg_ms": None if avg is None else round(avg, 1),
    }


def _ping_all():
    devices, err = _load_devices()
    if err:
        return [], err
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(_ping_one, devices))
    return rows, None


def _store(payload):
    conn = _db()
    try:
        conn.execute(
            "INSERT INTO samples (at, ok, total_mbps, payload) VALUES (?, ?, ?, ?)",
            (
                payload.get("at") or _now().strftime("%Y-%m-%d %H:%M:%S"),
                1 if payload.get("ok") else 0,
                payload.get("total_mbps") if payload.get("ok") else None,
                json.dumps(payload),
            ),
        )
        conn.execute(
            "DELETE FROM samples WHERE id NOT IN (SELECT id FROM samples ORDER BY id DESC LIMIT 1500)"
        )
        conn.commit()
    finally:
        conn.close()


def _recent(limit=24):
    if not DB_PATH.exists():
        return []
    conn = _db()
    try:
        rows = conn.execute(
            "SELECT at, ok, total_mbps FROM samples ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return [{"at": at, "ok": bool(ok), "total_mbps": total} for at, ok, total in rows]


def _set_state(*, checking, next_check_at):
    with _lock:
        _state["checking"] = checking
        _state["next_check_at"] = next_check_at


def _take_once():
    global _once
    with _lock:
        if not _once:
            return False
        _once = False
        return True


def _once_pending():
    with _lock:
        return _once


def _sample_once():
    at_dt = _now()
    at = at_dt.strftime("%Y-%m-%d %H:%M:%S")
    try:
        remote = _ssh_sample()
        pings, ping_err = _ping_all()
        payload = {
            "ok": True,
            "error": ping_err,
            "at": at,
            "at_iso": _iso(at_dt),
            "failed_at": None,
            "total_mbps": remote["total_mbps"],
            "streams": remote["streams"],
            "fps": remote["fps"],
            "frigate": remote["frigate"],
            "pings": pings,
        }
    except Exception as exc:
        with _lock:
            previous = dict(_latest)
        payload = {
            "ok": False,
            "error": str(exc),
            "at": previous.get("at"),
            "at_iso": previous.get("at_iso"),
            "failed_at": at,
            "total_mbps": previous.get("total_mbps"),
            "streams": previous.get("streams") or [],
            "fps": previous.get("fps") or {},
            "frigate": previous.get("frigate"),
            "pings": previous.get("pings") or [],
        }
    with _lock:
        _latest.clear()
        _latest.update(payload)
    _store(payload)


def _loop():
    while True:
        if _take_once() or _load_settings()["enabled"]:
            _set_state(checking=True, next_check_at=None)
            started = time.monotonic()
            _sample_once()
            while True:
                if _once_pending():
                    break
                settings = _load_settings()
                if not settings["enabled"]:
                    _set_state(checking=False, next_check_at=None)
                    break
                remaining = settings["poll_seconds"] - (time.monotonic() - started)
                if remaining <= 0:
                    break
                _set_state(
                    checking=False,
                    next_check_at=_iso(_now() + timedelta(seconds=remaining)),
                )
                _wake.wait(min(remaining, 1.0))
                _wake.clear()
            continue
        _set_state(checking=False, next_check_at=None)
        _wake.wait(1.0)
        _wake.clear()


def _view():
    with _lock:
        payload = dict(_latest)
        state = dict(_state)
    payload["history"] = _recent()
    payload["settings"] = _load_settings()
    payload["state"] = state
    return payload


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LAN watch</title>
<style>
  :root {
    --bg: #101114;
    --card: #191b20;
    --line: #2d313a;
    --text: #f2f2f3;
    --muted: #a3a3ad;
    --accent: #9ec1ff;
    --ok: #8fd0a8;
    --warn: #e6c27a;
    --bad: #f0a3a3;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font: 15px/1.45 system-ui, sans-serif;
  }
  .wrap { max-width: 1080px; margin: 0 auto; padding: 1.4rem 1.15rem 3rem; }
  header { display: flex; justify-content: space-between; gap: 1rem; align-items: flex-end; flex-wrap: wrap; }
  h1 { font-size: 1.35rem; font-weight: 650; letter-spacing: -0.02em; margin: 0; }
  h2 { font-size: 0.98rem; margin: 0 0 0.15rem; }
  .sub, .note { color: var(--muted); margin: 0.2rem 0 0; }
  .status { margin-top: 0.35rem; }
  .pill {
    display: inline-flex; align-items: center; gap: 0.4rem;
    border: 1px solid var(--line); border-radius: 999px;
    padding: 0.18rem 0.6rem; color: var(--muted); font-size: 0.82rem;
  }
  .dot { width: 0.48rem; height: 0.48rem; border-radius: 50%; background: var(--muted); }
  .pill.ok .dot { background: var(--ok); }
  .pill.warn .dot { background: var(--warn); }
  .pill.bad .dot { background: var(--bad); }
  .controls {
    display: flex; flex-wrap: wrap; gap: 0.8rem 1rem; align-items: center;
    background: var(--card); border: 1px solid var(--line); border-radius: 14px;
    padding: 0.85rem 1rem; margin: 1rem 0 0.85rem;
  }
  .switch { display: inline-flex; align-items: center; gap: 0.55rem; cursor: pointer; user-select: none; }
  .switch input { position: absolute; opacity: 0; width: 1px; height: 1px; }
  .track { width: 2.45rem; height: 1.4rem; background: #3a3d46; border-radius: 999px; position: relative; }
  .track:before {
    content: ""; position: absolute; width: 1.08rem; height: 1.08rem; border-radius: 50%;
    background: white; top: 0.16rem; left: 0.16rem; transition: transform 0.15s ease;
  }
  .switch input:checked + .track { background: #2d6a4f; }
  .switch input:checked + .track:before { transform: translateX(1.05rem); }
  .switch input:focus-visible + .track { outline: 2px solid var(--accent); outline-offset: 2px; }
  .presets { display: flex; flex-wrap: wrap; gap: 0.35rem; }
  button, .custom button {
    background: transparent; color: var(--text); border: 1px solid var(--line);
    border-radius: 999px; padding: 0.35rem 0.75rem; cursor: pointer; font: inherit;
  }
  button:hover, .custom button:hover { border-color: #4a5160; }
  button.active { background: #243044; border-color: #446089; color: var(--accent); }
  button:disabled { opacity: 0.45; cursor: default; }
  .custom { display: flex; gap: 0.35rem; align-items: center; }
  .custom input {
    width: 5.2rem; background: #121318; color: var(--text); border: 1px solid var(--line);
    border-radius: 999px; padding: 0.35rem 0.7rem; font: inherit;
  }
  .hint { flex-basis: 100%; color: var(--muted); font-size: 0.86rem; }
  .banner {
    border-radius: 12px; padding: 0.65rem 0.85rem; margin-bottom: 0.75rem;
    background: #2a2418; color: var(--warn);
  }
  .banner.bad { background: #2c1c1c; color: var(--bad); }
  .hero { display: grid; grid-template-columns: 1.4fr 1fr 1fr; gap: 0.75rem; }
  .stat, .card {
    background: var(--card); border: 1px solid var(--line); border-radius: 14px; padding: 0.95rem 1rem;
  }
  .k { color: var(--muted); font-size: 0.75rem; letter-spacing: 0.05em; text-transform: uppercase; }
  .v { font-size: 2rem; font-variant-numeric: tabular-nums; letter-spacing: -0.03em; margin-top: 0.15rem; }
  .v small { font-size: 1rem; color: var(--muted); margin-left: 0.2rem; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.75rem; margin-top: 0.75rem; }
  .wide { margin-top: 0.75rem; }
  table { width: 100%; border-collapse: collapse; }
  th {
    color: var(--muted); font-weight: 550; font-size: 0.75rem; letter-spacing: 0.04em;
    text-transform: uppercase; text-align: left;
  }
  td, th { padding: 0.42rem 0.15rem; border-bottom: 1px solid var(--line); vertical-align: baseline; }
  tr:last-child td { border-bottom: 0; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  td.warn, .warn-text { color: var(--warn); font-weight: 650; }
  td.bad { color: var(--bad); font-weight: 650; }
  .ip { display: block; color: var(--muted); font-size: 0.78rem; font-weight: 450; }
  .tag {
    font-size: 0.72rem; color: var(--muted); border: 1px solid var(--line);
    border-radius: 999px; padding: 0 0.4rem; margin-left: 0.4rem; vertical-align: 1px;
  }
  details { margin-top: 0.7rem; color: var(--muted); }
  summary { cursor: pointer; }
  .bars { display: flex; align-items: flex-end; gap: 4px; height: 76px; margin-top: 0.8rem; }
  .bars span {
    flex: 1; background: #3c5678; border-radius: 3px 3px 0 0; min-height: 3px; display: block;
  }
  .bars span.fail { background: #7a3e3e; }
  .bars span:hover { background: var(--accent); }
  @media (max-width: 800px) {
    .hero, .grid { grid-template-columns: 1fr; }
    .v { font-size: 1.7rem; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>LAN watch</h1>
      <p class="sub" id="status">Loading the latest check.</p>
    </div>
    <div class="pill" id="frigate"><span class="dot"></span><span id="frigate-text">Frigate</span></div>
  </header>

  <section class="controls">
    <label class="switch">
      <input type="checkbox" id="enabled" checked>
      <span class="track"></span>
      <span id="enabled-label">Checks on</span>
    </label>
    <button type="button" id="now">Check now</button>
    <div class="presets" id="presets"></div>
    <form class="custom" id="custom">
      <input id="custom-seconds" type="number" min="30" max="3600" step="10" inputmode="numeric" aria-label="Seconds between checks">
      <button type="submit">Set</button>
    </form>
    <p class="hint" id="hint"></p>
  </section>

  <div id="banner"></div>

  <section class="hero">
    <article class="stat">
      <div class="k">Camera pull</div>
      <div class="v" id="total">—</div>
      <p class="note" id="total-note"></p>
    </article>
    <article class="stat">
      <div class="k">Streams pulling</div>
      <div class="v" id="stream-count">—</div>
      <p class="note" id="stream-note"></p>
    </article>
    <article class="stat">
      <div class="k">Slow or lossy</div>
      <div class="v" id="trouble-count">—</div>
      <p class="note" id="trouble-note"></p>
    </article>
  </section>

  <section class="card wide">
    <h2>Streams</h2>
    <p class="note">Sorted by bitrate. One viewer is Frigate holding the stream open.</p>
    <table>
      <thead>
        <tr><th>Stream</th><th class="num">Mbps</th><th class="num">Viewers</th><th class="num">fps</th></tr>
      </thead>
      <tbody id="streams"></tbody>
    </table>
    <details id="idle">
      <summary id="idle-summary">Idle streams</summary>
      <p id="idle-list"></p>
    </details>
  </section>

  <section class="grid">
    <article class="card">
      <h2>ESP devices</h2>
      <p class="note">Worst first. Five pings from this container. Yellow is 10% loss or a reply over 400 ms.</p>
      <table>
        <thead><tr><th>Device</th><th class="num">Loss</th><th class="num">Avg reply</th></tr></thead>
        <tbody id="esp"></tbody>
      </table>
    </article>
    <article class="card">
      <h2>Cameras</h2>
      <p class="note">Same five-ping check. A short sample can show 0% when a longer test would not.</p>
      <table>
        <thead><tr><th>Device</th><th class="num">Loss</th><th class="num">Avg reply</th></tr></thead>
        <tbody id="cameras"></tbody>
      </table>
    </article>
  </section>

  <section class="card wide">
    <h2>Recent pull</h2>
    <p class="note" id="history-note">Newest on the right.</p>
    <div class="bars" id="bars"></div>
  </section>
</div>
<script>
const PRESETS = [[30, "30s"], [60, "1 min"], [120, "2 min"], [300, "5 min"], [900, "15 min"]];
let latest = null;
let controlsKey = "";

function text(id, value) {
  document.getElementById(id).textContent = value;
}
function clear(id) {
  document.getElementById(id).replaceChildren();
}
function cell(value, className) {
  const td = document.createElement("td");
  td.textContent = value;
  if (className) td.className = className;
  return td;
}
function fmtDur(sec) {
  sec = Math.max(0, sec | 0);
  if (sec < 60) return sec + "s";
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  if (m < 60) return s ? m + "m " + s + "s" : m + "m";
  const h = Math.floor(m / 60);
  return h + "h " + (m % 60) + "m";
}
function ago(iso) {
  if (!iso) return "";
  const sec = Math.round((Date.now() - new Date(iso).getTime()) / 1000);
  if (!Number.isFinite(sec)) return "";
  if (sec < 5) return "just now";
  return fmtDur(sec) + " ago";
}
function streamParts(name) {
  let role = "main";
  let base = name;
  if (name.endsWith("_sub")) { role = "sub"; base = name.slice(0, -4); }
  else if (name.endsWith("_live")) { role = "live"; base = name.slice(0, -5); }
  return { label: base.replaceAll("_", " "), role: role };
}
function tone(row) {
  if (row.loss >= 30) return "bad";
  if (row.loss >= 10) return "warn";
  if (row.avg_ms != null && row.avg_ms >= 400) return "warn";
  if (row.avg_ms != null && row.avg_ms >= 150) return "slow";
  return "";
}
function pingClass(row, field) {
  const t = tone(row);
  if (field === "loss") return t === "bad" || t === "warn" ? t : "num";
  if (row.avg_ms != null && row.avg_ms >= 400) return row.avg_ms >= 800 ? "num bad" : "num warn";
  if (row.avg_ms != null && row.avg_ms >= 150) return "num warn";
  return "num";
}
function sortPings(rows) {
  return rows.slice().sort((a, b) => {
    const loss = b.loss - a.loss;
    if (loss) return loss;
    return (b.avg_ms || 0) - (a.avg_ms || 0);
  });
}
function renderPings(id, rows) {
  const body = document.getElementById(id);
  body.replaceChildren();
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = cell("No pings in this check.", "");
    td.colSpan = 3;
    tr.appendChild(td);
    body.appendChild(tr);
    return;
  }
  for (const row of sortPings(rows)) {
    const tr = document.createElement("tr");
    const name = document.createElement("td");
    name.textContent = row.name;
    const ip = document.createElement("span");
    ip.className = "ip";
    ip.textContent = row.ip;
    name.appendChild(ip);
    tr.appendChild(name);
    tr.appendChild(cell(Math.round(row.loss) + "%", pingClass(row, "loss")));
    tr.appendChild(cell(row.avg_ms == null ? "—" : Math.round(row.avg_ms) + " ms", pingClass(row, "avg")));
    body.appendChild(tr);
  }
}
function renderStreams(data) {
  const streams = data.streams || [];
  const pulling = streams.filter(s => s.mbps >= 0.01 || s.consumers);
  pulling.sort((a, b) => b.mbps - a.mbps);
  const idle = streams.filter(s => !pulling.includes(s));
  const body = document.getElementById("streams");
  body.replaceChildren();
  if (!pulling.length) {
    const tr = document.createElement("tr");
    const td = cell("No streams are pulling.", "");
    td.colSpan = 4;
    tr.appendChild(td);
    body.appendChild(tr);
  }
  for (const s of pulling) {
    const parts = streamParts(s.name);
    const cam = s.name.replace(/_sub$/, "").replace(/_live$/, "");
    const fps = (data.fps || {})[cam];
    const tr = document.createElement("tr");
    const name = document.createElement("td");
    name.textContent = parts.label;
    name.title = s.name;
    const tag = document.createElement("span");
    tag.className = "tag";
    tag.textContent = parts.role;
    name.appendChild(tag);
    tr.appendChild(name);
    tr.appendChild(cell(s.mbps.toFixed(2), "num"));
    tr.appendChild(cell(String(s.consumers), "num"));
    tr.appendChild(cell(fps == null ? "—" : Number(fps).toFixed(1), "num"));
    body.appendChild(tr);
  }
  text("stream-count", String(pulling.length));
  text("stream-note", idle.length ? idle.length + " idle" : "Nothing idle");
  text("idle-summary", idle.length ? idle.length + " idle streams" : "No idle streams");
  text("idle-list", idle.map(s => streamParts(s.name).label + " (" + streamParts(s.name).role + ")").join(", ") || "None");
  document.getElementById("idle").hidden = idle.length === 0;
}
function renderHistory(history) {
  const bars = document.getElementById("bars");
  const rows = (history || []).slice().reverse();
  bars.replaceChildren();
  const max = Math.max(0.1, ...rows.map(h => h.total_mbps || 0));
  if (!rows.length) {
    text("history-note", "No checks stored yet.");
    return;
  }
  text("history-note", "Last " + rows.length + " checks. Newest on the right. Hover a bar for the time.");
  for (const row of rows) {
    const bar = document.createElement("span");
    const value = row.ok ? (row.total_mbps || 0) : 0;
    bar.style.height = Math.max(4, (value / max) * 100) + "%";
    if (!row.ok) bar.className = "fail";
    bar.title = row.at + " — " + (row.total_mbps == null ? "failed" : row.total_mbps.toFixed(2) + " Mbps");
    bars.appendChild(bar);
  }
}
function renderBanner(data) {
  const banner = document.getElementById("banner");
  banner.className = "banner";
  const settings = data.settings || {};
  const parts = [];
  if (!settings.enabled) {
    parts.push("Checks are paused. The numbers stay on the last result.");
  }
  if (data.error) {
    if (!data.ok && data.at) banner.classList.add("bad");
    parts.push(data.error);
    if (!data.ok && data.at) parts.push("Showing the last successful numbers.");
  }
  if (!parts.length) {
    banner.hidden = true;
    banner.textContent = "";
    return;
  }
  banner.hidden = false;
  banner.textContent = parts.join(" ");
}
function renderStatus(data) {
  const settings = data.settings || {};
  const state = data.state || {};
  const when = data.at ? data.at.slice(11) : "—";
  const age = ago(data.at_iso);
  let next = "Paused";
  if (state.checking) next = "Checking now";
  else if (settings.enabled && state.next_check_at) {
    const sec = Math.round((new Date(state.next_check_at).getTime() - Date.now()) / 1000);
    next = sec > 0 ? "next check in " + fmtDur(sec) : "next check starting";
  } else if (settings.enabled) next = "next check soon";
  const ageTxt = age ? " · " + age : "";
  text("status", "Last check " + when + ageTxt + " · " + next);
  const fr = data.frigate || "unknown";
  text("frigate-text", "Frigate " + fr);
  const pill = document.getElementById("frigate");
  const low = fr.toLowerCase();
  pill.className = "pill " + (low.includes("healthy") ? "ok" : low.includes("running") ? "warn" : "bad");
  const total = data.total_mbps;
  const totalEl = document.getElementById("total");
  totalEl.replaceChildren();
  if (total == null) totalEl.textContent = "—";
  else {
    totalEl.textContent = total.toFixed(2);
    const unit = document.createElement("small");
    unit.textContent = "Mbps";
    totalEl.appendChild(unit);
  }
  const sample = settings.sample_seconds || 10;
  text("total-note", "Measured over " + sample + " seconds" + (settings.enabled ? "" : " · checks paused"));
  document.getElementById("now").disabled = !!state.checking;
}
function renderTrouble(data) {
  const rows = (data.pings || []).filter(row => tone(row));
  text("trouble-count", String(rows.length));
  if (!rows.length) {
    text("trouble-note", data.pings && data.pings.length ? "Replies look ordinary" : "Waiting for pings");
    return;
  }
  text("trouble-note", rows.slice(0, 3).map(row => row.name).join(", "));
}
function syncControls(settings) {
  document.getElementById("enabled").checked = !!settings.enabled;
  text("enabled-label", settings.enabled ? "Checks on" : "Checks off");
  for (const button of document.getElementById("presets").children) {
    button.classList.toggle("active", Number(button.dataset.sec) === settings.poll_seconds);
  }
  const input = document.getElementById("custom-seconds");
  if (document.activeElement !== input) input.value = settings.poll_seconds;
  const sample = settings.sample_seconds || 10;
  text("hint", "A check measures camera pull for " + sample + " seconds, then pings every device. Checks start this often. The choice is saved on this server. The page itself refreshes every few seconds.");
}
function render(data) {
  latest = data;
  const key = JSON.stringify(data.settings || {});
  if (key !== controlsKey) {
    controlsKey = key;
    syncControls(data.settings || {});
  }
  renderStatus(data);
  renderBanner(data);
  renderStreams(data);
  renderPings("esp", (data.pings || []).filter(row => row.kind === "esp"));
  renderPings("cameras", (data.pings || []).filter(row => row.kind === "cameras"));
  renderTrouble(data);
  renderHistory(data.history || []);
}
async function load() {
  try {
    const response = await fetch("/api/latest", {cache: "no-store"});
    if (!response.ok) throw new Error("status " + response.status);
    render(await response.json());
  } catch (err) {
    text("status", "The page cannot reach LAN watch.");
  }
}
async function save(body) {
  const response = await fetch("/api/settings", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const banner = document.getElementById("banner");
    banner.hidden = false;
    banner.className = "banner bad";
    banner.textContent = payload.error || "Could not save that setting.";
    return;
  }
  await load();
}

const presets = document.getElementById("presets");
for (const [sec, label] of PRESETS) {
  const button = document.createElement("button");
  button.type = "button";
  button.textContent = label;
  button.dataset.sec = String(sec);
  button.addEventListener("click", () => save({poll_seconds: sec}));
  presets.appendChild(button);
}
document.getElementById("enabled").addEventListener("change", (event) => {
  save({enabled: event.target.checked});
});
document.getElementById("custom").addEventListener("submit", (event) => {
  event.preventDefault();
  save({poll_seconds: Number(document.getElementById("custom-seconds").value)});
});
document.getElementById("now").addEventListener("click", async () => {
  document.getElementById("now").disabled = true;
  await fetch("/api/check", {method: "POST"});
  await load();
});
setInterval(() => { if (latest) renderStatus(latest); }, 1000);
setInterval(load, 5000);
load();
</script>
</body>
</html>
"""


@app.get("/health")
def health():
    return jsonify(ok=True)


@app.get("/favicon.ico")
def favicon():
    return ("", 204)


@app.get("/api/latest")
def latest():
    return jsonify(_view())


@app.post("/api/settings")
def update_settings():
    body = request.get_json(silent=True) or {}
    try:
        saved = _update_settings(body)
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400
    _wake.set()
    return jsonify(saved)


@app.post("/api/check")
def check_now():
    global _once
    with _lock:
        _once = True
    _wake.set()
    return jsonify(ok=True)


@app.get("/")
def index():
    return PAGE


def main():
    _ensure_settings()
    threading.Thread(target=_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, threaded=True)


if __name__ == "__main__":
    main()
