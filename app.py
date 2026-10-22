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
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, jsonify

BEELINK_HOST = os.environ.get("BEELINK_HOST", "192.168.86.16")
BEELINK_USER = os.environ.get("BEELINK_USER", "david")
SSH_KEY = os.environ.get("SSH_KEY", "/run/ssh/id_ed25519")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
SAMPLE_SECONDS = int(os.environ.get("SAMPLE_SECONDS", "10"))
TZ = ZoneInfo(os.environ.get("TZ", "America/New_York"))
DATA = Path(os.environ.get("DATA_DIR", "/data"))
DB_PATH = DATA / "lan-watch.db"
DEVICES_PATH = Path(os.environ.get("DEVICES_FILE", "/app/devices.json"))

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
_latest = {
    "ok": False,
    "error": "waiting for the first sample",
    "at": None,
}


def _now():
    return datetime.now(TZ)


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
    return conn


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
    if "://" in line or "@" in line and "rtsp" in line.lower():
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
            # "5 packets transmitted, 4 received, 20% packet loss, time 4006ms"
            for part in line.split(","):
                part = part.strip()
                if part.endswith("packet loss"):
                    loss = float(part.split("%")[0])
        if line.startswith("rtt") or line.startswith("round-trip"):
            # rtt min/avg/max/mdev = 1.2/3.4/5.6/0.7 ms
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
    rows.sort(key=lambda row: (row["kind"], row["name"]))
    return rows, None


def _store(payload):
    conn = _db()
    try:
        conn.execute(
            "INSERT INTO samples (at, ok, total_mbps, payload) VALUES (?, ?, ?, ?)",
            (
                payload["at"],
                1 if payload.get("ok") else 0,
                payload.get("total_mbps"),
                json.dumps(payload),
            ),
        )
        conn.execute(
            "DELETE FROM samples WHERE id NOT IN (SELECT id FROM samples ORDER BY id DESC LIMIT 1500)"
        )
        conn.commit()
    finally:
        conn.close()


def _recent(limit=12):
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


def _sample_once():
    at = _now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        remote = _ssh_sample()
        pings, ping_err = _ping_all()
        payload = {
            "ok": True,
            "error": ping_err,
            "at": at,
            "total_mbps": remote["total_mbps"],
            "streams": remote["streams"],
            "fps": remote["fps"],
            "frigate": remote["frigate"],
            "pings": pings,
        }
    except Exception as exc:
        payload = {
            "ok": False,
            "error": str(exc),
            "at": at,
            "total_mbps": None,
            "streams": [],
            "fps": {},
            "frigate": None,
            "pings": [],
        }
    with _lock:
        _latest.clear()
        _latest.update(payload)
    _store(payload)


def _loop():
    while True:
        started = time.monotonic()
        _sample_once()
        remaining = POLL_SECONDS - (time.monotonic() - started)
        if remaining > 0:
            time.sleep(remaining)


def _page(payload, history):
    def row_streams():
        lines = []
        pulling = [s for s in payload.get("streams") or [] if s["mbps"] >= 0.01 or s["consumers"]]
        idle = [s["name"] for s in payload.get("streams") or [] if s not in pulling]
        for s in pulling:
            cam = s["name"].removesuffix("_sub").removesuffix("_live")
            fps = (payload.get("fps") or {}).get(cam)
            fps_txt = "" if fps is None else f"{fps}"
            lines.append(
                f"<tr><td>{s['name']}</td><td class='num'>{s['mbps']:.2f}</td>"
                f"<td class='num'>{s['consumers']}</td><td class='num'>{fps_txt}</td></tr>"
            )
        body = "\n".join(lines) or "<tr><td colspan='4'>No streams are pulling.</td></tr>"
        idle_txt = ", ".join(idle) if idle else "none"
        return body, idle_txt

    def row_pings(kind, title):
        rows = [p for p in payload.get("pings") or [] if p["kind"] == kind]
        lines = [f"<h2>{title}</h2><table><tr><th>Device</th><th>Loss</th><th>Avg reply</th></tr>"]
        if not rows:
            lines.append("<tr><td colspan='3'>No ping results yet.</td></tr>")
        for p in rows:
            avg = "—" if p["avg_ms"] is None else f"{p['avg_ms']:.0f} ms"
            klass = "bad" if p["loss"] >= 10 else ""
            lines.append(
                f"<tr class='{klass}'><td>{p['name']}</td>"
                f"<td class='num'>{p['loss']:.0f}%</td><td class='num'>{avg}</td></tr>"
            )
        lines.append("</table>")
        return "\n".join(lines)

    err = payload.get("error") or ""
    err_html = f"<p class='bad'>{err}</p>" if err else ""
    total = payload.get("total_mbps")
    total_txt = "—" if total is None else f"{total:.2f} Mbps"
    streams, idle = row_streams()
    hist = "".join(
        f"<li>{h['at']} — {'—' if h['total_mbps'] is None else format(h['total_mbps'], '.2f')} Mbps</li>"
        for h in history
    ) or "<li>No history yet.</li>"
    frigate = payload.get("frigate") or "unknown"
    at = payload.get("at") or "—"
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LAN watch</title>
<style>
  body {{ font: 16px/1.4 sans-serif; background: #161616; color: #eee; margin: 2rem; }}
  h1 {{ font-size: 1.4rem; margin-bottom: 0.2rem; }}
  .total {{ font-size: 2.4rem; margin: 0.4rem 0 1rem; }}
  table {{ border-collapse: collapse; width: min(40rem, 100%); margin-bottom: 1.4rem; }}
  th, td {{ border-bottom: 1px solid #333; text-align: left; padding: 0.35rem 0.6rem; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .bad {{ color: #f0a0a0; }}
  .muted {{ color: #999; }}
</style>
</head>
<body>
<h1>LAN watch</h1>
<p class="muted">Frigate on the Beelink is {frigate}. Sample {at}.</p>
{err_html}
<p class="total">{total_txt}</p>
<h2>Streams being pulled</h2>
<table>
<tr><th>Stream</th><th>Mbps</th><th>Watchers</th><th>Camera fps</th></tr>
{streams}
</table>
<p class="muted">Idle: {idle}</p>
{row_pings("esp", "ESP devices")}
{row_pings("cameras", "Cameras")}
<h2>Recent totals</h2>
<ul>{hist}</ul>
</body>
</html>
"""


@app.get("/health")
def health():
    return jsonify(ok=True)


@app.get("/api/latest")
def latest():
    with _lock:
        payload = dict(_latest)
    payload["history"] = _recent()
    return jsonify(payload)


@app.get("/")
def index():
    with _lock:
        payload = dict(_latest)
    return _page(payload, _recent())


def main():
    _db().close()
    threading.Thread(target=_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, threaded=True)


if __name__ == "__main__":
    main()
