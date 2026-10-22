#!/usr/bin/env python3
"""Frigate Load: camera traffic into Frigate, and ping health around it.

The bitrate is go2rtc's own byte counter on the Frigate machine. This app SSHes
there and asks for it. It does not sniff the network. Stream URLs are stripped
on that machine before anything is printed, because those URLs contain the
camera password.
"""

import json
import os
import re
import socket
import sqlite3
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request

# FRIGATE_HOST is the machine running Frigate. BEELINK_* is the previous name.
FRIGATE_HOST = os.environ.get("FRIGATE_HOST") or os.environ.get("BEELINK_HOST") or ""
FRIGATE_USER = os.environ.get("FRIGATE_USER") or os.environ.get("BEELINK_USER") or ""
SSH_KEY = os.environ.get("SSH_KEY", "/run/ssh/id_ed25519")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
SAMPLE_SECONDS = int(os.environ.get("SAMPLE_SECONDS", "10"))
TZ = ZoneInfo(os.environ.get("TZ", "America/New_York"))
DATA = Path(os.environ.get("DATA_DIR", "/data"))
DB_PATH = DATA / "lan-watch.db"
DEVICES_PATH = Path(os.environ.get("DEVICES_FILE", "/app/devices.json"))
LIVE_DEVICES = DATA / "devices.json"
HA_FILE = DATA / "ha.json"
_devices_lock = threading.Lock()
_mdns_lock = threading.Lock()
POLL_MIN = 30
POLL_MAX = 3600
PRESETS = (30, 60, 120, 300, 900)

# Runs on the Frigate machine. Prints one JSON object and nothing else.
REMOTE = r"""
import json, subprocess, time
from urllib.parse import urlparse
SAMPLE = int(%(sample)d)

def host_of(url):
    if not isinstance(url, str):
        return None
    target = url
    if target.startswith("ffmpeg:"):
        target = target.split("ffmpeg:", 1)[1].split("#", 1)[0]
    if "://" not in target:
        return None
    try:
        host = urlparse(target).hostname
    except Exception:
        return None
    if not host or host in ("127.0.0.1", "localhost") or "@" in host or "/" in host:
        return None
    return host

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
        ip = None
        for prod in stream.get("producers") or []:
            if not isinstance(prod, dict):
                continue
            recv += prod.get("bytes_recv") or 0
            if ip is None:
                ip = host_of(prod.get("url"))
        out[name] = {"bytes": recv, "consumers": len(stream.get("consumers") or []), "ip": ip}
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
rates = fps()
cameras = []
for name in sorted(rates):
    if "://" in name or "@" in name:
        continue
    ip = second.get(name, {}).get("ip") or second.get(name + "_sub", {}).get("ip") or second.get(name + "_main", {}).get("ip")
    item = {"name": name}
    if ip:
        item["ip"] = ip
    cameras.append(item)
print(json.dumps({
    "streams": rows,
    "total_mbps": round(total, 3),
    "fps": rates,
    "frigate": health(),
    "cameras": cameras,
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


def _ensure_devices_file():
    DATA.mkdir(parents=True, exist_ok=True)
    if LIVE_DEVICES.exists():
        return
    if DEVICES_PATH.exists() and DEVICES_PATH.resolve() != LIVE_DEVICES.resolve():
        LIVE_DEVICES.write_text(DEVICES_PATH.read_text())
    else:
        LIVE_DEVICES.write_text('{"other": []}\n')


def _other_items(data):
    if "other" in data:
        return data.get("other") or []
    return data.get("esp") or []


def _clean_name(value):
    if not isinstance(value, str):
        raise ValueError("Use a short name made of letters, numbers, dashes, or underscores.")
    name = value.strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,39}", name):
        raise ValueError("Use a short name made of letters, numbers, dashes, or underscores.")
    return name


def _clean_ip(value):
    if not isinstance(value, str):
        raise ValueError("Enter an IPv4 address.")
    ip = value.strip()
    parts = ip.split(".")
    if len(parts) != 4:
        raise ValueError("Enter an IPv4 address.")
    try:
        ok = all(part.isdigit() and not (len(part) > 1 and part.startswith("0")) and 0 <= int(part) <= 255 for part in parts)
    except ValueError:
        ok = False
    if not ok:
        raise ValueError("Enter an IPv4 address.")
    return ip


def _load_devices():
    try:
        with _devices_lock:
            _ensure_devices_file()
            data = json.loads(LIVE_DEVICES.read_text())
    except Exception as exc:
        return [], str(exc)
    devices = []
    for item in _other_items(data):
        if isinstance(item, dict) and item.get("name") and item.get("ip"):
            devices.append({"kind": "other", "name": str(item["name"]), "ip": str(item["ip"])})
    return devices, None


def _change_device(body):
    if not isinstance(body, dict):
        raise ValueError("Could not read that change.")
    action = body.get("action")
    if action not in ("add", "update", "delete", "add_many"):
        raise ValueError("Use add, update, or delete.")
    name = None if action == "add_many" else _clean_name(body.get("name"))
    with _devices_lock:
        _ensure_devices_file()
        data = json.loads(LIVE_DEVICES.read_text())
        items = []
        for item in _other_items(data):
            if isinstance(item, dict) and item.get("name") and item.get("ip"):
                items.append({"name": str(item["name"]), "ip": str(item["ip"])})
        if action == "add_many":
            incoming = body.get("devices")
            if not isinstance(incoming, list) or not incoming:
                raise ValueError("Choose at least one device.")
            added = 0
            for raw in incoming:
                if not isinstance(raw, dict):
                    continue
                try:
                    new_name = _clean_name(raw.get("name"))
                    ip = _clean_ip(raw.get("ip"))
                except ValueError:
                    continue
                if any(item["name"] == new_name or item["ip"] == ip for item in items):
                    continue
                items.append({"name": new_name, "ip": ip})
                added += 1
            if added == 0:
                raise ValueError("Those devices are already in the list.")
        elif action == "delete":
            if not any(item["name"] == name for item in items):
                raise ValueError("That device is not in the list.")
            items = [item for item in items if item["name"] != name]
        else:
            ip = _clean_ip(body.get("ip"))
            new_name = name if action == "add" else _clean_name(body.get("new_name") or name)
            if any(item["name"] == new_name and item["name"] != name for item in items):
                raise ValueError("That name is already in the list.")
            if action == "add" and any(item["name"] == name for item in items):
                raise ValueError("That name is already in the list.")
            if any(item["ip"] == ip and item["name"] != name for item in items):
                raise ValueError("That address is already in the list.")
            if action == "add":
                items.append({"name": name, "ip": ip})
            else:
                found = False
                for item in items:
                    if item["name"] == name:
                        item["name"] = new_name
                        item["ip"] = ip
                        found = True
                if not found:
                    raise ValueError("That device is not in the list.")
        data.pop("esp", None)
        data["other"] = items
        temporary = LIVE_DEVICES.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(data, indent=2) + "\n")
        temporary.replace(LIVE_DEVICES)
    return items


def _is_ipv4(value):
    if not isinstance(value, str):
        return False
    parts = value.split(".")
    if len(parts) != 4:
        return False
    try:
        return all(
            part.isdigit() and not (len(part) > 1 and part.startswith("0")) and 0 <= int(part) <= 255
            for part in parts
        )
    except ValueError:
        return False


def _usable_ipv4(value):
    if not _is_ipv4(value):
        return False
    if value.startswith("127.") or value.startswith("0.") or value.startswith("169.254."):
        return False
    return True


def _is_private_ipv4(value):
    if not _usable_ipv4(value):
        return False
    first, second = (int(part) for part in value.split(".", 2)[:2])
    if first == 10 or (first == 192 and second == 168):
        return True
    return first == 172 and 16 <= second <= 31


def _resolve_ipv4(host):
    if not host or host in ("localhost",):
        return None
    if _is_ipv4(host):
        return host if _usable_ipv4(host) else None
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
    except socket.gaierror:
        infos = []
    for info in infos:
        ip = info[4][0]
        if _usable_ipv4(ip):
            return ip
    if host.endswith(".local"):
        return _resolve_mdns(host)
    return None


def _resolve_mdns(host):
    """Ask mDNS for a .local name. Docker has no local resolver for those."""
    try:
        from zeroconf import AddressResolver, IPVersion, Zeroconf
    except Exception:
        return None
    name = host if host.endswith(".") else host + "."
    with _mdns_lock:
        try:
            zc = Zeroconf()
        except Exception:
            return None
        try:
            resolver = AddressResolver(name)
            if not resolver.request(zc, 1500):
                return None
            for ip in resolver.parsed_addresses(IPVersion.V4Only):
                if _usable_ipv4(ip):
                    return ip
        except Exception:
            return None
        finally:
            zc.close()
    return None


def _slug_name(value):
    slug = re.sub(r"[^A-Za-z0-9]+", "-", str(value or "").strip()).strip("-").lower()
    slug = slug[:40].strip("-")
    if not slug or not slug[0].isalnum():
        return ""
    return slug


def _host_from_url(value):
    if not isinstance(value, str) or "://" not in value:
        return None
    try:
        parsed = urlparse(value)
    except ValueError:
        return None
    if parsed.scheme not in ("http", "https"):
        return None
    return parsed.hostname


def _is_esp_device(device):
    for ident in device.get("identifiers") or []:
        if isinstance(ident, (list, tuple)) and ident and ident[0] == "esphome":
            return True
    manufacturer = str(device.get("manufacturer") or "").lower()
    model = str(device.get("model") or "").lower()
    return "espressif" in manufacturer or "esp32" in model or "esp8266" in model


def _classify_ha_devices(devices, taken_ips, taken_names):
    pending = []
    for device in devices or []:
        if not isinstance(device, dict) or device.get("disabled_by"):
            continue
        host = _host_from_url(device.get("configuration_url"))
        ip = _resolve_ipv4(host)
        # Cloud integrations publish public addresses. Those are not LAN devices.
        if not ip or not _is_private_ipv4(ip) or ip in taken_ips:
            continue
        label = device.get("name_by_user") or device.get("name") or host
        slug = _slug_name(label)
        if not slug:
            continue
        detail = device.get("model") or device.get("manufacturer") or ""
        if detail == label:
            detail = ""
        pending.append({
            "label": str(label),
            "slug": slug,
            "ip": ip,
            "detail": str(detail),
            "esp": _is_esp_device(device),
            "user_named": bool(device.get("name_by_user")),
        })
    # Home Assistant often lists one device twice. Keep one row, and call it
    # an ESP device if either entry is one.
    chosen = {}
    order = []
    for item in pending:
        current = chosen.get(item["ip"])
        if current is None:
            chosen[item["ip"]] = item
            order.append(item["ip"])
            continue
        if item["esp"]:
            current["esp"] = True
            if item["detail"] and not current["detail"]:
                current["detail"] = item["detail"]
        if item["user_named"] and not current["user_named"]:
            current["label"] = item["label"]
            current["slug"] = item["slug"]
            current["user_named"] = True
    esp = []
    other = []
    used_names = set(taken_names)
    for ip in order:
        item = chosen[ip]
        slug = item["slug"]
        name = slug
        suffix = 2
        while name in used_names:
            extra = f"-{suffix}"
            name = slug[: 40 - len(extra)] + extra
            suffix += 1
        used_names.add(name)
        taken_ips.add(ip)
        row = {"name": name, "label": item["label"], "ip": ip, "detail": item["detail"]}
        (esp if item["esp"] else other).append(row)
    esp.sort(key=lambda item: item["label"].lower())
    other.sort(key=lambda item: item["label"].lower())
    return esp, other


def _clean_ha_url(value):
    if not isinstance(value, str):
        raise ValueError("Enter the Home Assistant address, including http:// or https://.")
    url = value.strip().rstrip("/")
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Enter the Home Assistant address, including http:// or https://.")
    return url


def _load_ha():
    url = os.environ.get("HA_URL", "").strip().rstrip("/")
    token = os.environ.get("HA_TOKEN", "").strip()
    if HA_FILE.exists():
        try:
            saved = json.loads(HA_FILE.read_text())
        except (OSError, json.JSONDecodeError):
            saved = {}
        if isinstance(saved, dict):
            if saved.get("url"):
                url = str(saved["url"]).strip().rstrip("/")
            if saved.get("token"):
                token = str(saved["token"]).strip()
    return {"url": url, "token": token}


def _save_ha(url, token):
    url = _clean_ha_url(url)
    current = _load_ha()
    if token is None or (isinstance(token, str) and not token.strip()):
        token = current["token"]
    else:
        token = str(token).strip()
    if len(token) < 20:
        raise ValueError("Paste a long-lived access token from the Home Assistant profile page.")
    working = _working_ha_url(url)
    if not working:
        raise ValueError("That address did not answer as Home Assistant.")
    url = working
    DATA.mkdir(parents=True, exist_ok=True)
    temporary = HA_FILE.with_suffix(".json.tmp")
    temporary.write_text(json.dumps({"url": url, "token": token}) + "\n")
    temporary.replace(HA_FILE)
    os.chmod(HA_FILE, 0o600)
    return {"url": url, "token_set": True}


def _redact(text, token):
    if token and token in text:
        text = text.replace(token, "token")
    return text[:300]


def _http_get(url):
    # Home networks often use a certificate this container does not trust.
    context = ssl._create_unverified_context()
    try:
        request_url = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(request_url, timeout=4, context=context) as response:
            return response.status, response.read(20000).decode(errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(20000).decode(errors="replace")
    except (OSError, urllib.error.URLError):
        return None, ""


def _looks_like_ha(status, body):
    if not status:
        return False
    if "API running" in body:
        return True
    return "<title>Home Assistant</title>" in body


def _working_ha_url(url):
    """Return the URL Home Assistant answered on.

    Some installs listen with TLS on the usual port and do not answer plain HTTP.
    Current Home Assistant also answers /api/ with a plain 401, so the page title
    or the login providers are enough to recognize it.
    """
    try:
        cleaned = _clean_ha_url(url)
    except ValueError:
        return None
    candidates = [cleaned]
    if cleaned.startswith("http://"):
        candidates.append("https://" + cleaned[len("http://"):])
    for candidate in candidates:
        base = candidate.rstrip("/")
        status, body = _http_get(base + "/")
        if _looks_like_ha(status, body):
            return candidate
        status, body = _http_get(base + "/api/")
        if _looks_like_ha(status, body):
            return candidate
        if status == 401:
            provider_status, providers = _http_get(base + "/auth/providers")
            if provider_status == 200 and "homeassistant" in providers:
                return candidate
    return None


def _probe_ha(url):
    return _working_ha_url(url) is not None


def _ha_from_service(info):
    props = {}
    for key, value in (info.properties or {}).items():
        try:
            text_key = key.decode() if isinstance(key, bytes) else str(key)
            text_value = value.decode(errors="replace") if isinstance(value, bytes) else str(value)
        except Exception:
            continue
        props[text_key] = text_value
    label = props.get("location_name") or "Home Assistant"
    direct = props.get("internal_url") or props.get("base_url") or ""
    if direct.startswith("http"):
        return {"url": direct.rstrip("/"), "name": label}
    addresses = [item for item in info.parsed_addresses() if _usable_ipv4(item)]
    if not addresses:
        return None
    port = info.port or 8123
    return {"url": f"http://{addresses[0]}:{port}", "name": label}


def _discover_ha_mdns():
    from zeroconf import ServiceBrowser, Zeroconf

    names = []
    service = "_home-assistant._tcp.local."

    class _Listener:
        def add_service(self, zc, type_, name):
            names.append(name)

        def update_service(self, zc, type_, name):
            return None

        def remove_service(self, zc, type_, name):
            return None

    # Resolve after the browse. get_service_info inside the callback can stall.
    with _mdns_lock:
        zc = Zeroconf()
        try:
            ServiceBrowser(zc, service, _Listener())
            time.sleep(2.5)
            found = []
            for name in names:
                info = zc.get_service_info(service, name, timeout=1000)
                if info is None:
                    continue
                item = _ha_from_service(info)
                if item:
                    found.append(item)
            return found
        finally:
            zc.close()


def _discover_ha():
    try:
        found = _discover_ha_mdns()
    except Exception:
        found = []
    if found:
        return found
    ip = _resolve_ipv4("homeassistant.local")
    url = f"http://{ip}:8123" if ip else ""
    working = _working_ha_url(url) if url else None
    if working:
        return [{"url": working, "name": "Home Assistant"}]
    return []


def _ha_device_registry(url, token):
    from websocket import create_connection

    parsed = urlparse(url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    port = parsed.port or (443 if scheme == "wss" else 80)
    path = (parsed.path or "").rstrip("/")
    ws_url = f"{scheme}://{parsed.hostname}:{port}{path}/api/websocket"
    options = {}
    if scheme == "wss":
        options["sslopt"] = {"cert_reqs": ssl.CERT_NONE}
    ws = create_connection(ws_url, timeout=8, **options)
    ws.settimeout(8)
    try:
        hello = json.loads(ws.recv())
        if hello.get("type") != "auth_required":
            raise RuntimeError("Home Assistant did not ask for a token.")
        ws.send(json.dumps({"type": "auth", "access_token": token}))
        auth = json.loads(ws.recv())
        if auth.get("type") != "auth_ok":
            raise RuntimeError("Home Assistant rejected the token.")
        ws.send(json.dumps({"id": 1, "type": "config/device_registry/list"}))
        while True:
            message = json.loads(ws.recv())
            if message.get("id") != 1:
                continue
            if not message.get("success"):
                error = message.get("error") or {}
                if error.get("code") == "unauthorized":
                    raise RuntimeError("That token cannot list devices. Create it from an owner account.")
                raise RuntimeError("Home Assistant did not return its devices.")
            return message.get("result") or []
    finally:
        ws.close()


def _ssh_sample():
    if not FRIGATE_HOST or not FRIGATE_USER:
        raise RuntimeError("Set FRIGATE_HOST and FRIGATE_USER")
    script = REMOTE % {"sample": SAMPLE_SECONDS}
    cmd = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "BatchMode=yes",
        "-o", "IdentitiesOnly=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "UserKnownHostsFile=/data/known_hosts",
        "-o", "ConnectTimeout=8",
        f"{FRIGATE_USER}@{FRIGATE_HOST}",
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
        err = err.replace(FRIGATE_HOST, "frigate-host")
        raise RuntimeError(err.splitlines()[-1][:300])
    line = proc.stdout.strip().splitlines()[-1]
    if "://" in line or ("@" in line and "rtsp" in line.lower()):
        raise RuntimeError("remote sample included a stream URL; discarded")
    return json.loads(line)


def _clean_cameras(remote):
    cameras = []
    for item in remote.get("cameras") or []:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        ip = item.get("ip")
        if not isinstance(name, str) or "://" in name or "@" in name:
            continue
        if ip is not None and (not isinstance(ip, str) or "://" in ip or "@" in ip or "/" in ip):
            continue
        cameras.append({"kind": "cameras", "name": name, "ip": ip})
    return cameras


def _ping_one(device):
    if not device.get("ip"):
        return {
            "kind": device["kind"],
            "name": device["name"],
            "ip": None,
            "loss": None,
            "avg_ms": None,
        }
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


def _ping_all(cameras):
    others, err = _load_devices()
    if err:
        return [], err
    devices = list(cameras) + others
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
        pings, ping_err = _ping_all(_clean_cameras(remote))
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
    devices, devices_error = _load_devices()
    payload["other_devices"] = [{"name": item["name"], "ip": item["ip"]} for item in devices]
    payload["devices_error"] = devices_error
    return payload


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Frigate Load</title>
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
  .sub, .note, .lead { color: var(--muted); margin: 0.2rem 0 0; }
  .lead { max-width: 44rem; margin-top: 0.7rem; color: #c8c8d0; }
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
  .custom input, .add-device input, #other input {
    background: #121318; color: var(--text); border: 1px solid var(--line);
    border-radius: 8px; padding: 0.35rem 0.55rem; font: inherit;
  }
  .custom input { width: 5.2rem; border-radius: 999px; padding: 0.35rem 0.7rem; }
  .add-device, .ha-setup { display: flex; flex-wrap: wrap; gap: 0.4rem; margin-top: 0.75rem; }
  .add-device input, .ha-setup input { min-width: 8rem; flex: 1; }
  .ha { margin-top: 0.9rem; padding-top: 0.75rem; border-top: 1px solid var(--line); }
  .ha-actions { display: flex; flex-wrap: wrap; gap: 0.4rem; }
  .suggest { margin-top: 0.55rem; }
  .suggest h3 { margin: 0.55rem 0 0.15rem; font-size: 0.82rem; font-weight: 650; letter-spacing: 0.04em; text-transform: uppercase; color: var(--muted); }
  .suggest label { display: flex; gap: 0.45rem; align-items: baseline; padding: 0.18rem 0; }
  .suggest .meta { color: var(--muted); font-size: 0.8rem; }
  #other button.danger { color: var(--bad); }
  #device-msg:empty { display: none; }
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
      <h1>Frigate Load</h1>
      <p class="lead">Frigate Load measures the video your Wi-Fi cameras are sending to Frigate, and whether that traffic is making the rest of the network slow to answer.</p>
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
      <div class="k">Camera traffic</div>
      <div class="v" id="total">—</div>
      <p class="note" id="total-note">What Frigate is receiving from the cameras.</p>
    </article>
    <article class="stat">
      <div class="k">Open streams</div>
      <div class="v" id="stream-count">—</div>
      <p class="note" id="stream-note"></p>
    </article>
    <article class="stat">
      <div class="k">Struggling</div>
      <div class="v" id="trouble-count">—</div>
      <p class="note" id="trouble-note"></p>
    </article>
  </section>

  <section class="card wide">
    <h2>By camera</h2>
    <p class="note">Heaviest first. Frigate counts as one viewer. Opening a live view adds a viewer on the stream Frigate already has open.</p>
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
    <article class="card" id="other-card">
      <h2>Other devices</h2>
      <p class="note">Anything else on the network you want to watch alongside the cameras. Add, edit, or remove them here. Worst first. Five pings a check. Yellow is 10% loss, or a reply slower than 400 ms.</p>
      <table>
        <thead><tr><th>Device</th><th class="num">Loss</th><th class="num">Avg reply</th><th></th></tr></thead>
        <tbody id="other"></tbody>
      </table>
      <form class="add-device" id="add-device">
        <input id="add-name" placeholder="Name" aria-label="New device name" autocomplete="off">
        <input id="add-ip" placeholder="192.168.1.30" aria-label="New device address" autocomplete="off" inputmode="decimal">
        <button type="submit">Add</button>
      </form>
      <p class="note" id="device-msg"></p>
      <div class="ha">
        <div class="ha-actions">
          <button type="button" id="ha-suggest">Suggest from Home Assistant</button>
          <button type="button" id="ha-find">Find Home Assistant</button>
        </div>
        <form class="ha-setup" id="ha-setup" hidden>
          <input id="ha-url" placeholder="http://192.168.1.2:8123" aria-label="Home Assistant address" autocomplete="off">
          <input id="ha-token" type="password" placeholder="Long-lived token" aria-label="Home Assistant token" autocomplete="off">
          <button type="submit">Save</button>
        </form>
        <p class="note" id="ha-msg"></p>
        <div class="suggest" id="ha-results"></div>
      </div>
    </article>
    <article class="card">
      <h2>Camera ping</h2>
      <p class="note">The cameras Frigate knows about. This ping is separate from the traffic number above. Worst first. Five pings a check, so a quiet result can still hide loss that a longer test would catch.</p>
      <table>
        <thead><tr><th>Device</th><th class="num">Loss</th><th class="num">Avg reply</th></tr></thead>
        <tbody id="cameras"></tbody>
      </table>
    </article>
  </section>

  <section class="card wide">
    <h2>Recent traffic</h2>
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
  if (row.loss == null) return "";
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
  const lossOf = (row) => row.loss == null ? -1 : row.loss;
  return rows.slice().sort((a, b) => {
    const loss = lossOf(b) - lossOf(a);
    if (loss) return loss;
    return (b.avg_ms || 0) - (a.avg_ms || 0);
  });
}
function renderPings(id, rows, emptyText) {
  const body = document.getElementById(id);
  body.replaceChildren();
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = cell(emptyText || "No devices in this check.", "");
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
    tr.appendChild(cell(row.loss == null ? "—" : Math.round(row.loss) + "%", pingClass(row, "loss")));
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
    const td = cell("No cameras are sending video right now.", "");
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
    text("history-note", "No measurements stored yet.");
    return;
  }
  text("history-note", "Last " + rows.length + " measurements, newest on the right. Point at a bar for the time.");
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
  text("total-note", "Received by Frigate over " + sample + " seconds" + (settings.enabled ? "" : " · monitoring paused"));
  document.getElementById("now").disabled = !!state.checking;
}
function renderTrouble(data) {
  const rows = (data.pings || []).filter(row => tone(row));
  text("trouble-count", String(rows.length));
  if (!rows.length) {
    text("trouble-note", data.pings && data.pings.length ? "Nothing looks slow or lossy" : "Waiting for pings");
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
  text("hint", "A check measures camera traffic for " + sample + " seconds, then pings every camera and every other device. Checks start this often. The page updates on its own.");
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
  renderOther(data);
  renderPings("cameras", (data.pings || []).filter(row => row.kind === "cameras"), "Frigate did not report any cameras.");
  renderTrouble(data);
  renderHistory(data.history || []);
}
let forceOther = false;
function renderOther(data) {
  const body = document.getElementById("other");
  if (!forceOther && body.contains(document.activeElement)) return;
  forceOther = false;
  const byName = {};
  for (const row of data.pings || []) {
    if (row.kind === "other" || row.kind === "esp") byName[row.name] = row;
  }
  const configured = (data.other_devices || []).map(item => {
    const ping = byName[item.name] || {};
    return {name: item.name, ip: item.ip, loss: ping.loss == null ? null : ping.loss, avg_ms: ping.avg_ms == null ? null : ping.avg_ms};
  });
  body.replaceChildren();
  if (!configured.length) {
    const tr = document.createElement("tr");
    const td = cell("No other devices yet.", "");
    td.colSpan = 4;
    tr.appendChild(td);
    body.appendChild(tr);
    return;
  }
  for (const row of sortPings(configured)) {
    const tr = document.createElement("tr");
    const name = document.createElement("td");
    name.textContent = row.name;
    const ip = document.createElement("span");
    ip.className = "ip";
    ip.textContent = row.ip;
    name.appendChild(ip);
    tr.appendChild(name);
    tr.appendChild(cell(row.loss == null ? "—" : Math.round(row.loss) + "%", pingClass(row, "loss")));
    tr.appendChild(cell(row.avg_ms == null ? "—" : Math.round(row.avg_ms) + " ms", pingClass(row, "avg")));
    const actions = document.createElement("td");
    actions.className = "num";
    const edit = document.createElement("button");
    edit.type = "button";
    edit.textContent = "Edit";
    edit.addEventListener("click", () => startEdit(tr, row));
    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "danger";
    remove.textContent = "Remove";
    remove.addEventListener("click", () => changeDevice({action: "delete", name: row.name}));
    actions.append(edit, remove);
    tr.appendChild(actions);
    body.appendChild(tr);
  }
}
function startEdit(tr, row) {
  tr.replaceChildren();
  const nameCell = document.createElement("td");
  const nameInput = document.createElement("input");
  nameInput.value = row.name;
  nameInput.setAttribute("aria-label", "Device name");
  nameCell.appendChild(nameInput);
  const ipCell = document.createElement("td");
  ipCell.colSpan = 2;
  const ipInput = document.createElement("input");
  ipInput.value = row.ip;
  ipInput.setAttribute("aria-label", "Device address");
  ipCell.appendChild(ipInput);
  const actions = document.createElement("td");
  actions.className = "num";
  const saveBtn = document.createElement("button");
  saveBtn.type = "button";
  saveBtn.textContent = "Save";
  saveBtn.addEventListener("click", () => changeDevice({
    action: "update", name: row.name, new_name: nameInput.value, ip: ipInput.value,
  }));
  const cancel = document.createElement("button");
  cancel.type = "button";
  cancel.textContent = "Cancel";
  cancel.addEventListener("click", () => { forceOther = true; load(); });
  actions.append(saveBtn, cancel);
  tr.append(nameCell, ipCell, actions);
  nameInput.focus();
}
async function changeDevice(body) {
  const msg = document.getElementById("device-msg");
  const response = await fetch("/api/devices", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    msg.textContent = payload.error || "Could not change the list.";
    return false;
  }
  msg.textContent = body.action === "delete" ? "Removed. A check is starting." : "Saved. A check is starting.";
  forceOther = true;
  document.getElementById("add-name").value = body.action === "add" ? "" : document.getElementById("add-name").value;
  document.getElementById("add-ip").value = body.action === "add" ? "" : document.getElementById("add-ip").value;
  await load();
  return true;
}
async function load() {
  try {
    const response = await fetch("/api/latest", {cache: "no-store"});
    if (!response.ok) throw new Error("status " + response.status);
    render(await response.json());
  } catch (err) {
    text("status", "The page cannot reach Frigate Load.");
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
document.getElementById("add-device").addEventListener("submit", (event) => {
  event.preventDefault();
  changeDevice({
    action: "add",
    name: document.getElementById("add-name").value,
    ip: document.getElementById("add-ip").value,
  });
});
function haMessage(value) {
  text("ha-msg", value || "");
}
function renderSuggestions(payload) {
  const box = document.getElementById("ha-results");
  box.replaceChildren();
  const groups = [
    ["ESP devices", payload.esp || []],
    ["Other Home Assistant devices", payload.other || []],
  ];
  const any = groups.some(([, rows]) => rows.length);
  if (!any) return;
  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.textContent = "Deselect all";
  toggle.addEventListener("click", () => {
    const boxes = [...box.querySelectorAll('input[type="checkbox"]')];
    const select = boxes.some(input => !input.checked);
    for (const input of boxes) input.checked = select;
    toggle.textContent = select ? "Deselect all" : "Select all";
  });
  box.appendChild(toggle);
  for (const [title, rows] of groups) {
    if (!rows.length) continue;
    const heading = document.createElement("h3");
    heading.textContent = title;
    box.appendChild(heading);
    for (const row of rows) {
      const label = document.createElement("label");
      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = true;
      input.dataset.name = row.name;
      input.dataset.ip = row.ip;
      label.appendChild(input);
      const words = document.createElement("span");
      words.textContent = row.label + "  " + row.ip;
      label.appendChild(words);
      if (row.detail) {
        const meta = document.createElement("span");
        meta.className = "meta";
        meta.textContent = row.detail;
        label.appendChild(meta);
      }
      box.appendChild(label);
    }
  }
  const add = document.createElement("button");
  add.type = "button";
  add.textContent = "Add selected";
  add.addEventListener("click", async () => {
    const devices = [...box.querySelectorAll("input:checked")].map(input => ({
      name: input.dataset.name,
      ip: input.dataset.ip,
    }));
    if (!devices.length) {
      haMessage("Select at least one device.");
      return;
    }
    const ok = await changeDevice({action: "add_many", devices: devices});
    if (ok) {
      box.replaceChildren();
      haMessage("Added. A check is starting.");
    }
  });
  box.appendChild(add);
}
document.getElementById("ha-find").addEventListener("click", async () => {
  haMessage("Looking for Home Assistant.");
  const response = await fetch("/api/ha/discover", {cache: "no-store"});
  const payload = await response.json().catch(() => ({}));
  document.getElementById("ha-setup").hidden = false;
  if (payload.url) document.getElementById("ha-url").value = payload.url;
  if (!payload.found) {
    haMessage(payload.error || "No Home Assistant answered. Type its address.");
    return;
  }
  let message = payload.source === "saved"
    ? "Using the saved address. A network search cannot hear Home Assistant from inside Docker."
    : "Found " + (payload.name || "Home Assistant") + " on the network.";
  if (!payload.token_set) message += " Paste a long-lived token.";
  haMessage(message);
});
document.getElementById("ha-suggest").addEventListener("click", async () => {
  haMessage("Asking Home Assistant.");
  const response = await fetch("/api/ha/suggestions", {cache: "no-store"});
  const payload = await response.json().catch(() => ({}));
  if (payload.url) document.getElementById("ha-url").value = payload.url;
  if (!response.ok) {
    document.getElementById("ha-setup").hidden = false;
    haMessage(payload.error || "Could not read devices.");
    renderSuggestions(payload);
    return;
  }
  const count = (payload.esp || []).length + (payload.other || []).length;
  haMessage(count ? "ESP devices are listed first. Already watched devices are hidden." : "Home Assistant had no new devices with an address.");
  renderSuggestions(payload);
});
document.getElementById("ha-setup").addEventListener("submit", async (event) => {
  event.preventDefault();
  haMessage("Saving Home Assistant.");
  const response = await fetch("/api/ha", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      url: document.getElementById("ha-url").value,
      token: document.getElementById("ha-token").value,
    }),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    haMessage(payload.error || "Could not save Home Assistant.");
    return;
  }
  document.getElementById("ha-token").value = "";
  haMessage("Saved. Loading suggestions.");
  document.getElementById("ha-suggest").click();
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


@app.get("/api/ha/discover")
def ha_discover():
    cfg = _load_ha()
    if cfg["url"]:
        working = _working_ha_url(cfg["url"])
        if working:
            return jsonify(
                ok=True,
                found=True,
                url=working,
                name="Home Assistant",
                source="saved",
                token_set=bool(cfg["token"]),
            )
    found = _discover_ha()
    if found:
        return jsonify(
            ok=True,
            found=True,
            url=found[0]["url"],
            name=found[0].get("name") or "Home Assistant",
            source="network",
            token_set=bool(cfg["token"]),
        )
    return jsonify(
        ok=True,
        found=False,
        error="No announcement reached this app. Docker often cannot hear Home Assistant on the network. Type the address.",
    )


@app.get("/api/ha/suggestions")
def ha_suggestions():
    cfg = _load_ha()
    url = cfg["url"]
    name = ""
    if not url:
        found = _discover_ha()
        if found:
            url = found[0]["url"]
            name = found[0].get("name") or ""
    public = {"url": url, "name": name, "token_set": bool(cfg["token"]), "esp": [], "other": []}
    if not url:
        return jsonify(ok=False, error="No Home Assistant found. Type its address.", **public), 404
    working = _working_ha_url(url)
    if not working:
        return jsonify(ok=False, error="That address did not answer as Home Assistant.", **public), 404
    url = working
    public["url"] = url
    if not cfg["token"]:
        return jsonify(
            ok=False,
            error="Paste a long-lived access token from the Home Assistant profile page.",
            **public,
        ), 401
    try:
        devices = _ha_device_registry(url, cfg["token"])
    except Exception as exc:
        text = _redact(str(exc), cfg["token"])
        if "timed out" in text.lower():
            text = "Home Assistant stopped answering."
        return jsonify(ok=False, error=text, **public), 502
    taken_ips = set()
    taken_names = set()
    others, _err = _load_devices()
    for item in others:
        taken_ips.add(item["ip"])
        taken_names.add(item["name"])
    with _lock:
        pings = list(_latest.get("pings") or [])
    for row in pings:
        if row.get("kind") == "cameras" and row.get("ip"):
            taken_ips.add(row["ip"])
    esp, other = _classify_ha_devices(devices, taken_ips, taken_names)
    public["esp"] = esp
    public["other"] = other
    return jsonify(ok=True, error=None, **public)


@app.post("/api/ha")
def save_ha():
    body = request.get_json(silent=True) or {}
    try:
        saved = _save_ha(body.get("url"), body.get("token"))
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400
    return jsonify(ok=True, **saved)


@app.post("/api/devices")
def change_device():
    body = request.get_json(silent=True) or {}
    try:
        items = _change_device(body)
    except ValueError as exc:
        return jsonify(ok=False, error=str(exc)), 400
    global _once
    with _lock:
        _once = True
    _wake.set()
    return jsonify(ok=True, devices=items)


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
    _ensure_devices_file()
    threading.Thread(target=_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, threaded=True)


if __name__ == "__main__":
    main()
