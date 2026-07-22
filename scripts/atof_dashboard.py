#!/usr/bin/env python3
"""Live dashboard that streams NeMo Relay ATOF agent-trace events out of a
running OpenShell sandbox.

The trace files live *inside* the sandbox (default:
``/sandbox/.nemo-relay/atof/*.jsonl``) and are appended to as the agent runs.
This server tails them over ``nemoclaw <sandbox> exec`` (no sshfs / sudo needed),
parses each JSON line and exposes them to a small single-page UI that polls for
new events and renders them as a live trace timeline.

Usage:
    python3 scripts/atof_dashboard.py                 # defaults
    python3 scripts/atof_dashboard.py --sandbox hermes-health --port 8090

Then open http://localhost:8090
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# ---------------------------------------------------------------------------
# Config (overridable via CLI flags)
# ---------------------------------------------------------------------------
SANDBOX = "hermes-health"
ATOF_DIR = "/sandbox/.nemo-relay/atof"
POLL_INTERVAL = 1.0          # seconds between sandbox polls
MAX_EVENTS = 5000            # ring-buffer cap kept in memory / served to UI
EXEC_TIMEOUT = 30            # seconds for a single nemoclaw exec call
HERMES_API_HOST = "127.0.0.1"  # host serving the forwarded Hermes agent API
HERMES_API_PORT = 8642       # Hermes OpenAI-compatible agent API (inside sandbox, forwarded)
HERMES_DASHBOARD_PORT = 18790  # native Hermes web dashboard (forwarded to host)
HERMES_MODEL = "hermes-agent"
GATEWAY_TOKEN = ""           # bearer token for the Hermes agent API (discovered at startup)
_TOKEN_LOCK = threading.Lock()


class TraceStore:
    """Thread-safe, monotonically-sequenced ring buffer of ATOF events."""

    def __init__(self, max_events: int = MAX_EVENTS):
        self._lock = threading.Lock()
        self._events: list[dict] = []
        self._seq = 0
        self._max = max_events
        self.status = "connecting"   # connecting | live | error
        self.error = ""
        self.files: dict[str, int] = {}   # path -> bytes consumed
        self._file_counts: dict[str, int] = {}   # basename -> events seen

    def add(self, event: dict) -> None:
        with self._lock:
            self._seq += 1
            event["_seq"] = self._seq
            self._events.append(event)
            name = event.get("_file", "")
            self._file_counts[name] = self._file_counts.get(name, 0) + 1
            if len(self._events) > self._max:
                self._events = self._events[-self._max :]

    def since(self, seq: int, file: str | None = None) -> tuple[list[dict], int]:
        with self._lock:
            out = [
                e for e in self._events
                if e["_seq"] > seq and (not file or e.get("_file") == file)
            ]
            return out, self._seq

    def _file_list(self) -> list[dict]:
        # Collect every known basename, then sort by the timestamp embedded in
        # the filename (nemo-relay-events-YYYY-MM-DD-HH.MM.SS.jsonl), newest first.
        names: set[str] = set()
        for path in self.files:
            base = path.rsplit("/", 1)[-1]
            if base:
                names.add(base)
        for base in self._file_counts:
            if base:
                names.add(base)

        def sort_key(name: str) -> tuple[int, str]:
            m = re.search(r"(\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2})", name)
            # Files with a parseable timestamp rank ahead of any that lack one;
            # the timestamp string sorts chronologically as-is.
            return (1, m.group(1)) if m else (0, name)

        ordered = sorted(names, key=sort_key, reverse=True)
        return [{"name": n, "count": self._file_counts.get(n, 0)} for n in ordered]

    def snapshot_meta(self) -> dict:
        with self._lock:
            return {
                "status": self.status,
                "error": self.error,
                "seq": self._seq,
                "count": len(self._events),
                "files": self._file_list(),
                "sandbox": SANDBOX,
                "dir": ATOF_DIR,
            }


STORE = TraceStore()


# ---------------------------------------------------------------------------
# Sandbox access helpers
# ---------------------------------------------------------------------------
def _exec(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["nemoclaw", SANDBOX, "exec", "--no-tty", "--", *args],
        capture_output=True,
        timeout=EXEC_TIMEOUT,
    )


def list_files() -> dict[str, int]:
    """Return {path: size_in_bytes} for every jsonl file in the atof dir."""
    proc = _exec(["sh", "-c", f'stat -c "%s %n" {ATOF_DIR}/*.jsonl 2>/dev/null'])
    sizes: dict[str, int] = {}
    for line in proc.stdout.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        size_str, _, path = line.partition(" ")
        try:
            sizes[path] = int(size_str)
        except ValueError:
            continue
    return sizes


def fetch_new_bytes(path: str, offset: int) -> bytes:
    """Read bytes of `path` starting at 1-based byte `offset+1`."""
    proc = _exec(["tail", "-c", f"+{offset + 1}", path])
    return proc.stdout


def discover_gateway_token() -> str:
    """Fetch the sandbox agent auth token used as the Hermes agent API bearer.

    An explicit HERMES_GATEWAY_TOKEN env var wins (handy in containers that
    don't ship the nemoclaw CLI); otherwise we ask nemoclaw.
    """
    env_tok = os.environ.get("HERMES_GATEWAY_TOKEN", "").strip()
    if env_tok:
        return env_tok
    try:
        proc = subprocess.run(
            ["nemoclaw", SANDBOX, "gateway-token", "--quiet"],
            capture_output=True,
            timeout=15,
        )
        for line in reversed(proc.stdout.decode("utf-8", "replace").splitlines()):
            line = line.strip()
            if line:
                return line
    except Exception:  # noqa: BLE001
        pass
    return ""


def get_gateway_token(refresh: bool = False) -> str:
    global GATEWAY_TOKEN
    with _TOKEN_LOCK:
        if refresh or not GATEWAY_TOKEN:
            GATEWAY_TOKEN = discover_gateway_token()
        return GATEWAY_TOKEN


def poll_loop() -> None:
    """Background worker: discover files, tail new bytes, parse JSON lines."""
    buffers: dict[str, bytes] = {}

    while True:
        try:
            sizes = list_files()
            STORE.files = {p: STORE.files.get(p, 0) for p in sizes}

            for path, size in sizes.items():
                consumed = STORE.files.get(path, 0)
                if size < consumed:  # file truncated/rotated -> restart
                    consumed = 0
                    buffers[path] = b""
                if size <= consumed:
                    continue

                chunk = fetch_new_bytes(path, consumed)
                if not chunk:
                    continue

                buf = buffers.get(path, b"") + chunk
                *lines, remainder = buf.split(b"\n")
                buffers[path] = remainder

                for raw in lines:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        event = json.loads(raw.decode("utf-8", "replace"))
                    except json.JSONDecodeError:
                        continue
                    event["_file"] = path.rsplit("/", 1)[-1]
                    STORE.add(event)

                STORE.files[path] = consumed + len(chunk)

            STORE.status = "live"
            STORE.error = ""
        except subprocess.TimeoutExpired:
            STORE.status = "error"
            STORE.error = "sandbox exec timed out"
        except Exception as exc:  # noqa: BLE001 - surface any failure to UI
            STORE.status = "error"
            STORE.error = f"{type(exc).__name__}: {exc}"

        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence per-request logging
        pass

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send(200, SHELL_HTML.encode(), "text/html; charset=utf-8")
        elif parsed.path == "/traces":
            self._send(200, INDEX_HTML.encode(), "text/html; charset=utf-8")
        elif parsed.path == "/chat":
            self._send(200, CHAT_HTML.encode(), "text/html; charset=utf-8")
        elif parsed.path == "/hermes":
            html = HERMES_HTML.replace("__HERMES_DASHBOARD_PORT__", str(HERMES_DASHBOARD_PORT))
            self._send(200, html.encode(), "text/html; charset=utf-8")
        elif parsed.path == "/api/events":
            qs = parse_qs(parsed.query)
            since = int(qs.get("since", ["0"])[0])
            file = qs.get("file", [""])[0] or None
            events, seq = STORE.since(since, file)
            payload = json.dumps({"events": events, "seq": seq, **STORE.snapshot_meta()})
            self._send(200, payload.encode(), "application/json")
        elif parsed.path == "/api/download":
            qs = parse_qs(parsed.query)
            self._download(qs.get("file", [""])[0])
        else:
            self._send(404, b"not found", "text/plain")

    def _download(self, name: str) -> None:
        """Stream a raw .jsonl trace file out of the sandbox as an attachment."""
        known = {f["name"] for f in STORE.snapshot_meta()["files"]}
        if not name or "/" in name or ".." in name or name not in known:
            self._send(404, b"unknown file", "text/plain")
            return
        try:
            proc = _exec(["cat", f"{ATOF_DIR}/{name}"])
            body = proc.stdout
        except subprocess.TimeoutExpired:
            self._send(504, b"sandbox read timed out", "text/plain")
            return
        except Exception as exc:  # noqa: BLE001
            self._send(500, f"read failed: {exc}".encode(), "text/plain")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Disposition", f'attachment; filename="{name}"')
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/chat":
            self._proxy_chat()
        else:
            self._send(404, b"not found", "text/plain")

    def _open_hermes(self, payload: bytes, token: str):
        req = urllib.request.Request(
            f"http://{HERMES_API_HOST}:{HERMES_API_PORT}/v1/chat/completions",
            data=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            method="POST",
        )
        return urllib.request.urlopen(req, timeout=900)

    def _proxy_chat(self) -> None:
        """Proxy a chat turn to the Hermes agent API, streaming SSE back to the UI.

        The browser sends {"messages": [...]}. We inject the agent model + the
        gateway bearer token (never exposed to the client) and stream the
        OpenAI-style SSE chunks straight through.
        """
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b"{}"
            data = json.loads(body or b"{}")
        except (ValueError, json.JSONDecodeError):
            self._send(400, b'{"error":"invalid JSON body"}', "application/json")
            return

        messages = data.get("messages", [])
        if not isinstance(messages, list) or not messages:
            self._send(400, b'{"error":"messages[] required"}', "application/json")
            return

        payload = json.dumps({
            "model": HERMES_MODEL,
            "messages": messages,
            "stream": True,
        }).encode()

        token = get_gateway_token()
        try:
            upstream = self._open_hermes(payload, token)
        except urllib.error.HTTPError as e:
            if e.code == 401:  # token may have rotated — refresh once and retry
                try:
                    upstream = self._open_hermes(payload, get_gateway_token(refresh=True))
                except Exception as exc:  # noqa: BLE001
                    self._send(502, f'{{"error":"hermes auth failed: {exc}"}}'.encode(), "application/json")
                    return
            else:
                detail = e.read().decode("utf-8", "replace")[:500]
                self._send(502, json.dumps({"error": f"hermes {e.code}", "detail": detail}).encode(),
                           "application/json")
                return
        except Exception as exc:  # noqa: BLE001 - connection refused, etc.
            self._send(502, json.dumps({
                "error": f"cannot reach Hermes agent API on port {HERMES_API_PORT}: {exc}",
            }).encode(), "application/json")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        try:
            while True:
                chunk = upstream.read(512)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # client navigated away / hit stop
        finally:
            upstream.close()


SHELL_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Hermes Health</title>
<style>
  :root {
    --bg: #0b0e14; --panel: #131722; --panel2: #1a2030; --border: #232a3b;
    --text: #d7dce5; --muted: #8b93a7; --accent: #7c5cff;
    --header-a: #111623; --header-b: #0b0e14;
  }
  body.light {
    --bg: #f5f7fa; --panel: #ffffff; --panel2: #eef1f6; --border: #d9dee8;
    --text: #1b2330; --muted: #5b647a; --accent: #6b46ff;
    --header-a: #ffffff; --header-b: #eef1f6;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body { font: 14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
         background: var(--bg); color: var(--text); display: flex; flex-direction: column; }
  .topbar { flex: none; background: linear-gradient(180deg,var(--header-a),var(--header-b));
            border-bottom: 1px solid var(--border); padding: 8px 16px; display: flex; align-items: center; gap: 14px; }
  .brand { font-size: 14px; font-weight: 700; letter-spacing: .3px; display: flex; align-items: center; gap: 8px; }
  .brand .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--accent); box-shadow: 0 0 8px var(--accent); }
  .tabs { display: flex; gap: 4px; }
  .tab { cursor: pointer; user-select: none; padding: 5px 14px; border-radius: 7px; font-size: 13px;
         border: 1px solid var(--border); background: var(--panel); color: var(--muted); }
  .tab:hover { color: var(--text); }
  .tab.active { background: var(--accent); color: #fff; border-color: transparent; }
  .spacer { flex: 1; }
  .chip { cursor: pointer; user-select: none; padding: 5px 10px; border-radius: 7px; font-size: 12px;
          border: 1px solid var(--border); background: var(--panel); color: var(--muted); }
  .chip:hover { color: var(--text); }
  .views { position: relative; flex: 1; min-height: 0; }
  .view { position: absolute; inset: 0; display: none; }
  .view.active { display: block; }
  iframe { border: 0; width: 100%; height: 100%; display: block; background: var(--bg); }
</style>
</head>
<body>
<div class="topbar">
  <div class="brand"><span class="dot"></span> Hermes Health</div>
  <nav class="tabs" id="tabs">
    <div class="tab" data-view="traces">traces</div>
    <div class="tab" data-view="chat">chat</div>
    <div class="tab" data-view="hermes">hermes</div>
  </nav>
  <div class="spacer"></div>
  <button class="chip" id="themeToggle" title="toggle light / dark">🌙 dark</button>
</div>
<div class="views" id="views">
  <div class="view" data-view="traces"><iframe data-src="/traces?embed=1" title="Traces"></iframe></div>
  <div class="view" data-view="chat"><iframe data-src="/chat?embed=1" title="Chat"
        allow="clipboard-read; clipboard-write; microphone"></iframe></div>
  <div class="view" data-view="hermes"><iframe data-src="/hermes?embed=1" title="Hermes Dashboard"
        allow="clipboard-read; clipboard-write; microphone"></iframe></div>
</div>
<script>
const TABS = ['traces', 'chat', 'hermes'];

// Keep-alive routing: every view stays mounted once loaded, we only toggle
// visibility, so each iframe preserves its own state (chat history/stream,
// trace scroll & filters, the native dashboard session).
function activate(name) {
  if (!TABS.includes(name)) name = 'traces';
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.view === name));
  document.querySelectorAll('.view').forEach(v => {
    const on = v.dataset.view === name;
    v.classList.toggle('active', on);
    if (on) {
      const f = v.querySelector('iframe');
      if (!f.src && f.dataset.src) f.src = f.dataset.src;   // lazy-load on first visit
    }
  });
  if (location.hash.slice(1) !== name) history.replaceState(null, '', '#' + name);
}

document.querySelectorAll('.tab').forEach(t =>
  t.addEventListener('click', () => activate(t.dataset.view)));
window.addEventListener('hashchange', () => activate(location.hash.slice(1)));

// Theme lives here and is mirrored into the iframes via the localStorage
// 'storage' event (they share this origin), plus applied on their own load.
const themeBtn = document.getElementById('themeToggle');
function applyTheme(t) {
  document.body.classList.toggle('light', t === 'light');
  themeBtn.textContent = t === 'light' ? '☀ light' : '🌙 dark';
}
let theme = 'dark';
try { theme = localStorage.getItem('atof-theme') || 'dark'; } catch (e) {}
applyTheme(theme);
themeBtn.addEventListener('click', () => {
  theme = document.body.classList.contains('light') ? 'dark' : 'light';
  try { localStorage.setItem('atof-theme', theme); } catch (e) {}
  applyTheme(theme);
});
window.addEventListener('storage', e => { if (e.key === 'atof-theme') applyTheme(e.newValue || 'dark'); });

activate(location.hash.slice(1) || 'traces');
</script>
</body>
</html>
"""


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>ATOF Trace Dashboard</title>
<script>if(new URLSearchParams(location.search).has('embed'))document.documentElement.classList.add('embed');</script>
<style>html.embed header .nav,html.embed header #themeToggle{display:none!important}</style>
<style>
  :root {
    --bg: #0b0e14; --panel: #131722; --panel2: #1a2030; --panel3: #0c1017; --border: #232a3b;
    --text: #d7dce5; --muted: #8b93a7; --accent: #7c5cff;
    --agent: #f0a020; --llm: #4aa8ff; --tool: #3ecf8e; --mark: #b06cff; --err: #ff5c72;
    --jkey: #7aa2f7; --jstr: #9ece6a; --jnum: #e0af68; --jbool: #bb9af7;
    --code-bg: #05070b; --code-fg: #c3ccda; --code-accent: #d7f5e6;
    --hover: #202840; --header-a: #111623; --header-b: #0b0e14; --strong: #cbbcff;
  }
  body.light {
    --bg: #f5f7fa; --panel: #ffffff; --panel2: #eef1f6; --panel3: #f4f6f9; --border: #d9dee8;
    --text: #1b2330; --muted: #5b647a; --accent: #6b46ff;
    --agent: #b7791f; --llm: #1f6feb; --tool: #1a9d6b; --mark: #7c3aed; --err: #d92d3f;
    --jkey: #1f6feb; --jstr: #2f8a3e; --jnum: #b7791f; --jbool: #7c3aed;
    --code-bg: #eef1f6; --code-fg: #3a4353; --code-accent: #10683f;
    --hover: #e2e7f0; --header-a: #ffffff; --header-b: #eef1f6; --strong: #5b3ecc;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
         background: var(--bg); color: var(--text); }
  header { position: sticky; top: 0; z-index: 10; background: linear-gradient(180deg,var(--header-a),var(--header-b));
           border-bottom: 1px solid var(--border); padding: 12px 18px; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
  h1 { font-size: 15px; margin: 0; letter-spacing: .3px; }
  h1 .sub { color: var(--muted); font-weight: 400; font-size: 12px; margin-left: 8px; }
  .spacer { flex: 1; }
  .pill { display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px; border-radius: 999px;
          background: var(--panel2); border: 1px solid var(--border); font-size: 12px; color: var(--muted); }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--muted); }
  .dot.live { background: var(--tool); box-shadow: 0 0 0 0 rgba(62,207,142,.6); animation: pulse 1.6s infinite; }
  .dot.error { background: var(--err); }
  @keyframes pulse { 0%{box-shadow:0 0 0 0 rgba(62,207,142,.5)} 70%{box-shadow:0 0 0 6px rgba(62,207,142,0)} 100%{box-shadow:0 0 0 0 rgba(62,207,142,0)} }
  .filters { display: flex; gap: 6px; }
  .chip { cursor: pointer; user-select: none; padding: 4px 10px; border-radius: 6px; font-size: 12px;
          border: 1px solid var(--border); background: var(--panel); color: var(--muted); }
  .chip.on { color: #fff; border-color: transparent; }
  a.chip { text-decoration: none; display: inline-flex; align-items: center; }
  .nav { display: flex; gap: 4px; }
  .navlink.active { background: var(--accent); color: #fff; border-color: transparent; }
  .chip[data-cat=agent].on { background: var(--agent); color:#221600; }
  .chip[data-cat=llm].on { background: var(--llm); color:#001427; }
  .chip[data-cat=tool].on { background: var(--tool); color:#00291a; }
  .chip[data-cat=mark].on { background: var(--mark); color:#1a0033; }
  label.toggle { font-size: 12px; color: var(--muted); display: inline-flex; align-items: center; gap: 6px; cursor: pointer; }
  .layout { display: flex; align-items: stretch; }
  .sidebar { width: 260px; flex: none; border-right: 1px solid var(--border); background: var(--panel);
             height: calc(100vh - 53px); position: sticky; top: 53px; overflow-y: auto; }
  .sidebar .sb-head { font-size: 11px; text-transform: uppercase; letter-spacing: .5px; color: var(--muted);
                      padding: 12px 14px 6px; }
  .filelist { display: flex; flex-direction: column; padding: 0 8px 12px; gap: 2px; }
  .file { display: flex; align-items: center; gap: 8px; padding: 8px 10px; border-radius: 6px; cursor: pointer;
          color: var(--text); border: 1px solid transparent; }
  .file:hover { background: var(--panel2); }
  .file.sel { background: var(--panel2); border-color: var(--accent); }
  .file .fname { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
                 font-size: 12.5px; font-family: "SF Mono",ui-monospace,Menlo,Consolas,monospace; }
  .file .fcount { font-size: 11px; color: var(--muted); background: var(--bg); border: 1px solid var(--border);
                  border-radius: 999px; padding: 1px 8px; font-variant-numeric: tabular-nums; flex: none; }
  .file .fdl { flex: none; display: inline-flex; align-items: center; color: var(--muted); text-decoration: none;
               padding: 3px 4px; border-radius: 5px; opacity: .55; }
  .file .fdl:hover { color: var(--accent); opacity: 1; background: var(--bg); }
  .file .fdl svg { display: block; }
  .file.all .fname { font-family: inherit; }
  .sidebar .empty-files { color: var(--muted); font-size: 12px; padding: 6px 14px; }
  main { flex: 1; min-width: 0; max-width: 1100px; margin: 0 auto; padding: 16px 18px 80px; }

  .row { border: 1px solid var(--border); border-left: 3px solid var(--muted); background: var(--panel);
         border-radius: 8px; margin-bottom: 8px; overflow: hidden; }
  .row.agent { border-left-color: var(--agent); }
  .row.llm { border-left-color: var(--llm); }
  .row.tool { border-left-color: var(--tool); }
  .row.mark { border-left-color: var(--mark); }
  .row.err { border-left-color: var(--err); }
  .head { display: flex; align-items: center; gap: 10px; padding: 9px 12px; cursor: pointer; }
  .head:hover { background: var(--panel2); }
  .tag { font-size: 10px; text-transform: uppercase; letter-spacing: .5px; font-weight: 700; padding: 2px 7px; border-radius: 4px; flex: none; }
  .tag.agent { background: rgba(240,160,32,.15); color: var(--agent); }
  .tag.llm { background: rgba(74,168,255,.15); color: var(--llm); }
  .tag.tool { background: rgba(62,207,142,.15); color: var(--tool); }
  .tag.mark { background: rgba(176,108,255,.15); color: var(--mark); }
  .name { font-weight: 600; flex: none; }
  .summary { color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; min-width: 0; }
  .meta { color: var(--muted); font-size: 12px; white-space: nowrap; display: inline-flex; align-items: center; gap: 6px; }
  .meta .pending { color: var(--llm); animation: pulse2 1.2s infinite; }
  @keyframes pulse2 { 0%,100%{opacity:.3} 50%{opacity:1} }
  .body { display: none; border-top: 1px solid var(--border); background: var(--panel3); padding: 6px 0 10px; }
  .row.open .body { display: block; }
  .caret { color: var(--muted); transition: transform .15s; flex: none; }
  .row.open .caret { transform: rotate(90deg); }
  .empty { text-align: center; color: var(--muted); padding: 60px 0; }

  /* section + generic blocks */
  .section-label { color: var(--muted); font-size: 10.5px; text-transform: uppercase; letter-spacing:.6px;
                   padding: 12px 14px 4px; font-weight: 600; }
  .muted-note { color: var(--muted); padding: 6px 14px; font-style: italic; }
  .kv { display: grid; grid-template-columns: 130px 1fr; gap: 4px 14px; padding: 6px 14px; align-items: start; }
  .kv .k { color: var(--muted); }
  .kv .v.mono { font-family: "SF Mono",ui-monospace,Menlo,Consolas,monospace; font-size: 12px; word-break: break-all; }

  /* badges */
  .badges { display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 14px 2px; }
  .badge { font-size: 11px; padding: 2px 9px; border-radius: 999px; background: var(--panel2);
           border: 1px solid var(--border); color: var(--muted); font-variant-numeric: tabular-nums; }
  .badge b { color: var(--text); font-weight: 600; }
  .badge.strong { background: rgba(124,92,255,.14); border-color: rgba(124,92,255,.4); color: var(--strong); }
  .badge.ok { color: var(--tool); border-color: rgba(62,207,142,.4); }
  .badge.warn { color: var(--err); border-color: rgba(255,92,114,.4); }

  /* command + text output */
  .cmd { margin: 4px 14px; padding: 10px 12px 10px 30px; background: var(--code-bg); border: 1px solid var(--border);
         border-radius: 7px; position: relative; font: 12.5px/1.55 "SF Mono",ui-monospace,Menlo,Consolas,monospace;
         color: var(--code-accent); white-space: pre-wrap; word-break: break-word; }
  .cmd::before { content: "$"; position: absolute; left: 12px; top: 10px; color: var(--tool); font-weight: 700; }
  .out-text { margin: 4px 14px; padding: 10px 12px; background: var(--code-bg); border: 1px solid var(--border);
              border-radius: 7px; max-height: 460px; overflow: auto; white-space: pre-wrap; word-break: break-word;
              font: 12px/1.55 "SF Mono",ui-monospace,Menlo,Consolas,monospace; color: var(--code-fg); }

  /* chat bubbles + markdown */
  .bubble { margin: 6px 14px; padding: 10px 13px; border-radius: 10px; max-width: 92%; }
  .bubble .role { font-size: 10px; text-transform: uppercase; letter-spacing: .5px; color: var(--muted); margin-bottom: 4px; }
  .bubble.user { background: rgba(124,92,255,.10); border: 1px solid rgba(124,92,255,.3); }
  .bubble.assistant { background: var(--panel2); border: 1px solid var(--border); }
  .md { white-space: normal; word-break: break-word; }
  .md p { margin: 6px 0; }
  .md ul, .md ol { margin: 6px 0; padding-left: 22px; }
  .md li { margin: 2px 0; }
  .md code { background: var(--code-bg); border: 1px solid var(--border); border-radius: 4px; padding: 1px 5px;
             font: 12px/1.4 "SF Mono",ui-monospace,Menlo,Consolas,monospace; color: var(--jnum); }
  .md-code { background: var(--code-bg); border: 1px solid var(--border); border-radius: 7px; padding: 10px 12px; margin: 6px 0;
             overflow: auto; white-space: pre-wrap; word-break: break-word; color: var(--code-fg);
             font: 12px/1.5 "SF Mono",ui-monospace,Menlo,Consolas,monospace; }
  .md strong { color: var(--text); font-weight: 700; }

  /* reasoning */
  .reasoning { color: var(--muted); font-style: italic; white-space: pre-wrap; word-break: break-word; line-height: 1.6; }

  /* tool calls */
  .tcall { margin: 6px 14px; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; background: var(--panel2); }
  .tcall-head { display: flex; align-items: center; gap: 8px; padding: 7px 10px; background: rgba(62,207,142,.08);
                border-bottom: 1px solid var(--border); }
  .tcall-head .arrow { color: var(--tool); font-weight: 700; }
  .tcall-head .name { font-family: "SF Mono",ui-monospace,Menlo,Consolas,monospace; font-size: 12.5px; color: var(--code-accent); }
  .tcall .json { padding: 8px 12px; }

  /* clarify */
  .choices { list-style: none; margin: 4px 14px; padding: 0; }
  .choices li { padding: 7px 12px; border: 1px solid var(--border); border-radius: 7px; margin-bottom: 5px;
                background: var(--panel2); color: var(--text); }
  .choices li.chosen { border-color: var(--tool); background: rgba(62,207,142,.12); color: var(--code-accent); }
  .choices li.chosen::after { content: " ✓ chosen"; color: var(--tool); font-size: 11px; }
  .answer { margin: 4px 14px; padding: 9px 12px; border-radius: 8px; background: rgba(62,207,142,.12);
            border: 1px solid rgba(62,207,142,.35); color: var(--code-accent); }

  /* collapsible */
  .coll { margin: 4px 14px; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
  .coll-head { display: flex; align-items: center; gap: 8px; padding: 7px 10px; cursor: pointer;
               color: var(--text); background: var(--panel2); font-size: 12.5px; user-select: none; }
  .coll-head:hover { background: var(--hover); }
  .coll-head .jcaret { transition: transform .12s; }
  .coll.open > .coll-head .jcaret { transform: rotate(90deg); }
  .coll-body { display: none; padding: 6px 4px; }
  .coll.open > .coll-body { display: block; }
  .req-meta { display: flex; flex-wrap: wrap; gap: 6px; padding: 6px 10px; }
  pre.raw { margin: 6px 10px; padding: 10px 12px; background: var(--code-bg); border: 1px solid var(--border); border-radius: 7px;
            max-height: 420px; overflow: auto; white-space: pre-wrap; word-break: break-word;
            font: 11.5px/1.5 "SF Mono",ui-monospace,Menlo,Consolas,monospace; color: var(--muted); }

  /* JSON tree */
  .json { font: 12px/1.6 "SF Mono",ui-monospace,Menlo,Consolas,monospace; color: var(--code-fg); padding: 4px 14px; }
  .tcall .json, .coll .json { padding-left: 4px; padding-right: 4px; }
  .jkids { padding-left: 15px; border-left: 1px solid var(--border); margin-left: 3px; }
  .jnode.collapsed > .jkids, .jnode.collapsed > .jtail { display: none; }
  .jnode:not(.collapsed) > .jhead .jprev { display: none; }
  .jhead { display: block; }
  .jcaret { display: inline-block; width: 11px; color: var(--muted); transition: transform .12s; }
  .jnode:not(.collapsed) > .jhead .jcaret { transform: rotate(90deg); }
  .jline { padding-left: 11px; }
  .jkey { color: var(--jkey); }
  .jstr { color: var(--jstr); white-space: pre-wrap; word-break: break-word; }
  .jstr.clip { cursor: pointer; }
  .jstr.clip:hover { text-decoration: underline dotted; }
  .jnum { color: var(--jnum); }
  .jbool { color: var(--jbool); }
  .jnull { color: var(--muted); }
  .jpunc, .jprev { color: var(--muted); }
</style>
</head>
<body>
<header>
  <h1>ATOF Trace <span class="sub" id="src"></span></h1>
  <nav class="nav">
    <a class="chip navlink active" href="/">traces</a>
    <a class="chip navlink" href="/chat">chat</a>
    <a class="chip navlink" href="/hermes">hermes</a>
  </nav>
  <span class="pill"><span class="dot" id="statusDot"></span><span id="statusText">connecting</span></span>
  <span class="pill" id="countPill">0 events</span>
  <div class="spacer"></div>
  <div class="filters" id="filters">
    <span class="chip on" data-cat="agent">agent</span>
    <span class="chip on" data-cat="llm">llm</span>
    <span class="chip on" data-cat="tool">tool</span>
    <span class="chip on" data-cat="mark">mark</span>
  </div>
  <button class="chip" id="expandAll">expand all</button>
  <button class="chip" id="collapseAll">collapse all</button>
  <button class="chip" id="themeToggle" title="toggle light / dark">🌙 dark</button>
  <label class="toggle"><input type="checkbox" id="follow" checked /> auto-scroll</label>
</header>
<div class="layout">
  <aside class="sidebar">
    <div class="sb-head">Trace files</div>
    <div class="filelist" id="filelist">
      <div class="empty-files">scanning sandbox…</div>
    </div>
  </aside>
  <main>
    <div id="stream"></div>
    <div class="empty" id="empty">Waiting for trace events from the sandbox…</div>
  </main>
</div>
<script>
const stream = document.getElementById('stream');
const empty = document.getElementById('empty');
const statusDot = document.getElementById('statusDot');
const statusText = document.getElementById('statusText');
const countPill = document.getElementById('countPill');
const srcEl = document.getElementById('src');
const followBox = document.getElementById('follow');
const filelistEl = document.getElementById('filelist');

let sinceSeq = 0;
let selectedFile = null;   // null = all files
let knownFiles = "";       // signature to avoid needless re-render
const active = { agent: true, llm: true, tool: true, mark: true };
const openScopes = {};     // uuid -> { el, start }
let gen = 0;

/* ---------------------------------------------------------------- utils */
function esc(s) {
  return String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
}
function fmt(n) { return (typeof n === 'number') ? n.toLocaleString() : (n ?? '—'); }
function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}
function catOf(ev) { return ev.kind === 'mark' ? 'mark' : (ev.category || 'mark'); }
function statusBad(ev) {
  return !!(ev && ev.metadata && ev.metadata['otel.status_code'] &&
            ev.metadata['otel.status_code'] !== 'OK');
}
function tsMillis(ts) {
  if (!ts) return null;
  const m = ts.match(/^(.*\.\d{3})\d*(.*)$/);
  const t = Date.parse(m ? m[1] + m[2] : ts);
  return isNaN(t) ? null : t;
}
function durationMs(s, e) {
  if (!e) return null;
  const a = tsMillis(s && s.timestamp), b = tsMillis(e.timestamp);
  return (a != null && b != null) ? (b - a) : null;
}
function fmtDur(ms) {
  if (ms == null) return null;
  if (ms < 1000) return ms + ' ms';
  return (ms / 1000).toFixed(ms < 10000 ? 2 : 1) + ' s';
}

// Recursively parse JSON embedded in strings so nested payloads render as trees.
function deepParse(v, depth) {
  depth = depth || 0;
  if (depth > 8) return v;
  if (typeof v === 'string') {
    const t = v.trim();
    if (t.length > 1 && (t[0] === '{' || t[0] === '[')) {
      try { return deepParse(JSON.parse(t), depth + 1); } catch (e) { return v; }
    }
    return v;
  }
  if (Array.isArray(v)) return v.map(x => deepParse(x, depth + 1));
  if (v && typeof v === 'object') {
    const o = {};
    for (const k in v) o[k] = deepParse(v[k], depth + 1);
    return o;
  }
  return v;
}

/* ---------------------------------------------- interactive JSON tree */
function jsonTree(value, autoDepth) {
  if (autoDepth == null) autoDepth = 2;
  const wrap = el('div', 'json');
  wrap.appendChild(buildNode(value, null, 0, autoDepth));
  return wrap;
}
function primNode(value) {
  const span = document.createElement('span');
  if (value === null) { span.className = 'jnull'; span.textContent = 'null'; return span; }
  const t = typeof value;
  if (t === 'number') { span.className = 'jnum'; span.textContent = String(value); return span; }
  if (t === 'boolean') { span.className = 'jbool'; span.textContent = String(value); return span; }
  span.className = 'jstr';
  const s = String(value);
  if (s.length > 500) {
    span.classList.add('clip');
    span.dataset.full = s;
    span.title = 'click to expand / collapse';
    span.textContent = '"' + s.slice(0, 500) + '… "';
    span.addEventListener('click', ev => {
      ev.stopPropagation();
      if (span.classList.contains('clip')) {
        span.textContent = '"' + span.dataset.full + '"'; span.classList.remove('clip');
      } else {
        span.textContent = '"' + span.dataset.full.slice(0, 500) + '… "'; span.classList.add('clip');
      }
    });
  } else {
    span.textContent = '"' + s + '"';
  }
  return span;
}
function buildNode(value, key, depth, autoDepth) {
  const isObj = value && typeof value === 'object';
  if (!isObj) {
    const line = el('div', 'jline');
    if (key !== null) {
      line.appendChild(el('span', 'jkey', key));
      line.appendChild(document.createTextNode(': '));
    }
    line.appendChild(primNode(value));
    return line;
  }
  const isArr = Array.isArray(value);
  const entries = isArr ? value.map((v, i) => [i, v]) : Object.entries(value);
  const open = isArr ? '[' : '{', close = isArr ? ']' : '}';
  const node = el('div', 'jnode');
  if (depth >= autoDepth && entries.length) node.classList.add('collapsed');

  const head = el('div', 'jhead');
  head.appendChild(el('span', 'jcaret', '▸'));
  if (key !== null) {
    head.appendChild(el('span', 'jkey', key));
    head.appendChild(document.createTextNode(': '));
  }
  head.appendChild(el('span', 'jpunc', open));
  head.appendChild(el('span', 'jprev', (entries.length ? ' … ' + entries.length + ' ' : '') + close));
  node.appendChild(head);

  const kids = el('div', 'jkids');
  entries.forEach(([k, v]) => kids.appendChild(buildNode(v, isArr ? null : k, depth + 1, autoDepth)));
  node.appendChild(kids);
  node.appendChild(el('div', 'jtail jpunc', close));

  if (entries.length) {
    head.style.cursor = 'pointer';
    head.addEventListener('click', () => node.classList.toggle('collapsed'));
  } else {
    head.querySelector('.jcaret').style.visibility = 'hidden';
    head.querySelector('.jprev').textContent = close;
  }
  return node;
}

/* ------------------------------------------------------ markdown-lite */
function mdLite(text) {
  const lines = esc(text).split(/\r?\n/);
  let html = '', inUl = false, inOl = false, inCode = false, code = '';
  const closeLists = () => { if (inUl) { html += '</ul>'; inUl = false; } if (inOl) { html += '</ol>'; inOl = false; } };
  const inline = s => s.replace(/`([^`]+)`/g, '<code>$1</code>').replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  for (const raw of lines) {
    if (/^\s*```/.test(raw)) {
      if (inCode) { html += '<pre class="md-code">' + code.replace(/\n$/, '') + '</pre>'; inCode = false; code = ''; }
      else { closeLists(); inCode = true; code = ''; }
      continue;
    }
    if (inCode) { code += raw + '\n'; continue; }
    const ol = raw.match(/^\s*\d+\.\s+(.*)/);
    const ul = raw.match(/^\s*[-*]\s+(.*)/);
    if (ol) { if (!inOl) { closeLists(); html += '<ol>'; inOl = true; } html += '<li>' + inline(ol[1]) + '</li>'; continue; }
    if (ul) { if (!inUl) { closeLists(); html += '<ul>'; inUl = true; } html += '<li>' + inline(ul[1]) + '</li>'; continue; }
    closeLists();
    if (raw.trim() !== '') html += '<p>' + inline(raw) + '</p>';
  }
  if (inCode) html += '<pre class="md-code">' + code.replace(/\n$/, '') + '</pre>';
  closeLists();
  return html;
}

/* ---------------------------------------------------- block builders */
function section(label) {
  const w = el('div');
  w.appendChild(el('div', 'section-label', label));
  return w;
}
function collapsibleEl(title, open) {
  const box = el('div', 'coll' + (open ? ' open' : ''));
  const head = el('div', 'coll-head');
  head.appendChild(el('span', 'jcaret', '▸'));
  head.appendChild(el('span', null, title));
  const body = el('div', 'coll-body');
  head.addEventListener('click', () => box.classList.toggle('open'));
  box.appendChild(head); box.appendChild(body);
  return { el: box, body };
}
function badge(label, value, cls) {
  const b = el('span', 'badge' + (cls ? ' ' + cls : ''));
  b.innerHTML = esc(label) + ' <b>' + esc(fmt(value)) + '</b>';
  return b;
}
function kvGrid(pairs) {
  const g = el('div', 'kv');
  pairs.forEach(([k, v]) => {
    if (v == null || v === '') return;
    g.appendChild(el('div', 'k', k));
    g.appendChild(el('div', 'v mono', v));
  });
  return g;
}
function mdBlock(text) { const d = el('div', 'md'); d.innerHTML = mdLite(text || ''); return d; }
function renderValue(v, asMd) {
  if (typeof v === 'string') {
    if (asMd) return mdBlock(v);
    const d = el('div', 'out-text'); d.textContent = v; return d;
  }
  return jsonTree(v, 1);
}

/* ------------------------------------------------- per-type renderers */
function renderLLM(body, s, e) {
  const resp = e && e.data ? e.data : null;
  if (resp) {
    const am = resp.assistant_message || {};
    if (resp.usage) {
      const u = resp.usage, badges = el('div', 'badges');
      badges.appendChild(badge('prompt', u.prompt_tokens));
      badges.appendChild(badge('completion', u.completion_tokens));
      const rt = u.completion_tokens_details && u.completion_tokens_details.reasoning_tokens;
      if (rt) badges.appendChild(badge('reasoning', rt));
      badges.appendChild(badge('total', u.total_tokens, 'strong'));
      if (resp.finish_reason)
        badges.appendChild(badge('finish', resp.finish_reason, resp.finish_reason === 'stop' ? 'ok' : ''));
      body.appendChild(badges);
    }
    if (am.reasoning_content && am.reasoning_content.trim()) {
      const c = collapsibleEl('Reasoning', false);
      const r = el('div', 'reasoning'); r.textContent = am.reasoning_content;
      c.body.appendChild(r); body.appendChild(c.el);
    }
    if (am.content && am.content.trim()) {
      const sec = section('Response');
      sec.appendChild(mdBlock(am.content));
      body.appendChild(sec);
    }
    if (am.tool_calls && am.tool_calls.length) {
      const sec = section('Tool calls (' + am.tool_calls.length + ')');
      am.tool_calls.forEach(tc => {
        const fn = tc.function || {};
        let args = fn.arguments;
        try { args = JSON.parse(args); } catch (err) { /* leave as string */ }
        const card = el('div', 'tcall');
        const hd = el('div', 'tcall-head');
        hd.appendChild(el('span', 'arrow', '→'));
        hd.appendChild(el('span', 'name', fn.name || '(unknown)'));
        card.appendChild(hd);
        card.appendChild(jsonTree(args, 2));
        sec.appendChild(card);
      });
      body.appendChild(sec);
    }
  }
  const req = s && s.data && s.data.content ? s.data.content : null;
  if (req) {
    const c = collapsibleEl('Request  ·  ' + (req.model || ''), false);
    const meta = el('div', 'req-meta');
    meta.appendChild(badge('max_tokens', req.max_tokens));
    meta.appendChild(badge('messages', (req.messages || []).length));
    meta.appendChild(badge('tools', (req.tools || []).length));
    c.body.appendChild(meta);
    if (req.tools && req.tools.length) {
      const names = req.tools.map(t => (t.function && t.function.name) || t.name).filter(Boolean);
      c.body.appendChild(kvGrid([['available tools', names.join(', ')]]));
    }
    if (req.messages && req.messages.length) {
      const mc = collapsibleEl('messages (' + req.messages.length + ')', false);
      mc.body.appendChild(jsonTree(req.messages, 1));
      c.body.appendChild(mc.el);
    }
    body.appendChild(c.el);
  }
}

function renderTerminal(body, s, e) {
  const cmd = s && s.data && s.data.command;
  if (cmd) {
    const sec = section('Command');
    const c = el('div', 'cmd'); c.textContent = cmd;
    sec.appendChild(c); body.appendChild(sec);
  }
  if (e && e.data != null) {
    let out = deepParse(e.data), label = 'Output';
    if (out && typeof out === 'object' && !Array.isArray(out) &&
        'output' in out && Object.keys(out).length <= 2) {
      out = deepParse(out.output);
    }
    const sec = section(label);
    sec.appendChild(renderValue(out));
    body.appendChild(sec);
  }
}

function renderClarify(body, s, e) {
  const q = s && s.data && s.data.question;
  const choices = (s && s.data && s.data.choices) || [];
  const end = e ? deepParse(e.data) : null;
  const chosen = end && typeof end === 'object' ? end.user_response : null;
  if (q) { const sec = section('Question'); sec.appendChild(mdBlock(q)); body.appendChild(sec); }
  if (choices.length) {
    const sec = section('Choices');
    const ul = el('ul', 'choices');
    choices.forEach(ch => {
      const li = el('li', chosen && ch === chosen ? 'chosen' : null, ch);
      ul.appendChild(li);
    });
    sec.appendChild(ul); body.appendChild(sec);
  }
  if (chosen) { const sec = section('User response'); sec.appendChild(el('div', 'answer', chosen)); body.appendChild(sec); }
}

function renderSkill(body, s, e) {
  const name = s && s.data && (typeof s.data === 'object' ? s.data.name : null);
  if (name) body.appendChild(kvGrid([['skill', name]]));
  if (e && e.data != null) {
    const sec = section('Skill content');
    sec.appendChild(renderValue(deepParse(e.data), true));
    body.appendChild(sec);
  }
}

function renderMark(body, ev) {
  const d = ev.data || {};
  if (ev.name === 'hermes.turn.start') {
    const b = el('div', 'bubble user');
    b.appendChild(el('div', 'role', 'user'));
    b.appendChild(mdBlock(d.user_message || '(empty)'));
    body.appendChild(b);
    if (d.model || d.platform) body.appendChild(kvGrid([['model', d.model], ['platform', d.platform]]));
  } else if (ev.name === 'hermes.turn.end') {
    const b = el('div', 'bubble assistant');
    b.appendChild(el('div', 'role', 'assistant'));
    b.appendChild(mdBlock(d.assistant_response || '(empty)'));
    body.appendChild(b);
  } else if (ev.name === 'hermes.session.end') {
    body.appendChild(kvGrid([
      ['completed', String(d.completed)],
      ['interrupted', String(d.interrupted)],
    ]));
  } else {
    body.appendChild(renderValue(deepParse(d), false));
  }
}

function dispatch(body, s, e) {
  const cat = s.category, name = s.name;
  if (cat === 'llm') return renderLLM(body, s, e);
  if (cat === 'tool' && name === 'terminal') return renderTerminal(body, s, e);
  if (cat === 'tool' && name === 'clarify') return renderClarify(body, s, e);
  if (cat === 'tool' && name === 'skill_view') return renderSkill(body, s, e);
  if (s.kind === 'mark') return renderMark(body, s);
  // agent scope / unknown fallback
  let any = false;
  if (s.data != null) { const w = section('data'); w.appendChild(renderValue(deepParse(s.data), false)); body.appendChild(w); any = true; }
  if (e && e.data != null) { const w = section('result'); w.appendChild(renderValue(deepParse(e.data), false)); body.appendChild(w); any = true; }
  if (!any) body.appendChild(el('div', 'muted-note', '(no data)'));
}

function metaDetails(s, e) {
  const c = collapsibleEl('trace details', false);
  const m = s.metadata || {};
  const st = (e && e.metadata && e.metadata['otel.status_code']) || m['otel.status_code'];
  c.body.appendChild(kvGrid([
    ['uuid', s.uuid],
    ['parent', s.parent_uuid],
    ['session', m.session_id],
    ['turn', m.turn_id],
    ['tool_call_id', m.tool_call_id],
    ['status', st],
    ['file', s._file],
  ]));
  const raw = collapsibleEl('raw JSON', false);
  const pre = el('pre', 'raw');
  pre.textContent = JSON.stringify(e ? { start: s, end: e } : s, null, 2);
  raw.body.appendChild(pre);
  c.body.appendChild(raw.el);
  return c.el;
}

/* -------------------------------------------------------- summaries */
function displayName(s) {
  if (s.kind === 'mark') return (s.name || '').replace('hermes.', '');
  if (s.category === 'llm') return 'LLM';
  if (s.category === 'agent') return 'session';
  return s.name || '';
}
function clip(str, n) { str = String(str || '').replace(/\s+/g, ' ').trim(); return str.length > n ? str.slice(0, n) + '…' : str; }
function summarize(s, e) {
  const d = s.data;
  if (s.name === 'hermes.turn.start') return '▶ ' + clip(d && d.user_message, 160);
  if (s.name === 'hermes.turn.end') return '■ ' + clip(d && d.assistant_response, 160);
  if (s.name === 'hermes.session.end') return 'session ended';
  if (s.category === 'agent') return clip(s.name, 60);
  if (s.category === 'tool' && s.name === 'terminal') return clip(d && d.command, 160);
  if (s.category === 'tool' && s.name === 'clarify') return clip(d && d.question, 160);
  if (s.category === 'tool' && s.name === 'skill_view') return 'view skill: ' + ((d && d.name) || '');
  if (s.category === 'llm') {
    const am = e && e.data && e.data.assistant_message;
    if (am && am.tool_calls && am.tool_calls.length)
      return '→ ' + am.tool_calls.map(t => (t.function && t.function.name) || '?').join(', ');
    if (am && am.content && am.content.trim()) return clip(am.content, 160);
    if (am && am.reasoning_content) return clip(am.reasoning_content, 160);
    return e ? 'llm response' : 'llm request…';
  }
  return s.name || '';
}

/* --------------------------------------------------------- rendering */
function fillCard(row, s, e) {
  const cat = catOf(s);
  const wasOpen = row.classList.contains('open');
  row.className = 'row ' + cat + ' cat-' + cat + (wasOpen ? ' open' : '');
  if (statusBad(s) || statusBad(e)) row.classList.add('err');

  const model = (s.category_profile && s.category_profile.model_name) || (e && e.data && e.data.model) || '';
  const dur = fmtDur(durationMs(s, e));
  let tok = '';
  if (cat === 'llm' && e && e.data && e.data.usage) tok = fmt(e.data.usage.total_tokens) + ' tok';
  const time = s.timestamp ? s.timestamp.slice(11, 23) : '';
  const isScope = s.kind === 'scope';
  const pending = isScope && !e;

  const head = row.querySelector('.head');
  head.innerHTML =
    '<span class="caret">▸</span>' +
    '<span class="tag ' + cat + '">' + cat + '</span>' +
    '<span class="name">' + esc(displayName(s)) + '</span>' +
    '<span class="summary">' + esc(summarize(s, e)) + '</span>' +
    '<span class="meta">' +
      (model ? esc(model) + ' · ' : '') +
      (tok ? tok + ' · ' : '') +
      (dur ? dur + ' · ' : '') +
      esc(time) +
      (pending ? ' <span class="pending" title="in progress">●</span>' : '') +
    '</span>';

  const body = row.querySelector('.body');
  body.innerHTML = '';
  dispatch(body, s, e);
  body.appendChild(metaDetails(s, e));

  row.style.display = active[cat] ? '' : 'none';
}

function handleEvent(ev) {
  const isScope = ev.kind === 'scope';
  const phase = ev.scope_category;

  if (isScope && phase === 'end' && openScopes[ev.uuid]) {
    const rec = openScopes[ev.uuid];
    fillCard(rec.el, rec.start, ev);
    delete openScopes[ev.uuid];
    return;
  }

  const row = el('div', 'row');
  const head = el('div', 'head');
  const body = el('div', 'body');
  head.addEventListener('click', () => row.classList.toggle('open'));
  row.appendChild(head); row.appendChild(body);
  stream.appendChild(row);
  fillCard(row, ev, null);
  if (isScope && phase === 'start') openScopes[ev.uuid] = { el: row, start: ev };

  empty.style.display = 'none';
  if (followBox.checked) window.scrollTo(0, document.body.scrollHeight);
}

/* ------------------------------------------------------ file browser */
function selectFile(name) {
  if (selectedFile === name) return;
  gen++;
  selectedFile = name;
  sinceSeq = 0;
  for (const k in openScopes) delete openScopes[k];
  stream.innerHTML = '';
  empty.style.display = '';
  empty.textContent = 'Loading events…';
  renderFileSelection();
}
function renderFileList(files) {
  const sig = JSON.stringify(files) + '|' + selectedFile;
  if (sig === knownFiles) { renderFileSelection(); return; }
  knownFiles = sig;
  const total = files.reduce((n, f) => n + f.count, 0);
  let html = '<div class="file all' + (selectedFile === null ? ' sel' : '') + '" data-file="">' +
             '<span class="fname">All files</span><span class="fcount">' + total + '</span></div>';
  if (!files.length) html += '<div class="empty-files">no .jsonl files found</div>';
  for (const f of files) {
    html += '<div class="file' + (selectedFile === f.name ? ' sel' : '') + '" data-file="' + esc(f.name) + '">' +
            '<span class="fname" title="' + esc(f.name) + '">' + esc(f.name) + '</span>' +
            '<span class="fcount">' + f.count + '</span>' +
            '<a class="fdl" title="download raw .jsonl" href="/api/download?file=' + encodeURIComponent(f.name) +
            '" download="' + esc(f.name) + '">' +
            '<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" ' +
            'stroke-width="2" stroke-linecap="round" stroke-linejoin="round">' +
            '<path d="M12 3v11"/><path d="M7.5 10.5 12 15l4.5-4.5"/><path d="M5 20h14"/></svg></a></div>';
  }
  filelistEl.innerHTML = html;
  filelistEl.querySelectorAll('.file').forEach(e =>
    e.addEventListener('click', () => selectFile(e.dataset.file || null)));
  filelistEl.querySelectorAll('.fdl').forEach(a =>
    a.addEventListener('click', ev => ev.stopPropagation()));
}
function renderFileSelection() {
  filelistEl.querySelectorAll('.file').forEach(e =>
    e.classList.toggle('sel', (e.dataset.file || null) === selectedFile));
}

document.querySelectorAll('.chip[data-cat]').forEach(chip => {
  chip.addEventListener('click', () => {
    const cat = chip.dataset.cat;
    active[cat] = !active[cat];
    chip.classList.toggle('on', active[cat]);
    document.querySelectorAll('.row.cat-' + cat).forEach(r => { r.style.display = active[cat] ? '' : 'none'; });
  });
});
document.getElementById('expandAll').addEventListener('click', () =>
  stream.querySelectorAll('.row').forEach(r => r.classList.add('open')));
document.getElementById('collapseAll').addEventListener('click', () =>
  stream.querySelectorAll('.row').forEach(r => r.classList.remove('open')));

const themeBtn = document.getElementById('themeToggle');
function applyTheme(t) {
  document.body.classList.toggle('light', t === 'light');
  themeBtn.textContent = t === 'light' ? '☀ light' : '🌙 dark';
}
let theme = 'dark';
try { theme = localStorage.getItem('atof-theme') || 'dark'; } catch (e) {}
applyTheme(theme);
themeBtn.addEventListener('click', () => {
  theme = document.body.classList.contains('light') ? 'dark' : 'light';
  try { localStorage.setItem('atof-theme', theme); } catch (e) {}
  applyTheme(theme);
});
window.addEventListener('storage', e => { if (e.key === 'atof-theme') applyTheme(e.newValue || 'dark'); });

/* ------------------------------------------------------------- poll */
async function pollOnce() {
  const myGen = gen;
  const fileParam = selectedFile ? '&file=' + encodeURIComponent(selectedFile) : '';
  const res = await fetch('/api/events?since=' + sinceSeq + fileParam);
  const data = await res.json();
  if (myGen !== gen) return;

  sinceSeq = data.seq;
  statusDot.className = 'dot ' + (data.status === 'live' ? 'live' : data.status === 'error' ? 'error' : '');
  statusText.textContent = data.status === 'error' ? ('error: ' + (data.error || '')) : data.status;
  const shown = selectedFile
    ? ((data.files.find(f => f.name === selectedFile) || {}).count || 0)
    : data.count;
  countPill.textContent = shown + ' events';
  srcEl.textContent = data.sandbox + ' : ' + (selectedFile || data.dir);
  renderFileList(data.files || []);

  const evs = data.events || [];
  if (evs.length) empty.style.display = 'none';
  evs.forEach(handleEvent);
  if (!stream.children.length) {
    empty.style.display = '';
    empty.textContent = selectedFile ? 'No events in this file yet…'
                                     : 'Waiting for trace events from the sandbox…';
  }
}
async function loop() {
  try { await pollOnce(); }
  catch (e) { statusDot.className = 'dot error'; statusText.textContent = 'disconnected'; }
  finally { setTimeout(loop, 1000); }
}
loop();
</script>
</body>
</html>
"""


CHAT_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Chat</title>
<script>if(new URLSearchParams(location.search).has('embed'))document.documentElement.classList.add('embed');</script>
<style>html.embed header .nav,html.embed header #themeToggle{display:none!important}</style>
<style>
  :root {
    --bg: #0b0e14; --panel: #131722; --panel2: #1a2030; --border: #232a3b;
    --text: #d7dce5; --muted: #8b93a7; --accent: #7c5cff; --tool: #3ecf8e; --err: #ff5c72;
    --header-a: #111623; --header-b: #0b0e14; --code-bg: #05070b; --code-fg: #c3ccda;
  }
  body.light {
    --bg: #f5f7fa; --panel: #ffffff; --panel2: #eef1f6; --border: #d9dee8;
    --text: #1b2330; --muted: #5b647a; --accent: #6b46ff; --tool: #1a9d6b; --err: #d92d3f;
    --header-a: #ffffff; --header-b: #eef1f6; --code-bg: #eef1f6; --code-fg: #3a4353;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body { font: 14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
         background: var(--bg); color: var(--text); display: flex; flex-direction: column; }
  header { flex: none; background: linear-gradient(180deg,var(--header-a),var(--header-b));
           border-bottom: 1px solid var(--border); padding: 12px 18px; display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
  h1 { font-size: 15px; margin: 0; letter-spacing: .3px; }
  h1 .sub { color: var(--muted); font-weight: 400; font-size: 12px; margin-left: 8px; }
  .spacer { flex: 1; }
  .nav { display: flex; gap: 4px; }
  .chip { cursor: pointer; user-select: none; padding: 4px 10px; border-radius: 6px; font-size: 12px;
          border: 1px solid var(--border); background: var(--panel); color: var(--muted); }
  .chip:hover { color: var(--text); }
  a.chip { text-decoration: none; display: inline-flex; align-items: center; }
  .navlink.active { background: var(--accent); color: #fff; border-color: transparent; }

  main { flex: 1; min-height: 0; display: flex; flex-direction: column; align-items: center; }
  .thread { flex: 1; width: 100%; max-width: 820px; overflow-y: auto; padding: 22px 18px 8px; }
  .msg { display: flex; gap: 10px; margin-bottom: 16px; }
  .msg .avatar { flex: none; width: 28px; height: 28px; border-radius: 7px; display: grid; place-items: center;
                 font-size: 12px; font-weight: 700; }
  .msg.user .avatar { background: rgba(124,92,255,.18); color: var(--accent); }
  .msg.assistant .avatar { background: rgba(62,207,142,.16); color: var(--tool); }
  .msg .who { font-size: 11px; color: var(--muted); margin-bottom: 3px; text-transform: uppercase; letter-spacing: .5px; }
  .msg .content { min-width: 0; }
  .bubble { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; padding: 10px 13px; }
  .msg.user .bubble { background: var(--panel2); }
  .bubble.err { border-color: var(--err); color: var(--err); }
  .cursor::after { content: '▋'; color: var(--accent); animation: blink 1s steps(2) infinite; margin-left: 1px; }
  @keyframes blink { 0%,50%{opacity:1} 50.01%,100%{opacity:0} }
  .typing { display: inline-flex; gap: 4px; padding: 3px 0; }
  .typing i { width: 6px; height: 6px; border-radius: 50%; background: var(--muted); animation: bounce 1.2s infinite; }
  .typing i:nth-child(2){ animation-delay:.15s } .typing i:nth-child(3){ animation-delay:.3s }
  @keyframes bounce { 0%,80%,100%{transform:translateY(0);opacity:.4} 40%{transform:translateY(-4px);opacity:1} }

  /* markdown */
  .md > :first-child { margin-top: 0; } .md > :last-child { margin-bottom: 0; }
  .md p { margin: 7px 0; } .md ul, .md ol { margin: 7px 0; padding-left: 22px; } .md li { margin: 3px 0; }
  .md strong { color: var(--text); font-weight: 700; }
  .md code { background: var(--code-bg); border: 1px solid var(--border); border-radius: 4px; padding: 1px 5px;
             font: 12px/1.4 "SF Mono",ui-monospace,Menlo,Consolas,monospace; }
  .md pre.md-code { background: var(--code-bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px;
             margin: 8px 0; overflow: auto; white-space: pre-wrap; word-break: break-word; color: var(--code-fg);
             font: 12px/1.5 "SF Mono",ui-monospace,Menlo,Consolas,monospace; }

  .composer { width: 100%; max-width: 820px; padding: 10px 18px 18px; }
  .inputbar { display: flex; gap: 10px; align-items: flex-end; background: var(--panel); border: 1px solid var(--border);
              border-radius: 14px; padding: 8px 8px 8px 14px; }
  .inputbar:focus-within { border-color: var(--accent); }
  textarea { flex: 1; resize: none; border: 0; outline: 0; background: transparent; color: var(--text);
             font: 14px/1.5 inherit; max-height: 200px; padding: 6px 0; }
  .send { flex: none; width: 36px; height: 36px; border-radius: 10px; border: 0; cursor: pointer;
          background: var(--accent); color: #fff; font-size: 16px; display: grid; place-items: center; }
  .send:disabled { opacity: .4; cursor: not-allowed; }
  .send.stop { background: var(--err); }
  .hint { color: var(--muted); font-size: 11px; text-align: center; margin-top: 8px; }
  .empty { color: var(--muted); text-align: center; margin: 12% auto 0; max-width: 460px; }
  .empty h2 { color: var(--text); font-size: 17px; margin-bottom: 6px; }
  .suggest { display: flex; flex-wrap: wrap; gap: 8px; justify-content: center; margin-top: 16px; }
  .suggest button { background: var(--panel); border: 1px solid var(--border); color: var(--text);
                    padding: 8px 12px; border-radius: 9px; font-size: 13px; cursor: pointer; }
  .suggest button:hover { border-color: var(--accent); }
</style>
</head>
<body>
<header>
  <h1>Hermes Chat <span class="sub" id="sub"></span></h1>
  <nav class="nav">
    <a class="chip navlink" href="/">traces</a>
    <a class="chip navlink active" href="/chat">chat</a>
    <a class="chip navlink" href="/hermes">hermes</a>
  </nav>
  <div class="spacer"></div>
  <button class="chip" id="newChat">new chat</button>
  <button class="chip" id="themeToggle" title="toggle light / dark">🌙 dark</button>
</header>
<main>
  <div class="thread" id="thread">
    <div class="empty" id="empty">
      <h2>Chat with EHRClaw</h2>
      <div>Ask about the patient records — the agent queries FHIR, checks drug labels, and more. Tool calls show up live in the <a href="/#traces" target="_top" style="color:var(--accent)">Traces</a> tab.</div>
      <div class="suggest" id="suggest">
        <button>What can you do?</button>
        <button>How many patients are in the system?</button>
        <button>What medications is patient 1000 on?</button>
      </div>
    </div>
  </div>
  <div class="composer">
    <div class="inputbar">
      <textarea id="input" rows="1" placeholder="Type your message here…"></textarea>
      <button class="send" id="send" title="Send" disabled>➤</button>
    </div>
    <div class="hint">Enter to send · Shift+Enter for newline · responses may take a moment while the agent runs tools</div>
  </div>
</main>
<script>
const thread = document.getElementById('thread');
const emptyEl = document.getElementById('empty');
const input = document.getElementById('input');
const sendBtn = document.getElementById('send');
const newChatBtn = document.getElementById('newChat');
document.getElementById('sub').textContent = 'hermes-agent';

let messages = [];          // {role, content} conversation history
let streaming = false;
let controller = null;

function esc(s) { return String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
function mdLite(text) {
  const lines = esc(text).split(/\r?\n/);
  let html = '', inUl = false, inOl = false, inCode = false, code = '';
  const closeLists = () => { if (inUl) { html += '</ul>'; inUl = false; } if (inOl) { html += '</ol>'; inOl = false; } };
  const inline = s => s.replace(/`([^`]+)`/g, '<code>$1</code>').replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  for (const raw of lines) {
    if (/^\s*```/.test(raw)) {
      if (inCode) { html += '<pre class="md-code">' + code.replace(/\n$/, '') + '</pre>'; inCode = false; code = ''; }
      else { closeLists(); inCode = true; code = ''; }
      continue;
    }
    if (inCode) { code += raw + '\n'; continue; }
    const ol = raw.match(/^\s*\d+\.\s+(.*)/), ul = raw.match(/^\s*[-*]\s+(.*)/);
    if (ol) { if (!inOl) { closeLists(); html += '<ol>'; inOl = true; } html += '<li>' + inline(ol[1]) + '</li>'; continue; }
    if (ul) { if (!inUl) { closeLists(); html += '<ul>'; inUl = true; } html += '<li>' + inline(ul[1]) + '</li>'; continue; }
    closeLists();
    if (raw.trim() !== '') html += '<p>' + inline(raw) + '</p>';
  }
  if (inCode) html += '<pre class="md-code">' + code.replace(/\n$/, '') + '</pre>';
  closeLists();
  return html;
}

function addMessage(role, text) {
  emptyEl.style.display = 'none';
  const wrap = document.createElement('div');
  wrap.className = 'msg ' + role;
  const av = document.createElement('div');
  av.className = 'avatar';
  av.textContent = role === 'user' ? 'You'.slice(0,1) : 'H';
  const col = document.createElement('div');
  col.className = 'content';
  col.style.flex = '1';
  const who = document.createElement('div');
  who.className = 'who';
  who.textContent = role === 'user' ? 'you' : 'EHRClaw';
  const bubble = document.createElement('div');
  bubble.className = 'bubble md';
  if (text != null) bubble.innerHTML = mdLite(text);
  col.appendChild(who); col.appendChild(bubble);
  wrap.appendChild(av); wrap.appendChild(col);
  thread.appendChild(wrap);
  scrollDown();
  return bubble;
}
function scrollDown() { thread.scrollTop = thread.scrollHeight; }

function setStreaming(on) {
  streaming = on;
  sendBtn.classList.toggle('stop', on);
  sendBtn.textContent = on ? '■' : '➤';
  sendBtn.disabled = on ? false : input.value.trim() === '';
  sendBtn.title = on ? 'Stop' : 'Send';
}

async function send() {
  const text = input.value.trim();
  if (!text || streaming) return;
  input.value = ''; autosize();
  messages.push({ role: 'user', content: text });
  addMessage('user', text);

  const bubble = addMessage('assistant', null);
  bubble.innerHTML = '<span class="typing"><i></i><i></i><i></i></span>';
  setStreaming(true);
  controller = new AbortController();

  let acc = '';
  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages }),
      signal: controller.signal,
    });
    if (!res.ok) {
      let detail = '';
      try { detail = (await res.json()).error || (await res.text()); } catch (e) {}
      throw new Error(detail || ('HTTP ' + res.status));
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const parts = buf.split('\n');
      buf = parts.pop();
      for (const line of parts) {
        const s = line.trim();
        if (!s.startsWith('data:')) continue;
        const payload = s.slice(5).trim();
        if (payload === '[DONE]') continue;
        try {
          const j = JSON.parse(payload);
          const delta = j.choices && j.choices[0] && j.choices[0].delta;
          if (delta && delta.content) {
            acc += delta.content;
            bubble.innerHTML = mdLite(acc);
            bubble.classList.add('cursor');
            scrollDown();
          }
        } catch (e) { /* ignore keep-alive / partial */ }
      }
    }
    bubble.classList.remove('cursor');
    if (!acc.trim()) bubble.innerHTML = '<span style="color:var(--muted)">(no content returned)</span>';
    messages.push({ role: 'assistant', content: acc });
  } catch (err) {
    bubble.classList.remove('cursor');
    if (err.name === 'AbortError') {
      if (acc.trim()) { bubble.innerHTML = mdLite(acc) + '<div style="color:var(--muted);font-size:12px">⏹ stopped</div>';
                        messages.push({ role: 'assistant', content: acc }); }
      else { bubble.classList.add('err'); bubble.textContent = '⏹ stopped'; }
    } else {
      bubble.classList.add('err');
      bubble.textContent = 'Error: ' + err.message;
    }
  } finally {
    setStreaming(false);
    controller = null;
    input.focus();
  }
}

function stop() { if (controller) controller.abort(); }

function autosize() {
  input.style.height = 'auto';
  input.style.height = Math.min(input.scrollHeight, 200) + 'px';
}

sendBtn.addEventListener('click', () => { streaming ? stop() : send(); });
input.addEventListener('input', () => { autosize(); if (!streaming) sendBtn.disabled = input.value.trim() === ''; });
input.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
});
newChatBtn.addEventListener('click', () => {
  if (streaming) stop();
  messages = [];
  thread.querySelectorAll('.msg').forEach(m => m.remove());
  emptyEl.style.display = '';
  input.focus();
});
document.querySelectorAll('#suggest button').forEach(b =>
  b.addEventListener('click', () => { input.value = b.textContent; autosize(); sendBtn.disabled = false; send(); }));

const themeBtn = document.getElementById('themeToggle');
function applyTheme(t) {
  document.body.classList.toggle('light', t === 'light');
  themeBtn.textContent = t === 'light' ? '☀ light' : '🌙 dark';
}
let theme = 'dark';
try { theme = localStorage.getItem('atof-theme') || 'dark'; } catch (e) {}
applyTheme(theme);
themeBtn.addEventListener('click', () => {
  theme = document.body.classList.contains('light') ? 'dark' : 'light';
  try { localStorage.setItem('atof-theme', theme); } catch (e) {}
  applyTheme(theme);
});
window.addEventListener('storage', e => { if (e.key === 'atof-theme') applyTheme(e.newValue || 'dark'); });
input.focus();
</script>
</body>
</html>
"""


HERMES_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Hermes Dashboard</title>
<script>if(new URLSearchParams(location.search).has('embed'))document.documentElement.classList.add('embed');</script>
<style>html.embed header .nav,html.embed header #themeToggle{display:none!important}</style>
<style>
  :root {
    --bg: #0b0e14; --panel: #131722; --panel2: #1a2030; --border: #232a3b;
    --text: #d7dce5; --muted: #8b93a7; --accent: #7c5cff; --err: #ff5c72;
    --header-a: #111623; --header-b: #0b0e14;
  }
  body.light {
    --bg: #f5f7fa; --panel: #ffffff; --panel2: #eef1f6; --border: #d9dee8;
    --text: #1b2330; --muted: #5b647a; --accent: #6b46ff; --err: #d92d3f;
    --header-a: #ffffff; --header-b: #eef1f6;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body { font: 14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
         background: var(--bg); color: var(--text); display: flex; flex-direction: column; }
  header { flex: none; background: linear-gradient(180deg,var(--header-a),var(--header-b));
           border-bottom: 1px solid var(--border); padding: 12px 18px; display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
  h1 { font-size: 15px; margin: 0; letter-spacing: .3px; }
  h1 .sub { color: var(--muted); font-weight: 400; font-size: 12px; margin-left: 8px; }
  .spacer { flex: 1; }
  .nav { display: flex; gap: 4px; }
  .chip { cursor: pointer; user-select: none; padding: 4px 10px; border-radius: 6px; font-size: 12px;
          border: 1px solid var(--border); background: var(--panel); color: var(--muted); }
  .chip:hover { color: var(--text); }
  a.chip { text-decoration: none; display: inline-flex; align-items: center; }
  .navlink.active { background: var(--accent); color: #fff; border-color: transparent; }
  .frame-wrap { position: relative; flex: 1; min-height: 0; }
  iframe { border: 0; width: 100%; height: 100%; display: block; background: var(--bg); }
  .fallback { position: absolute; inset: 0; display: none; place-items: center; padding: 24px; background: var(--bg); }
  .fallback.show { display: grid; }
  .card { max-width: 560px; text-align: center; background: var(--panel); border: 1px solid var(--border);
          border-radius: 12px; padding: 28px 26px; }
  .card h2 { margin: 0 0 8px; font-size: 16px; }
  .card p { color: var(--muted); margin: 8px 0; }
  .card code { background: var(--panel2); border: 1px solid var(--border); border-radius: 5px; padding: 2px 7px;
               font-family: "SF Mono",ui-monospace,Menlo,Consolas,monospace; color: var(--text); }
  .card a.btn { display: inline-block; margin-top: 14px; padding: 8px 16px; border-radius: 8px; text-decoration: none;
                background: var(--accent); color: #fff; font-weight: 600; }
  .spin { width: 34px; height: 34px; border-radius: 50%; border: 3px solid var(--border);
          border-top-color: var(--accent); animation: spin 0.9s linear infinite; margin: 0 auto 14px; }
  @keyframes spin { to { transform: rotate(360deg); } }
</style>
</head>
<body>
<header>
  <h1>Hermes Dashboard <span class="sub" id="sub"></span></h1>
  <nav class="nav">
    <a class="chip navlink" href="/">traces</a>
    <a class="chip navlink" href="/chat">chat</a>
    <a class="chip navlink active" href="/hermes">hermes</a>
  </nav>
  <div class="spacer"></div>
  <a class="chip" id="openTab" href="#" target="_blank" rel="noopener">open in new tab ↗</a>
  <button class="chip" id="themeToggle" title="toggle light / dark">🌙 dark</button>
</header>
<div class="frame-wrap">
  <iframe id="frame" title="Native Hermes Dashboard"
          allow="clipboard-read; clipboard-write; microphone"></iframe>
  <div class="fallback" id="fallback">
    <div class="card">
      <div class="spin" id="spin"></div>
      <h2 id="fbTitle">Loading the native Hermes dashboard…</h2>
      <p id="fbMsg">Connecting to <code id="fbUrl"></code>.</p>
      <p>If this doesn't load, make sure that port is forwarded to your machine
         (the same way this dashboard is), then reload.</p>
      <a class="btn" id="fbOpen" href="#" target="_blank" rel="noopener">Open dashboard directly ↗</a>
    </div>
  </div>
</div>
<script>
const PORT = "__HERMES_DASHBOARD_PORT__";
const url = location.protocol + '//' + location.hostname + ':' + PORT + '/';
const frame = document.getElementById('frame');
const fallback = document.getElementById('fallback');
const spin = document.getElementById('spin');
document.getElementById('sub').textContent = location.hostname + ':' + PORT;
document.getElementById('fbUrl').textContent = url;
document.getElementById('openTab').href = url;
document.getElementById('fbOpen').href = url;

fallback.classList.add('show');
let loaded = false;
frame.addEventListener('load', () => { loaded = true; fallback.classList.remove('show'); });
setTimeout(() => {
  if (!loaded) {
    spin.style.display = 'none';
    document.getElementById('fbTitle').textContent = 'Could not reach the Hermes dashboard';
    document.getElementById('fbMsg').innerHTML =
      'The native dashboard on <code>' + location.hostname + ':' + PORT +
      '</code> did not load. Forward that port (as you did for this page) and reload.';
  }
}, 6000);
frame.src = url;

const themeBtn = document.getElementById('themeToggle');
function applyTheme(t) {
  document.body.classList.toggle('light', t === 'light');
  themeBtn.textContent = t === 'light' ? '☀ light' : '🌙 dark';
}
let theme = 'dark';
try { theme = localStorage.getItem('atof-theme') || 'dark'; } catch (e) {}
applyTheme(theme);
themeBtn.addEventListener('click', () => {
  theme = document.body.classList.contains('light') ? 'dark' : 'light';
  try { localStorage.setItem('atof-theme', theme); } catch (e) {}
  applyTheme(theme);
});
window.addEventListener('storage', e => { if (e.key === 'atof-theme') applyTheme(e.newValue || 'dark'); });
</script>
</body>
</html>
"""


def _env(name: str, default: str) -> str:
    val = os.environ.get(name)
    return val if val not in (None, "") else default


def main() -> None:
    global SANDBOX, ATOF_DIR, POLL_INTERVAL, HERMES_API_HOST, HERMES_API_PORT, HERMES_DASHBOARD_PORT
    # Env vars provide the defaults (convenient for docker-compose); explicit
    # CLI flags still win over them.
    parser = argparse.ArgumentParser(description="Live ATOF trace dashboard + Hermes chat")
    parser.add_argument("--sandbox", default=_env("ATOF_SANDBOX", SANDBOX),
                        help="nemoclaw sandbox name [env ATOF_SANDBOX]")
    parser.add_argument("--dir", default=_env("ATOF_DIR", ATOF_DIR),
                        help="atof directory inside the sandbox [env ATOF_DIR]")
    parser.add_argument("--port", type=int, default=int(_env("ATOF_PORT", "8090")),
                        help="HTTP port [env ATOF_PORT]")
    parser.add_argument("--host", default=_env("ATOF_HOST", "127.0.0.1"),
                        help="bind host [env ATOF_HOST]")
    parser.add_argument("--interval", type=float, default=float(_env("ATOF_POLL_INTERVAL", str(POLL_INTERVAL))),
                        help="poll interval (s) [env ATOF_POLL_INTERVAL]")
    parser.add_argument("--hermes-api-host", default=_env("HERMES_API_HOST", HERMES_API_HOST),
                        help="host serving the forwarded Hermes agent API [env HERMES_API_HOST]")
    parser.add_argument("--hermes-api-port", type=int, default=int(_env("HERMES_API_PORT", str(HERMES_API_PORT))),
                        help="forwarded Hermes OpenAI-compatible agent API port [env HERMES_API_PORT]")
    parser.add_argument("--hermes-dashboard-port", type=int,
                        default=int(_env("HERMES_DASHBOARD_PORT", str(HERMES_DASHBOARD_PORT))),
                        help="forwarded native Hermes web dashboard port for the /hermes tab [env HERMES_DASHBOARD_PORT]")
    args = parser.parse_args()

    SANDBOX = args.sandbox
    ATOF_DIR = args.dir.rstrip("/")
    POLL_INTERVAL = args.interval
    HERMES_API_HOST = args.hermes_api_host
    HERMES_API_PORT = args.hermes_api_port
    HERMES_DASHBOARD_PORT = args.hermes_dashboard_port

    token = get_gateway_token()
    threading.Thread(target=poll_loop, daemon=True).start()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}"
    print(f"ATOF dashboard streaming '{SANDBOX}:{ATOF_DIR}'")
    print(f"  -> traces: {url}/")
    print(f"  -> chat:   {url}/chat  (agent '{HERMES_MODEL}' via port {HERMES_API_PORT}, "
          f"token {'ok' if token else 'MISSING'})")
    print(f"  -> hermes: {url}/hermes  (embeds native dashboard on port {HERMES_DASHBOARD_PORT})")
    print("  (Ctrl-C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")
        server.shutdown()


if __name__ == "__main__":
    main()
