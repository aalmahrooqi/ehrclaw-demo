#!/usr/bin/env python3
"""Live dashboard that streams NeMo Relay ATOF agent-trace events out of a
running OpenShell sandbox.

The trace files live *inside* the sandbox (default:
``/sandbox/.nemo-relay/atof/*.jsonl``) and are appended to as the agent runs.
This server tails them over ``nemoclaw <sandbox> exec`` (no sshfs / sudo needed),
parses each JSON line and exposes them to a small single-page UI that polls for
new events and renders them as a live trace timeline.

Usage:
    python3 scripts/dashboard.py                 # defaults
    python3 scripts/dashboard.py --sandbox hermes-health --port 8090

Then open http://localhost:8090
"""
from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import shlex
import socket
import subprocess
import threading
import time
import uuid
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# ---------------------------------------------------------------------------
# Config (overridable via CLI flags)
# ---------------------------------------------------------------------------
SANDBOX = "hermes-health"
ATOF_DIR = "/sandbox/.nemo-relay/atof"
MEMORIES_DIR = "/sandbox/.hermes/memories"  # agent long-term memory (markdown files)
SKILLS_DIR = "/sandbox/.hermes/skills"      # agent skill library (SKILL.md per skill)
# Skills to surface under "Agent Custom Skills" (by dir id or leaf name under
# SKILLS_DIR). Override via HERMES_CUSTOM_SKILLS / --custom-skills. If left empty
# we fall back to auto-detecting anything not shipped in the base image.
CUSTOM_SKILLS: set[str] = {
    "summarize-patient-chart", "fhir-data-access",
    "fhir-medication-request-write", "clinical-trial-cohort-prescreener",
    "drug-label-interactions", "rxnorm-resolve-rxcui",
}
POLL_INTERVAL = 1.0          # seconds between sandbox polls
MEMORY_POLL_INTERVAL = 5.0   # seconds between memory-dir polls
SKILL_POLL_INTERVAL = 8.0    # seconds between skill-dir polls
MAX_EVENTS = 5000            # ring-buffer cap kept in memory / served to UI
# Directory where the memory/skill activity feeds are persisted so they survive
# a dashboard restart. Empty disables persistence (env DASHBOARD_STATE_DIR).
STATE_DIR = ""
EXEC_TIMEOUT = 30            # seconds for a single nemoclaw exec call
HERMES_API_HOST = "127.0.0.1"  # host serving the forwarded Hermes agent API
HERMES_API_PORT = 8642       # Hermes OpenAI-compatible agent API (inside sandbox, forwarded)
HERMES_DASHBOARD_PORT = 18790  # native Hermes web dashboard (forwarded to host)
HERMES_MODEL = "hermes-agent"
GATEWAY_TOKEN = ""           # bearer token for the Hermes agent API (discovered at startup)
_TOKEN_LOCK = threading.Lock()
_NEMOCLAW_COMMAND: list[str] | None = None
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,256}$")


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
        self._file_counts: dict[str, int] = {}   # virtual name -> events seen
        self._file_first_seq: dict[str, int] = {}  # virtual name -> first _seq

    def add(self, event: dict) -> None:
        with self._lock:
            self._seq += 1
            event["_seq"] = self._seq
            self._events.append(event)
            name = event.get("_file", "")
            if name and name not in self._file_counts:
                self._file_first_seq[name] = self._seq
            self._file_counts[name] = self._file_counts.get(name, 0) + 1
            if len(self._events) > self._max:
                dropped = self._events[: len(self._events) - self._max]
                self._events = self._events[-self._max :]
                if dropped:
                    self._rebuild_file_index_unlocked()

    def since(self, seq: int, file: str | None = None) -> tuple[list[dict], int]:
        with self._lock:
            out = [
                e for e in self._events
                if e["_seq"] > seq and (not file or e.get("_file") == file)
            ]
            return out, self._seq

    def export_file(self, name: str) -> bytes:
        """NDJSON for one virtual session file (original ATOF objects)."""
        with self._lock:
            lines = []
            for event in self._events:
                if event.get("_file") != name:
                    continue
                lines.append(json.dumps(
                    {k: v for k, v in event.items() if not str(k).startswith("_")},
                    ensure_ascii=False,
                ))
            return ("\n".join(lines) + "\n").encode() if lines else b""

    def prune(self, present_sources: set[str]) -> list[str]:
        """Drop events whose physical sandbox source file is gone."""
        with self._lock:
            before = {n for n in self._file_counts if n}
            self._events = [
                e for e in self._events
                if (src := e.get("_source")) in present_sources or not src
            ]
            self._rebuild_file_index_unlocked()
            return sorted(before - {n for n in self._file_counts if n})

    def _rebuild_file_index_unlocked(self) -> None:
        counts: dict[str, int] = {}
        first: dict[str, int] = {}
        for event in self._events:
            name = event.get("_file") or ""
            if not name:
                continue
            if name not in first:
                first[name] = int(event.get("_seq") or 0)
            counts[name] = counts.get(name, 0) + 1
        self._file_counts = counts
        self._file_first_seq = first

    def _file_list(self) -> list[dict]:
        # One sidebar entry per Hermes/Relay session (not per process-wide
        # ATOF jsonl). Newest session first.
        names = [n for n in self._file_counts if n]
        names.sort(key=lambda n: self._file_first_seq.get(n, 0), reverse=True)
        return [{"name": n, "count": self._file_counts.get(n, 0)} for n in names]

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


def _event_session_id(event: dict) -> str:
    """Relay/Hermes session id on an ATOF event, if any."""
    for blob in (event.get("metadata"), event.get("data")):
        if not isinstance(blob, dict):
            continue
        sid = blob.get("session_id")
        if isinstance(sid, str) and sid.strip() and "/" not in sid and ".." not in sid:
            return sid.strip()
    return ""


def _session_file_name(session_id: str, source: str) -> str:
    """Virtual jsonl name shown in the traces sidebar (one per session)."""
    if session_id:
        return f"{session_id}.jsonl"
    return source or "unattributed.jsonl"


def _mem_lines(content: str) -> list[str]:
    """Split a memory file into the lines that carry meaning for a diff — drop
    the ``§`` entry separators and blank lines so the diff shows facts, not
    bookkeeping."""
    return [ln for ln in content.splitlines() if ln.strip() not in ("", "§")]


def _diff(old: str, new: str, lines_fn=None, context: int = 2, cap: int = 400) -> tuple[list[str], int, int]:
    """Return (unified-diff lines, additions, deletions) between two blobs,
    git-style. File header lines are stripped; hunk headers (@@) kept.
    ``lines_fn`` splits a blob into comparable lines (defaults to raw
    ``splitlines``; memory passes ``_mem_lines`` to ignore ``§`` separators)."""
    split = lines_fn or (lambda s: s.splitlines())
    lines = [
        ln for ln in difflib.unified_diff(split(old), split(new), lineterm="", n=context)
        if not ln.startswith(("---", "+++"))
    ]
    add = sum(1 for ln in lines if ln.startswith("+"))
    dele = sum(1 for ln in lines if ln.startswith("-"))
    return lines[:cap], add, dele


def _save_state(name: str, data: dict) -> None:
    """Atomically persist a feed's state under STATE_DIR (no-op if disabled)."""
    if not STATE_DIR:
        return
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        path = os.path.join(STATE_DIR, name)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        os.replace(tmp, path)
    except Exception:  # noqa: BLE001 - persistence is best-effort
        pass


def _load_state(name: str) -> dict | None:
    if not STATE_DIR:
        return None
    try:
        with open(os.path.join(STATE_DIR, name), encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return None
    except Exception:  # noqa: BLE001
        return None


class MemoryStore:
    """Snapshot of the agent's markdown memory files plus a change log, so the
    UI can show both what the agent currently remembers and what it has learned
    (added / updated / forgotten) over time."""

    def __init__(self, max_events: int = 300):
        self._lock = threading.Lock()
        self._mem: dict[str, dict] = {}   # name -> {content, mtime, size}
        self._events: list[dict] = []     # [{seq, name, ts, action, size}]
        self._seq = 0
        self._max = max_events
        self._loaded = False
        self._dirty = False
        self.status = "connecting"        # connecting | live | error
        self.error = ""

    def _record(self, name: str, ts: float, action: str, size: int,
                diff: list[str] | None = None, add: int = 0, dele: int = 0) -> None:
        self._seq += 1
        self._events.append(
            {"seq": self._seq, "name": name, "ts": ts, "action": action, "size": size,
             "diff": diff or [], "add": add, "del": dele}
        )
        if len(self._events) > self._max:
            self._events = self._events[-self._max :]
        self._dirty = True

    def load(self, name: str) -> None:
        """Restore a previously persisted feed so diffs survive a restart."""
        data = _load_state(name)
        if not data:
            return
        with self._lock:
            self._seq = int(data.get("seq") or 0)
            self._loaded = bool(data.get("loaded"))
            self._events = data.get("events") or []
            self._mem = data.get("mem") or {}

    def maybe_save(self, name: str) -> None:
        with self._lock:
            if not self._dirty:
                return
            data = {"seq": self._seq, "loaded": self._loaded,
                    "events": self._events, "mem": self._mem}
            self._dirty = False
        _save_state(name, data)

    def update(self, snapshot: dict[str, dict]) -> None:
        """Diff a freshly-read {name: {content, mtime, size}} snapshot against
        the previous one, recording added / updated / removed events."""
        with self._lock:
            if not self._loaded:
                # Seed the timeline from the files themselves, using each file's
                # real mtime, so history reflects when memories were written even
                # before this dashboard started. The diff is the whole file as
                # additions (its state when we first saw it).
                for name, meta in sorted(snapshot.items(), key=lambda kv: kv[1]["mtime"]):
                    diff, add, dele = _diff("", meta["content"], _mem_lines)
                    self._record(name, float(meta["mtime"]), "recorded", meta["size"], diff, add, dele)
                self._mem = snapshot
                self._loaded = True
                return
            now = time.time()
            for name, meta in snapshot.items():
                prev = self._mem.get(name)
                if prev is None:
                    diff, add, dele = _diff("", meta["content"], _mem_lines)
                    self._record(name, now, "added", meta["size"], diff, add, dele)
                elif prev["content"] != meta["content"]:
                    diff, add, dele = _diff(prev["content"], meta["content"], _mem_lines)
                    self._record(name, now, "updated", meta["size"], diff, add, dele)
            for name in list(self._mem):
                if name not in snapshot:
                    diff, add, dele = _diff(self._mem[name]["content"], "", _mem_lines)
                    self._record(name, now, "removed", 0, diff, add, dele)
            self._mem = snapshot

    def snapshot(self) -> dict:
        with self._lock:
            mems = [
                {"name": n, "content": m["content"], "mtime": m["mtime"], "size": m["size"]}
                for n, m in sorted(self._mem.items())
            ]
            return {
                "status": self.status,
                "error": self.error,
                "dir": MEMORIES_DIR,
                "memories": mems,
                "events": list(reversed(self._events)),  # newest first
                "seq": self._seq,
            }


MEMORY_STORE = MemoryStore()


class SkillStore:
    """Snapshot of the agent's skill library (one SKILL.md per skill) plus a
    change log, so the UI can show which skills the agent has and how they
    evolve (added / patched / removed) over time.

    Skill content is cached so we can (a) diff a skill when its SKILL.md
    changes and (b) serve the current text on demand without another exec.
    Polls only re-read the files that actually changed (by mtime/size)."""

    def __init__(self, max_events: int = 300):
        self._lock = threading.Lock()
        self._skills: dict[str, dict] = {}  # id -> {content, mtime, size, name, category}
        self._events: list[dict] = []
        self._usage: dict = {}              # from .usage.json (per-skill metadata)
        self._bundled: set[str] = set()     # skill names shipped in the base image
        self._seq = 0
        self._max = max_events
        self._loaded = False
        self._dirty = False
        self.status = "connecting"          # connecting | live | error
        self.error = ""

    def _record(self, sid: str, ts: float, action: str, size: int,
                diff: list[str] | None = None, add: int = 0, dele: int = 0) -> None:
        self._seq += 1
        self._events.append(
            {"seq": self._seq, "id": sid, "ts": ts, "action": action, "size": size,
             "diff": diff or [], "add": add, "del": dele}
        )
        if len(self._events) > self._max:
            self._events = self._events[-self._max :]
        self._dirty = True

    def load(self, name: str) -> None:
        """Restore a previously persisted feed so diffs survive a restart."""
        data = _load_state(name)
        if not data:
            return
        with self._lock:
            self._seq = int(data.get("seq") or 0)
            self._loaded = bool(data.get("loaded"))
            self._events = data.get("events") or []
            self._skills = data.get("skills") or {}

    def maybe_save(self, name: str) -> None:
        with self._lock:
            if not self._dirty:
                return
            data = {"seq": self._seq, "loaded": self._loaded,
                    "events": self._events, "skills": self._skills}
            self._dirty = False
        _save_state(name, data)

    @staticmethod
    def _meta(sid: str) -> tuple[str, str]:
        parts = sid.split("/")
        return parts[-1], "/".join(parts[:-1])

    def set_meta(self, usage: dict, bundled: set[str]) -> None:
        with self._lock:
            self._usage = usage or {}
            self._bundled = bundled or set()

    def update(self, index: dict[str, dict], read_contents) -> None:
        """``index`` is {id: {mtime, size}} for every SKILL.md. ``read_contents``
        is called with the list of ids whose text we still need (first load, or
        newly-changed skills) and returns {id: content}."""
        with self._lock:
            if not self._loaded:
                contents = read_contents(list(index))
                for sid, meta in index.items():
                    name, cat = self._meta(sid)
                    self._skills[sid] = {
                        "content": contents.get(sid, ""), "mtime": meta["mtime"],
                        "size": meta["size"], "name": name, "category": cat,
                    }
                self._loaded = True
                return

            changed = [
                sid for sid, meta in index.items()
                if sid not in self._skills
                or meta["mtime"] != self._skills[sid]["mtime"]
                or meta["size"] != self._skills[sid]["size"]
            ]
            contents = read_contents(changed) if changed else {}
            now = time.time()
            for sid in changed:
                meta = index[sid]
                new_c = contents.get(sid, "")
                prev = self._skills.get(sid)
                name, cat = self._meta(sid)
                if prev is None:
                    diff, add, dele = _diff("", new_c)
                    self._record(sid, now, "added", meta["size"], diff, add, dele)
                elif prev["content"] != new_c:
                    diff, add, dele = _diff(prev["content"], new_c)
                    self._record(sid, now, "patched", meta["size"], diff, add, dele)
                self._skills[sid] = {
                    "content": new_c, "mtime": meta["mtime"], "size": meta["size"],
                    "name": name, "category": cat,
                }
            for sid in list(self._skills):
                if sid not in index:
                    diff, add, dele = _diff(self._skills[sid]["content"], "")
                    self._record(sid, now, "removed", 0, diff, add, dele)
                    del self._skills[sid]

    def content_of(self, sid: str) -> str | None:
        with self._lock:
            s = self._skills.get(sid)
            return s["content"] if s else None

    def snapshot(self) -> dict:
        with self._lock:
            skills = []
            for sid, s in self._skills.items():
                u = self._usage.get(s["name"]) or self._usage.get(sid) or {}
                skills.append({
                    "id": sid,
                    "name": s["name"],
                    "category": s["category"],
                    "mtime": s["mtime"],
                    "size": s["size"],
                    "used": int(u.get("use_count") or 0),
                    "patched": int(u.get("patch_count") or 0),
                    "pinned": bool(u.get("pinned")),
                    "state": u.get("state") or "active",
                })
            # Most-used first; then most-patched, most-recently-touched, name.
            skills.sort(key=lambda x: (-x["used"], -x["patched"], -x["mtime"], x["name"]))
            return {
                "status": self.status,
                "error": self.error,
                "dir": SKILLS_DIR,
                "count": len(skills),
                "skills": skills,
                "events": list(reversed(self._events)),  # newest first
                "seq": self._seq,
            }


SKILL_STORE = SkillStore()


# ---------------------------------------------------------------------------
# Sandbox access helpers
# ---------------------------------------------------------------------------
def _nemoclaw_command() -> list[str]:
    """Resolve the current CLI without relying on a stale global npm symlink."""
    global _NEMOCLAW_COMMAND
    if _NEMOCLAW_COMMAND is not None:
        return _NEMOCLAW_COMMAND

    override = os.environ.get("NEMOCLAW_CLI", "").strip()
    if override:
        command = shlex.split(override)
        if not command:
            raise RuntimeError("NEMOCLAW_CLI is empty")
        _NEMOCLAW_COMMAND = command
        return command

    home = Path.home()
    source_cli = home / ".nemoclaw/source/bin/nemoclaw.js"
    node_candidates = sorted((home / ".nvm/versions/node").glob("v*/bin/node"))
    if source_cli.is_file() and node_candidates:
        _NEMOCLAW_COMMAND = [str(node_candidates[-1]), str(source_cli)]
    else:
        _NEMOCLAW_COMMAND = ["nemoclaw"]
    return _NEMOCLAW_COMMAND


def _exec(args: list[str]) -> subprocess.CompletedProcess:
    proc = subprocess.run(
        [*_nemoclaw_command(), SANDBOX, "exec", "--no-tty", "--", *args],
        capture_output=True,
        timeout=EXEC_TIMEOUT,
    )
    if proc.returncode != 0:
        detail = proc.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(detail or f"NemoClaw exec failed with status {proc.returncode}")
    return proc


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
            [*_nemoclaw_command(), SANDBOX, "gateway-token", "--quiet"],
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
    last_sid: dict[str, str] = {}

    while True:
        try:
            sizes = list_files()
            STORE.files = {p: STORE.files.get(p, 0) for p in sizes}

            # Mirror deletions: forget files (+ their buffered events) that are
            # no longer present in the sandbox dir.
            present_paths = set(sizes)
            for stale in [p for p in buffers if p not in present_paths]:
                buffers.pop(stale, None)
                last_sid.pop(stale, None)
            STORE.prune({p.rsplit("/", 1)[-1] for p in present_paths})

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
                    source = path.rsplit("/", 1)[-1]
                    sid = _event_session_id(event) or last_sid.get(path, "")
                    if sid:
                        last_sid[path] = sid
                    event["_source"] = source
                    event["_file"] = _session_file_name(sid, source)
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


_MEM_FILE = "<<<HD-MEMFILE>>>"
_MEM_END = "<<<HD-MEMEND>>>"


def read_memories() -> dict[str, dict]:
    """Read the agent's markdown memory files out of the sandbox in one exec.

    Returns {name: {"content": str, "mtime": int, "size": int}}. Lock files and
    non-markdown files are ignored.
    """
    script = (
        f'cd {MEMORIES_DIR} 2>/dev/null || exit 0; '
        'for f in *.md; do [ -e "$f" ] || continue; '
        f'printf "%s%s\\t%s\\t%s\\n" "{_MEM_FILE}" "$f" "$(stat -c %Y "$f")" "$(stat -c %s "$f")"; '
        'cat "$f"; '
        f'printf "\\n%s\\n" "{_MEM_END}"; done'
    )
    proc = _exec(["sh", "-c", script])
    text = proc.stdout.decode("utf-8", "replace")
    out: dict[str, dict] = {}
    for chunk in text.split(_MEM_FILE):
        if not chunk.strip("\n"):
            continue
        header, _, rest = chunk.partition("\n")
        parts = header.split("\t")
        if len(parts) != 3:
            continue
        name, mtime_s, size_s = parts
        content = rest
        idx = content.rfind(_MEM_END)
        if idx != -1:
            content = content[:idx]
        content = content.rstrip("\n")
        try:
            mtime, size = int(mtime_s), int(size_s)
        except ValueError:
            mtime, size = 0, len(content.encode("utf-8"))
        out[name] = {"content": content, "mtime": mtime, "size": size}
    return out


def memory_poll_loop() -> None:
    """Background worker: read the agent's memory files and diff them over time."""
    while True:
        try:
            MEMORY_STORE.update(read_memories())
            MEMORY_STORE.status = "live"
            MEMORY_STORE.error = ""
        except subprocess.TimeoutExpired:
            MEMORY_STORE.status = "error"
            MEMORY_STORE.error = "sandbox exec timed out"
        except Exception as exc:  # noqa: BLE001 - surface any failure to UI
            MEMORY_STORE.status = "error"
            MEMORY_STORE.error = f"{type(exc).__name__}: {exc}"
        MEMORY_STORE.maybe_save("memory_feed.json")
        time.sleep(MEMORY_POLL_INTERVAL)


_SKILL_FILE = "<<<HD-SKILLFILE>>>"
_SKILL_END = "<<<HD-SKILLEND>>>"
_SKILL_SPLIT = "<<<HD-SKILLMETA>>>"


def read_skill_index() -> dict[str, dict]:
    """Cheaply list every skill (its SKILL.md) with mtime + size, in one exec.

    Returns {id: {"mtime": int, "size": int}} where id is the skill dir relative
    to the skills root, e.g. ``apple/apple-notes`` or ``summarize-patient-chart``.
    """
    proc = _exec([
        "sh", "-c",
        f'cd {SKILLS_DIR} 2>/dev/null || exit 0; '
        r'find . -name SKILL.md -printf "%P\t%T@\t%s\n" 2>/dev/null',
    ])
    out: dict[str, dict] = {}
    for line in proc.stdout.decode("utf-8", "replace").splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        rel, mtime_s, size_s = parts
        suffix = "/SKILL.md"
        sid = rel[: -len(suffix)] if rel.endswith(suffix) else rel
        if not sid:
            continue
        try:
            mtime, size = int(float(mtime_s)), int(size_s)
        except ValueError:
            continue
        out[sid] = {"mtime": mtime, "size": size}
    return out


def read_skill_contents(ids: list[str]) -> dict[str, str]:
    """Read the SKILL.md text for the given skill ids in a single exec. Ids are
    shell-quoted and embedded in the script (nemoclaw exec doesn't forward extra
    argv past ``sh -c``)."""
    if not ids:
        return {}
    quoted = " ".join(shlex.quote(i) for i in ids)
    script = (
        f'cd {SKILLS_DIR} 2>/dev/null || exit 0; '
        f'for id in {quoted}; do f="$id/SKILL.md"; [ -f "$f" ] || continue; '
        f'printf "%s%s\\n" "{_SKILL_FILE}" "$id"; cat "$f"; '
        f'printf "\\n%s\\n" "{_SKILL_END}"; done'
    )
    proc = _exec(["sh", "-c", script])
    text = proc.stdout.decode("utf-8", "replace")
    out: dict[str, str] = {}
    for chunk in text.split(_SKILL_FILE):
        if not chunk.strip("\n"):
            continue
        header, _, rest = chunk.partition("\n")
        sid = header.strip()
        if not sid:
            continue
        idx = rest.rfind(_SKILL_END)
        if idx != -1:
            rest = rest[:idx]
        out[sid] = rest.rstrip("\n")
    return out


def read_skill_meta() -> tuple[dict, set[str]]:
    """Read .usage.json (per-skill use/patch counts) and .bundled_manifest
    (names shipped in the base image) in one exec."""
    proc = _exec([
        "sh", "-c",
        f'cat {SKILLS_DIR}/.usage.json 2>/dev/null; '
        f'printf "\\n{_SKILL_SPLIT}\\n"; '
        f'cat {SKILLS_DIR}/.bundled_manifest 2>/dev/null',
    ])
    text = proc.stdout.decode("utf-8", "replace")
    usage_txt, _, bundled_txt = text.partition(_SKILL_SPLIT)
    try:
        usage = json.loads(usage_txt) if usage_txt.strip() else {}
    except json.JSONDecodeError:
        usage = {}
    bundled: set[str] = set()
    for line in bundled_txt.splitlines():
        name = line.split(":", 1)[0].strip()
        if name:
            bundled.add(name)
    return usage, bundled


def select_tracked_skills(index: dict[str, dict], bundled: set[str]) -> dict[str, dict]:
    """Pick which skills the dashboard tracks: the configured ``CUSTOM_SKILLS``
    allowlist (matched by dir id or leaf name), or — if none configured — every
    skill not shipped in the base image."""
    out: dict[str, dict] = {}
    for sid, meta in index.items():
        name = sid.split("/")[-1]
        if CUSTOM_SKILLS:
            keep = sid in CUSTOM_SKILLS or name in CUSTOM_SKILLS
        else:
            keep = name not in bundled and sid not in bundled
        if keep:
            out[sid] = meta
    return out


def skill_poll_loop() -> None:
    """Background worker: track the agent's custom skills and diff SKILL.md
    files as they evolve."""
    while True:
        try:
            usage, bundled = read_skill_meta()
            SKILL_STORE.set_meta(usage, bundled)
            tracked = select_tracked_skills(read_skill_index(), bundled)
            SKILL_STORE.update(tracked, read_skill_contents)
            SKILL_STORE.status = "live"
            SKILL_STORE.error = ""
        except subprocess.TimeoutExpired:
            SKILL_STORE.status = "error"
            SKILL_STORE.error = "sandbox exec timed out"
        except Exception as exc:  # noqa: BLE001 - surface any failure to UI
            SKILL_STORE.status = "error"
            SKILL_STORE.error = f"{type(exc).__name__}: {exc}"
        SKILL_STORE.maybe_save("skill_feed.json")
        time.sleep(SKILL_POLL_INTERVAL)


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
        elif parsed.path == "/dashboard":
            self._send(200, DASHBOARD_HTML.encode(), "text/html; charset=utf-8")
        elif parsed.path == "/api/memories":
            self._send(200, json.dumps(MEMORY_STORE.snapshot()).encode(), "application/json")
        elif parsed.path == "/api/skills":
            self._send(200, json.dumps(SKILL_STORE.snapshot()).encode(), "application/json")
        elif parsed.path == "/api/skill":
            qs = parse_qs(parsed.query)
            sid = qs.get("id", [""])[0]
            content = SKILL_STORE.content_of(sid)
            if content is None:
                self._send(404, b'{"error":"unknown skill"}', "application/json")
            else:
                body = json.dumps({"id": sid, "content": content}).encode()
                self._send(200, body, "application/json")
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
        """Download one session's ATOF events as NDJSON."""
        known = {f["name"] for f in STORE.snapshot_meta()["files"]}
        if not name or "/" in name or ".." in name or name not in known:
            self._send(404, b"unknown file", "text/plain")
            return
        body = STORE.export_file(name)
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
        elif parsed.path == "/api/chat/new":
            self._new_chat_session()
        else:
            self._send(404, b"not found", "text/plain")

    def _hermes_call(
        self,
        method: str,
        path: str,
        token: str,
        payload: dict | None = None,
        extra_headers: dict | None = None,
        timeout: int = 15,
    ):
        headers = {"Authorization": f"Bearer {token}"}
        data = None
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        if extra_headers:
            headers.update({k: v for k, v in extra_headers.items() if v})
        req = urllib.request.Request(
            f"http://{HERMES_API_HOST}:{HERMES_API_PORT}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        return urllib.request.urlopen(req, timeout=timeout)

    def _open_hermes(self, payload: bytes, token: str, session_id: str = ""):
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if session_id:
            headers["X-Hermes-Session-Id"] = session_id
        req = urllib.request.Request(
            f"http://{HERMES_API_HOST}:{HERMES_API_PORT}/v1/chat/completions",
            data=payload,
            headers=headers,
            method="POST",
        )
        return urllib.request.urlopen(req, timeout=900)

    @staticmethod
    def _valid_session_id(value: object) -> str:
        if not isinstance(value, str):
            return ""
        sid = value.strip()
        return sid if _SESSION_ID_RE.fullmatch(sid) else ""

    @staticmethod
    def _parse_session_id(obj: object) -> str:
        if not isinstance(obj, dict):
            return ""
        for key in ("id", "session_id", "sessionId"):
            val = obj.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        nested = obj.get("session")
        if isinstance(nested, dict):
            return Handler._parse_session_id(nested)
        return ""

    @staticmethod
    def _upstream_session_id(upstream) -> str:
        headers = getattr(upstream, "headers", None)
        if headers is None:
            return ""
        return (
            headers.get("X-Hermes-Session-Id")
            or headers.get("X-Chat-ID")
            or headers.get("x-hermes-session-id")
            or ""
        ).strip()

    def _json_from_hermes(self, resp) -> dict:
        raw = resp.read() if resp else b""
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8", "replace"))
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _latest_user_turn(messages: list) -> list:
        """Return only the newest user message.

        Hermes session continuity loads prior turns from SessionDB when
        ``X-Hermes-Session-Id`` is set. Sending the full browser transcript
        makes the API fingerprint the *first* user message instead, so a new
        chat that repeats a prompt (or omits the header) stays on the old
        Relay session and never emits ``hermes.session`` / ``on_session_start``.
        """
        for msg in reversed(messages):
            if isinstance(msg, dict) and msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, str) or content:
                    return [{"role": "user", "content": content}]
        last = messages[-1] if messages else {}
        if isinstance(last, dict):
            return [last]
        return [{"role": "user", "content": str(last)}]

    def _end_hermes_session(self, session_id: str, token: str) -> None:
        """Best-effort close of the previous Hermes transcript (PATCH, then DELETE)."""
        path = f"/api/sessions/{session_id}"
        try:
            resp = self._hermes_call(
                "PATCH", path, token, payload={"end_reason": "session_reset"}, timeout=10,
            )
            resp.read()
            resp.close()
            return
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                try:
                    token = get_gateway_token(refresh=True)
                    resp = self._hermes_call(
                        "PATCH", path, token, payload={"end_reason": "session_reset"}, timeout=10,
                    )
                    resp.read()
                    resp.close()
                    return
                except Exception:  # noqa: BLE001
                    pass
            elif exc.code not in (404, 405):
                try:
                    exc.read()
                except Exception:  # noqa: BLE001
                    pass
        except Exception:  # noqa: BLE001
            pass
        try:
            resp = self._hermes_call("DELETE", path, token, timeout=10)
            resp.read()
            resp.close()
        except Exception:  # noqa: BLE001
            pass

    def _create_hermes_session(self, token: str) -> str:
        """Create an empty Hermes session; fall back to a fresh UUID."""
        for attempt, tok in enumerate((token, None)):
            use = tok or get_gateway_token(refresh=True)
            try:
                resp = self._hermes_call(
                    "POST", "/api/sessions", use, payload={"source": "dashboard"}, timeout=10,
                )
                created = self._json_from_hermes(resp)
                resp.close()
                sid = self._valid_session_id(self._parse_session_id(created))
                if sid:
                    return sid
            except urllib.error.HTTPError as exc:
                if exc.code == 401 and attempt == 0:
                    try:
                        exc.read()
                    except Exception:  # noqa: BLE001
                        pass
                    continue
                try:
                    exc.read()
                except Exception:  # noqa: BLE001
                    pass
                break
            except Exception:  # noqa: BLE001
                break
        return str(uuid.uuid4())

    def _new_chat_session(self) -> None:
        """End the current Hermes session and start a new one."""
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b"{}"
            data = json.loads(body or b"{}")
        except (ValueError, json.JSONDecodeError):
            data = {}
        old = self._valid_session_id(data.get("session_id") if isinstance(data, dict) else "")
        token = get_gateway_token()
        if old:
            self._end_hermes_session(old, token)
        new_id = self._create_hermes_session(token)
        self._send(200, json.dumps({"session_id": new_id}).encode(), "application/json")

    def _proxy_chat(self) -> None:
        """Proxy a chat turn to the Hermes agent API, streaming SSE back to the UI.

        The browser sends {"messages": [...], "session_id": "..."}. We inject the
        agent model, gateway bearer token, and X-Hermes-Session-Id so follow-up
        turns stay on one Hermes transcript. Only the latest user message is
        forwarded: with that header Hermes loads history from SessionDB, and an
        empty DB on a new session id is what fires on_session_start → Relay's
        hermes.session Agent scope. The session id is echoed back on the SSE
        response so the UI can reuse it.
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
        token = get_gateway_token()
        session_id = self._valid_session_id(data.get("session_id"))
        if not session_id:
            session_id = self._create_hermes_session(token)

        payload = json.dumps({
            "model": HERMES_MODEL,
            "messages": self._latest_user_turn(messages),
            "stream": True,
        }).encode()

        try:
            upstream = self._open_hermes(payload, token, session_id)
        except urllib.error.HTTPError as e:
            if e.code == 401:  # token may have rotated — refresh once and retry
                try:
                    upstream = self._open_hermes(payload, get_gateway_token(refresh=True), session_id)
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

        echoed = self._upstream_session_id(upstream) or session_id
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Accel-Buffering", "no")
        if echoed:
            self.send_header("X-Hermes-Session-Id", echoed)
            self.send_header("Access-Control-Expose-Headers", "X-Hermes-Session-Id")
        self.end_headers()
        try:
            self._pipe_sse(upstream)
        except (BrokenPipeError, ConnectionResetError):
            pass  # client navigated away / hit stop
        finally:
            upstream.close()

    @staticmethod
    def _tcp_nodelay(resp) -> None:
        """Disable Nagle so small SSE frames are not delayed on the Hermes socket."""
        try:
            fp = getattr(resp, "fp", None)
            raw = getattr(fp, "raw", None)
            sock = getattr(raw, "_sock", None)
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (AttributeError, OSError):
            pass

    def _pipe_sse(self, upstream) -> None:
        """Forward Hermes SSE bytes as they arrive.

        ``HTTPResponse.read(n)`` fills urllib's 8 KiB BufferedReader first, so
        tokens arrive in dumps. ``read1`` issues at most one underlying recv.
        """
        self._tcp_nodelay(upstream)
        read1 = getattr(upstream, "read1", None)
        while True:
            chunk = read1(256) if callable(read1) else upstream.read(256)
            if not chunk:
                break
            self.wfile.write(chunk)
            self.wfile.flush()


SHELL_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>CliniClaw</title>
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
  <div class="brand"><span class="dot"></span> CliniClaw</div>
  <nav class="tabs" id="tabs">
    <div class="tab" data-view="dashboard">dashboard</div>
    <div class="tab" data-view="chat">chat</div>
    <div class="tab" data-view="hermes">hermes</div>
    <div class="tab" data-view="traces">traces</div>
  </nav>
  <div class="spacer"></div>
  <button class="chip" id="themeToggle" title="toggle light / dark">🌙 dark</button>
</div>
<div class="views" id="views">
  <div class="view" data-view="dashboard"><iframe data-src="/dashboard?embed=1" title="Dashboard"></iframe></div>
  <div class="view" data-view="chat"><iframe data-src="/chat?embed=1" title="Chat"
        allow="clipboard-read; clipboard-write; microphone"></iframe></div>
  <div class="view" data-view="hermes"><iframe data-src="/hermes?embed=1" title="Hermes Dashboard"
        allow="clipboard-read; clipboard-write; microphone"></iframe></div>
  <div class="view" data-view="traces"><iframe data-src="/traces?embed=1" title="Traces"></iframe></div>
</div>
<script>
const TABS = ['dashboard', 'chat', 'hermes', 'traces'];

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
    <a class="chip navlink" href="/dashboard">dashboard</a>
    <a class="chip navlink" href="/chat">chat</a>
    <a class="chip navlink" href="/hermes">hermes</a>
    <a class="chip navlink active" href="/">traces</a>
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
</header>
<div class="layout">
  <aside class="sidebar">
    <div class="sb-head">Sessions</div>
    <div class="filelist" id="filelist">
      <div class="empty-files">waiting for a session…</div>
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
const filelistEl = document.getElementById('filelist');

let sinceSeq = 0;
let selectedFile = null;   // virtual session jsonl (one per Hermes session)
let followLatest = true;   // jump to a new session unless the user pinned one
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
  if (s.category === 'agent' || s.name === 'hermes.session') return 'session start';
  return s.name || '';
}
function clip(str, n) { str = String(str || '').replace(/\s+/g, ' ').trim(); return str.length > n ? str.slice(0, n) + '…' : str; }
function summarize(s, e) {
  const d = s.data;
  if (s.name === 'hermes.turn.start') return '▶ ' + clip(d && d.user_message, 160);
  if (s.name === 'hermes.turn.end') return '■ ' + clip(d && d.assistant_response, 160);
  if (s.name === 'hermes.session.end') return 'session ended';
  if (s.name === 'hermes.session' || s.category === 'agent') {
    const sid = (s.metadata && s.metadata.session_id) || (s.data && s.data.session_id) || s.name;
    return 'new session ' + clip(sid, 24);
  }
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
  row.dataset.file = ev._file || '';
  const head = el('div', 'head');
  const body = el('div', 'body');
  head.addEventListener('click', () => row.classList.toggle('open'));
  row.appendChild(head); row.appendChild(body);
  stream.appendChild(row);
  fillCard(row, ev, null);
  if (isScope && phase === 'start') openScopes[ev.uuid] = { el: row, start: ev };

  empty.style.display = 'none';
}

/* ------------------------------------------------------ file browser */
function selectFile(name, pin) {
  if (selectedFile === name) {
    if (pin) followLatest = false;
    return;
  }
  gen++;
  selectedFile = name;
  followLatest = !pin;
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
  let html = '';
  if (!files.length) html = '<div class="empty-files">no sessions yet</div>';
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
    e.addEventListener('click', () => selectFile(e.dataset.file, true)));
  filelistEl.querySelectorAll('.fdl').forEach(a =>
    a.addEventListener('click', ev => ev.stopPropagation()));
}
function renderFileSelection() {
  filelistEl.querySelectorAll('.file').forEach(e =>
    e.classList.toggle('sel', e.dataset.file === selectedFile));
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
  const files = data.files || [];
  const present = new Set(files.map(f => f.name));
  const newest = files.length ? files[0].name : null;
  if (followLatest && newest && selectedFile !== newest) {
    selectFile(newest, false);
    renderFileList(files);
    return;
  }
  if (!selectedFile || !present.has(selectedFile)) {
    selectFile(newest, false);
    renderFileList(files);
    return;
  }

  const shown = ((files.find(f => f.name === selectedFile) || {}).count || 0);
  countPill.textContent = shown + ' events';
  srcEl.textContent = data.sandbox + ' : ' + selectedFile;
  renderFileList(files);

  // Reflect deletions: if a file vanished from the sandbox, drop its rows.
  let removed = false;
  stream.querySelectorAll('.row').forEach(r => {
    const f = r.dataset.file;
    if (f && !present.has(f)) { r.remove(); removed = true; }
  });
  if (removed) {
    for (const k in openScopes) { if (!openScopes[k].el.isConnected) delete openScopes[k]; }
  }

  const evs = data.events || [];
  if (evs.length) empty.style.display = 'none';
  evs.forEach(handleEvent);
  if (!stream.children.length) {
    empty.style.display = '';
    empty.textContent = 'No events in this file yet…';
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

  /* steps: the agent's between-tool narration, kept out of the answer bubble */
  .steps { margin-bottom: 6px; font-size: 12px; color: var(--muted); }
  .steps summary { cursor: pointer; user-select: none; list-style: none; }
  .steps summary::-webkit-details-marker { display: none; }
  .steps summary::before { content: '▸ '; }
  .steps[open] summary::before { content: '▾ '; }
  .steps .note { font-style: italic; white-space: pre-wrap; margin: 4px 0 4px 14px; }
  .steps .tool { display: inline-flex; align-items: center; gap: 5px; margin: 3px 6px 3px 14px; padding: 2px 7px;
                 border: 1px solid var(--border); border-radius: 6px; background: var(--panel2); font-size: 11px; }
  .steps .tool.done { opacity: .5; }
  .steps .tool .lbl { max-width: 340px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .cursor::after { content: '▋'; color: var(--accent); animation: blink 1s steps(2) infinite; margin-left: 1px; }
  @keyframes blink { 0%,50%{opacity:1} 50.01%,100%{opacity:0} }
  .typing { display: inline-flex; gap: 4px; padding: 3px 0; }
  .typing i { width: 6px; height: 6px; border-radius: 50%; background: var(--muted); animation: bounce 1.2s infinite; }
  .typing i:nth-child(2){ animation-delay:.15s } .typing i:nth-child(3){ animation-delay:.3s }
  @keyframes bounce { 0%,80%,100%{transform:translateY(0);opacity:.4} 40%{transform:translateY(-4px);opacity:1} }

  /* markdown */
  .md > :first-child { margin-top: 0; } .md > :last-child { margin-bottom: 0; }
  .md p { margin: 7px 0; } .md ul, .md ol { margin: 7px 0; padding-left: 22px; } .md li { margin: 3px 0; }
  .md h1, .md h2, .md h3, .md h4, .md h5, .md h6 { margin: 13px 0 6px; line-height: 1.25; font-weight: 700; color: var(--text); }
  .md h1 { font-size: 19px; } .md h2 { font-size: 17px; } .md h3 { font-size: 15px; }
  .md h4, .md h5, .md h6 { font-size: 13px; } .md h1, .md h2 { border-bottom: 1px solid var(--border); padding-bottom: 4px; }
  .md strong { color: var(--text); font-weight: 700; }
  .md code { background: var(--code-bg); border: 1px solid var(--border); border-radius: 4px; padding: 1px 5px;
             font: 12px/1.4 "SF Mono",ui-monospace,Menlo,Consolas,monospace; }
  .md pre.md-code { background: var(--code-bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px;
             margin: 8px 0; overflow: auto; white-space: pre-wrap; word-break: break-word; color: var(--code-fg);
             font: 12px/1.5 "SF Mono",ui-monospace,Menlo,Consolas,monospace; }
  .md table.md-table { border-collapse: collapse; width: 100%; min-width: max-content; font-size: 13px; }
  .md .md-table-wrap { overflow-x: auto; margin: 8px 0; max-width: 100%; -webkit-overflow-scrolling: touch; }
  .md table.md-table th, .md table.md-table td { border: 1px solid var(--border); padding: 6px 10px; text-align: left;
             vertical-align: top; white-space: nowrap; }
  .md table.md-table td { white-space: normal; }
  .md table.md-table th { background: var(--panel2); color: var(--text); font-weight: 700; }
  .md table.md-table tr:nth-child(even) td { background: rgba(127,127,127,.06); }

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
    <a class="chip navlink" href="/dashboard">dashboard</a>
    <a class="chip navlink active" href="/chat">chat</a>
    <a class="chip navlink" href="/hermes">hermes</a>
    <a class="chip navlink" href="/">traces</a>
  </nav>
  <div class="spacer"></div>
  <button class="chip" id="newChat">new chat</button>
  <span class="sub" id="sessionLabel" title="Hermes session"></span>
  <button class="chip" id="themeToggle" title="toggle light / dark">🌙 dark</button>
</header>
<main>
  <div class="thread" id="thread">
    <div class="empty" id="empty">
      <h2>Chat with CliniClaw</h2>
      <div>Ask about the patient records — the agent's steps collapse above each reply; full traces are in the <a href="/#traces" target="_top" style="color:var(--accent)">Traces</a> tab.</div>
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
let sessionId = null;       // Hermes X-Hermes-Session-Id for this thread
let streaming = false;
let controller = null;

function setSession(id) {
  sessionId = id || null;
  const el = document.getElementById('sessionLabel');
  if (!el) return;
  el.textContent = sessionId ? ('session ' + sessionId.slice(0, 8)) : '';
}

function esc(s) { return String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
function mdLite(text) {
  const lines = esc(text).split(/\r?\n/);
  let html = '', inUl = false, inOl = false, inCode = false, code = '';
  const closeLists = () => { if (inUl) { html += '</ul>'; inUl = false; } if (inOl) { html += '</ol>'; inOl = false; } };
  const inline = s => s.replace(/`([^`]+)`/g, '<code>$1</code>').replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  // GitHub-style pipe tables
  const isSep = s => /^\s*\|?\s*:?-{1,}:?\s*(\|\s*:?-{1,}:?\s*)*\|?\s*$/.test(s) && s.includes('-');
  const isRow = s => /^\s*\|?.+\|/.test(s) && s.includes('|') && s.trim() !== '';
  const cells = s => s.trim().replace(/^\|/, '').replace(/\|$/, '').split('|').map(c => c.trim());
  const skipBlank = (arr, idx) => { while (idx < arr.length && arr[idx].trim() === '') idx++; return idx; };
  const emitTable = (header, align, bodyRows) => {
    const cell = (c, k, tag) =>
      '<' + tag + (align[k] ? ' style="text-align:' + align[k] + '"' : '') + '>' + inline(c) + '</' + tag + '>';
    const th = cells(header).map((c, k) => cell(c, k, 'th')).join('');
    const body = bodyRows.map(row =>
      '<tr>' + cells(row).map((c, k) => cell(c, k, 'td')).join('') + '</tr>'
    ).join('');
    return '<div class="md-table-wrap"><table class="md-table"><thead><tr>' + th +
           '</tr></thead><tbody>' + body + '</tbody></table></div>';
  };
  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    if (/^\s*```/.test(raw)) {
      if (inCode) { html += '<pre class="md-code">' + code.replace(/\n$/, '') + '</pre>'; inCode = false; code = ''; }
      else { closeLists(); inCode = true; code = ''; }
      continue;
    }
    if (inCode) { code += raw + '\n'; continue; }
    if (isRow(raw)) {
      const sepAt = skipBlank(lines, i + 1);
      if (sepAt < lines.length && isSep(lines[sepAt])) {
        closeLists();
        const align = cells(lines[sepAt]).map(c => {
          const l = c.startsWith(':'), r = c.endsWith(':');
          return (l && r) ? 'center' : r ? 'right' : l ? 'left' : '';
        });
        const bodyRows = [];
        let j = sepAt + 1;
        for (; j < lines.length && isRow(lines[j]) && !isSep(lines[j]); j++) bodyRows.push(lines[j]);
        html += emitTable(raw, align, bodyRows);
        i = j - 1;
        continue;
      }
      const nextAt = skipBlank(lines, i + 1);
      if (nextAt < lines.length && isRow(lines[nextAt]) && !isSep(lines[nextAt])) {
        closeLists();
        const colN = cells(raw).length;
        const align = Array(colN).fill('');
        const bodyRows = [];
        let j = nextAt;
        for (; j < lines.length && isRow(lines[j]) && !isSep(lines[j]); j++) bodyRows.push(lines[j]);
        html += emitTable(raw, align, bodyRows);
        i = j - 1;
        continue;
      }
    }
    const hd = raw.match(/^\s*(#{1,6})\s+(.*)$/);
    if (hd) { closeLists(); const lv = hd[1].length; html += '<h' + lv + '>' + inline(hd[2].replace(/\s+#+\s*$/, '')) + '</h' + lv + '>'; continue; }
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
  who.textContent = role === 'user' ? 'you' : 'CliniClaw';
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

// A turn arrives as: narration, tool call, narration, tool call, …, answer. Only
// the text after the last tool call is the reply; everything earlier is the
// agent talking through its steps, so it belongs in the collapsed steps block.
function addAssistantTurn() {
  emptyEl.style.display = 'none';
  const wrap = document.createElement('div');
  wrap.className = 'msg assistant';
  const av = document.createElement('div');
  av.className = 'avatar';
  av.textContent = 'H';
  const col = document.createElement('div');
  col.className = 'content';
  col.style.flex = '1';
  const who = document.createElement('div');
  who.className = 'who';
  who.textContent = 'CliniClaw';
  const steps = document.createElement('details');
  steps.className = 'steps';
  steps.hidden = true;
  const summary = document.createElement('summary');
  const stepBody = document.createElement('div');
  steps.appendChild(summary);
  steps.appendChild(stepBody);
  const bubble = document.createElement('div');
  bubble.className = 'bubble md';
  col.appendChild(who);
  col.appendChild(steps);
  col.appendChild(bubble);
  wrap.appendChild(av);
  wrap.appendChild(col);
  thread.appendChild(wrap);
  scrollDown();

  let count = 0;
  const reveal = () => {
    steps.hidden = false;
    summary.textContent = ++count === 1 ? '1 step' : count + ' steps';
    scrollDown();
  };
  return {
    bubble,
    note(text) {
      if (!text.trim()) return null;
      const d = document.createElement('div');
      d.className = 'note';
      d.textContent = text.trim();
      stepBody.appendChild(d);
      reveal();
      return d;
    },
    tool(ev) {
      const c = document.createElement('span');
      c.className = 'tool';
      c.innerHTML = (ev.emoji ? esc(ev.emoji) + ' ' : '') + '<b>' + esc(ev.tool || 'tool') + '</b>' +
                    (ev.label ? ' <span class="lbl">' + esc(String(ev.label).replace(/\s+/g, ' ')) + '</span>' : '');
      stepBody.appendChild(c);
      reveal();
      return c;
    },
  };
}

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

  const turn = addAssistantTurn();
  const bubble = turn.bubble;
  const TYPING = '<span class="typing"><i></i><i></i><i></i></span>';
  bubble.innerHTML = TYPING;
  setStreaming(true);
  controller = new AbortController();

  let seg = '';        // text since the last tool call
  let lastNote = '';   // most recent narration, promoted if the turn ends on a tool call
  let lastNoteEl = null;
  const chips = new Map();
  // Tokens often arrive in TCP-sized dumps. Queue them and drip onto the page
  // so a 10-word packet does not paint as a single jump.
  let pending = '';
  let pumpId = 0;

  const paint = () => {
    if (!seg) { bubble.innerHTML = TYPING; bubble.classList.remove('cursor'); }
    else { bubble.innerHTML = mdLite(seg); bubble.classList.add('cursor'); }
    scrollDown();
  };

  const drainPending = (all) => {
    if (!pending) return;
    if (all) { seg += pending; pending = ''; return; }
    // Empty the current queue over ~12 frames (~200ms), never slower than
    // 3 chars/frame so a long catch-up after a tool call does not crawl.
    const n = pending.length <= 3 ? pending.length
            : Math.max(3, Math.min(24, Math.ceil(pending.length / 12)));
    seg += pending.slice(0, n);
    pending = pending.slice(n);
  };

  const pump = () => {
    pumpId = 0;
    drainPending(false);
    paint();
    if (pending) pumpId = requestAnimationFrame(pump);
  };

  const enqueue = (text) => {
    if (!text) return;
    pending += text;
    if (!pumpId) pumpId = requestAnimationFrame(pump);
  };

  const flushPending = () => {
    if (pumpId) { cancelAnimationFrame(pumpId); pumpId = 0; }
    drainPending(true);
  };

  // A tool call means whatever was streaming was narration, not the answer.
  const cut = () => {
    flushPending();
    if (seg.trim()) { lastNote = seg.trim(); lastNoteEl = turn.note(lastNote); }
    seg = '';
    bubble.classList.remove('cursor');
    bubble.innerHTML = TYPING;
  };

  const settle = () => {
    flushPending();
    // Some models inline their reasoning in content rather than a separate field.
    seg = seg.replace(/<think(?:ing)?>([\s\S]*?)<\/think(?:ing)?>/g, (m, r) => { turn.note(r); return ''; })
             .replace(/<think(?:ing)?>[\s\S]*$/, m => { turn.note(m.replace(/^<think(?:ing)?>/, '')); return ''; });
    bubble.classList.remove('cursor');
    let answer = seg.trim();
    if (!answer && lastNote) {  // turn ended on a tool call — promote its narration
      answer = lastNote;
      if (lastNoteEl) lastNoteEl.remove();
    }
    if (answer) { bubble.innerHTML = mdLite(answer); messages.push({ role: 'assistant', content: answer }); }
    else bubble.innerHTML = '<span style="color:var(--muted)">(no content returned)</span>';
    scrollDown();
    return answer;
  };

  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ messages, session_id: sessionId }),
      signal: controller.signal,
    });
    if (!res.ok) {
      let detail = '';
      try { detail = (await res.json()).error || (await res.text()); } catch (e) {}
      throw new Error(detail || ('HTTP ' + res.status));
    }
    const sid = res.headers.get('X-Hermes-Session-Id') || res.headers.get('X-Chat-ID');
    if (sid) setSession(sid);
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
          if (j.tool) {  // Hermes tool-activity event, not an OpenAI chunk
            if (j.status === 'completed') {
              const c = chips.get(j.toolCallId);
              if (c) c.classList.add('done');
            } else {
              cut();
              chips.set(j.toolCallId, turn.tool(j));
            }
            continue;
          }
          const delta = j.choices && j.choices[0] && j.choices[0].delta;
          if (!delta) continue;
          if (delta.reasoning_content) turn.note(delta.reasoning_content);
          if (delta.content) enqueue(delta.content);
        } catch (e) { /* ignore keep-alive / partial */ }
      }
    }
    settle();
  } catch (err) {
    if (pumpId) { cancelAnimationFrame(pumpId); pumpId = 0; }
    if (err.name === 'AbortError') {
      const answer = settle();
      if (answer) bubble.innerHTML += '<div style="color:var(--muted);font-size:12px">⏹ stopped</div>';
      else { bubble.classList.add('err'); bubble.textContent = '⏹ stopped'; }
    } else {
      bubble.classList.remove('cursor');
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
newChatBtn.addEventListener('click', async () => {
  if (streaming) stop();
  newChatBtn.disabled = true;
  try {
    const res = await fetch('/api/chat/new', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: sessionId }),
    });
    const data = await res.json().catch(() => ({}));
    setSession(data.session_id || null);
  } catch (e) {
    setSession(null);
  }
  messages = [];
  thread.querySelectorAll('.msg').forEach(m => m.remove());
  emptyEl.style.display = '';
  newChatBtn.disabled = false;
  input.focus();
});
fetch('/api/chat/new', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({}),
}).then(r => r.json()).then(data => { if (data.session_id) setSession(data.session_id); }).catch(() => {});
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
    <a class="chip navlink" href="/dashboard">dashboard</a>
    <a class="chip navlink" href="/chat">chat</a>
    <a class="chip navlink active" href="/hermes">hermes</a>
    <a class="chip navlink" href="/">traces</a>
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


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Dashboard</title>
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
  .spacer { flex: 1; }
  .nav { display: flex; gap: 4px; }
  .chip { cursor: pointer; user-select: none; padding: 4px 10px; border-radius: 6px; font-size: 12px;
          border: 1px solid var(--border); background: var(--panel); color: var(--muted); }
  .chip:hover { color: var(--text); }
  a.chip { text-decoration: none; display: inline-flex; align-items: center; }
  .navlink.active { background: var(--accent); color: #fff; border-color: transparent; }
  main { flex: 1; min-height: 0; overflow: auto; padding: 22px 20px; }
  .dash-wrap { max-width: 1120px; margin: 0 auto; }
  .dash-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
  .dash-title { font-size: 18px; font-weight: 700; letter-spacing: .2px; }
  .dash-sub { color: var(--muted); font-size: 13px; margin-top: 2px; }
  .dash-status { display: flex; align-items: center; gap: 7px; color: var(--muted); font-size: 12px; white-space: nowrap; }
  .dash-status .sdot { width: 9px; height: 9px; border-radius: 50%; background: var(--muted); }
  .dash-status .sdot.live { background: #2ec07a; box-shadow: 0 0 8px #2ec07a; }
  .dash-status .sdot.error { background: var(--err); }
  .dash-cols { display: grid; grid-template-columns: 1fr 330px; gap: 18px; align-items: start; }
  @media (max-width: 860px) { .dash-cols { grid-template-columns: 1fr; } }
  .col-h { font-size: 11px; text-transform: uppercase; letter-spacing: .7px; color: var(--muted);
           margin: 0 2px 10px; display: flex; align-items: center; gap: 8px; }
  .pill { background: var(--panel2); border: 1px solid var(--border); border-radius: 20px; padding: 1px 8px; font-size: 11px; color: var(--text); }
  .mem-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 14px; }
  .mem-card { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; }
  .mem-card .mc-head { display: flex; align-items: center; justify-content: space-between; gap: 10px;
                       padding: 9px 13px; border-bottom: 1px solid var(--border); background: var(--panel2); }
  .mem-card .mc-name { font-weight: 700; font-size: 13px; display: flex; align-items: center; gap: 7px; }
  .mem-card .mc-name .ic { color: var(--accent); }
  .mem-card .mc-time { color: var(--muted); font-size: 11px; white-space: nowrap; }
  .mem-card .mc-body { padding: 4px 14px 8px; }
  .mem-item { padding: 9px 0; border-top: 1px dashed var(--border); }
  .mem-item:first-child { border-top: 0; }
  .mem-item.i-empty { color: var(--muted); font-style: italic; }
  .mem-card.changed { animation: mpulse 1.8s ease; }
  @keyframes mpulse { 0% { box-shadow: 0 0 0 2px var(--accent); } 100% { box-shadow: 0 0 0 2px transparent; } }
  .act-feed { display: flex; flex-direction: column; gap: 3px; }
  .act-row { display: flex; flex-direction: column; border-radius: 8px; border: 1px solid transparent; }
  .act-row:hover, .act-row.open { background: var(--panel); border-color: var(--border); }
  .act-top { display: flex; gap: 10px; align-items: center; padding: 8px 10px; }
  .act-row.has-diff .act-top { cursor: pointer; }
  .act-row .a-ic { width: 22px; height: 22px; border-radius: 6px; display: grid; place-items: center;
                   font-size: 12px; flex: none; background: var(--panel2); border: 1px solid var(--border); }
  .act-row .a-main { flex: 1; min-width: 0; }
  .act-row .a-name { font-weight: 600; font-size: 12.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .act-row .a-meta { color: var(--muted); font-size: 11px; }
  .a-delta { font-size: 11px; white-space: nowrap; font-family: "SF Mono",ui-monospace,Menlo,Consolas,monospace; }
  .a-delta .add { color: #2ec07a; } .a-delta .del { color: var(--err); }
  .a-caret { color: var(--muted); font-size: 10px; width: 10px; text-align: center; transition: transform .15s ease; }
  .act-row.open .a-caret { transform: rotate(90deg); }
  .a-diff { margin: 0; padding: 6px 10px 10px 42px; border-top: 1px solid var(--border);
            font-family: "SF Mono",ui-monospace,Menlo,Consolas,monospace; font-size: 11.5px; line-height: 1.55; overflow-x: auto; }
  .a-diff .dl { display: block; white-space: pre-wrap; word-break: break-word; }
  .a-diff .dl.add { color: #2ec07a; } .a-diff .dl.del { color: var(--err); }
  .a-diff .dl.hunk { color: var(--accent); } .a-diff .dl.ctx { color: var(--muted); opacity: .85; }
  .act-row.added .a-ic { color: #2ec07a; } .act-row.updated .a-ic { color: var(--accent); }
  .act-row.removed .a-ic { color: var(--err); } .act-row.recorded .a-ic { color: var(--muted); }
  .empty { color: var(--muted); text-align: center; border: 1px dashed var(--border); border-radius: 12px; padding: 40px 18px; }
  .md :first-child { margin-top: 0; } .md :last-child { margin-bottom: 0; }
  .md h1, .md h2, .md h3, .md h4 { font-size: 13.5px; margin: 10px 0 4px; }
  .md p { margin: 6px 0; } .md ul, .md ol { margin: 6px 0; padding-left: 20px; }
  .md li { margin: 2px 0; }
  .md code { background: var(--panel2); border: 1px solid var(--border); border-radius: 4px; padding: 1px 5px;
             font-family: "SF Mono",ui-monospace,Menlo,Consolas,monospace; font-size: 12px; }
  .md strong { color: var(--text); font-weight: 700; }
  /* skills */
  .dash-sep { height: 1px; background: var(--border); border: 0; margin: 28px 0 22px; }
  .sk-list { display: flex; flex-direction: column; gap: 6px; }
  .sk-row { border: 1px solid var(--border); border-radius: 10px; background: var(--panel); overflow: hidden; }
  .sk-row.open { border-color: var(--accent); }
  .sk-top { display: flex; align-items: center; gap: 10px; padding: 9px 12px; cursor: pointer; }
  .sk-caret { color: var(--muted); font-size: 10px; width: 10px; text-align: center; transition: transform .15s ease; flex: none; }
  .sk-row.open .sk-caret { transform: rotate(90deg); }
  .sk-main { flex: 1; min-width: 0; }
  .sk-name { font-weight: 600; font-size: 13px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .sk-cat { color: var(--muted); font-size: 11px; font-weight: 500; background: var(--panel2);
            border: 1px solid var(--border); border-radius: 5px; padding: 0 6px; }
  .sk-meta { margin-top: 4px; display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
  .sk-upd { color: var(--muted); font-size: 11px; }
  .sk-badge { font-size: 10px; font-weight: 600; letter-spacing: .3px; text-transform: uppercase;
              border-radius: 5px; padding: 1px 6px; border: 1px solid var(--border); color: var(--muted); }
  .sk-badge.custom { color: var(--accent); border-color: var(--accent); }
  .sk-badge.patched { color: #e0a800; border-color: #e0a800; }
  .sk-badge.used { color: #2ec07a; border-color: #2ec07a; }
  .sk-body { padding: 4px 14px 12px 32px; border-top: 1px solid var(--border); max-height: 380px; overflow: auto; }
  .sk-loading { color: var(--muted); padding: 10px 0; font-style: italic; }
  .act-row.patched .a-ic { color: var(--accent); } .act-row.added .a-ic { color: #2ec07a; }
</style>
</head>
<body>
<header>
  <h1>Dashboard</h1>
  <nav class="nav">
    <a class="chip navlink active" href="/dashboard">dashboard</a>
    <a class="chip navlink" href="/chat">chat</a>
    <a class="chip navlink" href="/hermes">hermes</a>
    <a class="chip navlink" href="/">traces</a>
  </nav>
  <div class="spacer"></div>
  <button class="chip" id="themeToggle" title="toggle light / dark">🌙 dark</button>
</header>
<main>
  <div class="dash-wrap">
    <div class="dash-head">
      <div>
        <div class="dash-title">Agent memory</div>
        <div class="dash-sub">what the agent remembers, and what it has learned over time</div>
      </div>
      <div class="dash-status"><span class="sdot" id="memDot"></span><span id="memStat">connecting…</span></div>
    </div>
    <div class="dash-cols">
      <section>
        <div class="col-h">Memories <span class="pill" id="memCount">0</span></div>
        <div id="memGrid" class="mem-grid"><div class="empty">Loading…</div></div>
      </section>
      <aside>
        <div class="col-h">Learning activity</div>
        <div id="actFeed" class="act-feed"><div class="empty">Loading…</div></div>
      </aside>
    </div>

    <hr class="dash-sep" />

    <div class="dash-head">
      <div>
        <div class="dash-title">Agent Custom Skills</div>
        <div class="dash-sub">the skills configured for this agent, ordered by usage — and how each one evolves over time</div>
      </div>
      <div class="dash-status"><span class="sdot" id="skDot"></span><span id="skStat">connecting…</span></div>
    </div>
    <div class="dash-cols">
      <section>
        <div class="col-h">Skills <span class="pill" id="skCount">0</span></div>
        <div id="skList" class="sk-list"><div class="empty">Loading…</div></div>
      </section>
      <aside>
        <div class="col-h">Skill evolution</div>
        <div id="skFeed" class="act-feed"><div class="empty">Loading…</div></div>
      </aside>
    </div>
  </div>
</main>
<script>
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

// ---- Agent memory ----
function esc(s){ return String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function mdLite(text){
  const lines = esc(text).split(/\r?\n/);
  let html = '', inUl = false, inOl = false;
  const closeL = () => { if (inUl) { html += '</ul>'; inUl = false; } if (inOl) { html += '</ol>'; inOl = false; } };
  const inl = s => s.replace(/`([^`]+)`/g, '<code>$1</code>').replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  for (const raw of lines) {
    const h = raw.match(/^\s*(#{1,6})\s+(.*)$/);
    if (h) { closeL(); const lv = Math.min(h[1].length, 4); html += '<h' + lv + '>' + inl(h[2]) + '</h' + lv + '>'; continue; }
    const ol = raw.match(/^\s*\d+\.\s+(.*)/), ul = raw.match(/^\s*[-*]\s+(.*)/);
    if (ol) { if (!inOl) { closeL(); html += '<ol>'; inOl = true; } html += '<li>' + inl(ol[1]) + '</li>'; continue; }
    if (ul) { if (!inUl) { closeL(); html += '<ul>'; inUl = true; } html += '<li>' + inl(ul[1]) + '</li>'; continue; }
    closeL(); if (raw.trim() !== '') html += '<p>' + inl(raw) + '</p>';
  }
  closeL(); return html;
}
function relTime(ts){
  const s = Math.max(0, Date.now()/1000 - ts);
  if (s < 45) return 'just now';
  if (s < 5400) return Math.max(1, Math.round(s/60)) + 'm ago';
  if (s < 86400) return Math.round(s/3600) + 'h ago';
  return Math.round(s/86400) + 'd ago';
}
const memGrid = document.getElementById('memGrid'), actFeed = document.getElementById('actFeed');
const memCount = document.getElementById('memCount'), memDot = document.getElementById('memDot'), memStat = document.getElementById('memStat');
const skList = document.getElementById('skList'), skFeed = document.getElementById('skFeed');
const skCount = document.getElementById('skCount'), skDot = document.getElementById('skDot'), skStat = document.getElementById('skStat');
const prevContent = {};
const memOpen = new Set();   // memory event seqs whose diff is expanded (kept across polls)
const skOpen = new Set();    // skill-evolution event seqs whose diff is expanded

function diffHtml(diff){
  return diff.map(ln => {
    const c = ln.charAt(0);
    const cls = c === '+' ? 'add' : c === '-' ? 'del' : c === '@' ? 'hunk' : 'ctx';
    return '<span class="dl ' + cls + '">' + esc(ln) + '</span>';
  }).join('');
}
function setStatus(dot, stat, d){
  dot.className = 'sdot ' + (d.status === 'live' ? 'live' : (d.status === 'error' ? 'error' : ''));
  stat.textContent = d.status === 'error' ? ('error: ' + (d.error || '')) : (d.status === 'live' ? 'live' : 'connecting…');
}

// Shared git-style activity feed, used by both memory learning and skill evolution.
function renderFeed(container, events, openSet, cfg){
  if (!events.length) { container.innerHTML = '<div class="empty">' + cfg.empty + '</div>'; return; }
  container.innerHTML = '';
  for (const e of events) {
    const hasDiff = !!(e.diff && e.diff.length);
    const open = openSet.has(e.seq);
    const row = document.createElement('div');
    row.className = 'act-row ' + e.action + (hasDiff ? ' has-diff' : '') + (open ? ' open' : '');
    const delta = (e.add || e.del)
      ? '<span class="a-delta">' + (e.add ? '<span class="add">+' + e.add + '</span> ' : '') +
        (e.del ? '<span class="del">\u2212' + e.del + '</span>' : '') + '</span>'
      : '';
    const caret = hasDiff ? '<span class="a-caret">\u25B8</span>' : '';
    row.innerHTML =
      '<div class="act-top">' +
        '<div class="a-ic">' + (cfg.icon[e.action] || '•') + '</div>' +
        '<div class="a-main"><div class="a-name">' + esc(cfg.nameOf(e)) + '</div>' +
        '<div class="a-meta">' + (cfg.verb[e.action] || e.action) + ' · ' + relTime(e.ts) + '</div></div>' +
        delta + caret +
      '</div>' +
      (hasDiff ? '<div class="a-diff"' + (open ? '' : ' hidden') + '>' + diffHtml(e.diff) + '</div>' : '');
    if (hasDiff) {
      row.querySelector('.act-top').addEventListener('click', () => {
        const nowOpen = !openSet.has(e.seq);
        if (nowOpen) openSet.add(e.seq); else openSet.delete(e.seq);
        row.classList.toggle('open', nowOpen);
        const dd = row.querySelector('.a-diff');
        if (dd) dd.hidden = !nowOpen;
      });
    }
    container.appendChild(row);
  }
}
const MEM_CFG = {
  icon: { added: '+', updated: '✎', removed: '−', recorded: '•' },
  verb: { added: 'learned', updated: 'updated', removed: 'forgot', recorded: 'recorded' },
  empty: 'No activity yet.',
  nameOf: e => e.name.replace(/\.md$/, ''),
};
const SK_CFG = {
  icon: { added: '+', patched: '✎', removed: '−' },
  verb: { added: 'added', patched: 'patched', removed: 'removed' },
  empty: 'No skill changes yet — when a skill is added or patched, the diff shows up here.',
  nameOf: e => e.id,
};

// ---- Agent memory ----
async function pollMem(){
  let d;
  try { const r = await fetch('/api/memories', { cache: 'no-store' }); d = await r.json(); }
  catch (e) { memDot.className = 'sdot error'; memStat.textContent = 'disconnected'; return; }
  setStatus(memDot, memStat, d);

  // Hermes stores multiple memory entries per file, separated by a line of "§".
  const splitEntries = c => (c || '').split(/^\s*§\s*$/m).map(s => s.trim()).filter(Boolean);

  const mems = d.memories || [];
  const totalEntries = mems.reduce((n, m) => n + splitEntries(m.content).length, 0);
  memCount.textContent = totalEntries;
  if (!mems.length) {
    memGrid.innerHTML = '<div class="empty">No memories yet — the agent hasn\'t written anything to remember.</div>';
  } else {
    memGrid.innerHTML = '';
    for (const m of mems) {
      const changed = prevContent[m.name] !== undefined && prevContent[m.name] !== m.content;
      const title = m.name.replace(/\.md$/, '');
      const entries = splitEntries(m.content);
      const body = entries.length
        ? entries.map(e => '<div class="mem-item"><div class="md">' + mdLite(e) + '</div></div>').join('')
        : '<div class="mem-item i-empty">empty</div>';
      const card = document.createElement('div');
      card.className = 'mem-card' + (changed ? ' changed' : '');
      card.innerHTML =
        '<div class="mc-head"><div class="mc-name"><span class="ic">▤</span>' + esc(title) + '</div>' +
        '<div class="mc-time" title="' + esc(m.name) + '">' + entries.length + ' ' +
        (entries.length === 1 ? 'entry' : 'entries') + ' · updated ' + relTime(m.mtime) + '</div></div>' +
        '<div class="mc-body">' + body + '</div>';
      memGrid.appendChild(card);
      prevContent[m.name] = m.content;
    }
    const names = new Set(mems.map(m => m.name));
    Object.keys(prevContent).forEach(k => { if (!names.has(k)) delete prevContent[k]; });
  }
  renderFeed(actFeed, d.events || [], memOpen, MEM_CFG);
}

// ---- Agent skills ----
const skContentCache = {};    // id -> SKILL.md text (fetched lazily on expand)
const skBodyOpen = new Set(); // skill ids whose SKILL.md is expanded
async function fetchSkill(id){
  if (skContentCache[id] !== undefined) return skContentCache[id];
  try {
    const r = await fetch('/api/skill?id=' + encodeURIComponent(id), { cache: 'no-store' });
    const j = await r.json();
    skContentCache[id] = (j && typeof j.content === 'string') ? j.content : null;
  } catch (e) { skContentCache[id] = null; }
  return skContentCache[id];
}
async function renderSkillBody(bodyEl, id){
  bodyEl.innerHTML = '<div class="sk-loading">Loading…</div>';
  const c = await fetchSkill(id);
  bodyEl.innerHTML = c === null ? '<div class="sk-loading">Failed to load SKILL.md.</div>'
                                : '<div class="md">' + mdLite(c) + '</div>';
}
function skBadges(s){
  let b = '';
  if (s.patched) b += '<span class="sk-badge patched">patched ' + s.patched + '\u00d7</span>';
  b += '<span class="sk-badge used">used ' + (s.used || 0) + '\u00d7</span>';
  return b;
}
async function pollSkills(){
  let d;
  try { const r = await fetch('/api/skills', { cache: 'no-store' }); d = await r.json(); }
  catch (e) { skDot.className = 'sdot error'; skStat.textContent = 'disconnected'; return; }
  setStatus(skDot, skStat, d);

  const list = d.skills || [];
  skCount.textContent = list.length;

  if (!list.length) {
    skList.innerHTML = '<div class="empty">No custom skills found — set HERMES_CUSTOM_SKILLS in docker-compose to the skills you care about.</div>';
  } else {
    skList.innerHTML = '';
    for (const s of list) {
      const open = skBodyOpen.has(s.id);
      const cat = s.category ? '<span class="sk-cat">' + esc(s.category) + '</span>' : '';
      const row = document.createElement('div');
      row.className = 'sk-row' + (open ? ' open' : '');
      row.innerHTML =
        '<div class="sk-top">' +
          '<span class="sk-caret">\u25B8</span>' +
          '<div class="sk-main"><div class="sk-name">' + esc(s.name) + cat + '</div>' +
          '<div class="sk-meta">' + skBadges(s) + '<span class="sk-upd">updated ' + relTime(s.mtime) + '</span></div>' +
          '</div>' +
        '</div>' +
        '<div class="sk-body"' + (open ? '' : ' hidden') + '></div>';
      const bodyEl = row.querySelector('.sk-body');
      if (open) renderSkillBody(bodyEl, s.id);
      row.querySelector('.sk-top').addEventListener('click', () => {
        const nowOpen = !skBodyOpen.has(s.id);
        if (nowOpen) skBodyOpen.add(s.id); else skBodyOpen.delete(s.id);
        row.classList.toggle('open', nowOpen);
        bodyEl.hidden = !nowOpen;
        if (nowOpen) renderSkillBody(bodyEl, s.id);
      });
      skList.appendChild(row);
    }
  }
  renderFeed(skFeed, d.events || [], skOpen, SK_CFG);
}

async function tick(){
  await Promise.allSettled([pollMem(), pollSkills()]);
  setTimeout(tick, 3500);
}
tick();
</script>
</body>
</html>
"""


def _env(name: str, default: str) -> str:
    val = os.environ.get(name)
    return val if val not in (None, "") else default


def main() -> None:
    global SANDBOX, ATOF_DIR, MEMORIES_DIR, SKILLS_DIR, CUSTOM_SKILLS, STATE_DIR, POLL_INTERVAL, HERMES_API_HOST, HERMES_API_PORT, HERMES_DASHBOARD_PORT
    # Env vars provide the defaults (convenient for docker-compose); explicit
    # CLI flags still win over them.
    parser = argparse.ArgumentParser(description="Live ATOF trace dashboard + Hermes chat")
    parser.add_argument("--sandbox", default=_env("ATOF_SANDBOX", SANDBOX),
                        help="nemoclaw sandbox name [env ATOF_SANDBOX]")
    parser.add_argument("--dir", default=_env("ATOF_DIR", ATOF_DIR),
                        help="atof directory inside the sandbox [env ATOF_DIR]")
    parser.add_argument("--memories-dir", default=_env("HERMES_MEMORIES_DIR", MEMORIES_DIR),
                        help="agent memory directory inside the sandbox [env HERMES_MEMORIES_DIR]")
    parser.add_argument("--skills-dir", default=_env("HERMES_SKILLS_DIR", SKILLS_DIR),
                        help="agent skills directory inside the sandbox [env HERMES_SKILLS_DIR]")
    parser.add_argument("--custom-skills", default=_env("HERMES_CUSTOM_SKILLS", ""),
                        help="comma/space-separated skills to show under 'Agent Custom Skills' "
                             "(dir id or leaf name under the skills root); empty keeps the built-in "
                             "default set [env HERMES_CUSTOM_SKILLS]")
    parser.add_argument("--state-dir", default=_env("DASHBOARD_STATE_DIR", ""),
                        help="directory to persist the memory/skill activity feeds so they "
                             "survive a restart; empty disables it [env DASHBOARD_STATE_DIR]")
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
    MEMORIES_DIR = args.memories_dir.rstrip("/")
    SKILLS_DIR = args.skills_dir.rstrip("/")
    _custom = args.custom_skills.replace(",", " ").split()
    if _custom:
        CUSTOM_SKILLS = set(_custom)
    STATE_DIR = args.state_dir.rstrip("/")
    POLL_INTERVAL = args.interval
    HERMES_API_HOST = args.hermes_api_host
    HERMES_API_PORT = args.hermes_api_port
    HERMES_DASHBOARD_PORT = args.hermes_dashboard_port

    token = get_gateway_token()
    if STATE_DIR:
        MEMORY_STORE.load("memory_feed.json")
        SKILL_STORE.load("skill_feed.json")
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=memory_poll_loop, daemon=True).start()
    threading.Thread(target=skill_poll_loop, daemon=True).start()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}"
    print(f"ATOF dashboard streaming '{SANDBOX}:{ATOF_DIR}'")
    print(f"  -> traces: {url}/")
    print(f"  -> chat:   {url}/chat  (agent '{HERMES_MODEL}' via port {HERMES_API_PORT}, "
          f"token {'ok' if token else 'MISSING'})")
    print(f"  -> hermes: {url}/hermes  (embeds native dashboard on port {HERMES_DASHBOARD_PORT})")
    print(f"  -> dashboard: {url}/dashboard")
    print(f"  -> feeds persisted to: {STATE_DIR}" if STATE_DIR else "  -> feeds in-memory only (set DASHBOARD_STATE_DIR to persist)")
    print("  (Ctrl-C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")
        server.shutdown()


if __name__ == "__main__":
    main()
