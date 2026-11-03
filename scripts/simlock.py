#!/usr/bin/env python3
"""simlock — cooperative exclusive leases for CoreSimulator devices.

This tool lets several Claude Code sessions (or shells) share the simulators
managed by Xcode's DeviceHub / CoreSimulator.  It is a *cooperative* advisory
lock: it does not stop DeviceHub.app, Xcode, or any tool that does not use
simlock from touching a device.

Design
------
* One lease per device, stored as JSON under ``${SIMLOCK_HOME:-~/.simlock}``.
* All read/modify/write cycles run under ``fcntl.flock`` on a *stable*
  ``control.lock`` inode.  The state itself is replaced atomically with
  ``os.replace``, so readers never observe a partial document and the lock
  inode is never swapped out from under a waiter.
* Waiters join a FIFO queue inside the state document.  Only the queue head
  may claim a free/reclaimable lease, so waiters do not stampede and order is
  preserved.
* A lease is reclaimable when its owner process is gone (PID + process start
  time match) or when its TTL has elapsed.  TTL is the backstop for a hung
  owner: keep long device work under ``--ttl 3600`` or call ``renew``.
* Python 3.9, standard library only.

Exit codes
----------
0   success
2   busy (--no-wait / --wait-timeout)
3   device not found, ambiguous, or unavailable
4   not the lease owner
5   state directory, state file, lock, or simctl error
64  usage error
130 interrupted
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import getpass
import json
import os
import random
import re
import secrets
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

SCHEMA_VERSION = 1
DEFAULT_TTL = 900
MAX_TTL = 14400
DEFAULT_LOCK_TIMEOUT = 10.0
POLL_MIN = 0.5
POLL_MAX = 2.0
PROGRESS_EVERY = 5.0
TMP_MAX_AGE = 3600.0

UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

EXIT_OK = 0
EXIT_BUSY = 2
EXIT_DEVICE = 3
EXIT_NOT_OWNER = 4
EXIT_STATE = 5
EXIT_USAGE = 64
EXIT_INTERRUPTED = 130


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------


class SimlockError(Exception):
    def __init__(self, code, message, exit_code=EXIT_STATE, hint=None,
                 candidates=None, extra=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.exit_code = exit_code
        self.hint = hint
        self.candidates = candidates or []
        self.extra = extra or {}

    def payload(self):
        err = {"code": self.code, "message": self.message}
        if self.hint:
            err["hint"] = self.hint
        if self.candidates:
            err["candidates"] = self.candidates
        if self.extra:
            err.update(self.extra)
        return err


class UsageError(SimlockError):
    def __init__(self, message):
        super().__init__("USAGE", message, EXIT_USAGE)


class Interrupted(Exception):
    pass


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------


def iso(epoch):
    if not epoch:
        return None
    try:
        return datetime.fromtimestamp(float(epoch), timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except (OverflowError, OSError, ValueError):
        return None


def short_id(value, length=8):
    if not value:
        return "-"
    value = str(value)
    if len(value) <= 16:
        return value
    return value[:length]


def fmt_duration(seconds):
    try:
        seconds = max(0, int(round(float(seconds))))
    except (TypeError, ValueError):
        return "-"
    if seconds >= 3600:
        return "%dh%02dm" % (seconds // 3600, (seconds % 3600) // 60)
    if seconds >= 60:
        return "%dm%02ds" % (seconds // 60, seconds % 60)
    return "%ds" % seconds


def hostname():
    try:
        return socket.gethostname()
    except OSError:
        return "unknown"


def username():
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


def log_event(udid, event, session_id, detail=None):
    """Best-effort append-only audit log; never breaks a command."""
    try:
        directory = device_dir(udid)
        os.makedirs(directory, mode=0o700, exist_ok=True)
        record = {
            "ts": time.time(),
            "ts_iso": iso(time.time()),
            "event": event,
            "session_id": session_id,
            "host": hostname(),
            "detail": detail or {},
        }
        data = (json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        fd = os.open(
            os.path.join(directory, "events.jsonl"),
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_CLOEXEC,
            0o600,
        )
        try:
            os.write(fd, data.encode("utf-8"))
        finally:
            os.close(fd)
    except OSError:
        pass


# --------------------------------------------------------------------------
# State layout
# --------------------------------------------------------------------------


def home_root():
    root = os.environ.get("SIMLOCK_HOME") or os.path.expanduser("~/.simlock")
    return os.path.abspath(os.path.expanduser(root))


def ensure_home():
    root = home_root()
    try:
        os.makedirs(root, mode=0o700, exist_ok=True)
        st = os.stat(root)
        if st.st_uid != os.getuid():
            raise SimlockError(
                "STATE_DIR_ERROR",
                "%s is owned by another user" % root,
                EXIT_STATE,
                hint="Set SIMLOCK_HOME to a directory you own.",
            )
        if st.st_mode & 0o077:
            try:
                os.chmod(root, 0o700)
            except OSError:
                pass
        os.makedirs(os.path.join(root, "devices"), mode=0o700, exist_ok=True)
    except SimlockError:
        raise
    except OSError as exc:
        raise SimlockError(
            "STATE_DIR_ERROR",
            "cannot use state directory %s: %s" % (root, exc),
            EXIT_STATE,
        )
    return root


def device_dir(udid):
    return os.path.join(home_root(), "devices", udid.upper())


def state_path(udid):
    return os.path.join(device_dir(udid), "state.json")


def state_udids():
    root = os.path.join(home_root(), "devices")
    try:
        names = os.listdir(root)
    except OSError:
        return []
    found = []
    for name in names:
        if not UUID_RE.match(name):
            continue
        path = os.path.join(root, name)
        if os.path.isdir(path) and os.path.exists(
            os.path.join(path, "state.json")
        ):
            found.append(name.upper())
    return sorted(found)


def default_state(udid, dev=None):
    return {
        "schema_version": SCHEMA_VERSION,
        "udid": udid.upper(),
        "device_name": (dev or {}).get("name", ""),
        "runtime_id": (dev or {}).get("runtime_id", ""),
        "seq_counter": 0,
        "lease": None,
        "queue": [],
        "updated_at": time.time(),
    }


def read_state(udid):
    path = state_path(udid)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return default_state(udid)
    except (OSError, ValueError) as exc:
        raise SimlockError(
            "STATE_CORRUPT",
            "cannot read %s: %s" % (path, exc),
            EXIT_STATE,
            hint="Inspect or move the file aside; `simlock gc --dry-run` lists "
                 "state directories.",
        )
    if not isinstance(data, dict):
        raise SimlockError("STATE_CORRUPT", "%s is not a JSON object" % path,
                           EXIT_STATE)
    if data.get("schema_version") != SCHEMA_VERSION:
        raise SimlockError(
            "STATE_CORRUPT",
            "%s has unsupported schema_version %r" % (
                path, data.get("schema_version")),
            EXIT_STATE,
        )
    if not isinstance(data.get("queue"), list):
        data["queue"] = []
    return data


def atomic_write_json(path, obj):
    directory = os.path.dirname(path) or "."
    data = (json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n").encode("utf-8")
    fd, tmp = tempfile.mkstemp(prefix=".state.tmp-", dir=directory)
    try:
        os.fchmod(fd, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(fd, view)
            view = view[written:]
        try:
            fcntl.fcntl(fd, getattr(fcntl, "F_FULLFSYNC", 51))
        except OSError:
            os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(tmp, path)
        try:
            dfd = os.open(directory, os.O_RDONLY | os.O_CLOEXEC)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
    finally:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            if os.path.exists(tmp):
                os.unlink(tmp)
        except OSError:
            pass


@contextlib.contextmanager
def device_lock(udid, timeout=None):
    """Exclusive flock on <device>/control.lock, a stable inode.

    control.lock is intentionally never unlinked: flock follows the inode,
    and replacing it would give two processes locks on different files.
    """
    if timeout is None:
        try:
            timeout = float(os.environ.get("SIMLOCK_LOCK_TIMEOUT",
                                           DEFAULT_LOCK_TIMEOUT))
        except ValueError:
            timeout = DEFAULT_LOCK_TIMEOUT
    directory = device_dir(udid)
    try:
        os.makedirs(directory, mode=0o700, exist_ok=True)
    except OSError as exc:
        raise SimlockError(
            "STATE_DIR_ERROR",
            "cannot create %s: %s" % (directory, exc),
            EXIT_STATE,
        )
    lock_path = os.path.join(directory, "control.lock")
    try:
        fd = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
    except OSError as exc:
        raise SimlockError(
            "STATE_DIR_ERROR",
            "cannot open %s: %s" % (lock_path, exc),
            EXIT_STATE,
        )
    deadline = time.monotonic() + max(0.001, timeout)
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EACCES,
                                     errno.EWOULDBLOCK):
                    raise
                if time.monotonic() >= deadline:
                    raise SimlockError(
                        "LOCK_TIMEOUT",
                        "timed out waiting for the state lock of %s" % udid,
                        EXIT_STATE,
                        hint="Another simlock process may be stuck; retry.",
                    )
                time.sleep(0.01 + random.random() * 0.02)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass


# --------------------------------------------------------------------------
# Process liveness (PID reuse aware)
# --------------------------------------------------------------------------


def _ps_info(pid):
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    env["TZ"] = "UTC"
    try:
        proc = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart=,stat="],
            capture_output=True,
            text=True,
            timeout=5,
            env=env,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    line = proc.stdout.strip()
    if proc.returncode != 0 or not line:
        return None
    parts = line.split()
    if len(parts) < 2:
        return None
    stat = parts[-1]
    start = " ".join(parts[:-1])
    return {
        "token": "ps:" + start,
        "zombie": stat.startswith("Z"),
        "stat": stat,
    }


def pid_probe(pid, stored_token=None):
    """Return {alive, verified, reason, token} without ever guessing death.

    verified=True means we positively identified the process (or its absence)
    via ps.  verified=False means we could not inspect it (permissions, ps
    failure); callers treat that as alive to avoid stealing a live lease.
    """
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        pid = None
    if not pid or pid <= 1:
        return {"alive": False, "verified": True, "reason": "no_pid",
                "token": None}

    info = _ps_info(pid)
    if info is not None:
        if info["zombie"]:
            return {"alive": False, "verified": True, "reason": "zombie",
                    "token": info["token"]}
        if (stored_token and info["token"]
                and str(stored_token).startswith("ps:")
                and info["token"] != stored_token):
            return {"alive": False, "verified": True, "reason": "pid_reused",
                    "token": info["token"]}
        return {"alive": True, "verified": True, "reason": "ps",
                "token": info["token"]}

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return {"alive": False, "verified": True, "reason": "no_such_pid",
                "token": None}
    except PermissionError:
        return {"alive": True, "verified": False, "reason": "permission",
                "token": None}
    except OSError:
        return {"alive": True, "verified": False, "reason": "unknown",
                "token": None}
    return {"alive": True, "verified": False, "reason": "kill0",
            "token": None}


def check_owner(lease):
    """Liveness of the lease owner, honouring the cross-host rule."""
    if not lease:
        return {"alive": False, "verified": True, "reason": "no_lease"}
    if lease.get("host") and lease.get("host") != hostname():
        return {"alive": True, "verified": False, "reason": "remote_host"}
    return pid_probe(lease.get("owner_pid"), lease.get("owner_pid_start"))


# --------------------------------------------------------------------------
# Device discovery / resolution
# --------------------------------------------------------------------------


def _runtime_display(runtime_id):
    value = runtime_id or ""
    prefix = "com.apple.CoreSimulator.SimRuntime."
    if value.startswith(prefix):
        value = value[len(prefix):]
    match = re.match(r"^([A-Za-z]+)-(\d+)-(\d+)$", value)
    if match:
        name, major, minor = match.groups()
        if name == "xrOS":
            name = "visionOS"
        return "%s %s.%s" % (name, major, minor)
    return value


def simctl_base():
    raw = os.environ.get("SIMLOCK_SIMCTL")
    if raw:
        return shlex.split(raw)
    return ["xcrun", "simctl"]


def load_devices():
    """Return a list of simulator dicts.

    SIMLOCK_DEVICES_JSON bypasses simctl, for tests and offline use.
    """
    fixture = os.environ.get("SIMLOCK_DEVICES_JSON")
    if fixture:
        try:
            with open(os.path.expanduser(fixture), "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (OSError, ValueError) as exc:
            raise SimlockError(
                "SIMCTL_ERROR",
                "cannot read SIMLOCK_DEVICES_JSON=%s: %s" % (fixture, exc),
                EXIT_STATE,
            )
    else:
        argv = simctl_base()
        devset = os.environ.get("SIMLOCK_SIMCTL_SET")
        if devset:
            argv = argv + ["--set", devset]
        argv = argv + ["list", "devices", "--json"]
        try:
            proc = subprocess.run(
                argv, capture_output=True, text=True, timeout=20
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise SimlockError(
                "SIMCTL_ERROR",
                "cannot run %s: %s" % (" ".join(argv), exc),
                EXIT_STATE,
                hint="Is Xcode installed and selected? Try `xcrun simctl list`.",
            )
        if proc.returncode != 0:
            raise SimlockError(
                "SIMCTL_ERROR",
                "simctl failed: %s" % (proc.stderr.strip() or
                                        "exit %d" % proc.returncode),
                EXIT_STATE,
            )
        try:
            raw = json.loads(proc.stdout)
        except ValueError as exc:
            raise SimlockError(
                "SIMCTL_ERROR",
                "simctl returned invalid JSON: %s" % exc,
                EXIT_STATE,
            )

    if isinstance(raw, dict) and "devices" in raw:
        raw = raw["devices"]
    if not isinstance(raw, dict):
        raise SimlockError("SIMCTL_ERROR", "unexpected simctl JSON shape",
                           EXIT_STATE)

    devices = []
    for runtime_id, items in raw.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            udid = item.get("udid") or item.get("UDID")
            if not udid:
                continue
            devices.append({
                "udid": str(udid).upper(),
                "name": item.get("name", ""),
                "runtime_id": runtime_id,
                "runtime": _runtime_display(runtime_id),
                "sim_state": item.get("state", "Unknown"),
                "is_available": bool(item.get("isAvailable", True)),
                "device_type": item.get("deviceTypeIdentifier", ""),
                "known": True,
            })
    devices.sort(key=lambda d: (d["name"].lower(), d["runtime"], d["udid"]))
    return devices


def find_device(devices, udid):
    target = udid.upper()
    for dev in devices:
        if dev["udid"] == target:
            return dev
    return None


def unknown_device(udid):
    return {
        "udid": udid.upper(),
        "name": "",
        "runtime_id": "",
        "runtime": "",
        "sim_state": "",
        "is_available": True,
        "device_type": "",
        "known": False,
    }


def _name_candidates(devices, arg, limit=8):
    lowered = arg.lower()
    scored = []
    for dev in devices:
        name = dev["name"].lower()
        if lowered in name or name in lowered:
            scored.append(dev)
    if not scored:
        scored = devices
    return [
        {"udid": d["udid"], "name": d["name"], "runtime": d["runtime"],
         "sim_state": d["sim_state"]}
        for d in scored[:limit]
    ]


def resolve_device(arg, need_available=False, must_exist=True):
    """Resolve a UDID or an exact simulator name.

    must_exist=True requires the UDID to be present in simctl (acquire/run).
    must_exist=False allows a well-formed UDID with state but no live device
    (release/renew/status after a device was deleted).
    """
    if UUID_RE.match(arg):
        udid = arg.upper()
        dev = None
        try:
            dev = find_device(load_devices(), udid)
        except SimlockError:
            if must_exist:
                raise
        if dev is None:
            if must_exist:
                raise SimlockError(
                    "DEVICE_NOT_FOUND",
                    "no simulator with UDID %s" % udid,
                    EXIT_DEVICE,
                    hint="Run `xcrun simctl list devices` or "
                         "`simlock list --json` to see available devices.",
                )
            return unknown_device(udid)
        if need_available and not dev["is_available"]:
            raise SimlockError(
                "DEVICE_UNAVAILABLE",
                "%s (%s) is not available" % (dev["name"], udid),
                EXIT_DEVICE,
                hint="The runtime may be missing; inspect Xcode > Settings > "
                     "Components.",
            )
        return dev

    devices = load_devices()
    matches = [d for d in devices if d["name"].lower() == arg.lower()]
    if not matches:
        raise SimlockError(
            "DEVICE_NOT_FOUND",
            "no simulator named %r" % arg,
            EXIT_DEVICE,
            hint="Names are resolved exactly; use a UDID. Similar devices are "
                 "listed in candidates.",
            candidates=_name_candidates(devices, arg),
        )
    if len(matches) > 1:
        raise SimlockError(
            "DEVICE_AMBIGUOUS",
            "%r matches %d simulators; use a UDID" % (arg, len(matches)),
            EXIT_DEVICE,
            hint="Duplicate simulator names are common.",
            candidates=[
                {"udid": d["udid"], "name": d["name"],
                 "runtime": d["runtime"], "sim_state": d["sim_state"]}
                for d in matches
            ],
        )
    dev = matches[0]
    if need_available and not dev["is_available"]:
        raise SimlockError(
            "DEVICE_UNAVAILABLE",
            "%s (%s) is not available" % (dev["name"], dev["udid"]),
            EXIT_DEVICE,
        )
    return dev


# --------------------------------------------------------------------------
# Identity
# --------------------------------------------------------------------------


def resolve_identity(args):
    sid = getattr(args, "owner", None)
    sid_source = "--owner"
    if not sid:
        sid = os.environ.get("SIMLOCK_OWNER_ID")
        sid_source = "SIMLOCK_OWNER_ID"
    if not sid:
        sid = os.environ.get("CLAUDE_CODE_SESSION_ID")
        sid_source = "CLAUDE_CODE_SESSION_ID"
    if not sid:
        sid = "local:%s:%s:%d" % (username(), hostname(), os.getppid())
        sid_source = "fallback:ppid"

    pid = getattr(args, "owner_pid", None)
    pid_source = "--owner-pid"
    if pid is None:
        raw = os.environ.get("CLAUDE_PID")
        if raw:
            try:
                pid = int(raw)
                pid_source = "CLAUDE_PID"
            except ValueError:
                pid = None
    if pid is None:
        pid = os.getppid()
        pid_source = "ppid"
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        pid = os.getppid()
        pid_source = "ppid"
    if pid <= 1:
        pid = os.getppid()
        pid_source = "ppid"

    return str(sid), pid, {
        "session_source": sid_source,
        "pid_source": pid_source,
        "host": hostname(),
        "user": username(),
    }


# --------------------------------------------------------------------------
# Lease / queue model
# --------------------------------------------------------------------------


def validate_ttl(value):
    if value is None:
        return DEFAULT_TTL
    try:
        value = int(value)
    except (TypeError, ValueError):
        raise UsageError("--ttl must be an integer number of seconds")
    if value <= 0 or value > MAX_TTL:
        raise UsageError("--ttl must be between 1 and %d seconds" % MAX_TTL)
    return value


def make_lease(sid, pid, ttl, label, token, now):
    probe = pid_probe(pid)
    return {
        "token": token,
        "session_id": sid,
        "label": label or "",
        "owner_pid": pid,
        "owner_pid_start": probe.get("token") if probe.get("verified") else None,
        "host": hostname(),
        "user": username(),
        "acquired_at": now,
        "renewed_at": now,
        "expires_at": now + ttl,
        "ttl": ttl,
    }


def classify_lease(lease, now):
    info = check_owner(lease)
    expires = float(lease.get("expires_at") or 0)
    expired = now >= expires
    if not info["alive"]:
        state = "stale"
    elif expired:
        state = "expired"
    else:
        state = "held"
    return {
        "state": state,
        "expired": expired,
        "owner_alive": info["alive"],
        "owner_verified": info["verified"],
        "owner_reason": info.get("reason"),
        "reclaimable": state in ("stale", "expired"),
    }


def live_queue(state):
    entries = []
    for entry in state.get("queue", []):
        if not isinstance(entry, dict):
            continue
        info = pid_probe(entry.get("waiter_pid"),
                         entry.get("waiter_pid_start"))
        if info["alive"]:
            entries.append(entry)
    entries.sort(key=lambda e: e.get("seq", 0))
    return entries


def sweep_queue(state):
    """Drop queue entries whose waiting process is gone. Returns True if changed."""
    queue = state.get("queue", [])
    kept = []
    changed = False
    for entry in queue:
        if not isinstance(entry, dict):
            changed = True
            continue
        info = pid_probe(entry.get("waiter_pid"),
                         entry.get("waiter_pid_start"))
        if info["alive"]:
            kept.append(entry)
        else:
            changed = True
    if changed:
        state["queue"] = kept
    return changed


def can_claim(state, token, sid, force=False):
    if force:
        return True
    queue = live_queue(state)
    if not queue:
        return True
    head = queue[0]
    return head.get("token") == token or head.get("session_id") == sid


def enqueue(state, sid, waiter_pid, token, label, now):
    probe = pid_probe(waiter_pid)
    for entry in state.get("queue", []):
        if not isinstance(entry, dict):
            continue
        if entry.get("session_id") == sid:
            entry["token"] = token
            entry["waiter_pid"] = waiter_pid
            entry["waiter_pid_start"] = (
                probe.get("token") if probe.get("verified") else None
            )
            entry["last_seen"] = now
            if label:
                entry["label"] = label
            return entry
        if entry.get("token") == token:
            entry["last_seen"] = now
            return entry
    state["seq_counter"] = int(state.get("seq_counter") or 0) + 1
    entry = {
        "token": token,
        "seq": state["seq_counter"],
        "session_id": sid,
        "waiter_pid": waiter_pid,
        "waiter_pid_start": probe.get("token") if probe.get("verified") else None,
        "label": label or "",
        "enqueued_at": now,
        "last_seen": now,
    }
    state.setdefault("queue", []).append(entry)
    state["queue"].sort(key=lambda e: e.get("seq", 0))
    return entry


def dequeue_token(state, token):
    queue = state.get("queue", [])
    kept = [e for e in queue if e.get("token") != token]
    changed = len(kept) != len(queue)
    if changed:
        state["queue"] = kept
    return changed


def dequeue_session(state, sid):
    queue = state.get("queue", [])
    kept = [e for e in queue if e.get("session_id") != sid]
    changed = len(kept) != len(queue)
    if changed:
        state["queue"] = kept
    return changed


def refresh_owner(lease, pid):
    """Keep the session's long-lived PID; replace it only if it died."""
    current = pid_probe(lease.get("owner_pid"), lease.get("owner_pid_start"))
    if current["alive"]:
        return
    probe = pid_probe(pid)
    lease["owner_pid"] = pid
    lease["owner_pid_start"] = (
        probe.get("token") if probe.get("verified") else None
    )


def best_effort_dequeue(udid, token, sid):
    """Remove this process's queue entry after a timeout/interrupt.

    Deliberately does not remove other processes' entries for the same
    session: a subagent may share the session id while waiting separately.
    """
    try:
        with device_lock(udid, timeout=2.0):
            state = read_state(udid)
            changed = dequeue_token(state, token)
            me = os.getpid()
            queue = state.get("queue", [])
            kept = [e for e in queue
                    if not (e.get("session_id") == sid
                            and e.get("waiter_pid") == me)]
            if len(kept) != len(queue):
                state["queue"] = kept
                changed = True
            if changed:
                state["updated_at"] = time.time()
                atomic_write_json(state_path(udid), state)
    except (SimlockError, OSError):
        pass


def queue_view(state, sid, token, now):
    entries = live_queue(state)
    rendered = []
    position = None
    for index, entry in enumerate(entries, 1):
        info = pid_probe(entry.get("waiter_pid"),
                         entry.get("waiter_pid_start"))
        mine = entry.get("session_id") == sid
        if mine and position is None:
            position = index
        enqueued = float(entry.get("enqueued_at") or now)
        rendered.append({
            "position": index,
            "session_id": entry.get("session_id"),
            "session_short": short_id(entry.get("session_id")),
            "waiter_pid": entry.get("waiter_pid"),
            "waiter_alive": info["alive"],
            "mine": mine,
            "label": entry.get("label", ""),
            "enqueued_at": entry.get("enqueued_at"),
            "enqueued_at_iso": iso(entry.get("enqueued_at")),
            "wait_seconds": round(max(0.0, now - enqueued), 1),
        })
    return {
        "length": len(rendered),
        "position": position,
        "mine": position is not None,
        "entries": rendered,
    }


def lease_view(lease, sid, now, cls=None):
    if not lease:
        return None
    cls = cls or classify_lease(lease, now)
    expires = float(lease.get("expires_at") or 0)
    mine = lease.get("session_id") == sid
    return {
        # The token authorizes release; only its owner may read it.
        "token": lease.get("token") if mine else None,
        "session_id": lease.get("session_id"),
        "session_short": short_id(lease.get("session_id")),
        "label": lease.get("label", ""),
        "owner_pid": lease.get("owner_pid"),
        "owner_alive": cls["owner_alive"],
        "owner_verified": cls["owner_verified"],
        "owner_reason": cls.get("owner_reason"),
        "host": lease.get("host"),
        "user": lease.get("user"),
        "acquired_at": lease.get("acquired_at"),
        "acquired_at_iso": iso(lease.get("acquired_at")),
        "renewed_at": lease.get("renewed_at"),
        "renewed_at_iso": iso(lease.get("renewed_at")),
        "expires_at": expires,
        "expires_at_iso": iso(expires),
        "expires_in_seconds": round(max(0.0, expires - now), 1),
        "ttl": lease.get("ttl"),
        "mine": mine,
        "expired": cls["expired"],
        "state": cls["state"],
    }


def device_view(dev):
    return {
        "udid": dev["udid"],
        "name": dev["name"],
        "runtime": dev["runtime"],
        "runtime_id": dev["runtime_id"],
        "sim_state": dev["sim_state"],
        "is_available": dev["is_available"],
        "known": dev.get("known", True),
    }


def state_view(state, dev, sid, token, now):
    lease = state.get("lease")
    cls = classify_lease(lease, now) if lease else None
    derived = "free" if not lease else cls["state"]
    return {
        "device": device_view(dev),
        "state": derived,
        "lease": lease_view(lease, sid, now, cls),
        "queue": queue_view(state, sid, token, now),
    }


def reclaim_summary(lease, cls):
    return {
        "session_id": lease.get("session_id"),
        "session_short": short_id(lease.get("session_id")),
        "owner_pid": lease.get("owner_pid"),
        "reason": "ttl_expired" if cls["expired"] and cls["owner_alive"]
                  else "owner_gone",
        "owner_alive": cls["owner_alive"],
        "acquired_at": lease.get("acquired_at"),
        "expires_at": lease.get("expires_at"),
    }


# --------------------------------------------------------------------------
# Acquire / wait
# --------------------------------------------------------------------------


def acquire_attempt(dev, sid, pid, ttl, label, token, force, no_wait):
    udid = dev["udid"]
    now = time.time()
    with device_lock(udid):
        state = read_state(udid)
        if dev.get("known", True):
            state["device_name"] = dev["name"]
            state["runtime_id"] = dev["runtime_id"]
        changed = sweep_queue(state)
        lease = state.get("lease")

        if force and lease and lease.get("session_id") != sid:
            log_event(udid, "force_steal", sid, {
                "from_session": lease.get("session_id"),
                "from_pid": lease.get("owner_pid"),
            })
            state["lease"] = None
            lease = None
            changed = True

        granted_lease = None
        reclaimed = None
        reentrant = False
        queued = False

        if lease is None:
            if can_claim(state, token, sid, force=force):
                granted_lease = make_lease(sid, pid, ttl, label, token, now)
            elif not no_wait:
                enqueue(state, sid, os.getpid(), token, label, now)
                queued = True
                changed = True
        elif lease.get("session_id") == sid:
            reentrant = True
            granted_lease = lease
            refresh_owner(lease, pid)
            new_expiry = now + ttl
            lease["expires_at"] = max(
                float(lease.get("expires_at") or 0), new_expiry
            )
            lease["ttl"] = max(int(lease.get("ttl") or 0), ttl)
            lease["renewed_at"] = now
            changed = True
        else:
            cls = classify_lease(lease, now)
            if cls["reclaimable"] and can_claim(state, token, sid,
                                                force=force):
                reclaimed = reclaim_summary(lease, cls)
                log_event(udid, "reclaim", sid, reclaimed)
                granted_lease = make_lease(sid, pid, ttl, label, token, now)
            elif not no_wait:
                enqueue(state, sid, os.getpid(), token, label, now)
                queued = True
                changed = True

        if granted_lease is not None:
            state["lease"] = granted_lease
            dequeue_session(state, sid)
            changed = True
        if changed:
            state["updated_at"] = now
            atomic_write_json(state_path(udid), state)
        if granted_lease is not None:
            log_event(udid, "renew" if reentrant else "acquire", sid, {
                "token": granted_lease.get("token"),
                "owner_pid": pid,
                "label": granted_lease.get("label"),
                "ttl": granted_lease.get("ttl"),
                "reclaimed": reclaimed,
            })

        view = state_view(state, dev, sid, token, now)

    if granted_lease is not None:
        return {
            "granted": True,
            "reentrant": reentrant,
            "reclaimed": reclaimed,
            "queued": False,
            "token": granted_lease.get("token"),
            "view": view,
        }
    return {"granted": False, "reentrant": False, "reclaimed": None,
            "queued": queued, "token": None, "view": view}


def acquired_payload(outcome, sid, identity_info, waited_ms, device_arg):
    view = outcome["view"]
    lease = view["lease"]
    payload = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "command": "acquire",
        "event": "acquired",
        "state": "held",
        "advisory": True,
        "device": view["device"],
        "device_arg": device_arg,
        "lease": lease,
        "queue": view["queue"],
        "token": outcome["token"],
        "owner": {
            "session_id": sid,
            "pid": (lease or {}).get("owner_pid"),
            "host": identity_info["host"],
            "user": identity_info["user"],
            "session_source": identity_info["session_source"],
            "pid_source": identity_info["pid_source"],
        },
        "waited_ms": int(round(waited_ms * 1000)),
        "reentrant": outcome["reentrant"],
        "reclaimed": outcome["reclaimed"],
    }
    return payload


def acquire_lease(device_arg, sid, identity_info, ttl, label, no_wait,
                  wait_timeout, force, quiet=False):
    """Block until the lease is granted or the wait policy says stop.

    Returns (payload, exit_code). payload["ok"] distinguishes success/busy.
    """
    dev = resolve_device(device_arg, need_available=True, must_exist=True)
    udid = dev["udid"]
    pid = identity_info["pid"]
    token = secrets.token_hex(8)
    started = time.monotonic()
    backoff = POLL_MIN
    granted = False
    queued = False
    last_message = None
    last_print = 0.0

    def cleanup():
        if not granted and queued:
            best_effort_dequeue(udid, token, sid)

    def interrupt(signum, frame):
        raise Interrupted()

    old_handlers = {}
    for signum in (signal.SIGTERM, signal.SIGHUP):
        try:
            old_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, interrupt)
        except (OSError, ValueError):
            pass

    try:
        while True:
            outcome = acquire_attempt(
                dev, sid, pid, ttl, label, token, force, no_wait
            )
            if outcome["granted"]:
                granted = True
                waited = time.monotonic() - started
                return acquired_payload(
                    outcome, sid, identity_info, waited, device_arg
                ), EXIT_OK

            if outcome.get("queued"):
                queued = True
            snapshot = outcome["view"]
            elapsed = time.monotonic() - started

            if no_wait:
                return busy_payload(snapshot, "BUSY",
                                    "device is held by another session",
                                    device_arg), EXIT_BUSY

            if wait_timeout and elapsed >= wait_timeout:
                cleanup()
                return busy_payload(
                    snapshot, "BUSY",
                    "wait timed out after %s" % fmt_duration(wait_timeout),
                    device_arg,
                    extra={"wait_timeout_seconds": wait_timeout},
                ), EXIT_BUSY

            message = wait_message(snapshot)
            now = time.monotonic()
            if (not quiet and message != last_message) or (
                    not quiet and now - last_print >= PROGRESS_EVERY):
                print("simlock: " + message, file=sys.stderr, flush=True)
                last_print = now
                last_message = message

            time.sleep(backoff + random.random() * 0.1)
            backoff = min(POLL_MAX, backoff * 1.5)
    except BaseException:
        cleanup()
        raise
    finally:
        cleanup()
        for signum, handler in old_handlers.items():
            try:
                signal.signal(signum, handler)
            except (OSError, ValueError):
                pass


def wait_message(snapshot):
    device = snapshot["device"]
    lease = snapshot["lease"]
    queue = snapshot["queue"]
    position = queue.get("position")
    length = queue.get("length") or 0
    where = device.get("name") or device["udid"]
    if length:
        place = "position %s of %s" % (position if position else "?",
                                        length)
    else:
        place = "waiting"
    if lease:
        holder = short_id(lease.get("session_id"))
        remaining = fmt_duration(lease.get("expires_in_seconds") or 0)
        detail = "held by session %s, expires in %s" % (holder, remaining)
        if lease.get("state") == "stale":
            detail = "holder session %s is gone; reclaiming" % holder
        elif lease.get("state") == "expired":
            detail = "holder session %s lease expired; reclaiming" % holder
        return "%s (%s): %s; %s" % (where, device["udid"], place, detail)
    return "%s (%s): %s; waiting for earlier requesters" % (
        where, device["udid"], place)


def busy_payload(snapshot, code, message, device_arg, extra=None):
    lease = snapshot.get("lease") or {}
    queue = snapshot.get("queue") or {}
    detail = {
        "held_by": lease.get("session_id"),
        "held_by_short": lease.get("session_short"),
        "owner_pid": lease.get("owner_pid"),
        "expires_in_seconds": lease.get("expires_in_seconds"),
        "queue_length": queue.get("length"),
    }
    if extra:
        detail.update(extra)
    return {
        "ok": False,
        "schema_version": SCHEMA_VERSION,
        "command": "acquire",
        "event": "busy",
        "state": "busy",
        "advisory": True,
        "device": snapshot.get("device"),
        "device_arg": device_arg,
        "lease": snapshot.get("lease"),
        "queue": queue,
        "error": {
            "code": code,
            "message": message,
            "hint": "Retry without --no-wait to queue, or inspect with "
                    "`simlock status <device> --json`.",
            **detail,
        },
    }


# --------------------------------------------------------------------------
# Human-friendly rendering
# --------------------------------------------------------------------------


def human_acquired(payload):
    lease = payload["lease"]
    device = payload["device"]
    name = device.get("name") or device["udid"]
    suffix = ""
    if payload.get("reentrant"):
        suffix = " (reentrant; session already held it)"
    elif payload.get("reclaimed"):
        suffix = " (reclaimed from %s)" % payload["reclaimed"].get(
            "session_short", "?")
    return "acquired %s (%s) for session %s until %s%s" % (
        name,
        device["udid"],
        short_id(lease.get("session_id")),
        lease.get("expires_at_iso") or "?",
        suffix,
    )


def human_busy(payload):
    err = payload.get("error") or {}
    parts = ["busy: %s" % err.get("message", "device is in use")]
    if err.get("held_by"):
        parts.append("held by session %s" % short_id(err["held_by"]))
    if err.get("expires_in_seconds") is not None:
        parts.append("expires in %s" % fmt_duration(
            err["expires_in_seconds"]))
    if err.get("queue_length"):
        parts.append("%d waiting" % err["queue_length"])
    return "; ".join(parts)


def human_status(payload):
    device = payload["device"]
    lease = payload.get("lease")
    queue = payload.get("queue") or {}
    name = device.get("name") or device["udid"]
    lines = ["%s (%s)" % (name, device["udid"])]
    sim = device.get("sim_state") or "unknown"
    runtime = device.get("runtime") or "unknown runtime"
    lines.append("  simulator: %s, %s" % (sim, runtime))
    if not device.get("known", True):
        lines.append("  note: not present in simctl (device may be deleted)")
    if not lease:
        lines.append("  lock: free")
    else:
        who = "you" if lease.get("mine") else "session %s" % (
            lease.get("session_short"))
        state = lease.get("state")
        if state == "stale":
            lines.append("  lock: stale (owner is gone; reclaimable); "
                         "would be session %s" % short_id(
                             lease.get("session_id")))
        elif state == "expired":
            lines.append("  lock: expired (reclaimable); held by %s" % who)
        else:
            lines.append("  lock: held by %s, expires in %s" % (
                who, fmt_duration(lease.get("expires_in_seconds") or 0)))
            lines.append("  owner pid: %s (%s)" % (
                lease.get("owner_pid"),
                "alive" if lease.get("owner_alive") else "gone"))
    if queue.get("length"):
        lines.append("  queue: %d waiting" % queue["length"])
        for entry in queue.get("entries", []):
            lines.append("    #%d session %s (pid %s) waiting %s" % (
                entry["position"],
                short_id(entry.get("session_id")),
                entry.get("waiter_pid"),
                fmt_duration(entry.get("wait_seconds") or 0),
            ))
    else:
        lines.append("  queue: empty")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------


def cmd_acquire(args):
    sid, pid, identity_info = resolve_identity(args)
    identity_info["pid"] = pid
    ttl = validate_ttl(args.ttl)
    payload, code = acquire_lease(
        args.device,
        sid,
        identity_info,
        ttl,
        args.label,
        args.no_wait,
        args.wait_timeout,
        args.force,
        quiet=args.quiet,
    )
    emit(payload, human_acquired if payload.get("ok") else human_busy,
         args.json, error=not payload.get("ok"))
    return code


def cmd_release(args):
    sid, pid, identity_info = resolve_identity(args)
    quiet = getattr(args, "quiet", False)
    dev = resolve_device(args.device, must_exist=False)
    udid = dev["udid"]
    now = time.time()
    with device_lock(udid):
        state = read_state(udid)
        changed = sweep_queue(state)
        lease = state.get("lease")
        if not lease:
            if dequeue_session(state, sid):
                changed = True
            if changed:
                state["updated_at"] = now
                atomic_write_json(state_path(udid), state)
            payload = {
                "ok": True,
                "schema_version": SCHEMA_VERSION,
                "command": "release",
                "event": "not_held",
                "state": "free",
                "advisory": True,
                "device": device_view(dev),
                "lease": None,
                "queue": queue_view(state, sid, None, now),
                "released": False,
            }
            if not quiet:
                emit(payload, lambda p: "not held; nothing to release",
                     args.json)
            return EXIT_OK

        cls = classify_lease(lease, now)
        mine = lease.get("session_id") == sid
        token_ok = bool(args.token) and lease.get("token") == args.token
        force = bool(args.force)
        cleaned_dead = not cls["owner_alive"]

        if force or mine or token_ok or cleaned_dead:
            summary = {
                "session_id": lease.get("session_id"),
                "session_short": short_id(lease.get("session_id")),
                "token": lease.get("token"),
                "owner_pid": lease.get("owner_pid"),
                "reason": "forced" if force and not (mine or token_ok)
                          else ("owner_gone" if cleaned_dead and not
                                (mine or token_ok) else "released"),
            }
            log_event(udid, "release", sid, summary)
            state["lease"] = None
            dequeue_session(state, sid)
            state["updated_at"] = now
            atomic_write_json(state_path(udid), state)
            payload = {
                "ok": True,
                "schema_version": SCHEMA_VERSION,
                "command": "release",
                "event": "released",
                "state": "free",
                "advisory": True,
                "device": device_view(dev),
                "lease": lease_view(lease, sid, now, cls),
                "queue": queue_view(state, sid, args.token, now),
                "released": True,
                "released_owner": summary,
            }
            if not quiet:
                emit(payload, lambda p: "released %s (%s)" % (
                    p["device"].get("name") or udid, udid), args.json)
            return EXIT_OK

        raise SimlockError(
            "NOT_OWNER",
            "session %s does not own the lease on %s" % (
                short_id(sid), udid),
            EXIT_NOT_OWNER,
            hint="Pass --token, the owning session's credentials, or --force "
                 "after confirming with the user.",
            extra={
                "device": device_view(dev),
                "lease": lease_view(lease, sid, now, cls),
                "queue": queue_view(state, sid, args.token, now),
            },
        )


def cmd_renew(args):
    sid, pid, identity_info = resolve_identity(args)
    ttl = validate_ttl(args.ttl)
    dev = resolve_device(args.device, must_exist=False)
    udid = dev["udid"]
    now = time.time()
    with device_lock(udid):
        state = read_state(udid)
        changed = sweep_queue(state)
        lease = state.get("lease")
        if not lease:
            if changed:
                state["updated_at"] = now
                atomic_write_json(state_path(udid), state)
            raise SimlockError(
                "NOT_HELD",
                "no lease exists on %s" % udid,
                EXIT_NOT_OWNER,
                hint="Acquire the lease before renewing it.",
            )
        cls = classify_lease(lease, now)
        mine = lease.get("session_id") == sid
        token_ok = bool(args.token) and lease.get("token") == args.token
        if not (mine or token_ok):
            raise SimlockError(
                "NOT_OWNER",
                "session %s does not own the lease on %s" % (
                    short_id(sid), udid),
                EXIT_NOT_OWNER,
                extra={
                    "device": device_view(dev),
                    "lease": lease_view(lease, sid, now, cls),
                },
            )
        if mine:
            refresh_owner(lease, pid)
        lease["ttl"] = ttl
        lease["renewed_at"] = now
        lease["expires_at"] = now + ttl
        state["updated_at"] = now
        atomic_write_json(state_path(udid), state)
        log_event(udid, "renew", sid, {
            "token": lease.get("token"),
            "owner_pid": lease.get("owner_pid"),
            "ttl": ttl,
        })
        payload = {
            "ok": True,
            "schema_version": SCHEMA_VERSION,
            "command": "renew",
            "event": "renewed",
            "state": "held",
            "advisory": True,
            "device": device_view(dev),
            "lease": lease_view(lease, sid, now),
            "queue": queue_view(state, sid, args.token, now),
            "renewed": True,
        }
    emit(payload, lambda p: "renewed lease on %s until %s" % (
        p["device"].get("name") or udid,
        p["lease"].get("expires_at_iso")), args.json)
    return EXIT_OK


def cmd_status(args):
    if not args.device:
        return cmd_list(args)
    sid, pid, identity_info = resolve_identity(args)
    dev = resolve_device(args.device, must_exist=False)
    udid = dev["udid"]
    now = time.time()
    try:
        state = read_state(udid)
    except SimlockError as exc:
        if exc.code == "STATE_CORRUPT":
            raise
        state = default_state(udid, dev)
    view = state_view(state, dev, sid, args.token, now)
    payload = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "command": "status",
        "advisory": True,
        "device": view["device"],
        "state": view["state"],
        "lease": view["lease"],
        "queue": view["queue"],
        "identity": identity_info,
    }
    emit(payload, human_status, args.json)
    return EXIT_OK


def cmd_list(args):
    sid, pid, identity_info = resolve_identity(args)
    warning = None
    try:
        devices = load_devices()
    except SimlockError as exc:
        devices = []
        warning = {"code": exc.code, "message": exc.message}
    by_udid = {dev["udid"]: dev for dev in devices}
    for udid in state_udids():
        if udid not in by_udid:
            by_udid[udid] = unknown_device(udid)

    now = time.time()
    entries = []
    for udid in sorted(by_udid, key=lambda u: (
            by_udid[u]["name"].lower(), by_udid[u]["runtime"], u)):
        dev = by_udid[udid]
        try:
            state = read_state(udid)
            view = state_view(state, dev, sid, None, now)
            entries.append({
                "device": view["device"],
                "state": view["state"],
                "lease": view["lease"],
                "queue": view["queue"],
            })
        except SimlockError as exc:
            entries.append({
                "device": device_view(dev),
                "state": "corrupt",
                "lease": None,
                "queue": {"length": 0, "position": None, "mine": False,
                          "entries": []},
                "error": exc.payload(),
            })
    payload = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "command": "list",
        "advisory": True,
        "devices": entries,
        "identity": identity_info,
    }
    if warning:
        payload["warning"] = warning
    emit(payload, human_list, args.json)
    return EXIT_OK


def human_list(payload):
    entries = payload.get("devices") or []
    if not entries:
        return "no simulators found"
    rows = [("NAME", "UDID", "SIM", "LOCK", "QUEUE")]
    for entry in entries:
        dev = entry["device"]
        lease = entry.get("lease")
        if entry.get("state") == "corrupt":
            lock = "CORRUPT"
        elif not lease:
            lock = "free"
        elif lease.get("state") == "stale":
            lock = "stale"
        elif lease.get("state") == "expired":
            lock = "expired"
        else:
            lock = "held %s" % short_id(lease.get("session_id"))
        queue = entry.get("queue") or {}
        rows.append((
            (dev.get("name") or "(removed)")[:28],
            dev["udid"][:8],
            (dev.get("sim_state") or "-")[:10],
            lock,
            str(queue.get("length") or "-"),
        ))
    widths = [max(len(str(row[i])) for row in rows) for i in range(5)]
    lines = []
    for row in rows:
        lines.append("  ".join(str(cell).ljust(widths[i])
                               for i, cell in enumerate(row)).rstrip())
    if payload.get("warning"):
        lines.append("warning: %s" % payload["warning"].get("message"))
    return "\n".join(lines)


def cmd_gc(args):
    ensure_home()
    now = time.time()
    actions = []
    for udid in state_udids():
        try:
            with device_lock(udid):
                state = read_state(udid)
                changed = sweep_queue(state)
                if changed:
                    actions.append({"udid": udid,
                                    "action": "swept_dead_queue"})
                lease = state.get("lease")
                if lease:
                    cls = classify_lease(lease, now)
                    if not cls["owner_alive"]:
                        actions.append({
                            "udid": udid,
                            "action": "cleared_stale_lease",
                            "session_id": lease.get("session_id"),
                            "owner_pid": lease.get("owner_pid"),
                        })
                        state["lease"] = None
                        changed = True
                directory = device_dir(udid)
                try:
                    for name in os.listdir(directory):
                        if not name.startswith(".state.tmp-"):
                            continue
                        path = os.path.join(directory, name)
                        try:
                            age = now - os.path.getmtime(path)
                        except OSError:
                            continue
                        if age > TMP_MAX_AGE:
                            actions.append({"udid": udid,
                                            "action": "removed_temp_file",
                                            "file": name})
                            if not args.dry_run:
                                try:
                                    os.unlink(path)
                                except OSError:
                                    pass
                except OSError:
                    pass
                if changed and not args.dry_run:
                    state["updated_at"] = now
                    atomic_write_json(state_path(udid), state)
        except SimlockError as exc:
            actions.append({"udid": udid, "action": "error",
                            "error": exc.payload()})
    payload = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "command": "gc",
        "dry_run": bool(args.dry_run),
        "actions": actions,
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    elif actions:
        for action in actions:
            print("%s: %s" % (action["udid"][:8], action["action"]))
    else:
        print("nothing to clean")
    return EXIT_OK


def cmd_run(args):
    if not args.cmd_argv:
        raise UsageError("run requires `-- <command...>`")
    sid, pid, identity_info = resolve_identity(args)
    identity_info["pid"] = pid
    ttl = validate_ttl(args.ttl)
    payload, code = acquire_lease(
        args.device, sid, identity_info, ttl, args.label or "simlock run",
        args.no_wait, args.wait_timeout, args.force, quiet=args.quiet,
    )
    if code != EXIT_OK or not payload.get("ok"):
        emit(payload, human_busy, False, error=True)
        return code

    reentrant = bool(payload.get("reentrant"))
    lease = payload["lease"]
    device = payload["device"]
    name = device.get("name") or device["udid"]
    if not args.quiet:
        print("simlock: acquired %s (%s)%s; running: %s" % (
            name,
            device["udid"],
            " [reentrant; outer lease kept]" if reentrant else "",
            " ".join(shlex.quote(part) for part in args.cmd_argv),
        ), file=sys.stderr, flush=True)
    try:
        completed = subprocess.run(args.cmd_argv, env=dict(os.environ))
        rc = completed.returncode
    except FileNotFoundError:
        print("simlock: command not found: %s" % args.cmd_argv[0],
              file=sys.stderr)
        rc = 127
    except OSError as exc:
        print("simlock: cannot run %s: %s" % (args.cmd_argv[0], exc),
              file=sys.stderr)
        rc = 126
    finally:
        if not reentrant:
            try:
                release_args = argparse.Namespace(
                    device=device["udid"], token=lease.get("token"),
                    force=False, json=False, owner=sid, owner_pid=pid,
                    ttl=None, quiet=True,
                )
                cmd_release(release_args)
            except SimlockError as exc:
                print("simlock: warning: release failed: %s" % exc.message,
                      file=sys.stderr)
            except Exception as exc:  # never mask the child's exit code
                print("simlock: warning: release failed: %s" % exc,
                      file=sys.stderr)
        elif not args.quiet:
            print("simlock: reentrant acquire; outer lease left in place",
                  file=sys.stderr)
    if rc < 0:
        return 128 + abs(rc)
    return rc


def cmd_whoami(args):
    sid, pid, identity_info = resolve_identity(args)
    payload = {
        "ok": True,
        "schema_version": SCHEMA_VERSION,
        "command": "whoami",
        "session_id": sid,
        "session_short": short_id(sid),
        "owner_pid": pid,
        "session_source": identity_info["session_source"],
        "pid_source": identity_info["pid_source"],
        "host": identity_info["host"],
        "user": identity_info["user"],
        "home": home_root(),
        "in_claude": bool(os.environ.get("CLAUDE_CODE_SESSION_ID")),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print("session: %s (%s)" % (sid, identity_info["session_source"]))
        print("pid:     %s (%s)" % (pid, identity_info["pid_source"]))
        print("host:    %s" % identity_info["host"])
        print("state:   %s" % home_root())
        print("claude:  %s" % ("yes" if payload["in_claude"] else "no"))
    return EXIT_OK


# --------------------------------------------------------------------------
# Output / CLI
# --------------------------------------------------------------------------


def emit(payload, human, json_mode, error=False):
    if json_mode:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return
    text = human(payload)
    if not text:
        return
    stream = sys.stderr if error else sys.stdout
    print(text, file=stream)


class SimlockParser(argparse.ArgumentParser):
    def error(self, message):
        raise UsageError(message)


def add_identity(parser):
    parser.add_argument("--json", action="store_true",
                        help="emit stable JSON on stdout")
    parser.add_argument("--owner", metavar="ID",
                        help="session id (default: CLAUDE_CODE_SESSION_ID)")
    parser.add_argument("--owner-pid", metavar="PID", type=int,
                        help="owner PID (default: CLAUDE_PID)")
    parser.add_argument("--quiet", action="store_true",
                        help="suppress progress on stderr")


def add_wait(parser):
    parser.add_argument("--ttl", type=int, default=DEFAULT_TTL,
                        metavar="SECONDS",
                        help="lease lifetime (default %d, max %d)" % (
                            DEFAULT_TTL, MAX_TTL))
    parser.add_argument("--label", default="", metavar="TEXT",
                        help="human label stored in the lease")
    parser.add_argument("--no-wait", action="store_true",
                        help="fail immediately instead of queueing")
    parser.add_argument("--wait", action="store_true",
                        help="queue (default; kept for symmetry)")
    parser.add_argument("--wait-timeout", type=float, default=0.0,
                        metavar="SECONDS",
                        help="give up after SECONDS; 0 waits forever")
    parser.add_argument("--force", action="store_true",
                        help="steal a lease held by another session")


def build_parser():
    parser = SimlockParser(
        prog="simlock.py",
        description="Cross-session exclusive leases for DeviceHub simulators.",
    )
    parser.add_argument("--version", action="version",
                        version="simlock 1.0.0")
    sub = parser.add_subparsers(dest="command")

    acquire = sub.add_parser("acquire", aliases=["lock"],
                             help="acquire or wait for a device lease")
    acquire.add_argument("device", help="UDID or exact simulator name")
    add_wait(acquire)
    add_identity(acquire)
    acquire.set_defaults(func=cmd_acquire)

    release = sub.add_parser("release", aliases=["unlock"],
                             help="release a device lease")
    release.add_argument("device", help="UDID or exact simulator name")
    release.add_argument("--token", metavar="T",
                         help="lease token (for non-session callers)")
    release.add_argument("--force", action="store_true",
                         help="release even if another session owns it")
    add_identity(release)
    release.set_defaults(func=cmd_release, ttl=None)

    renew = sub.add_parser("renew", help="extend a held lease")
    renew.add_argument("device", help="UDID or exact simulator name")
    renew.add_argument("--token", metavar="T",
                       help="lease token (for non-session callers)")
    renew.add_argument("--ttl", type=int, default=DEFAULT_TTL,
                       metavar="SECONDS",
                       help="new lifetime (default %d, max %d)" % (
                           DEFAULT_TTL, MAX_TTL))
    add_identity(renew)
    renew.set_defaults(func=cmd_renew)

    status = sub.add_parser("status", help="show one device's lease")
    status.add_argument("device", nargs="?", metavar="DEVICE",
                        help="UDID or exact name; omit for a summary")
    status.add_argument("--token", metavar="T", help=argparse.SUPPRESS)
    add_identity(status)
    status.set_defaults(func=cmd_status)

    listing = sub.add_parser("list", help="list devices and leases")
    add_identity(listing)
    listing.set_defaults(func=cmd_list)

    gc = sub.add_parser("gc", help="clean dead waiters and stale leases")
    gc.add_argument("--dry-run", action="store_true",
                    help="report without changing state")
    gc.add_argument("--json", action="store_true")
    gc.set_defaults(func=cmd_gc)

    run = sub.add_parser("run", aliases=["exec"],
                         help="acquire, run a command, release")
    run.add_argument("device", help="UDID or exact simulator name")
    add_wait(run)
    run.add_argument("cmd_argv", nargs=argparse.REMAINDER,
                     help="command after --")
    add_identity(run)
    run.set_defaults(func=cmd_run)

    whoami = sub.add_parser("whoami", help="show the current identity")
    add_identity(whoami)
    whoami.set_defaults(func=cmd_whoami)

    return parser


def split_run_argv(argv):
    """Handle `run <device> [opts] -- cmd ...` before argparse sees the command."""
    if not argv or argv[0] not in ("run", "exec"):
        return argv, None
    if "--" in argv:
        index = argv.index("--")
        return argv[:index], argv[index + 1:]
    if "-h" in argv or "--help" in argv:
        # Let argparse print the command's help instead of the split error.
        return argv, None
    raise UsageError("run requires `-- <command...>`")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    json_mode = "--json" in argv
    command = argv[0] if argv else ""
    try:
        parser = build_parser()
        left, child_argv = split_run_argv(argv)
        args = parser.parse_args(left)
        if not getattr(args, "func", None):
            raise UsageError("missing command; see `simlock.py --help`")
        if child_argv is not None:
            args.cmd_argv = child_argv
        return args.func(args)
    except UsageError as exc:
        payload = {
            "ok": False,
            "schema_version": SCHEMA_VERSION,
            "command": command or None,
            "advisory": True,
            "error": exc.payload(),
        }
        if json_mode:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        print("simlock: %s" % exc.message, file=sys.stderr)
        return EXIT_USAGE
    except SimlockError as exc:
        payload = {
            "ok": False,
            "schema_version": SCHEMA_VERSION,
            "command": command or None,
            "advisory": True,
            "error": exc.payload(),
        }
        if json_mode:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        if not json_mode:
            print("simlock: %s" % exc.message, file=sys.stderr)
            if exc.hint:
                print("hint: %s" % exc.hint, file=sys.stderr)
        return exc.exit_code
    except KeyboardInterrupt:
        print("simlock: interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED
    except Interrupted:
        print("simlock: interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED
    except BrokenPipeError:
        return EXIT_STATE


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SimlockError as exc:  # defensive: errors raised before main's try
        print("simlock: %s" % exc.message, file=sys.stderr)
        sys.exit(exc.exit_code)
