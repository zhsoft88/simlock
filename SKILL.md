---
name: simlock
description: Cross-session exclusive lease for DeviceHub/Xcode simulators. Use before any simulator interaction (argent MCP tools, xcrun simctl, xcodebuild test, installing/launching apps) and whenever another Claude Code session may be using the same device — lock/unlock, 等待设备, 共享模拟器, 模拟器被占用, 模拟器锁. Ensures one session holds a simulator at a time with a FIFO wait queue.
---

# simlock — exclusive simulator leases across sessions

Multiple Claude Code sessions share the same Mac and the same simulators. Nothing in
DeviceHub.app/Xcode prevents two sessions from driving one device at once. `simlock` is a
**cooperative lease**: a session claims a device before touching it, other sessions queue, and
the device is released when the work is done.

The lock is advisory. DeviceHub's UI, Xcode, and sessions that ignore this skill are not
blocked; it works only because every cooperating session follows the rules below.

## Hard rules

1. **Acquire the lease before any simulator interaction** on that UDID: any
   `mcp__plugin_goldie_argent__*` call, `xcrun simctl ...`, `xcodebuild ... -destination`,
   installing, launching, booting, screenshots, describe, taps.
2. **Resolve the UDID first.** `xcrun simctl` device names are not unique (for example several
   "Apple Watch Series 12 (46mm)"). Use `list --json` or argent `list-devices`; if a name is
   ambiguous, ask the user to choose. Never guess.
3. **Hold the lease for the whole task.** Do not release between taps or between
   `describe`/`tap` calls. Acquire once, work, release when the device is no longer needed.
4. **Always release when done**, unless the user explicitly asks to keep the device.
5. **Never `release --force` (or `acquire --force`) another session's lease without explicit
   user confirmation.** Report the holder shown by `status` and ask.
6. Repeat interactions that use the same UDID in one session do not need to re-acquire:
   acquisition is idempotent per session. A nested `run` does not release the outer lease.

## Command

```bash
SIMLOCK="$HOME/.claude/skills/simlock/scripts/simlock.py"
```

All commands accept `--json` (stable stdout) and `--owner ID` / `--owner-pid PID` overrides.
Identity defaults come from `CLAUDE_CODE_SESSION_ID` and `CLAUDE_PID`.

### 1. See what is available / who holds what

```bash
python3 "$SIMLOCK" list --json           # all known simulators + lease state
python3 "$SIMLOCK" status <UDID> --json  # one device, queue, holder, TTL
python3 "$SIMLOCK" whoami --json         # current session identity
```

States: `free`, `held` (live owner), `expired` (TTL passed, reclaimable), `stale` (owner PID
gone, reclaimable).

### 2. Acquire (lock)

```bash
python3 "$SIMLOCK" acquire <UDID> --ttl 3600 --json
```

- Free device: returns immediately (exit 0) with a `token`.
- Held by another session: **queues by default and waits indefinitely**; prints progress on
  stderr every ~5 s. It grants the lease as soon as the holder releases or its lease expires.
- `--ttl SECONDS`: lease lifetime, default 900, max 14400. Use **3600 for real device
  workflows**; use the default for quick queries.
- `--no-wait`: fail immediately with exit code 2 (`BUSY`).
- `--wait-timeout SECONDS`: give up after N seconds (0 = wait forever, the default).
- `--force`: steal the lease from another session. Requires user confirmation.
- `--label TEXT`: note shown in `status` (e.g. "boot + smoke test").

**Waiting longer than the Bash tool's 120 s foreground limit:** run the acquire with
`run_in_background: true`; when the background command completes you are notified, then start
device work. Short waits (< ~90 s) can stay in the foreground with `--wait-timeout 90`.

### 3. Work with the device while holding the lease

Use argent tools / simctl / xcodebuild as usual. If the task may exceed the TTL, either
acquire with `--ttl 3600` or renew before expiry:

```bash
python3 "$SIMLOCK" renew <UDID> --ttl 3600 --json
```

### 4. Release (unlock)

```bash
python3 "$SIMLOCK" release <UDID> --json
```

Release is idempotent (exit 0 even if the lease is already gone). `--force` releases another
session's lease — only with user confirmation. `--token T` releases by token.

### One-shot wrapper

```bash
python3 "$SIMLOCK" run <UDID> --ttl 3600 -- xcrun simctl bootstatus <UDID> -b
```

`run` acquires, starts the command, and releases only if it created the lease. If this session
already held the device, the outer lease stays after the command finishes. The child's exit
code is passed through (127 command not found, 126 not executable). Output goes to stdout;
acquire/release notices go to stderr.

## Handling a busy or stuck device

1. `status <UDID> --json` shows the holder's session id, PID, host, label and remaining TTL.
2. Tell the user who holds the device and how long until it expires; offer to wait.
3. If the holder is another session the user controls, the clean fix is for that session to
   `release`. Killing the holder process (or its Claude session) also frees it automatically
   once the PID is gone.
4. Only after explicit user approval: `release <UDID> --force`, or `acquire --force`.
5. `gc --json` sweeps dead waiters and stale leases; acquisition already reclaims a dead
   owner or an expired lease automatically, so `gc` is rarely needed.

## TTL is a hard deadline

When `expires_at` passes, the lease is reclaimable **even if the holder is still running** —
that is the backstop against crashed/hung sessions. A long task that reaches its TTL can lose
the device to another session mid-run. Acquire long tasks with `--ttl 3600`, or call `renew`
before the deadline. `status` reports `expires_in_seconds`.

## Exit codes

| code | meaning |
|------|---------|
| 0 | success |
| 2 | `BUSY`: another session holds the lease and waiting was disabled/timed out |
| 3 | `DEVICE_NOT_FOUND` / `DEVICE_AMBIGUOUS` / `DEVICE_UNAVAILABLE` |
| 4 | `NOT_OWNER` / `NOT_HELD` (release/renew by a non-holder) |
| 5 | `STATE_CORRUPT` / state directory error |
| 64 | usage error |
| 130 | interrupted (SIGINT/SIGTERM while waiting; the queue entry is cleaned up) |
| other | `run` passes through the child command's exit code |

Errors are also JSON when `--json` is given: `{"ok": false, "error": {"code": ..., "message": ...}}`.

## State and diagnostics

- State root: `${SIMLOCK_HOME:-~/.simlock}` (mode 0700), one directory per UDID with
  `state.json`, a stable `control.lock`, and an append-only `events.jsonl` audit trail.
- Both the lease and the FIFO queue live in `state.json`; all mutations are serialized by
  `fcntl.flock` and written atomically. No daemon runs.
- Tests/offline: `SIMLOCK_DEVICES_JSON=/path/to/simctl.json` bypasses `xcrun simctl`;
  `SIMLOCK_HOME` isolates state.
- Scope: simulators managed by `xcrun simctl` only (iOS/watchOS/tvOS/visionOS). Not physical
  devices.

## What not to do

- Do not boot, erase, install on, or screenshot a simulator before acquiring its lease.
- Do not treat the lease as a UI screen lock: it does not change DeviceHub's appearance.
- Do not release between steps; starting and stopping work is cheap only for the holder.
- Do not force-release another session's lease without the user's explicit approval.
