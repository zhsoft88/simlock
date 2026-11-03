# simlock

Cross-session **exclusive leases for DeviceHub / Xcode simulators**.

Several Claude Code sessions often share one Mac and the same simulators. DeviceHub.app
(Xcode 27, `com.apple.dt.Devices`) has no locking API, so two sessions can drive one device at
the same time and corrupt each other's app state, screenshots and test runs. `simlock` adds a
cooperative lease in front of the tools: one session holds a device, everyone else queues in
FIFO order until it is released.

- No daemon, no database, no dependencies: one Python 3 file using only the standard library.
- Atomic state under `${SIMLOCK_HOME:-~/.simlock}`, safe across processes and terminals.
- Stale-lease recovery: owner PID identity (start time, zombie detection) plus a TTL backstop.
- Advisory: DeviceHub's UI, Xcode and sessions that ignore this skill are not blocked.

## Install

```bash
./install.sh
```

This creates the symlink `~/.claude/skills/simlock -> <this repository>`, which makes the
skill available to Claude Code. `install.sh --force` replaces an existing entry.

Manual install:

```bash
ln -s "$(pwd)" ~/.claude/skills/simlock
```

Requirements: macOS, Python 3.9+ (the system `/usr/bin/python3` is enough), Xcode command line
tools (`xcrun simctl`). The CLI itself needs no Python packages.

## Quick start

```bash
SIMLOCK="$HOME/.claude/skills/simlock/scripts/simlock.py"

# What exists, and who is using it?
python3 "$SIMLOCK" list
python3 "$SIMLOCK" status 2C4E94C2-9074-40EA-BFFF-714049438172

# Lock the device for up to an hour (waits in line if another session holds it)
python3 "$SIMLOCK" acquire 2C4E94C2-9074-40EA-BFFF-714049438172 --ttl 3600

# ... do simulator work with argent / simctl / xcodebuild ...

python3 "$SIMLOCK" release 2C4E94C2-9074-40EA-BFFF-714049438172
```

Acquire waits indefinitely by default. To wait in a Claude Code session without hitting the
Bash tool's 120 s foreground limit, run the acquire command with `run_in_background: true` and
start device work when it completes. Use `--no-wait` for a non-blocking `BUSY` check and
`--wait-timeout N` to give up after N seconds.

One-shot command:

```bash
python3 "$SIMLOCK" run <UDID> --ttl 3600 -- xcrun simctl boot <UDID>
```

## Commands

| Command | Purpose |
|---------|---------|
| `acquire` (alias `lock`) `<device>` | Take the lease, or queue until it is free. `--ttl`, `--no-wait`, `--wait-timeout`, `--force`, `--label`, `--json` |
| `release` (alias `unlock`) `<device>` | Give the lease up. `--token`, `--force`, `--json` |
| `renew` `<device>` | Extend the current lease. `--ttl`, `--token`, `--json` |
| `status` `[device]` | Lease, queue and holder details for one device, or a summary of all state |
| `list` | All simulators known to `simctl` with their lease state |
| `gc` | Sweep dead queue entries and stale leases (`--dry-run` to preview) |
| `run` (alias `exec`) `<device> -- cmd...` | Acquire, run a command, release; passes the child exit code through |
| `whoami` | Show the identity used for ownership |

`<device>` is a UDID or an **exact** simulator name. Names are not unique in `simctl`
("Apple Watch Series 12 (46mm)" appears several times); an ambiguous name fails with exit 3
and lists the candidate UDIDs rather than guessing. A UDID prefix is not accepted.

All commands accept `--json` for stable, machine-readable output (also on errors), and
`--owner` / `--owner-pid` to override the default identity taken from
`CLAUDE_CODE_SESSION_ID` / `CLAUDE_PID`. Outside Claude Code the session id falls back to a
per-process identity — pass `--owner` if you need several shells to share one owner.

Exit codes: `0` success; `2` busy; `3` device missing/ambiguous/unavailable; `4` not the
owner; `5` corrupt state or state-directory error; `64` usage; `130` interrupted while
waiting. `run` returns the child command's exit code (`127` not found, `126` not executable).

## How it works

### One stable lock file per device

State lives in `<device-dir>/state.json` and is rewritten atomically
(`mkstemp` → `F_FULLFSYNC`/`fsync` → `os.replace` → directory `fsync`). All mutations happen
under `fcntl.flock(LOCK_EX)` on a separate **`control.lock` file that is never unlinked or
replaced**. Locking the atomically-replaced `state.json` itself would be a bug: `flock`
follows the inode, so a waiter holding the old inode would no longer exclude writers using the
new file.

### Lease

```json
{
  "token": "a1b2c3d4e5f60718",
  "session_id": "8890e8c9-...",
  "label": "boot + smoke test",
  "owner_pid": 97013,
  "owner_pid_start": "Tue Nov  3 17:10:57 2026",
  "host": "zhsoft88-mac",
  "user": "zhsoft88",
  "acquired_at": 1762000000.0,
  "renewed_at": 1762000000.0,
  "expires_at": 1762003600.0,
  "ttl": 3600
}
```

The owner is `(session_id, pid)`; the lease is held by the task, not by an open file
descriptor, so the acquiring CLI process may exit. Re-acquiring from the same session is
idempotent: it keeps the existing token and extends the expiry only if the requested TTL is
longer. The token is returned only to the owner; `status`/`list` show `token: null` for other
sessions, so a token can never be read and used to release someone else's lease.

### FIFO queue

Waiters live inside `state.json` as `queue: [{seq, session_id, waiter_pid,
waiter_pid_start, created_at, ...}]`, ordered by a monotonic `seq_counter`. Only the **head**
of the queue may claim a free lease, which prevents thundering-herd reordering. Waiters poll
with 0.5 s → 2 s randomized backoff and print their position every ~5 s; nothing holds the
flock while sleeping. A waiter that times out, is interrupted (SIGINT/SIGTERM/SIGHUP), or dies
has its entry removed — dead entries are also filtered on read.

### Reclaiming abandoned leases

A lease is reclaimable when:

1. the owner PID is verifiably dead on this host, or
2. `now >= expires_at` (TTL backstop), even if the owner process is still alive, or
3. someone explicitly passes `--force`.

PID identity is checked with `LC_ALL=C TZ=UTC ps -p PID -o lstart=,stat=`. A zombie
(`stat` starts with `Z`) counts as dead; a start-time mismatch means the PID was reused and
the original owner is gone. If the process cannot be inspected, or the lease was acquired on
another host, it is treated as alive-unverified and only the TTL reclaims it.

Because TTL expiry is a hard deadline, a long-running task can lose the device mid-run.
Acquire long workflows with `--ttl 3600`, or call `renew` before expiry; `status --json`
reports `expires_in_seconds`.

### Nested use / `run`

`run` releases only a lease it created. If the session already held the outer lease
(`reentrant`), the outer lease is left in place. A `run` inside an outer lease and an `acquire`
from the same session therefore cannot accidentally unlock the task's device.

## State layout

```
~/.simlock/                                  # 0700
└── devices/
    └── 2C4E94C2-.../                        # 0700
        ├── state.json                       # 0600: lease + queue + seq counter
        ├── control.lock                     # 0600: stable flock inode (never unlinked)
        └── events.jsonl                     # 0600: append-only audit (acquire/renew/release/reclaim/force_steal)
```

Environment variables:

| Variable | Meaning |
|----------|---------|
| `SIMLOCK_HOME` | State root (default `~/.simlock`) |
| `SIMLOCK_DEVICES_JSON` | Read devices from a `simctl list devices --json` fixture instead of `xcrun` |
| `SIMLOCK_SIMCTL_SET` | Pass `--set <path>` to `simctl` for an extra CoreSimulator device set |
| `SIMLOCK_OWNER_ID` | Default owner session id when `--owner` is not given |

Corrupt state is reported as `STATE_CORRUPT` (exit 5) instead of being silently discarded.
Inspect the file, move it aside, and let the next acquire recreate it.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

The suite uses a temporary `SIMLOCK_HOME` and a fixture device set; it covers acquire / release
/ renew, idempotent re-entry, non-blocking busy, waiting and FIFO order with three processes,
dead-owner and TTL reclaim, force steal, ambiguous/missing/unavailable devices, dead-waiter
sweep, `run` exit-code passthrough and reentrancy, corrupt state, and a multi-process stress
test that asserts at most one holder at any instant.

## Limitations

- **Cooperative only.** The lease does not stop DeviceHub, Xcode, or an agent that does not use
  the skill from touching the device.
- **Simulators only.** Devices managed by `xcrun simctl` (iOS/watchOS/tvOS/visionOS). Physical
  devices are out of scope.
- **No automatic release on session exit.** A crashed or exited session is recovered by PID
  liveness (immediately) or TTL (at most the lease TTL). There is no SessionEnd hook.
- **Clock and host assumptions.** Cross-host leases rely on wall-clock TTL; they are not
  intended for a shared network filesystem with unsynchronized clocks.
- `simlock` never boots, shuts down, creates or erases devices — it only manages leases.
