#!/usr/bin/env python3
"""Unit and concurrency tests for simlock.

Run with:  python3 -m unittest discover -s tests -v
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "simlock.py"
FIXTURE = ROOT / "tests" / "fixtures" / "simctl_devices.json"

UDID_IPHONE = "2C4E94C2-9074-40EA-BFFF-714049438172"
UDID_IPAD = "C5354249-018C-49EF-AA69-1C88BC8E2901"
UDID_UNAVAILABLE = "9A9D1C6E-2A3C-4E5F-8B7A-6C5D4E3F2A10"
UDID_WATCH = "C7A7B492-121B-40E6-9F12-2F929FE534F1"
UDID_UNKNOWN = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"


class SimlockTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="simlock-test-")
        self.home = Path(self.tmp.name) / "state"
        self.env = os.environ.copy()
        self.env.update({
            "SIMLOCK_HOME": str(self.home),
            "SIMLOCK_DEVICES_JSON": str(FIXTURE),
            "CLAUDE_CODE_SESSION_ID": "session-a",
            "CLAUDE_PID": str(os.getpid()),
        })
        self.env.pop("SIMLOCK_OWNER_ID", None)
        self.env.pop("SIMLOCK_SIMCTL_SET", None)
        self.env.pop("SIMLOCK_SIMCTL", None)
        self.sleepers = []

    def tearDown(self):
        for proc in self.sleepers:
            if proc.poll() is None:
                proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        self.tmp.cleanup()

    # -- helpers ----------------------------------------------------------

    def env_for(self, session, pid=None):
        env = self.env.copy()
        env["CLAUDE_CODE_SESSION_ID"] = session
        env["CLAUDE_PID"] = str(pid if pid is not None else os.getpid())
        return env

    def cli(self, *args, session="session-a", pid=None, timeout=60):
        return subprocess.run(
            [sys.executable, "-I", str(SCRIPT)] + [str(a) for a in args],
            capture_output=True,
            text=True,
            env=self.env_for(session, pid),
            timeout=timeout,
        )

    def jcli(self, *args, session="session-a", pid=None, timeout=60):
        proc = self.cli(*args, "--json", session=session, pid=pid,
                        timeout=timeout)
        self.assertTrue(proc.stdout.strip(),
                        "no JSON on stdout (rc=%s, stderr=%s)" % (
                            proc.returncode, proc.stderr))
        return proc, json.loads(proc.stdout)

    def sleeper(self):
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(300)"]
        )
        self.sleepers.append(proc)
        return proc

    def status(self, udid=UDID_IPHONE, session="session-a"):
        proc, data = self.jcli("status", udid, session=session)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return data

    def acquire(self, udid=UDID_IPHONE, session="session-a", pid=None,
                extra=()):
        args = ["acquire", udid, "--ttl", "120"] + list(extra)
        proc, data = self.jcli(*args, session=session, pid=pid)
        return proc, data

    def release(self, udid=UDID_IPHONE, session="session-a", extra=()):
        args = ["release", udid] + list(extra)
        return self.jcli(*args, session=session)

    def start_waiter(self, udid, session, extra=("--wait-timeout", "30")):
        args = [sys.executable, "-I", str(SCRIPT), "acquire", udid,
                "--ttl", "120", "--json", "--quiet"] + list(extra)
        return subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=self.env_for(session),
        )

    def wait_for_queue(self, length, udid=UDID_IPHONE, timeout=10):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            last = self.status(udid)
            if (last.get("queue") or {}).get("length", 0) >= length:
                return last
            time.sleep(0.1)
        self.fail("queue did not reach %d: %r" % (length, last))

    def state_file(self, udid=UDID_IPHONE):
        return self.home / "devices" / udid / "state.json"

    # -- tests ------------------------------------------------------------

    def test_acquire_status_release(self):
        proc, data = self.acquire()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(data["ok"])
        self.assertEqual(data["state"], "held")
        self.assertTrue(data["lease"]["mine"])
        self.assertEqual(data["lease"]["session_id"], "session-a")
        self.assertTrue(data["token"])
        self.assertIsNone(data["reclaimed"])
        self.assertFalse(data["reentrant"])

        status = self.status()
        self.assertEqual(status["state"], "held")
        self.assertTrue(status["lease"]["mine"])
        self.assertEqual(status["queue"]["length"], 0)

        proc, data = self.release()
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(data["released"])
        self.assertEqual(self.status()["state"], "free")

        proc, data = self.release()
        self.assertEqual(proc.returncode, 0)
        self.assertFalse(data["released"])

    def test_reentrant_same_session_keeps_longer_ttl(self):
        _, first = self.acquire(extra=("--ttl", "60"))
        token = first["token"]
        first_expiry = first["lease"]["expires_at"]

        proc, second = self.acquire(extra=("--ttl", "10"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(second["reentrant"])
        self.assertEqual(second["token"], token)
        self.assertGreaterEqual(second["lease"]["expires_at"], first_expiry)
        self.assertEqual(second["lease"]["session_id"], "session-a")

        self.release()
        self.assertEqual(self.status()["state"], "free")

    def test_no_wait_busy_and_timeout_cleanup(self):
        holder = self.sleeper()
        proc, data = self.acquire(pid=holder.pid)
        self.assertEqual(proc.returncode, 0, proc.stderr)

        proc, data = self.jcli("acquire", UDID_IPHONE, "--no-wait",
                               session="session-b")
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertFalse(data["ok"])
        self.assertEqual(data["error"]["code"], "BUSY")
        self.assertEqual(data["error"]["held_by"], "session-a")
        self.assertEqual(self.status()["queue"]["length"], 0)

        proc, data = self.jcli("acquire", UDID_IPHONE, "--wait-timeout", "0.4",
                               session="session-b")
        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertEqual(data["error"]["code"], "BUSY")
        self.assertIn("timed out", data["error"]["message"])
        self.assertEqual(self.status()["queue"]["length"], 0)

    def test_waiter_queues_and_gets_lease_after_release(self):
        self.acquire()
        waiter = self.start_waiter(UDID_IPHONE, "session-b")
        try:
            snapshot = self.wait_for_queue(1)
            entry = snapshot["queue"]["entries"][0]
            self.assertEqual(entry["session_id"], "session-b")
            self.assertEqual(entry["position"], 1)

            self.release()
            out, err = waiter.communicate(timeout=30)
            self.assertEqual(waiter.returncode, 0, err)
            data = json.loads(out)
            self.assertTrue(data["ok"])
            self.assertEqual(data["lease"]["session_id"], "session-b")
            self.assertGreaterEqual(data["waited_ms"], 0)

            status = self.status()
            self.assertEqual(status["lease"]["session_id"], "session-b")
            self.release(session="session-b")
        finally:
            if waiter.poll() is None:
                waiter.kill()
                waiter.wait()
            for stream in (waiter.stdout, waiter.stderr):
                if stream and not stream.closed:
                    stream.close()

    def test_release_not_owner_then_force(self):
        self.acquire()
        proc, data = self.release(session="session-b")
        self.assertEqual(proc.returncode, 4, proc.stderr)
        self.assertEqual(data["error"]["code"], "NOT_OWNER")

        proc, data = self.release(session="session-b", extra=("--force",))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(data["released"])
        self.assertEqual(self.status()["state"], "free")

    def test_release_by_token(self):
        _, data = self.acquire()
        token = data["token"]
        other = self.status(session="session-b")
        self.assertFalse(other["lease"]["mine"])
        self.assertIsNone(other["lease"]["token"])
        proc, data = self.release(session="session-b",
                                  extra=("--token", token))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(data["released"])

    def test_renew_and_errors(self):
        proc, data = self.acquire(extra=("--ttl", "30"))
        self.assertEqual(proc.returncode, 0, proc.stderr)

        proc, data = self.jcli("renew", UDID_IPHONE, "--ttl", "300")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(data["renewed"])
        self.assertGreater(self.status()["lease"]["expires_in_seconds"], 250)

        proc, data = self.jcli("renew", UDID_IPHONE, "--ttl", "300",
                               session="session-b")
        self.assertEqual(proc.returncode, 4)
        self.assertEqual(data["error"]["code"], "NOT_OWNER")

        self.release()
        proc, data = self.jcli("renew", UDID_IPHONE, "--ttl", "300",
                               session="session-b")
        self.assertEqual(proc.returncode, 4)
        self.assertEqual(data["error"]["code"], "NOT_HELD")

    def test_dead_owner_is_reclaimed(self):
        holder = self.sleeper()
        proc, _ = self.acquire(pid=holder.pid, extra=("--ttl", "300"))
        self.assertEqual(proc.returncode, 0, proc.stderr)

        holder.kill()
        holder.wait(timeout=5)

        proc, data = self.jcli("acquire", UDID_IPHONE, "--no-wait",
                               session="session-b")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(data["ok"])
        self.assertEqual(data["lease"]["session_id"], "session-b")
        self.assertIsNotNone(data["reclaimed"])
        self.assertEqual(data["reclaimed"]["reason"], "owner_gone")
        self.assertEqual(data["reclaimed"]["session_id"], "session-a")

        self.release(session="session-b")

    def test_ttl_expiry_reclaims_even_if_owner_alive(self):
        holder = self.sleeper()
        proc, _ = self.acquire(pid=holder.pid, extra=("--ttl", "1"))
        self.assertEqual(proc.returncode, 0, proc.stderr)
        time.sleep(1.4)

        proc, data = self.jcli("acquire", UDID_IPHONE, "--no-wait",
                               session="session-b")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(data["lease"]["session_id"], "session-b")
        self.assertEqual(data["reclaimed"]["reason"], "ttl_expired")
        self.assertTrue(data["reclaimed"]["owner_alive"])

        self.release(session="session-b")

    def test_acquire_force_steals(self):
        self.acquire()
        proc, data = self.jcli("acquire", UDID_IPHONE, "--force",
                               session="session-b")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(data["lease"]["session_id"], "session-b")
        self.assertTrue(data["lease"]["mine"])

        proc, _ = self.release()  # session-a is no longer the owner
        self.assertEqual(proc.returncode, 4)
        self.release(session="session-b")

    def test_device_resolution_errors(self):
        proc, data = self.jcli("acquire", "Apple Watch Series 12 (46mm)",
                               "--no-wait")
        self.assertEqual(proc.returncode, 3, proc.stderr)
        self.assertEqual(data["error"]["code"], "DEVICE_AMBIGUOUS")
        self.assertEqual(len(data["error"]["candidates"]), 2)

        proc, data = self.jcli("acquire", "No Such Device")
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(data["error"]["code"], "DEVICE_NOT_FOUND")
        self.assertTrue(data["error"]["candidates"])

        proc, data = self.jcli("acquire", UDID_UNAVAILABLE)
        self.assertEqual(proc.returncode, 3)
        self.assertEqual(data["error"]["code"], "DEVICE_UNAVAILABLE")

        proc, data = self.jcli("acquire", UDID_IPHONE.lower())
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(data["device"]["udid"], UDID_IPHONE)
        self.release()

    def test_unique_name_resolves(self):
        proc, data = self.jcli("acquire", "iPhone 18 Pro Max")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(data["device"]["udid"], UDID_IPHONE)
        self.release()

    def test_status_unknown_udid_does_not_create_state(self):
        data = self.status(UDID_UNKNOWN)
        self.assertEqual(data["state"], "free")
        self.assertFalse(data["device"]["known"])
        self.assertFalse((self.home / "devices" / UDID_UNKNOWN).exists())

    def test_list_and_corrupt_state(self):
        _, data = self.jcli("list")
        self.assertTrue(data["ok"])
        udids = {entry["device"]["udid"] for entry in data["devices"]}
        self.assertIn(UDID_IPHONE, udids)
        self.assertIn(UDID_WATCH, udids)
        self.assertEqual(len(data["devices"]), 6)
        watch = [entry for entry in data["devices"]
                 if entry["device"]["udid"] == UDID_WATCH][0]
        self.assertEqual(watch["state"], "free")

        directory = self.home / "devices" / UDID_UNKNOWN
        directory.mkdir(parents=True, mode=0o700)
        (directory / "state.json").write_text("{not json", encoding="utf-8")

        proc, data = self.jcli("status", UDID_UNKNOWN)
        self.assertEqual(proc.returncode, 5)
        self.assertEqual(data["error"]["code"], "STATE_CORRUPT")

        proc, data = self.jcli("gc")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        actions = [a for a in data["actions"] if a["udid"] == UDID_UNKNOWN]
        self.assertTrue(actions)
        self.assertEqual(actions[0]["action"], "error")

    def test_fifo_order(self):
        self.acquire()
        waiters = []
        try:
            for index, session in enumerate(
                    ("session-b", "session-c", "session-d")):
                waiters.append(self.start_waiter(UDID_IPHONE, session))
                time.sleep(0.5)
            snapshot = self.wait_for_queue(3)
            order = [entry["session_id"]
                     for entry in snapshot["queue"]["entries"]]
            self.assertEqual(order, ["session-b", "session-c", "session-d"])

            self.release()
            for waiter, session in zip(waiters,
                                       ("session-b", "session-c",
                                        "session-d")):
                out, err = waiter.communicate(timeout=30)
                self.assertEqual(waiter.returncode, 0, err)
                data = json.loads(out)
                self.assertTrue(data["ok"])
                self.assertEqual(data["lease"]["session_id"], session)
                status = self.status()
                self.assertEqual(status["lease"]["session_id"], session)
                self.release(session=session)
        finally:
            for waiter in waiters:
                if waiter.poll() is None:
                    waiter.kill()
                    waiter.wait()
                for stream in (waiter.stdout, waiter.stderr):
                    if stream and not stream.closed:
                        stream.close()

    def test_dead_waiter_is_swept(self):
        self.acquire()
        waiter = self.start_waiter(UDID_IPHONE, "session-b")
        try:
            self.wait_for_queue(1)
            waiter.kill()
            waiter.wait(timeout=5)
            self.assertEqual(self.status()["queue"]["length"], 0)
            proc, data = self.jcli("gc")
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertTrue(any(a["action"] == "swept_dead_queue"
                                for a in data["actions"]))
        finally:
            if waiter.poll() is None:
                waiter.kill()
                waiter.wait()
            waiter.stdout.close()
            waiter.stderr.close()

        self.release()
        proc, data = self.jcli("acquire", UDID_IPHONE, "--wait-timeout", "10",
                               session="session-c")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.release(session="session-c")

    def test_gc_clears_stale_lease(self):
        holder = self.sleeper()
        self.acquire(pid=holder.pid, extra=("--ttl", "300"))
        holder.kill()
        holder.wait(timeout=5)

        proc, data = self.jcli("gc", "--dry-run")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue(any(a["action"] == "cleared_stale_lease"
                            for a in data["actions"]))
        self.assertEqual(self.status()["state"], "stale")

        proc, data = self.jcli("gc")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.status()["state"], "free")

    def test_run_passthrough_and_release(self):
        proc = self.cli("run", UDID_IPHONE, "--",
                        sys.executable, "-c",
                        "import sys; print('hello-run'); sys.exit(7)")
        self.assertEqual(proc.returncode, 7, proc.stderr)
        self.assertIn("hello-run", proc.stdout)
        self.assertIn("acquired", proc.stderr)
        self.assertEqual(self.status()["state"], "free")

    def test_run_reentrant_keeps_outer_lease(self):
        self.acquire()
        proc = self.cli("run", UDID_IPHONE, "--",
                        sys.executable, "-c", "print('inner')")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("inner", proc.stdout)
        status = self.status()
        self.assertEqual(status["state"], "held")
        self.assertTrue(status["lease"]["mine"])
        self.release()
        self.assertEqual(self.status()["state"], "free")

    def test_whoami(self):
        proc, data = self.jcli("whoami")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(data["session_id"], "session-a")
        self.assertEqual(data["session_source"], "CLAUDE_CODE_SESSION_ID")
        self.assertEqual(data["pid_source"], "CLAUDE_PID")
        self.assertTrue(data["in_claude"])

        proc, data = self.jcli("whoami", "--owner", "explicit",
                               "--owner-pid", "123")
        self.assertEqual(data["session_id"], "explicit")
        self.assertEqual(data["owner_pid"], 123)
        self.assertEqual(data["pid_source"], "--owner-pid")

    def test_usage_errors_are_json(self):
        proc, data = self.jcli("acquire")
        self.assertEqual(proc.returncode, 64, proc.stderr)
        self.assertEqual(data["error"]["code"], "USAGE")

        proc, data = self.jcli("acquire", UDID_IPHONE, "--ttl", "0")
        self.assertEqual(proc.returncode, 64)
        self.assertEqual(data["error"]["code"], "USAGE")

        proc, data = self.jcli("run", UDID_IPHONE)
        self.assertEqual(proc.returncode, 64)
        self.assertEqual(data["error"]["code"], "USAGE")

    def test_stress_never_two_holders(self):
        log = Path(self.tmp.name) / "intervals.log"
        worker = Path(self.tmp.name) / "worker.py"
        worker.write_text(textwrap.dedent("""
            import json, os, subprocess, sys, time
            script, udid, session, cycles, log = sys.argv[1:6]
            env = os.environ.copy()
            for _ in range(int(cycles)):
                proc = subprocess.run(
                    [sys.executable, "-I", script, "acquire", udid,
                     "--ttl", "120", "--wait-timeout", "60", "--json",
                     "--quiet"],
                    capture_output=True, text=True, env=env)
                if proc.returncode != 0:
                    sys.stderr.write("acquire failed: %s %s\\n" % (
                        proc.returncode, proc.stderr))
                    sys.exit(10)
                fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                             0o600)
                os.write(fd, ("ENTER %s %.6f\\n" % (
                    session, time.time())).encode())
                os.close(fd)
                time.sleep(0.05)
                fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                             0o600)
                os.write(fd, ("EXIT %s %.6f\\n" % (
                    session, time.time())).encode())
                os.close(fd)
                proc = subprocess.run(
                    [sys.executable, "-I", script, "release", udid,
                     "--json", "--quiet"],
                    capture_output=True, text=True, env=env)
                if proc.returncode != 0:
                    sys.stderr.write("release failed: %s %s\\n" % (
                        proc.returncode, proc.stderr))
                    sys.exit(11)
        """).lstrip(), encoding="utf-8")

        workers = []
        for index in range(3):
            session = "stress-%d" % index
            env = self.env_for(session)
            workers.append(subprocess.Popen(
                [sys.executable, "-I", str(worker), str(SCRIPT),
                 UDID_IPHONE, session, "2", str(log)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                env=env,
            ))
        for proc in workers:
            _, err = proc.communicate(timeout=120)
            self.assertEqual(proc.returncode, 0, err)

        events = []
        for line in log.read_text(encoding="utf-8").splitlines():
            kind, session, stamp = line.split()
            events.append((float(stamp), kind, session))
        events.sort()
        self.assertEqual(sum(1 for e in events if e[1] == "ENTER"), 6)
        depth = 0
        for _, kind, session in events:
            depth += 1 if kind == "ENTER" else -1
            self.assertIn(depth, (0, 1),
                          "overlapping holders at %s (%s)" % (session, kind))
        self.assertEqual(depth, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
