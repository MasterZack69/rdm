#!/usr/bin/env python3
"""Compare RDM binaries against a controlled HTTP 429 wave on loopback."""
import argparse
import hashlib
import http.server
import os
import signal
from pathlib import Path
import re
import socketserver
import subprocess
import tempfile
import threading
import time

from download_gate import write_results

SIZE = 16 * 1024 * 1024
PROBE = 65536
PAYLOAD = bytes(range(256)) * (SIZE // 256)
EXPECTED_HASH = hashlib.sha256(PAYLOAD).hexdigest()


class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.first_wave = threading.Event()
        self.first = 0
        self.active = 0
        self.peak = 0
        self.rejected = 0
        self.success = 0


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
        if not match:
            self.send_error(416)
            return
        start, end = map(int, match.groups())
        end = min(end, SIZE - 1)
        if start == 0 and end == PROBE - 1:
            self.send_response(206)
            self.send_header("Content-Length", str(PROBE))
            self.send_header("Content-Range", f"bytes 0-{end}/{SIZE}")
            self.end_headers()
            self.wfile.write(PAYLOAD[:PROBE])
            return

        state = self.server.state
        with state.lock:
            first = state.first < 8
            if first:
                state.first += 1
                state.rejected += 1
                if state.first == 8:
                    state.first_wave.set()
        if first:
            if not state.first_wave.wait(5):
                self.send_error(500)
                return
            self.reject()
            return

        with state.lock:
            rejected = state.active >= 4
            if rejected:
                state.rejected += 1
            else:
                state.active += 1
                state.peak = max(state.peak, state.active)
        if rejected:
            self.reject()
            return
        try:
            # Keep accepted requests active long enough to reveal retry storms.
            time.sleep(0.7)
            self.send_response(206)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Content-Range", f"bytes {start}-{end}/{SIZE}")
            self.end_headers()
            self.wfile.write(PAYLOAD[start:end + 1])
            with state.lock:
                state.success += 1
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with state.lock:
                state.active -= 1

    def reject(self):
        self.send_response(429)
        self.send_header("Content-Length", "0")
        self.end_headers()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--main", required=True, help="Baseline RDM binary")
    parser.add_argument("--candidate", required=True, help="Candidate RDM binary")
    parser.add_argument("--results", required=True, help="Fresh JSON output path")
    parser.add_argument("--workdir", default=".", help="Filesystem for temporary files")
    parser.add_argument("--rounds", type=int, default=4)
    args = parser.parse_args()
    workdir, output = Path(args.workdir).resolve(), Path(args.results)
    if output.exists() or not 1 <= args.rounds <= 50:
        raise SystemExit("Choose a fresh results file and 1–50 rounds.")
    tools = {"main": Path(args.main).resolve(), "candidate": Path(args.candidate).resolve()}
    server = Server(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_port}/file.bin"
    records = []
    try:
        for round_no in range(1, args.rounds + 1):
            order = ["main", "candidate"] if round_no % 2 else ["candidate", "main"]
            for position, name in enumerate(order, 1):
                state = server.state = State()
                with tempfile.TemporaryDirectory(dir=workdir, prefix="overload-") as home:
                    destination = Path(home) / "file.bin"
                    env = {"HOME": home, "XDG_CONFIG_HOME": home + "/c", "XDG_DATA_HOME": home + "/d",
                           "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
                    command = [str(tools[name]), "--allow-private", "-q", "-c", "8", "-o", str(destination), url]
                    started = time.monotonic()
                    proc = subprocess.Popen(command, env=env, stdout=subprocess.DEVNULL,
                                            stderr=subprocess.DEVNULL, start_new_session=True)
                    timed_out = False
                    try:
                        status = proc.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        timed_out = True
                        os.killpg(proc.pid, signal.SIGKILL)
                        status = proc.wait()
                    elapsed = time.monotonic() - started
                    size = destination.stat().st_size if destination.is_file() else None
                    digest = hashlib.sha256(destination.read_bytes()).hexdigest() if size is not None else None
                    valid = status == 0 and not timed_out and size == SIZE and digest == EXPECTED_HASH
                    records.append({"round": round_no, "position": position, "tool": name,
                                    "elapsed_s": elapsed, "size_bytes": size, "sha256": digest,
                                    "valid": valid, "exit_code": status, "timed_out": timed_out,
                                    "rejected_429": state.rejected, "accepted_peak": state.peak,
                                    "accepted": state.success})
                    write_results(output, records)
                    print(round_no, name, "valid", valid, "seconds", round(elapsed, 2),
                          "429", state.rejected, "peak", state.peak, flush=True)
                    if not valid:
                        raise SystemExit("Invalid trial; stopped.")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
