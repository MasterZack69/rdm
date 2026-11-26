#!/usr/bin/env python3
"""Paired, integrity-checked external download comparisons. Stdlib only."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import random
import shutil
import re
import signal
import statistics
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

LABEL = re.compile(r"[A-Za-z0-9_.-]{1,48}\Z")
SHA = re.compile(r"[0-9a-f]{64}\Z")


def load_config(path):
    cfg = json.loads(Path(path).read_text(encoding="utf-8"))
    targets, tools = cfg["targets"], cfg["tools"]
    if len(targets) < 1 or len(tools) < 2:
        raise ValueError("Need at least one target and two tools")
    for collection in (targets, tools):
        labels = [item["label"] for item in collection]
        if len(set(labels)) != len(labels) or any(not LABEL.fullmatch(s) for s in labels):
            raise ValueError("Labels must be unique, short, and non-sensitive")
    for target in targets:
        if not isinstance(target["size_bytes"], int) or target["size_bytes"] <= 0:
            raise ValueError("Expected size must be a positive integer")
        if not SHA.fullmatch(target["sha256"]):
            raise ValueError("Expected SHA-256 must be lowercase hex")
    for tool in tools:
        argv = tool["argv"]
        env_names = tool.get("inherit_env", [])
        if not isinstance(env_names, list) or any(not isinstance(n, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", n) for n in env_names):
            raise ValueError("Invalid environment allowlist")
        if not isinstance(argv, list) or not argv or not all(isinstance(a, str) for a in argv):
            raise ValueError("Each tool needs an argument array")
        if sum(a.count("{url}") for a in argv) != 1 or sum(a.count("{output}") for a in argv) != 1:
            raise ValueError("Each tool requires exactly one {url} and {output}")
    return cfg


def read_url(path):
    # URL content, command arguments, and subprocess output are never printed
    # or persisted: signed links and provider diagnostics can contain secrets.
    value = Path(path).read_text(encoding="utf-8").strip()
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or "\n" in value or "\r" in value:
        raise ValueError("URL file must contain one HTTP(S) URL")
    return value


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_results(path, doc):
    # Save after every trial without ever leaving a partially written result.
    path = Path(path)
    fd, tmp = tempfile.mkstemp(prefix=".gate-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2)
            f.write("\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def run_one(tool, target, url, home, timeout):
    home.mkdir()
    destination = home / "download.bin"
    argv = [a.replace("{url}", url).replace("{output}", str(destination)) for a in tool["argv"]]
    # Isolate user config and proxy settings by default. Credentials or
    # library paths may be explicitly passed by name, without saving values.
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C.UTF-8"}
    env.update({name: os.environ[name] for name in tool.get("inherit_env", []) if name in os.environ})
    env.update(HOME=str(home), XDG_CONFIG_HOME=str(home / "config"), XDG_DATA_HOME=str(home / "data"))
    started = time.monotonic()
    status, timed_out = None, False
    try:
        proc = subprocess.Popen(argv, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            status = proc.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as exc:
            timed_out = isinstance(exc, subprocess.TimeoutExpired)
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.wait()
            if isinstance(exc, KeyboardInterrupt):
                raise
    except OSError:
        pass  # Do not expose the command or URL via exceptions.
    elapsed = time.monotonic() - started
    size = destination.stat().st_size if destination.is_file() else None
    actual_hash = digest(destination) if size == target["size_bytes"] else None
    valid = status == 0 and size == target["size_bytes"] and actual_hash == target["sha256"]
    return {"elapsed_s": elapsed, "size_bytes": size, "sha256": actual_hash, "exit_code": status,
            "timed_out": timed_out, "valid": valid,
            "throughput_MiB_s": size / 1048576 / elapsed if valid and elapsed else None}


def summarize(doc):
    result = {}
    for target in doc["targets"]:
        subset = [r for r in doc["runs"] if r["target"] == target]
        result[target] = {}
        for tool in doc["tools"]:
            rows = [r for r in subset if r["tool"] == tool and r["valid"]]
            if rows:
                speeds = [r["throughput_MiB_s"] for r in rows]
                result[target][tool] = {"count": len(rows), "median_MiB_s": statistics.median(speeds),
                                        "min_MiB_s": min(speeds), "max_MiB_s": max(speeds)}
        base = {r["round"]: r for r in subset if r["tool"] == doc["baseline"] and r["valid"]}
        for tool in doc["tools"]:
            if tool == doc["baseline"] or tool not in result[target]:
                continue
            other = {r["round"]: r for r in subset if r["tool"] == tool and r["valid"]}
            rounds = sorted(base.keys() & other.keys())
            result[target][tool]["paired_rounds"] = len(rounds)
            result[target][tool]["paired_wins"] = sum(other[n]["elapsed_s"] < base[n]["elapsed_s"] for n in rounds)
            result[target][tool]["median_change_percent"] = 100 * (result[target][tool]["median_MiB_s"] / result[target][doc["baseline"]]["median_MiB_s"] - 1) if rounds else None
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", required=True, help="Private JSON configuration")
    ap.add_argument("--results", required=True, help="New JSON output path")
    ap.add_argument("--baseline", required=True, help="Baseline tool label")
    ap.add_argument("--rounds", type=int, default=8)
    ap.add_argument("--seed", type=int, default=724)
    ap.add_argument("--timeout", type=float, default=180)
    ap.add_argument("--workdir", default=".", help="Filesystem to use for temporary downloads")
    args = ap.parse_args()
    try:
        if not 1 <= args.rounds <= 50 or not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ValueError("Invalid rounds or timeout")
        path = Path(args.results)
        if path.exists():
            raise ValueError("Results path already exists; choose a fresh file")
        cfg = load_config(args.config)
        labels = [t["label"] for t in cfg["tools"]]
        if args.baseline not in labels:
            raise ValueError("Baseline must match a tool label")
        urls = {t["label"]: read_url(t["url_file"]) for t in cfg["targets"]}
        tool_hashes = {}
        for tool in cfg["tools"]:
            binary = tool.get("fingerprint_file", tool["argv"][0])
            binary = shutil.which(binary) or binary
            tool_hashes[tool["label"]] = digest(Path(binary))
        target_specs = [{"label": t["label"], "size_bytes": t["size_bytes"], "sha256": t["sha256"]}
                        for t in cfg["targets"]]
        doc = {"version": 1, "tool_sha256": tool_hashes, "target_specs": target_specs, "baseline": args.baseline, "tools": labels, "targets": [t["label"] for t in cfg["targets"]],
               "rounds_requested": args.rounds, "seed": args.seed, "timeout_s": args.timeout, "runs": [], "summary": {}}
        rng = random.Random(args.seed)
        with tempfile.TemporaryDirectory(prefix="rdm-gate-", dir=args.workdir) as root:
            for target in cfg["targets"]:
                order = list(cfg["tools"])
                rng.shuffle(order)
                for round_no in range(1, args.rounds + 1):
                    # Rotate a shuffled order: each tool visits each position
                    # equally often when rounds is a multiple of tool count.
                    offset = (round_no - 1) % len(order)
                    for pos, tool in enumerate(order[offset:] + order[:offset], 1):
                        home = Path(root) / f'{target["label"]}-{round_no}-{pos}'
                        row = {"target": target["label"], "round": round_no, "position": pos, "tool": tool["label"]}
                        row.update(run_one(tool, target, urls[target["label"]], home, args.timeout))
                        doc["runs"].append(row)
                        doc["summary"] = summarize(doc)
                        write_results(path, doc)
                        print(f'{row["target"]} round={round_no} tool={row["tool"]} valid={row["valid"]}', flush=True)
                        # Delete each file immediately so storage use is bounded.
                        if home.exists():
                            shutil.rmtree(home)
                        if not row["valid"]:
                            print("Invalid trial: stopped; see the result status, not private logs.")
                            return 1
        print("Completed. Interpret paired runs and correctness separately; no automatic merge decision.")
        return 0
    except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError):
        # Configuration and path errors may include credentials; keep them out
        # of logs and ask for a local config check without echoing the value.
        print("Invalid or unreadable configuration/results path; check the local configuration.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
