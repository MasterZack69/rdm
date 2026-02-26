#!/usr/bin/env python3
import argparse
import csv
import os
import shutil
import statistics
import subprocess
import time
from datetime import datetime
from pathlib import Path


def format_mib_per_s(size_bytes: int, elapsed_s: float) -> float | None:
    if elapsed_s <= 0 or size_bytes <= 0:
        return None
    return (size_bytes / (1024 * 1024)) / elapsed_s


def run_trial(command: list[str]) -> tuple[int, float, float, float, int]:
    start = time.perf_counter()
    proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    max_rss_kb = 0
    user_cpu = 0.0
    sys_cpu = 0.0
    ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    while proc.poll() is None:
        status_path = Path(f"/proc/{proc.pid}/status")
        if status_path.exists():
            for line in status_path.read_text().splitlines():
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        max_rss_kb = max(max_rss_kb, int(parts[1]))
                    break

        stat_path = Path(f"/proc/{proc.pid}/stat")
        if stat_path.exists():
            fields = stat_path.read_text().split()
            user_cpu = float(fields[13]) / ticks
            sys_cpu = float(fields[14]) / ticks
        time.sleep(0.01)

    _, _ = proc.communicate()
    elapsed = time.perf_counter() - start

    return proc.returncode, elapsed, user_cpu, sys_cpu, max_rss_kb


def mean_or_none(values: list[float]) -> float | None:
    return statistics.mean(values) if values else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark rdm vs wget/curl")
    parser.add_argument("url", nargs="?", default="https://zackie.site/zenitsu.mp4")
    parser.add_argument("--trials", type=int, default=int(os.getenv("TRIALS", "3")))
    parser.add_argument("--rdm-bin", default=os.getenv("RDM_BIN", "target/release/rdm"))
    parser.add_argument("--rdm-connections", type=int, default=int(os.getenv("RDM_CONNECTIONS", "12")))
    parser.add_argument("--out-dir", default=os.getenv("OUT_DIR", "benchmark_runs"))
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    work_dir = out_dir / f"work_{stamp}"
    work_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"results_{stamp}.csv"
    summary_path = out_dir / f"summary_{stamp}.md"

    tools: dict[str, list[str]] = {}
    if Path(args.rdm_bin).exists():
        tools["rdm"] = [args.rdm_bin, "download", args.url, "-c", str(args.rdm_connections)]
    else:
        print(f"warning: skipping rdm; binary not found at {args.rdm_bin}")

    if shutil.which("wget"):
        tools["wget"] = ["wget", "-q"]
    else:
        print("warning: skipping wget; command not found")

    if shutil.which("curl"):
        tools["curl"] = ["curl", "-L", "-sS"]
    else:
        print("warning: skipping curl; command not found")

    rows: list[dict[str, str]] = []

    for tool, base_cmd in tools.items():
        for trial in range(1, args.trials + 1):
            outfile = work_dir / f"{tool}_trial{trial}.bin"
            if outfile.exists():
                outfile.unlink()
            resume_file = outfile.with_suffix(outfile.suffix + ".rdmresume")
            if resume_file.exists():
                resume_file.unlink()

            if tool == "rdm":
                downloads_dir = Path.home() / "Downloads"
                downloads_dir.mkdir(parents=True, exist_ok=True)
                rdm_out = downloads_dir / outfile.name
                if rdm_out.exists():
                    rdm_out.unlink()
                cmd = base_cmd + ["-o", outfile.name]
            elif tool == "wget":
                cmd = base_cmd + ["-O", str(outfile), args.url]
            else:
                cmd = base_cmd + ["-o", str(outfile), args.url]

            exit_code, elapsed, user_cpu, sys_cpu, max_rss_kb = run_trial(cmd)
            if tool == "rdm":
                rdm_out = (Path.home() / "Downloads") / outfile.name
                size_bytes = rdm_out.stat().st_size if rdm_out.exists() else 0
                if rdm_out.exists():
                    rdm_out.unlink()
            else:
                size_bytes = outfile.stat().st_size if outfile.exists() else 0
            throughput = format_mib_per_s(size_bytes, elapsed) if exit_code == 0 else None
            cpu_pct = ((user_cpu + sys_cpu) / elapsed * 100.0) if elapsed > 0 else None

            rows.append(
                {
                    "tool": tool,
                    "trial": str(trial),
                    "exit_code": str(exit_code),
                    "elapsed_seconds": f"{elapsed:.6f}",
                    "max_rss_kb": str(max_rss_kb),
                    "cpu_percent": f"{cpu_pct:.3f}" if cpu_pct is not None else "",
                    "file_bytes": str(size_bytes),
                    "throughput_mib_s": f"{throughput:.3f}" if throughput is not None else "",
                }
            )

    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["tool", "trial", "exit_code", "elapsed_seconds", "max_rss_kb", "cpu_percent", "file_bytes", "throughput_mib_s"],
        )
        writer.writeheader()
        writer.writerows(rows)

    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row["tool"], []).append(row)

    with summary_path.open("w") as f:
        f.write("# Download Manager Benchmark Summary\n\n")
        f.write(f"- URL: `{args.url}`\n")
        f.write(f"- Trials per tool: `{args.trials}`\n")
        f.write(f"- CSV: `{csv_path}`\n\n")
        f.write("| Tool | Successes | Avg Throughput (MiB/s) | Avg Elapsed (s) | Avg Max RSS (KiB) | Avg CPU (%) |\n")
        f.write("|---|---:|---:|---:|---:|---:|\n")

        for tool in sorted(grouped.keys()):
            successful = [r for r in grouped[tool] if r["exit_code"] == "0"]
            if not successful:
                f.write(f"| {tool} | 0/{len(grouped[tool])} | n/a | n/a | n/a | n/a |\n")
                continue
            avg_tp = mean_or_none([float(r["throughput_mib_s"]) for r in successful if r["throughput_mib_s"]])
            avg_elapsed = mean_or_none([float(r["elapsed_seconds"]) for r in successful])
            avg_rss = mean_or_none([float(r["max_rss_kb"]) for r in successful])
            avg_cpu = mean_or_none([float(r["cpu_percent"]) for r in successful if r["cpu_percent"]])
            def fmt(v, digits):
                if v is None:
                    return "n/a"
                return f"{v:.{digits}f}"

            f.write(
                f"| {tool} | {len(successful)}/{len(grouped[tool])} | "
                f"{fmt(avg_tp, 3)} | {fmt(avg_elapsed, 3)} | {fmt(avg_rss, 1)} | {fmt(avg_cpu, 1)} |\n"
            )

        failures = [r for r in rows if r["exit_code"] != "0"]
        if failures:
            f.write("\n## Failures\n\n")
            for row in failures:
                f.write(f"- {row['tool']} trial {row['trial']} exited with code {row['exit_code']}.\n")

    print(f"Wrote: {csv_path}")
    print(f"Wrote: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
