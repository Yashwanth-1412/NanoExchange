#!/usr/bin/env python3
"""Package a NanoExchange instrumentation run for analysis in Colab."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import re
import shutil
import tempfile
import zipfile
from collections import Counter, defaultdict
from bisect import bisect_right
from pathlib import Path
from statistics import mean


TSC_CYCLES_PER_NS = 3.792892
REQUEST_RE = re.compile(r"Processing MEClientRequest \[Action=(\d+)")
RESPONSE_RE = re.compile(r"MEClientResponse .* Status=(\d+)")
RDTSC_RE = re.compile(r"^RDTSC (\S+) (\d+)$")
TTT_RE = re.compile(r"^TTT (\S+) (\d+)$")


def phase_for(request_index: int, warmup: int) -> str:
    return "measured" if request_index > warmup else "warmup"


def parse_log(log_path: Path, warmup: int) -> tuple[list[dict], list[dict], dict]:
    rdtsc_rows: list[dict] = []
    ttt_rows: list[dict] = []
    ttt_sequences: defaultdict[str, int] = defaultdict(int)
    action_counts: Counter[str] = Counter()
    response_counts: Counter[str] = Counter()
    request_index = 0
    log_lines = 0

    with log_path.open("r", encoding="utf-8", errors="replace") as log_file:
        for line in log_file:
            log_lines += 1
            line = line.rstrip("\n")

            request_match = REQUEST_RE.search(line)
            if request_match:
                request_index += 1
                action_counts[request_match.group(1)] += 1
                continue

            response_match = RESPONSE_RE.search(line)
            if response_match:
                response_counts[response_match.group(1)] += 1
                continue

            rdtsc_match = RDTSC_RE.match(line)
            if rdtsc_match:
                cycles = int(rdtsc_match.group(2))
                rdtsc_rows.append(
                    {
                        "event": len(rdtsc_rows),
                        "request_index": request_index,
                        "phase": phase_for(request_index, warmup),
                        "tag": rdtsc_match.group(1),
                        "cycles": cycles,
                        "nanoseconds": cycles / TSC_CYCLES_PER_NS,
                    }
                )
                continue

            ttt_match = TTT_RE.match(line)
            if ttt_match:
                tag = ttt_match.group(1)
                ttt_sequences[tag] += 1
                ttt_rows.append(
                    {
                        "event": len(ttt_rows),
                        "request_index": request_index,
                        "phase": phase_for(request_index, warmup),
                        "tag": tag,
                        "sequence": ttt_sequences[tag] - 1,
                        "timestamp_ns": int(ttt_match.group(2)),
                    }
                )

    metadata = {
        "source_log": str(log_path),
        "source_log_bytes": log_path.stat().st_size,
        "source_log_lines": log_lines,
        "tsc_cycles_per_ns": TSC_CYCLES_PER_NS,
        "warmup_requests": warmup,
        "total_requests": request_index,
        "measured_requests": max(0, request_index - warmup),
        "action_counts": dict(action_counts),
        "response_status_counts": dict(response_counts),
        "rdtsc_records": len(rdtsc_rows),
        "ttt_records": len(ttt_rows),
    }
    return rdtsc_rows, ttt_rows, metadata


def measured_values(rows: list[dict], key: str = "nanoseconds") -> list[float]:
    return [float(row[key]) for row in rows if row["phase"] == "measured"]


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def stats(values: list[float]) -> dict:
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "min_ns": min(values),
        "avg_ns": mean(values),
        "p50_ns": percentile(values, 0.50),
        "p99_ns": percentile(values, 0.99),
        "p999_ns": percentile(values, 0.999),
        "max_ns": max(values),
    }


def make_hops(ttt_rows: list[dict], warmup: int) -> list[dict]:
    by_tag: defaultdict[str, list[dict]] = defaultdict(list)
    for row in ttt_rows:
        by_tag[row["tag"]].append(row)

    def pair_by_order(name: str, start_tag: str, end_tag: str) -> list[dict]:
        result = []
        for sample, (start, end) in enumerate(zip(by_tag[start_tag], by_tag[end_tag])):
            result.append(
                {
                    "hop": name,
                    "sample": sample,
                    "request_index": end["request_index"],
                    "phase": phase_for(end["request_index"], warmup),
                    "latency_ns": end["timestamp_ns"] - start["timestamp_ns"],
                }
            )
        return result

    def pair_latest(name: str, start_tag: str, end_tag: str) -> list[dict]:
        starts = by_tag[start_tag]
        start_times = [row["timestamp_ns"] for row in starts]
        result = []
        for sample, end in enumerate(by_tag[end_tag]):
            index = bisect_right(start_times, end["timestamp_ns"]) - 1
            if index < 0:
                continue
            start = starts[index]
            result.append(
                {
                    "hop": name,
                    "sample": sample,
                    "request_index": end["request_index"],
                    "phase": phase_for(end["request_index"], warmup),
                    "latency_ns": end["timestamp_ns"] - start["timestamp_ns"],
                }
            )
        return result

    hops: list[dict] = []
    hops.extend(pair_latest("T1_TCP_read_to_T2_request_queue", "T1_OrderServer_TCP_read", "T2_OrderServer_LFQueue_write"))
    hops.extend(pair_by_order("T2_request_queue_to_T3_engine_read", "T2_OrderServer_LFQueue_write", "T3_MatchingEngine_LFQueue_read"))
    hops.extend(pair_latest("T3_engine_read_to_T4_market_queue", "T3_MatchingEngine_LFQueue_read", "T4_MatchingEngine_LFQueue_write"))
    hops.extend(pair_latest("T3_engine_read_to_T4t_response_queue", "T3_MatchingEngine_LFQueue_read", "T4t_MatchingEngine_LFQueue_write"))
    hops.extend(pair_by_order("T4_market_queue_to_T5_publisher_read", "T4_MatchingEngine_LFQueue_write", "T5_MarketDataPublisher_LFQueue_read"))
    hops.extend(pair_by_order("T5_publisher_read_to_T6_udp_write", "T5_MarketDataPublisher_LFQueue_read", "T6_MarketDataPublisher_UDP_write"))
    hops.extend(pair_by_order("T4t_response_queue_to_T5t_gateway_read", "T4t_MatchingEngine_LFQueue_write", "T5t_OrderServer_LFQueue_read"))
    hops.extend(pair_by_order("T5t_gateway_read_to_T6t_tcp_stage", "T5t_OrderServer_LFQueue_read", "T6t_OrderServer_TCP_write"))
    return hops


def write_gzip_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def create_summary(rdtsc_rows: list[dict], hops: list[dict], metadata: dict) -> dict:
    rdtsc_stats = {}
    by_tag: defaultdict[str, list[dict]] = defaultdict(list)
    for row in rdtsc_rows:
        by_tag[row["tag"]].append(row)
    for tag, rows in sorted(by_tag.items()):
        rdtsc_stats[tag] = stats(measured_values(rows))

    hop_stats = {}
    by_hop: defaultdict[str, list[dict]] = defaultdict(list)
    for row in hops:
        if row["phase"] == "measured" and row["latency_ns"] >= 0:
            by_hop[row["hop"]].append(row["latency_ns"])
    for hop, values in sorted(by_hop.items()):
        hop_stats[hop] = stats(values)

    return {**metadata, "rdtsc_stats": rdtsc_stats, "hop_stats": hop_stats}


def package_run(run_dir: Path, output: Path, warmup: int, include_raw: bool) -> None:
    log_path = run_dir / "nanoexchange.log"
    if not log_path.is_file():
        raise FileNotFoundError(log_path)

    rdtsc_rows, ttt_rows, metadata = parse_log(log_path, warmup)
    hops = make_hops(ttt_rows, warmup)
    summary = create_summary(rdtsc_rows, hops, metadata)
    summary["run_dir"] = str(run_dir)
    summary["bundle_format"] = 1

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="nanoexchange-perf-") as temporary:
        staging = Path(temporary)
        write_gzip_csv(
            staging / "rdtsc.csv.gz",
            rdtsc_rows,
            ["event", "request_index", "phase", "tag", "cycles", "nanoseconds"],
        )
        write_gzip_csv(
            staging / "ttt.csv.gz",
            ttt_rows,
            ["event", "request_index", "phase", "tag", "sequence", "timestamp_ns"],
        )
        write_gzip_csv(
            staging / "hops.csv.gz",
            hops,
            ["hop", "sample", "request_index", "phase", "latency_ns"],
        )
        (staging / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        stdout_path = run_dir / "engine.stdout.log"
        if stdout_path.exists():
            shutil.copyfile(stdout_path, staging / "engine.stdout.log")
        visualize_script = Path(__file__).with_name("colab_visualize.py")
        shutil.copyfile(visualize_script, staging / "colab_visualize.py")
        if include_raw:
            with log_path.open("rb") as source, gzip.open(staging / "nanoexchange.log.gz", "wb", compresslevel=6) as target:
                shutil.copyfileobj(source, target)

        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
            for file_path in sorted(staging.iterdir()):
                archive.write(file_path, file_path.name)

    print(f"bundle={output}")
    print(f"source_log_bytes={metadata['source_log_bytes']}")
    print(f"rdtsc_records={metadata['rdtsc_records']}")
    print(f"ttt_records={metadata['ttt_records']}")
    print(f"hop_records={len(hops)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True, help="completed run directory")
    parser.add_argument("--output", type=Path, required=True, help="output ZIP path")
    parser.add_argument("--warmup", type=int, default=10_000)
    parser.add_argument("--no-raw", action="store_true", help="omit compressed raw log from the bundle")
    args = parser.parse_args()
    package_run(args.run, args.output, args.warmup, not args.no_raw)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
