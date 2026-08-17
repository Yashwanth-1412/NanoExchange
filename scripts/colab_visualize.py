#!/usr/bin/env python3
"""Create Colab/local plots from a NanoExchange performance bundle."""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import pandas as pd
except ModuleNotFoundError:
    print("Install dependencies first: pip install pandas matplotlib", file=sys.stderr)
    raise


def show_table(table: pd.DataFrame) -> None:
    try:
        display(table)
    except NameError:
        print(table.to_string())


def choose_input() -> Path:
    try:
        from google.colab import files  # type: ignore

        uploaded = files.upload()
        if not uploaded:
            raise RuntimeError("no bundle uploaded")
        return Path(next(iter(uploaded)))
    except ImportError as error:
        raise SystemExit("pass --input BUNDLE.zip outside Colab") from error


def read_bundle(bundle: Path, output_dir: Path) -> tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    output_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(bundle) as archive:
        summary = json.loads(archive.read("summary.json"))
        with archive.open("rdtsc.csv.gz") as source:
            rdtsc = pd.read_csv(source, compression="gzip")
        with archive.open("ttt.csv.gz") as source:
            ttt = pd.read_csv(source, compression="gzip")
        with archive.open("hops.csv.gz") as source:
            hops = pd.read_csv(source, compression="gzip")
    return summary, rdtsc, ttt, hops


def save_figure(figure: plt.Figure, output_dir: Path, name: str) -> None:
    figure.tight_layout()
    figure.savefig(output_dir / name, dpi=160, bbox_inches="tight")
    plt.show()
    plt.close(figure)


def percentile_table(rows: pd.DataFrame, group: str, value: str) -> pd.DataFrame:
    measured = rows[(rows["phase"] == "measured") & (rows[value] >= 0)]
    result = measured.groupby(group)[value].quantile([0.5, 0.99, 0.999]).unstack()
    result.columns = ["p50_ns", "p99_ns", "p999_ns"]
    return result.sort_values("p99_ns", ascending=False)


def plot_rdtsc(summary: dict, rdtsc: pd.DataFrame, output_dir: Path) -> None:
    table = percentile_table(rdtsc, "tag", "nanoseconds")
    print("RDTSC percentile table (nanoseconds)")
    show_table(table)

    figure, axis = plt.subplots(figsize=(14, 7))
    table.sort_values("p99_ns").plot.barh(ax=axis, logx=True)
    axis.set_title("Internal latency percentiles")
    axis.set_xlabel("Nanoseconds, logarithmic scale")
    axis.set_ylabel("")
    save_figure(figure, output_dir, "rdtsc_percentiles.png")

    selected = table.sort_values("p99_ns", ascending=False).head(6).index
    figure, axes = plt.subplots(2, 3, figsize=(16, 9))
    for axis, tag in zip(axes.flat, selected):
        values = rdtsc.loc[(rdtsc["tag"] == tag) & (rdtsc["phase"] == "measured"), "nanoseconds"]
        axis.hist(values, bins=100, log=True, color="#386cb0", alpha=0.85)
        axis.set_title(tag, fontsize=9)
        axis.set_xlabel("ns")
        axis.set_ylabel("count (log)")
    save_figure(figure, output_dir, "rdtsc_distributions.png")

    figure, axis = plt.subplots(figsize=(14, 7))
    for tag in selected[:4]:
        values = rdtsc.loc[(rdtsc["tag"] == tag) & (rdtsc["phase"] == "measured"), "nanoseconds"].sort_values().reset_index(drop=True)
        axis.plot(values.index / len(values) * 100, values, label=tag)
    axis.set_yscale("log")
    axis.set_xlabel("Percentile")
    axis.set_ylabel("Nanoseconds, logarithmic scale")
    axis.set_title("Internal latency CDF")
    axis.legend(fontsize=8)
    save_figure(figure, output_dir, "rdtsc_cdf.png")


def plot_hops(hops: pd.DataFrame, output_dir: Path) -> None:
    table = percentile_table(hops, "hop", "latency_ns")
    print("TTT hop percentile table (nanoseconds)")
    show_table(table)

    figure, axis = plt.subplots(figsize=(14, 7))
    table.sort_values("p99_ns").plot.barh(ax=axis, logx=True, color=["#fdc086", "#beaed4", "#7fc97f"])
    axis.set_title("Cross-component hop latency percentiles")
    axis.set_xlabel("Nanoseconds, logarithmic scale")
    axis.set_ylabel("")
    save_figure(figure, output_dir, "hop_percentiles.png")

    selected = table.sort_values("p99_ns", ascending=False).head(6).index
    figure, axes = plt.subplots(2, 3, figsize=(16, 9))
    for axis, hop in zip(axes.flat, selected):
        values = hops.loc[(hops["hop"] == hop) & (hops["phase"] == "measured"), "latency_ns"]
        axis.hist(values, bins=100, log=True, color="#7fc97f", alpha=0.85)
        axis.set_title(hop, fontsize=8)
        axis.set_xlabel("ns")
        axis.set_ylabel("count (log)")
    save_figure(figure, output_dir, "hop_distributions.png")

    figure, axis = plt.subplots(figsize=(14, 7))
    for hop in selected[:4]:
        values = hops.loc[(hops["hop"] == hop) & (hops["phase"] == "measured"), "latency_ns"].sort_values().reset_index(drop=True)
        axis.plot(values.index / len(values) * 100, values, label=hop)
    axis.set_yscale("log")
    axis.set_xlabel("Percentile")
    axis.set_ylabel("Nanoseconds, logarithmic scale")
    axis.set_title("Cross-component hop CDF")
    axis.legend(fontsize=8)
    save_figure(figure, output_dir, "hop_cdf.png")


def plot_timeline(rdtsc: pd.DataFrame, hops: pd.DataFrame, output_dir: Path) -> None:
    tags = [
        "Exchange_MatchingEngine_processClientRequest",
        "Exchange_MEOrderBook_addOrder",
        "Exchange_MEOrderBook_cancelOrder",
    ]
    figure, axis = plt.subplots(figsize=(14, 7))
    for tag in tags:
        values = rdtsc.loc[(rdtsc["tag"] == tag) & (rdtsc["phase"] == "measured")]
        if not values.empty:
            axis.plot(values["request_index"], values["nanoseconds"], ".", markersize=1, alpha=0.25, label=tag)
    axis.set_yscale("log")
    axis.set_xlabel("Request index")
    axis.set_ylabel("Nanoseconds, logarithmic scale")
    axis.set_title("Internal latency over the measured run")
    axis.legend(fontsize=8)
    save_figure(figure, output_dir, "internal_timeline.png")

    figure, axis = plt.subplots(figsize=(14, 7))
    for hop in hops["hop"].drop_duplicates()[:4]:
        values = hops.loc[(hops["hop"] == hop) & (hops["phase"] == "measured")]
        axis.plot(values["request_index"], values["latency_ns"], ".", markersize=1, alpha=0.2, label=hop)
    axis.set_yscale("log")
    axis.set_xlabel("Request index")
    axis.set_ylabel("Nanoseconds, logarithmic scale")
    axis.set_title("Queue/socket hop latency over the measured run")
    axis.legend(fontsize=8)
    save_figure(figure, output_dir, "hop_timeline.png")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="performance bundle ZIP; omit this in Colab to upload")
    parser.add_argument("--output", type=Path, default=Path("nanoexchange_visuals"))
    args = parser.parse_args()
    bundle = args.input or choose_input()
    summary, rdtsc, _ttt, hops = read_bundle(bundle, args.output)

    print(json.dumps({key: summary[key] for key in ("total_requests", "measured_requests", "action_counts", "response_status_counts", "rdtsc_records", "ttt_records")}, indent=2))
    plot_rdtsc(summary, rdtsc, args.output)
    plot_hops(hops, args.output)
    plot_timeline(rdtsc, hops, args.output)
    print(f"plots_written_to={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
