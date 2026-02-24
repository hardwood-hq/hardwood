#!/usr/bin/env python3
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Python (PyArrow) counterpart of FlatPerformanceTest.java.
Reads NYC Yellow Taxi Trip Records and sums passenger_count, trip_distance, and fare_amount.

Note: This test uses column projection (reading only the 3 summed columns), which matches
the Hardwood projection and column-reader contenders in FlatPerformanceTest.java. The
parquet-java contenders read all ~19 columns without projection, so direct comparison
against parquet-java is not apples-to-apples.

Usage:
    python taxi_sum.py                          # run all contenders, 5 runs each
    python taxi_sum.py -c single_threaded       # single-threaded only
    python taxi_sum.py -c multi_threaded -r 10  # multi-threaded, 10 runs
"""

import argparse
import os
import platform
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DATA_DIR = Path("../test-data-setup/target/tlc-trip-record-data")
DEFAULT_RUNS = 5
COLUMNS = ["passenger_count", "trip_distance", "fare_amount"]

CONTENDERS = {
    "single_threaded": ("PyArrow (single-threaded)", False),
    "multi_threaded": ("PyArrow (multi-threaded)", True),
}


def get_available_files(start_year=2016, start_month=1, end_year=2025, end_month=11):
    """Find all available taxi data files in the date range."""
    files = []
    year, month = start_year, start_month

    while (year, month) <= (end_year, end_month):
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        file_path = DATA_DIR / filename
        if file_path.exists() and file_path.stat().st_size > 0:
            files.append(file_path)

        month += 1
        if month > 12:
            month = 1
            year += 1

    return files


def sum_columns(files, use_threads):
    """Read parquet files and sum the three columns."""
    passenger_count = 0
    trip_distance = 0.0
    fare_amount = 0.0
    row_count = 0

    for file_path in files:
        table = pq.read_table(file_path, columns=COLUMNS, use_threads=use_threads)

        row_count += table.num_rows

        pc_sum = pc.sum(table.column("passenger_count")).as_py()
        if pc_sum is not None:
            passenger_count += int(pc_sum)

        td_sum = pc.sum(table.column("trip_distance")).as_py()
        if td_sum is not None:
            trip_distance += td_sum

        fa_sum = pc.sum(table.column("fare_amount")).as_py()
        if fa_sum is not None:
            fare_amount += fa_sum

    return passenger_count, trip_distance, fare_amount, row_count


def parse_contenders(value):
    """Parse contender specification from command line."""
    if value is None or value.strip().lower() == "all":
        return list(CONTENDERS.keys())
    names = [n.strip() for n in value.split(",") if n.strip()]
    for name in names:
        if name not in CONTENDERS:
            valid = ", ".join(CONTENDERS.keys())
            raise argparse.ArgumentTypeError(f"Unknown contender: {name}. Valid values: {valid}")
    return names


def print_result_row(name, row_count, duration_s, cpu_cores, total_bytes):
    rps = row_count / duration_s
    rps_core = rps / cpu_cores
    mbps = (total_bytes / (1024 * 1024)) / duration_s
    print(f"  {name:<30s} {duration_s:>12.2f} {rps:>15,.0f} {rps_core:>18,.0f} {mbps:>12.1f}")


def print_results(files, total_bytes, run_count, enabled, results):
    cpu_count = os.cpu_count() or 1
    first_result = results[enabled[0]][0]
    row_count = first_result[3]

    print("\n" + "=" * 100)
    print("FLAT SCHEMA PERFORMANCE TEST RESULTS (PyArrow)")
    print("=" * 100)
    print()
    print("Environment:")
    print(f"  CPU cores:       {cpu_count}")
    print(f"  Python version:  {platform.python_version()}")
    print(f"  PyArrow version: {pa.__version__}")
    print(f"  OS:              {platform.system()} {platform.machine()}")
    print()
    print("Data:")
    print(f"  Files processed: {len(files)}")
    print(f"  Total rows:      {row_count:,}")
    print(f"  Total size:      {total_bytes / (1024 * 1024):,.1f} MB")
    print(f"  Runs per contender: {run_count}")
    print()

    # Sums (from first contender, first run)
    r = first_result
    print("Sums:")
    print(f"  passenger_count: {r[0]:,}")
    print(f"  trip_distance:   {r[1]:,.2f}")
    print(f"  fare_amount:     {r[2]:,.2f}")
    print()

    # Correctness verification (only when multiple contenders)
    if len(enabled) > 1:
        print("Correctness Verification:")
        print(f"  {'':25s} {'passenger_count':>17s} {'trip_distance':>17s} {'fare_amount':>17s}")
        for contender_key in enabled:
            display_name = CONTENDERS[contender_key][0]
            cr = results[contender_key][0]
            print(f"  {display_name:25s} {cr[0]:>17,} {cr[1]:>17,.2f} {cr[2]:>17,.2f}")
        print()

    # Performance
    print("Performance (all runs):")
    print(f"  {'Contender':<30s} {'Time (s)':>12s} {'Records/sec':>15s} {'Records/sec/core':>18s} {'MB/sec':>12s}")
    print("  " + "-" * 95)

    for contender_key in enabled:
        display_name, use_threads = CONTENDERS[contender_key]
        contender_results = results[contender_key]
        cores = cpu_count if use_threads else 1

        for i, cr in enumerate(contender_results):
            label = f"{display_name} [{i + 1}]"
            print_result_row(label, row_count, cr[4], cores, total_bytes)

        durations = [cr[4] for cr in contender_results]
        avg_duration = sum(durations) / len(durations)
        print_result_row(f"{display_name} [AVG]", row_count, avg_duration, cores, total_bytes)

        min_d = min(durations)
        max_d = max(durations)
        print(f"  {'':30s}   min: {min_d:.2f}s, max: {max_d:.2f}s, spread: {max_d - min_d:.2f}s")
        print()

    print("=" * 100)


def main():
    parser = argparse.ArgumentParser(description="PyArrow flat schema performance test")
    parser.add_argument("-c", "--contenders", default="all",
                        help="Contenders to run (comma-separated or 'all'). "
                             f"Valid: {', '.join(CONTENDERS.keys())}")
    parser.add_argument("-r", "--runs", type=int, default=DEFAULT_RUNS,
                        help=f"Number of timed runs per contender (default: {DEFAULT_RUNS})")
    args = parser.parse_args()

    enabled = parse_contenders(args.contenders)
    run_count = args.runs

    files = get_available_files()
    if not files:
        print("No data files found. Run test-data-setup first.")
        return

    total_bytes = sum(f.stat().st_size for f in files)

    print("\n=== Python Flat Schema Performance Test ===")
    print(f"Files available: {len(files)}")
    print(f"Runs per contender: {run_count}")
    print(f"Enabled contenders: {', '.join(CONTENDERS[c][0] for c in enabled)}")

    # Warmup
    print("\nWarmup run...")
    warmup_files = files[:min(len(files), 12)]
    _, use_threads = CONTENDERS[enabled[0]]
    sum_columns(warmup_files, use_threads)

    # Timed runs
    print("\nTimed runs:")
    results = {}
    for contender_key in enabled:
        display_name, use_threads = CONTENDERS[contender_key]
        contender_results = []
        for i in range(run_count):
            label = f"{display_name} [{i + 1}/{run_count}]"
            print(f"  Running {label}...")
            start = time.time()
            passenger_count, trip_distance, fare_amount, row_count = sum_columns(files, use_threads)
            duration = time.time() - start
            contender_results.append((passenger_count, trip_distance, fare_amount, row_count, duration))
        results[contender_key] = contender_results

    print_results(files, total_bytes, run_count, enabled, results)


if __name__ == "__main__":
    main()
