#!/usr/bin/env python3
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Python (PyArrow) counterpart of NestedPerformanceTest.java.
Reads an Overture Maps places file (deeply nested schema) and computes the same
aggregates as the Java test: version range, confidence range, bbox extents,
list sizes (websites, sources, addresses), map sizes (names.common),
and max primary name length.

Note: PyArrow uses vectorized columnar operations (C++ engine) rather than row-by-row
iteration. The use_threads toggle controls parallelism in Parquet column reading.

Usage:
    python nested_perf.py                          # run all contenders, 5 runs each
    python nested_perf.py -c single_threaded       # single-threaded only
    python nested_perf.py -c multi_threaded -r 10  # multi-threaded, 10 runs
"""

import argparse
import os
import platform
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DATA_FILE = Path("../test-data-setup/target/overture-maps-data/overture_places.zstd.parquet")
DEFAULT_RUNS = 5

CONTENDERS = {
    "single_threaded": ("PyArrow (single-threaded)", False),
    "multi_threaded": ("PyArrow (multi-threaded)", True),
}


def _map_value_length(map_column):
    """Compute per-row entry count for a map column.

    pc.list_value_length doesn't support map types, so we compute sizes
    from the underlying offsets (end - start) for each chunk.
    """
    chunks = []
    for chunk in map_column.chunks:
        n = len(chunk)
        starts = chunk.offsets.slice(0, n)
        ends = chunk.offsets.slice(1)
        sizes = pc.subtract(ends, starts)
        if chunk.null_count > 0:
            sizes = pc.if_else(chunk.is_valid(), sizes, None)
        chunks.append(sizes)
    return pa.chunked_array(chunks)


def compute_aggregates(file_path, use_threads):
    """Read the parquet file and compute nested aggregates."""
    table = pq.read_table(file_path, use_threads=use_threads)
    row_count = table.num_rows

    # version (int): min/max
    version = table.column("version")
    min_version = pc.min(version).as_py()
    max_version = pc.max(version).as_py()

    # confidence (double): min/max
    confidence = table.column("confidence")
    min_confidence = pc.min(confidence).as_py()
    max_confidence = pc.max(confidence).as_py()

    # bbox (struct): xmin min, xmax max
    bbox = table.column("bbox")
    bbox_xmin = pc.struct_field(bbox, "xmin")
    bbox_xmax = pc.struct_field(bbox, "xmax")
    min_bbox_xmin = pc.min(bbox_xmin).as_py()
    max_bbox_xmax = pc.max(bbox_xmax).as_py()

    # websites (list<string>): total count, max per row
    websites = table.column("websites")
    website_sizes = pc.list_value_length(websites)
    total_website_count = pc.sum(website_sizes, skip_nulls=True).as_py() or 0
    max_website_count = pc.max(website_sizes, skip_nulls=True).as_py() or 0

    # sources (list<struct>): total count, max per row
    sources = table.column("sources")
    source_sizes = pc.list_value_length(sources)
    total_source_count = pc.sum(source_sizes, skip_nulls=True).as_py() or 0
    max_source_count = pc.max(source_sizes, skip_nulls=True).as_py() or 0

    # addresses (list<struct>): total count, max per row
    addresses = table.column("addresses")
    address_sizes = pc.list_value_length(addresses)
    total_address_count = pc.sum(address_sizes, skip_nulls=True).as_py() or 0
    max_address_count = pc.max(address_sizes, skip_nulls=True).as_py() or 0

    # names.common (map<string, string>): total entries, max per row
    names = table.column("names")
    names_common = pc.struct_field(names, "common")
    common_sizes = _map_value_length(names_common)
    total_name_entries = pc.sum(common_sizes, skip_nulls=True).as_py() or 0
    max_name_entries = pc.max(common_sizes, skip_nulls=True).as_py() or 0

    # names.primary (string): max length
    names_primary = pc.struct_field(names, "primary")
    primary_lengths = pc.utf8_length(names_primary)
    max_primary_name_length = pc.max(primary_lengths, skip_nulls=True).as_py() or 0

    return {
        "row_count": row_count,
        "min_version": min_version,
        "max_version": max_version,
        "min_confidence": min_confidence,
        "max_confidence": max_confidence,
        "min_bbox_xmin": float(min_bbox_xmin) if min_bbox_xmin is not None else None,
        "max_bbox_xmax": float(max_bbox_xmax) if max_bbox_xmax is not None else None,
        "total_website_count": total_website_count,
        "max_website_count": max_website_count,
        "total_source_count": total_source_count,
        "max_source_count": max_source_count,
        "total_address_count": total_address_count,
        "max_address_count": max_address_count,
        "total_name_entries": total_name_entries,
        "max_name_entries": max_name_entries,
        "max_primary_name_length": max_primary_name_length,
    }


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


def print_results(file_size, run_count, enabled, results):
    cpu_count = os.cpu_count() or 1
    first_result = results[enabled[0]][0][0]
    row_count = first_result["row_count"]

    print("\n" + "=" * 100)
    print("NESTED SCHEMA PERFORMANCE TEST RESULTS (PyArrow)")
    print("=" * 100)
    print()
    print("Environment:")
    print(f"  CPU cores:       {cpu_count}")
    print(f"  Python version:  {platform.python_version()}")
    print(f"  PyArrow version: {pa.__version__}")
    print(f"  OS:              {platform.system()} {platform.machine()}")
    print()
    print("Data:")
    print(f"  Total rows:      {row_count:,}")
    print(f"  File size:       {file_size / (1024 * 1024):,.1f} MB")
    print(f"  Runs per contender: {run_count}")
    print()

    # Aggregates (from first contender, first run)
    r = first_result
    print("Aggregates:")
    print(f"  version:         [{r['min_version']}, {r['max_version']}]")
    print(f"  confidence:      [{r['min_confidence']}, {r['max_confidence']}]")
    print(f"  bbox.xmin min:   {r['min_bbox_xmin']}")
    print(f"  bbox.xmax max:   {r['max_bbox_xmax']}")
    print(f"  websites:        total={r['total_website_count']:,}, max/row={r['max_website_count']}")
    print(f"  sources:         total={r['total_source_count']:,}, max/row={r['max_source_count']}")
    print(f"  addresses:       total={r['total_address_count']:,}, max/row={r['max_address_count']}")
    print(f"  names.common:    total={r['total_name_entries']:,}, max/row={r['max_name_entries']}")
    print(f"  names.primary:   max_length={r['max_primary_name_length']}")
    print()

    # Correctness verification (only when multiple contenders)
    if len(enabled) > 1:
        print("Correctness Verification:")
        print(f"  {'':25s} {'min_ver':>10s} {'max_ver':>10s} {'rows':>10s}"
              f" {'websites':>12s} {'sources':>12s} {'addresses':>10s}")
        for contender_key in enabled:
            display_name = CONTENDERS[contender_key][0]
            cr = results[contender_key][0][0]
            print(f"  {display_name:25s} {cr['min_version']:>10,} {cr['max_version']:>10,}"
                  f" {cr['row_count']:>10,} {cr['total_website_count']:>12,}"
                  f" {cr['total_source_count']:>12,} {cr['total_address_count']:>10,}")
        print()

    # Performance
    print("Performance (all runs):")
    print(f"  {'Contender':<30s} {'Time (s)':>12s} {'Records/sec':>15s}"
          f" {'Records/sec/core':>18s} {'MB/sec':>12s}")
    print("  " + "-" * 95)

    for contender_key in enabled:
        display_name, use_threads = CONTENDERS[contender_key]
        contender_results = results[contender_key]
        cores = cpu_count if use_threads else 1

        for i, (_, duration) in enumerate(contender_results):
            label = f"{display_name} [{i + 1}]"
            print_result_row(label, row_count, duration, cores, file_size)

        durations = [d for _, d in contender_results]
        avg_duration = sum(durations) / len(durations)
        print_result_row(f"{display_name} [AVG]", row_count, avg_duration, cores, file_size)

        min_d = min(durations)
        max_d = max(durations)
        print(f"  {'':30s}   min: {min_d:.2f}s, max: {max_d:.2f}s, spread: {max_d - min_d:.2f}s")
        print()

    print("=" * 100)


def main():
    parser = argparse.ArgumentParser(description="PyArrow nested schema performance test")
    parser.add_argument("-c", "--contenders", default="all",
                        help="Contenders to run (comma-separated or 'all'). "
                             f"Valid: {', '.join(CONTENDERS.keys())}")
    parser.add_argument("-r", "--runs", type=int, default=DEFAULT_RUNS,
                        help=f"Number of timed runs per contender (default: {DEFAULT_RUNS})")
    args = parser.parse_args()

    enabled = parse_contenders(args.contenders)
    run_count = args.runs

    if not DATA_FILE.exists():
        print(f"Data file not found: {DATA_FILE}")
        print("Run test-data-setup first (./mvnw verify -Pperformance-test).")
        return

    file_size = DATA_FILE.stat().st_size

    print("\n=== Python Nested Schema Performance Test ===")
    print(f"File: {DATA_FILE.name}")
    print(f"File size: {file_size / (1024 * 1024):,.1f} MB")
    print(f"Runs per contender: {run_count}")
    print(f"Enabled contenders: {', '.join(CONTENDERS[c][0] for c in enabled)}")

    # Warmup
    print("\nWarmup run...")
    _, use_threads = CONTENDERS[enabled[0]]
    compute_aggregates(DATA_FILE, use_threads)

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
            result = compute_aggregates(DATA_FILE, use_threads)
            duration = time.time() - start
            contender_results.append((result, duration))
        results[contender_key] = contender_results

    print_results(file_size, run_count, enabled, results)


if __name__ == "__main__":
    main()
