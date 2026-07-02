#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Generates the corpus for FixedSizeListDecodeBenchmark: fixed-shape float32
vectors written under the standard 3-level LIST encoding, across a sweep of
vector lengths k that spans both repetition-level encoding regimes (small k is
fully bit-packed; large k emits an RLE interior).

For each k two files are written into the benchmark-data directory:

  - fixed_size_list_k{k}.parquet       LIST<float32> (required element), the
                                       shape the reader's fast path targets.
  - fixed_size_list_k{k}_flat.parquet  the identical values as a plain float32
                                       column (no list) — the decode floor.

Every file holds the same total number of leaf values, so decode throughput is
comparable across k. Files use DataPageV2 (which the fast path detects), no
dictionary, and no compression, so the benchmark measures level handling and
value decode rather than codec cost.

It also writes an almost-fixed corpus (`nonfixed_k{k}_{last,second}.parquet` plus
a flat floor) for FixedSizeListFallbackBenchmark, which measures the price of a
failing detector scan on a page that ultimately falls back to the regular decode.

Usage:
    python performance-testing/generate_fixed_size_list_data.py [output_dir]
"""

import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# k=3 covers the multi-byte-period bit-packed regime (period 3 bytes, the RGB /
# 3D-vector case); 4 and 8 are single-byte-period; 16+ have an RLE interior.
K_SWEEP = [3, 4, 8, 16, 128, 768, 1536]
TOTAL_VALUES = 8_000_000  # leaf floats per file; rows = TOTAL_VALUES // k

# Almost-fixed corpus for FixedSizeListFallbackBenchmark (the detector's fallback
# price). k=4 is the bit-packed regime, k=768 the RLE interior.
NONFIXED_K = [4, 768]
NONFIXED_VALUES = 2_000_000
PAGE_ROWS = 20_000  # PyArrow's default rows-per-page cap

DEFAULT_OUTPUT_DIR = os.path.join(
    "performance-testing", "test-data-setup", "target", "benchmark-data"
)


def write_pair(output_dir, k, rng):
    rows = TOTAL_VALUES // k
    flat = rng.standard_normal(rows * k, dtype=np.float32)

    # LIST<float32> with a required element (leaf max definition level 2).
    offsets = np.arange(rows + 1, dtype=np.int32) * k
    vec = pa.ListArray.from_arrays(
        pa.array(offsets),
        pa.array(flat, type=pa.float32()))
    list_table = pa.table(
        {"vec": vec},
        schema=pa.schema([("vec", pa.list_(pa.field("element", pa.float32(), nullable=False)))]))
    # Written as both DataPageV2 (default) and DataPageV1, so the fast path can
    # be measured on each page layout.
    list_path = os.path.join(output_dir, f"fixed_size_list_k{k}.parquet")
    pq.write_table(list_table, list_path,
                   use_dictionary=False, compression=None, data_page_version="2.0")
    list_path_v1 = os.path.join(output_dir, f"fixed_size_list_k{k}_v1.parquet")
    pq.write_table(list_table, list_path_v1,
                   use_dictionary=False, compression=None, data_page_version="1.0")

    # Same values as a plain float32 column — the flat decode floor.
    flat_table = pa.table(
        {"value": pa.array(flat, type=pa.float32())},
        schema=pa.schema([("value", pa.float32(), False)]))
    flat_path = os.path.join(output_dir, f"fixed_size_list_k{k}_flat.parquet")
    pq.write_table(flat_table, flat_path,
                   use_dictionary=False, compression=None, data_page_version="2.0")

    print(f"k={k:5d}: {rows:>9d} rows x {k} float32 -> "
          f"{os.path.basename(list_path)} (+v1) + {os.path.basename(flat_path)}")


def write_nonfixed(output_dir, k, rng):
    """Almost-fixed LIST files for FixedSizeListFallbackBenchmark: every row is a
    present list of length k except one odd row (k + 1) placed in every page, so
    the detector's def-gate passes (all present) but its rep scan fails and the
    page falls back. `last` puts the odd row at each page's end (a near-full scan
    before failing), `second` near its start (an early exit); a `flat` floor for
    the same value count is written too."""
    rows = NONFIXED_VALUES // k
    for pos in ("last", "second"):
        lengths = np.full(rows, k, dtype=np.int64)
        for start in range(0, rows, PAGE_ROWS):
            page_end = min(start + PAGE_ROWS, rows)
            lengths[start + 1 if pos == "second" else page_end - 1] = k + 1
        flat = rng.standard_normal(int(lengths.sum()), dtype=np.float32)
        offsets = np.concatenate([[0], np.cumsum(lengths)]).astype(np.int32)
        vec = pa.ListArray.from_arrays(pa.array(offsets), pa.array(flat, type=pa.float32()))
        table = pa.table(
            {"vec": vec},
            schema=pa.schema([("vec", pa.list_(pa.field("element", pa.float32(), nullable=False)))]))
        pq.write_table(table, os.path.join(output_dir, f"nonfixed_k{k}_{pos}.parquet"),
                       use_dictionary=False, compression=None, data_page_version="2.0")

    flat = rng.standard_normal(NONFIXED_VALUES, dtype=np.float32)
    flat_table = pa.table(
        {"value": pa.array(flat, type=pa.float32())},
        schema=pa.schema([("value", pa.float32(), False)]))
    pq.write_table(flat_table, os.path.join(output_dir, f"nonfixed_k{k}_flat.parquet"),
                   use_dictionary=False, compression=None, data_page_version="2.0")
    print(f"k={k:5d}: non-fixed last/second (odd row per page) + flat floor")


def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    rng = np.random.default_rng(1234)
    for k in K_SWEEP:
        write_pair(output_dir, k, rng)
    for k in NONFIXED_K:
        write_nonfixed(output_dir, k, rng)
    print(f"\nWrote {3 * len(K_SWEEP) + 3 * len(NONFIXED_K)} files to {output_dir}")


if __name__ == "__main__":
    main()
