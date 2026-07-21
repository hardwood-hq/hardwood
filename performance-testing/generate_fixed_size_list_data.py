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
K_SWEEP = [1, 3, 4, 8, 9, 16, 128, 768, 1536]
TOTAL_VALUES = 8_000_000  # leaf floats per file; rows = TOTAL_VALUES // k

# Almost-fixed corpus for FixedSizeListFallbackBenchmark (the detector's fallback
# price). k=4 is the bit-packed regime, k=768 the RLE interior, and k=9 the scalar
# fallback (9..15 is not a byte-aligned per-record stride). Every real Parquet
# page must carry exactly one odd row so the detector fails and each page falls
# back — the whole file, page by page. PyArrow paginates by both a byte budget and
# a rows cap, so a fixed logical stride does not line up with real page boundaries
# (a wide-k file ends up with one odd row spread across many genuinely-fixed pages,
# which the fast path then legitimately accelerates). Make it deterministic instead:
# one row group == one page (row_group_size rows, data_page_size large enough not to
# split it), sized per k for a ~1 MiB page and kept under the rows cap.
NONFIXED_K = [4, 9, 768]
NONFIXED_VALUES = 2_000_000
NONFIXED_PAGE_BYTES = 1 << 20    # ~1 MiB target per page
NONFIXED_ROW_CAP = 16_000        # stay under PyArrow's rows-per-page cap

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
    present list of length k except one odd row (k + 1) in every page, so the
    detector's def-gate passes (all present) but its rep scan fails and each page
    falls back — the whole file, page by page. Each row group is one page of
    rows_per_page rows; `last` puts the odd row at the page's end (a near-full rep
    scan before failing), `second` at its second row (an early exit). A flat floor
    for the same value count is written too."""
    rows = NONFIXED_VALUES // k
    rows_per_page = max(2, min(NONFIXED_ROW_CAP, NONFIXED_PAGE_BYTES // (k * 4)))
    pages = -(-rows // rows_per_page)  # ceil
    for pos in ("last", "second"):
        lengths = np.full(rows, k, dtype=np.int64)
        for start in range(0, rows, rows_per_page):
            page_end = min(start + rows_per_page, rows)
            # Every page gets exactly one odd row; a 1-row tail page has no distinct
            # 'second' slot, so the odd row is its only (== last) row there.
            idx = start + 1 if (pos == "second" and page_end - start >= 2) else page_end - 1
            lengths[idx] = k + 1
        flat = rng.standard_normal(int(lengths.sum()), dtype=np.float32)
        offsets = np.concatenate([[0], np.cumsum(lengths)]).astype(np.int32)
        vec = pa.ListArray.from_arrays(pa.array(offsets), pa.array(flat, type=pa.float32()))
        table = pa.table(
            {"vec": vec},
            schema=pa.schema([("vec", pa.list_(pa.field("element", pa.float32(), nullable=False)))]))
        path = os.path.join(output_dir, f"nonfixed_k{k}_{pos}.parquet")
        pq.write_table(table, path, use_dictionary=False, compression=None,
                       data_page_version="2.0", row_group_size=rows_per_page,
                       data_page_size=NONFIXED_PAGE_BYTES * 8)
        # Fail loudly if PyArrow did not honour the one-page-per-row-group layout
        # this benchmark depends on (otherwise pages without an odd row fast-path).
        groups = pq.ParquetFile(path).metadata.num_row_groups
        if groups != pages:
            raise SystemExit(
                f"nonfixed k={k} {pos}: {groups} row groups, expected {pages} "
                f"(rows={rows}, rows_per_page={rows_per_page}) — page layout not as intended")

    flat = rng.standard_normal(NONFIXED_VALUES, dtype=np.float32)
    flat_table = pa.table(
        {"value": pa.array(flat, type=pa.float32())},
        schema=pa.schema([("value", pa.float32(), False)]))
    pq.write_table(flat_table, os.path.join(output_dir, f"nonfixed_k{k}_flat.parquet"),
                   use_dictionary=False, compression=None, data_page_version="2.0")
    print(f"k={k:5d}: non-fixed last/second ({pages} pages x ~{rows_per_page} rows, "
          f"odd row each) + flat floor")


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
