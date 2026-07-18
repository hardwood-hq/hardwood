#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Generate the sorted, multi-row-group fixture for the always-match filter
benchmark (AlwaysMatchReadBenchmark).

Writes:
  <output_dir>/sorted_filter.parquet
    - `id`: int64, 0..NUM_ROWS-1 sorted ascending.
    - `label`: 12-digit zero-padded string of `id`, so lexicographic order
      equals numeric order and a string range predicate maps onto row-group
      min/max exactly like the numeric one.
    - `value`: int64 payload equal to `id`, read on the non-filter side.

The sort plus ROW_GROUP_SIZE-row groups give each row group tight, disjoint
statistics, so a range predicate splits the file into dropped, partially
matching, and fully matching groups purely by its cutoff. Dictionary encoding
is disabled: every value is distinct, and a fallback mid-file would skew page
decode cost across row groups.

Usage: python performance-testing/generate_filter_pushdown_data.py [output_dir]
"""
import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_OUTPUT_DIR = os.path.join(
    "performance-testing", "test-data-setup", "target", "benchmark-data"
)
FILE_NAME = "sorted_filter.parquet"
NUM_ROWS = 10_000_000
ROW_GROUP_SIZE = 1_000_000
LABEL_WIDTH = 12


def build_table(num_rows: int) -> pa.Table:
    ids = np.arange(num_rows, dtype=np.int64)
    labels = np.char.zfill(ids.astype(str), LABEL_WIDTH)
    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "label": pa.array(labels, type=pa.string()),
            "value": pa.array(ids, type=pa.int64()),
        }
    )


def main() -> None:
    output_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, FILE_NAME)
    if os.path.exists(path):
        print(f"{path} already exists, skipping")
        return

    print(f"Generating {NUM_ROWS:,} sorted rows ...")
    table = build_table(NUM_ROWS)
    pq.write_table(
        table,
        path,
        row_group_size=ROW_GROUP_SIZE,
        use_dictionary=False,
        compression="snappy",
    )
    print(f"Wrote {path} ({os.path.getsize(path) / 1024 / 1024:.0f} MiB, "
          f"{NUM_ROWS // ROW_GROUP_SIZE} row groups)")


if __name__ == "__main__":
    main()
