#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Generate a dictionary-heavy, low-cardinality string column for the
column-reader interning benchmark (DictionaryStringReadBenchmark).

Writes:
  <output_dir>/dict_strings.parquet
    - `label`: 8 distinct short strings over NUM_ROWS rows, dictionary-encoded.
      Low cardinality with heavy repetition is the case interning targets.

Usage: python performance-testing/generate_dict_string_data.py [output_dir]
"""
import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_OUTPUT_DIR = os.path.join(
    "performance-testing", "test-data-setup", "target", "benchmark-data"
)
FILE_NAME = "dict_strings.parquet"
NUM_ROWS = 2_000_000
POOL = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"]


def main() -> None:
    output_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, FILE_NAME)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        print(f"{path} already exists — skipping generation.")
        return

    rng = np.random.default_rng(42)
    labels = np.array(POOL, dtype=object)[rng.integers(0, len(POOL), size=NUM_ROWS)]
    table = pa.table({"label": pa.array(labels, type=pa.string())})

    print(f"Writing {path} ({NUM_ROWS:,} rows, {len(POOL)} distinct labels) ...")
    pq.write_table(
        table,
        path,
        use_dictionary=True,
        compression="snappy",
        data_page_version="1.0",
    )
    print(f"  Done: {os.path.getsize(path) / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
