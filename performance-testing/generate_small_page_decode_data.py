#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""Generate the four fixtures used by SmallPageDecodeBenchmark.

Every file contains the same eight million random float32 values. The page-size
and compression axes reproduce the conditions from #810 independently:

* small: approximately 64 KiB pages
* large: approximately 1 MiB pages
* zstd and uncompressed versions of each pagination
"""

import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

TOTAL_VALUES = 8_000_000
DEFAULT_OUTPUT_DIR = os.path.join(
    "performance-testing", "test-data-setup", "target", "benchmark-data"
)


def write_fixture(output_dir, values, pagination, compression, page_bytes):
    path = os.path.join(
        output_dir, f"small_page_decode_{pagination}_{compression}.parquet"
    )
    table = pa.table(
        {"value": pa.array(values, type=pa.float32())},
        schema=pa.schema([("value", pa.float32(), False)]),
    )
    kwargs = {
        "use_dictionary": False,
        "compression": None if compression == "uncompressed" else "zstd",
        "data_page_version": "2.0",
        "data_page_size": page_bytes,
        "write_batch_size": page_bytes // np.dtype(np.float32).itemsize,
        "row_group_size": TOTAL_VALUES,
    }
    if compression == "zstd":
        kwargs["compression_level"] = 3
    pq.write_table(table, path, **kwargs)
    print(f"{pagination:5s} {compression:12s} {os.path.getsize(path):>10,d} bytes")


def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    values = np.random.default_rng(1234).standard_normal(TOTAL_VALUES, dtype=np.float32)
    for pagination, page_bytes in (("small", 64 << 10), ("large", 1 << 20)):
        for compression in ("zstd", "uncompressed"):
            write_fixture(output_dir, values, pagination, compression, page_bytes)


if __name__ == "__main__":
    main()
