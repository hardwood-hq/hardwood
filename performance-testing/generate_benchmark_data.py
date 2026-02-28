#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Generates two ~1GB Parquet files for PageScanBenchmark:

  - page_scan_no_index.parquet  — without page index (sequential scan path)
  - page_scan_with_index.parquet — with page index   (offset-index scan path)

Both files contain identical data with an 18-column taxi-like schema.

Usage:
    python performance-testing/generate_benchmark_data.py [output_dir]

Default output directory:
    performance-testing/test-data-setup/target/benchmark-data/
"""

import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NUM_ROWS = 9_000_000  # ~1GB uncompressed per file
EXPECTED_MIN_SIZE = 500_000_000  # 500MB — sanity check for idempotency

DEFAULT_OUTPUT_DIR = os.path.join(
    "performance-testing", "test-data-setup", "target", "benchmark-data"
)

FILE_NO_INDEX = "page_scan_no_index.parquet"
FILE_WITH_INDEX = "page_scan_with_index.parquet"


# ---------------------------------------------------------------------------
# Schema: 18-column taxi-like layout
# ---------------------------------------------------------------------------

SCHEMA = pa.schema([
    ("vendor_id", pa.int32(), False),
    ("pickup_datetime", pa.timestamp("us"), False),
    ("dropoff_datetime", pa.timestamp("us"), False),
    ("passenger_count", pa.int32(), False),
    ("trip_distance", pa.float64(), False),
    ("pickup_longitude", pa.float64(), False),
    ("pickup_latitude", pa.float64(), False),
    ("rate_code_id", pa.int32(), False),
    ("store_and_fwd_flag", pa.string(), False),
    ("dropoff_longitude", pa.float64(), False),
    ("dropoff_latitude", pa.float64(), False),
    ("payment_type", pa.int32(), False),
    ("fare_amount", pa.float64(), False),
    ("extra", pa.float64(), False),
    ("mta_tax", pa.float64(), False),
    ("tip_amount", pa.float64(), False),
    ("tolls_amount", pa.float64(), False),
    ("total_amount", pa.float64(), False),
])


def build_table(num_rows: int) -> pa.Table:
    """Build a PyArrow table with taxi-like random data."""
    rng = np.random.default_rng(42)

    # Timestamps: 2024-01-01 through ~2024-12-31
    base_ts = np.datetime64("2024-01-01T00:00:00", "us")
    max_offset = int(365 * 24 * 3600 * 1_000_000)  # one year in microseconds
    pickup = base_ts + rng.integers(0, max_offset, size=num_rows, dtype=np.int64)
    # Dropoff: 1–60 minutes after pickup
    trip_duration = rng.integers(
        60 * 1_000_000, 60 * 60 * 1_000_000, size=num_rows, dtype=np.int64
    )
    dropoff = pickup + trip_duration

    flags = np.array(["Y", "N"], dtype=object)
    flag_indices = rng.integers(0, 2, size=num_rows)

    data = {
        "vendor_id": rng.integers(1, 3, size=num_rows, dtype=np.int32),
        "pickup_datetime": pickup,
        "dropoff_datetime": dropoff,
        "passenger_count": rng.integers(1, 7, size=num_rows, dtype=np.int32),
        "trip_distance": np.round(rng.uniform(0.1, 50.0, size=num_rows), 2),
        "pickup_longitude": np.round(rng.uniform(-74.05, -73.75, size=num_rows), 6),
        "pickup_latitude": np.round(rng.uniform(40.63, 40.85, size=num_rows), 6),
        "rate_code_id": rng.integers(1, 7, size=num_rows, dtype=np.int32),
        "store_and_fwd_flag": pa.array(flags[flag_indices], type=pa.string()),
        "dropoff_longitude": np.round(rng.uniform(-74.05, -73.75, size=num_rows), 6),
        "dropoff_latitude": np.round(rng.uniform(40.63, 40.85, size=num_rows), 6),
        "payment_type": rng.integers(1, 5, size=num_rows, dtype=np.int32),
        "fare_amount": np.round(rng.uniform(2.5, 200.0, size=num_rows), 2),
        "extra": np.round(rng.choice([0.0, 0.5, 1.0], size=num_rows), 2),
        "mta_tax": np.full(num_rows, 0.5),
        "tip_amount": np.round(rng.uniform(0.0, 50.0, size=num_rows), 2),
        "tolls_amount": np.round(
            rng.choice([0.0, 0.0, 0.0, 5.54, 10.50], size=num_rows), 2
        ),
        "total_amount": np.round(rng.uniform(3.0, 260.0, size=num_rows), 2),
    }

    return pa.table(data, schema=SCHEMA)


def files_exist(output_dir: str) -> bool:
    """Check whether both files already exist with reasonable size."""
    for name in (FILE_NO_INDEX, FILE_WITH_INDEX):
        path = os.path.join(output_dir, name)
        if not os.path.exists(path):
            return False
        if os.path.getsize(path) < EXPECTED_MIN_SIZE:
            return False
    return True


def main() -> None:
    output_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR

    if files_exist(output_dir):
        print(f"Benchmark data already exists in {output_dir} — skipping generation.")
        return

    os.makedirs(output_dir, exist_ok=True)

    print(f"Generating {NUM_ROWS:,} rows of taxi-like data ...")
    table = build_table(NUM_ROWS)

    # --- File WITHOUT page index ---
    path_no_index = os.path.join(output_dir, FILE_NO_INDEX)
    print(f"Writing {path_no_index} (no page index) ...")
    pq.write_table(
        table,
        path_no_index,
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
    )
    size_no = os.path.getsize(path_no_index)
    print(f"  Done: {size_no / 1e9:.2f} GB")

    # --- File WITH page index ---
    path_with_index = os.path.join(output_dir, FILE_WITH_INDEX)
    print(f"Writing {path_with_index} (with page index) ...")
    pq.write_table(
        table,
        path_with_index,
        use_dictionary=False,
        compression=None,
        data_page_version="1.0",
        write_page_index=True,
    )
    size_with = os.path.getsize(path_with_index)
    print(f"  Done: {size_with / 1e9:.2f} GB")

    print("Benchmark data generation complete.")


if __name__ == "__main__":
    main()
