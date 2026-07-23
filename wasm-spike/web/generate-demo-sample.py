#!/usr/bin/env python3
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#
"""Generate demo-sample.parquet — the file behind the "Load a sample file" button
in the dive-on-the-web demo.

It carries the full 19-column NYC yellow-taxi schema so the demo shows a realistic
file: multiple row groups (for data-preview paging), Snappy compression, and
dictionary-encoded low-cardinality columns (for the Dictionary screen).

Run with the project's venv (see CLAUDE.md):
    source .docker-venv/bin/activate && python wasm-spike/web/generate-demo-sample.py
"""
import datetime as dt
import random

import pyarrow as pa
import pyarrow.parquet as pq

random.seed(11)
n = 1500
base = dt.datetime(2024, 1, 15, 6, 0, 0)
pick = [base + dt.timedelta(seconds=random.randint(0, 16 * 3600)) for _ in range(n)]
dur = [random.randint(180, 2400) for _ in range(n)]
drop = [pick[i] + dt.timedelta(seconds=dur[i]) for i in range(n)]
dist = [round(random.uniform(0.4, 25.0), 2) for _ in range(n)]
fare = [round(3.0 + dist[i] * random.uniform(2.2, 3.4), 2) for i in range(n)]
tip = [round(max(0.0, fare[i] * random.choice([0, 0, 0.1, 0.15, 0.2, 0.25])) * random.choice([0, 1, 1, 1]), 2)
       for i in range(n)]
tolls = [round(random.choice([0, 0, 0, 0, 6.55]), 2) for _ in range(n)]
cong = [random.choice([0.0, 0.0, 2.5]) for _ in range(n)]
air = [random.choice([0.0, 0.0, 0.0, 1.75]) for _ in range(n)]
imp = [0.3] * n
mta = [0.5] * n
extra = [random.choice([0.0, 0.5, 1.0]) for _ in range(n)]
total = [round(fare[i] + tip[i] + tolls[i] + cong[i] + air[i] + imp[i] + mta[i] + extra[i], 2) for i in range(n)]
ts = pa.timestamp('us')

table = pa.table({
    "VendorID":              pa.array([random.choice([1, 2]) for _ in range(n)], pa.int64()),
    "tpep_pickup_datetime":  pa.array(pick, ts),
    "tpep_dropoff_datetime": pa.array(drop, ts),
    "passenger_count":       pa.array([random.randint(1, 6) for _ in range(n)], pa.int64()),
    "trip_distance":         pa.array(dist, pa.float64()),
    "RatecodeID":            pa.array([random.choice([1, 1, 1, 2, 5]) for _ in range(n)], pa.int64()),
    "store_and_fwd_flag":    pa.array([random.choice(["N", "N", "N", "Y"]) for _ in range(n)]),
    "PULocationID":          pa.array([random.randint(1, 263) for _ in range(n)], pa.int64()),
    "DOLocationID":          pa.array([random.randint(1, 263) for _ in range(n)], pa.int64()),
    "payment_type":          pa.array([random.choice([1, 1, 2, 3, 4]) for _ in range(n)], pa.int64()),
    "fare_amount":           pa.array(fare, pa.float64()),
    "extra":                 pa.array(extra, pa.float64()),
    "mta_tax":               pa.array(mta, pa.float64()),
    "tip_amount":            pa.array(tip, pa.float64()),
    "tolls_amount":          pa.array(tolls, pa.float64()),
    "improvement_surcharge": pa.array(imp, pa.float64()),
    "total_amount":          pa.array(total, pa.float64()),
    "congestion_surcharge":  pa.array(cong, pa.float64()),
    "airport_fee":           pa.array(air, pa.float64()),
})

pq.write_table(table, "demo-sample.parquet", compression="snappy", row_group_size=500,
               use_dictionary=["VendorID", "RatecodeID", "store_and_fwd_flag", "payment_type"])
print("wrote demo-sample.parquet:", table.num_rows, "rows,", pq.ParquetFile("demo-sample.parquet").metadata.num_row_groups, "row groups")
