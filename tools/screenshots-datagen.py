#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "cli/src/test/resources/dive_screenshots_fixture.parquet"

SCHEMA = pa.schema([
    ("id", pa.string(), False),
    ("version", pa.int32(), False),
    ("confidence", pa.float64(), True),
    ("bbox", pa.struct([
        ("xmin", pa.float64()),
        ("xmax", pa.float64()),
        ("ymin", pa.float64()),
        ("ymax", pa.float64()),
    ])),
    ("names", pa.struct([
        ("primary", pa.string()),
        ("common", pa.map_(pa.string(), pa.string())),
    ])),
    ("sources", pa.list_(pa.struct([
        ("dataset", pa.string()),
        ("record_id", pa.string()),
        ("license", pa.string()),
    ]))),
    ("addresses", pa.list_(pa.struct([
        ("freeform", pa.string()),
        ("locality", pa.string()),
        ("region", pa.string()),
        ("country", pa.string()),
    ]))),
    ("websites", pa.list_(pa.string())),
    ("category", pa.string(), False),
    ("status", pa.string(), False),
    ("city", pa.string(), True),
    ("country_code", pa.string(), True),
    ("metric_a", pa.int64(), False),
    ("metric_b", pa.int64(), False),
    ("metric_c", pa.int64(), False),
    ("metric_d", pa.int64(), False),
    ("metric_e", pa.int64(), False),
    ("metric_f", pa.int64(), False),
    ("metric_g", pa.int64(), False),
    ("metric_h", pa.int64(), False),
])


def build_chunk(start: int, count: int) -> pa.Table:
    rows = []
    categories = ["restaurant", "cafe", "grocery", "pharmacy", "hotel", "museum"]
    statuses = ["active", "active", "active", "inactive"]
    cities = ["New York", "Boston", "Chicago", "Austin", "Seattle", "Denver"]

    for i in range(start, start + count):
        cat = categories[i % len(categories)]
        rows.append({
            "id": f"place-{i:06d}",
            "version": 1000 + i,
            "confidence": round(0.35 + ((i % 60) / 100.0), 2),
            "bbox": {
                "xmin": -123.0 + (i % 50) * 0.01,
                "xmax": -122.5 + (i % 50) * 0.01,
                "ymin": 37.0 + (i % 50) * 0.005,
                "ymax": 37.4 + (i % 50) * 0.005,
            },
            "names": {
                "primary": f"{cat.title()} #{i}",
                "common": {
                    "en": f"{cat.title()} {i}",
                    "es": f"{cat.title()} {i} ES",
                },
            },
            "sources": [
                {"dataset": "synthetic", "record_id": f"syn-{i}", "license": "CC-BY"},
                {"dataset": "seed", "record_id": f"seed-{i}", "license": "ODbL"},
            ],
            "addresses": [
                {
                    "freeform": f"{100 + (i % 900)} Main St",
                    "locality": cities[i % len(cities)],
                    "region": "NA",
                    "country": "United States",
                }
            ],
            "websites": [f"https://example.org/{cat}/{i}", f"https://m.example.org/{i}"],
            "category": cat,
            "status": statuses[i % len(statuses)],
            "city": cities[i % len(cities)],
            "country_code": "US",
            "metric_a": i * 3 + 1,
            "metric_b": i * 3 + 2,
            "metric_c": i * 3 + 3,
            "metric_d": i * 3 + 4,
            "metric_e": i * 3 + 5,
            "metric_f": i * 3 + 6,
            "metric_g": i * 3 + 7,
            "metric_h": i * 3 + 8,
        })
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    writer = pq.ParquetWriter(
        str(OUTPUT_PATH),
        schema=SCHEMA,
        compression="zstd",
        use_dictionary=["category", "status", "city", "country_code", "names.primary"],
        data_page_version="1.0",
    )
    for rg_start in (0, 150, 300, 450):
        writer.write_table(build_chunk(rg_start, 150))
    writer.close()

    print("Generated dive_screenshots_fixture.parquet:")
    print(f"  - path: {OUTPUT_PATH}")
    print("  - 4 row groups, 600 rows, nested/list/map fields + dictionary columns")
    print("  - wide metric_* tail columns for horizontal scrolling in data preview")


if __name__ == "__main__":
    main()
