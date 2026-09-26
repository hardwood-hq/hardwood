#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

"""
Generates the Parquet files for the masked nested read benchmarks in
micro-benchmarks (NestedMaskedTailBenchmark, OvertureMaskedReadBenchmark,
WikipediaMaskedReadBenchmark and MaskedThenUnmaskedReadBenchmark).

nested_masking_v2_no_index_zstd.parquet is a synthetic table of 2,000,000 rows
in two row groups of 1,000,000, with nested lists and a list inside a struct,
written with v2 pages, zstd and no page index, so a masked read locates its
pages by walking their headers.

If the Overture Maps places file downloaded by test-data-setup is present, it
is also rewritten with a page index and its row groups unchanged, as
overture_places_index.zstd.parquet.

With --wikipedia, one shard of the Cohere Wikipedia embeddings dataset
(CohereLabs/wikipedia-2023-11-embed-multilingual-v3, en/0079.parquet, 221 MB)
is downloaded and rewritten with a page index and its row groups unchanged, as
wikipedia_en_0079_index.parquet.

Usage:
    python performance-testing/generate_nested_masking_data.py [--wikipedia] [output_dir]

Default output directory:
    performance-testing/test-data-setup/target/benchmark-data/
"""

import hashlib
import os
import sys
import urllib.request

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

NUM_ROWS = 2_000_000
ROW_GROUP_SIZE = 1_000_000
DATA_PAGE_SIZE = 128 * 1024

DEFAULT_OUTPUT_DIR = os.path.join(
    "performance-testing", "test-data-setup", "target", "benchmark-data"
)
OVERTURE_SOURCE = os.path.join(
    "performance-testing", "test-data-setup", "target", "overture-maps-data",
    "overture_places.zstd.parquet"
)
OVERTURE_WITH_INDEX = "overture_places_index.zstd.parquet"
WIKIPEDIA_URL = ("https://huggingface.co/datasets/CohereLabs/wikipedia-2023-11-embed-multilingual-v3"
                 "/resolve/ade45fb52bd549f5e8c065636fe4160a43c2af36/en/0079.parquet")
WIKIPEDIA_SHA256 = "8862921bb94262ec93343e67aa33ad2217e8809c3f695bd08eebb49591020e71"
WIKIPEDIA_DOWNLOAD = "wikipedia_en_0079.parquet"
WIKIPEDIA_WITH_INDEX = "wikipedia_en_0079_index.parquet"

SYNTHETIC = "nested_masking_v2_no_index_zstd.parquet"

TAG_VOCABULARY = np.array(
    ["cafe", "museum", "park", "school", "station", "hotel", "shop", "bank"], dtype=object)
SOURCES = np.array(["meta", "osm", "foursquare", "microsoft"], dtype=object)


def list_array(rng, num_rows, max_len, values_for):
    """A list column with 0..max_len elements per row, built from offsets."""
    lengths = rng.integers(0, max_len + 1, size=num_rows)
    offsets = np.zeros(num_rows + 1, dtype=np.int32)
    np.cumsum(lengths, out=offsets[1:])
    return pa.ListArray.from_arrays(pa.array(offsets), values_for(int(offsets[-1])))


def build_table(num_rows: int) -> pa.Table:
    rng = np.random.default_rng(1306)
    tags = list_array(rng, num_rows, 4,
                      lambda n: pa.array(TAG_VOCABULARY[rng.integers(0, len(TAG_VOCABULARY), size=n)],
                                         type=pa.string()))
    scores = list_array(rng, num_rows, 8,
                        lambda n: pa.array(np.round(rng.uniform(0, 1, size=n), 4)))
    codes = list_array(rng, num_rows, 3,
                       lambda n: pa.array(rng.integers(0, 10_000, size=n, dtype=np.int32)))
    source = pa.array(SOURCES[rng.integers(0, len(SOURCES), size=num_rows)], type=pa.string())
    attrs = pa.StructArray.from_arrays([source, codes], names=["source", "codes"])
    return pa.table({
        "id": pa.array(np.arange(num_rows, dtype=np.int64)),
        "category": pa.array(rng.integers(0, 50, size=num_rows, dtype=np.int32)),
        "tags": tags,
        "scores": scores,
        "attrs": attrs,
    })


def write_synthetic(output_dir: str) -> None:
    path = os.path.join(output_dir, SYNTHETIC)
    if os.path.exists(path):
        print(f"{path} exists — skipping.")
        return
    print(f"Generating {NUM_ROWS:,} rows of nested data ...")
    table = build_table(NUM_ROWS)
    print(f"Writing {path} ...")
    pq.write_table(
        table,
        path,
        compression="zstd",
        row_group_size=ROW_GROUP_SIZE,
        data_page_size=DATA_PAGE_SIZE,
        data_page_version="2.0",
        write_page_index=False,
    )
    print(f"  Done: {os.path.getsize(path) / 1e6:.1f} MB")


def write_overture_with_index(output_dir: str) -> None:
    path = os.path.join(output_dir, OVERTURE_WITH_INDEX)
    if os.path.exists(path):
        print(f"{path} exists — skipping.")
        return
    if not os.path.exists(OVERTURE_SOURCE):
        print(f"{OVERTURE_SOURCE} not found (run the test-data-setup module) — "
              f"skipping {OVERTURE_WITH_INDEX}.")
        return
    write_with_index(OVERTURE_SOURCE, path, "zstd")


def write_with_index(source_path: str, path: str, compression: str) -> None:
    """Rewrites source_path with a page index, one output row group per input row group."""
    source = pq.ParquetFile(source_path)
    print(f"Writing {path} ({source.metadata.num_row_groups} row groups, with page index) ...")
    with pq.ParquetWriter(path, source.schema_arrow, compression=compression,
                          write_page_index=True) as writer:
        for i in range(source.metadata.num_row_groups):
            group = source.read_row_group(i)
            writer.write_table(group, row_group_size=max(1, group.num_rows))
    print(f"  Done: {os.path.getsize(path) / 1e6:.1f} MB")


def write_wikipedia_with_index(output_dir: str) -> None:
    path = os.path.join(output_dir, WIKIPEDIA_WITH_INDEX)
    if os.path.exists(path):
        print(f"{path} exists — skipping.")
        return
    download = os.path.join(output_dir, WIKIPEDIA_DOWNLOAD)
    if not os.path.exists(download):
        print(f"Downloading {WIKIPEDIA_URL} ...")
        urllib.request.urlretrieve(WIKIPEDIA_URL, download + ".part")
        os.replace(download + ".part", download)
    verify_sha256(download, WIKIPEDIA_SHA256)
    write_with_index(download, path, "zstd")


def main() -> None:
    args = sys.argv[1:]
    wikipedia = "--wikipedia" in args
    args = [arg for arg in args if arg != "--wikipedia"]
    output_dir = args[0] if args else DEFAULT_OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    write_synthetic(output_dir)
    write_overture_with_index(output_dir)
    if wikipedia:
        write_wikipedia_with_index(output_dir)
    print("Nested masking data generation complete.")


if __name__ == "__main__":
    main()
