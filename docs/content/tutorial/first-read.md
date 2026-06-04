<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Read Your First Parquet File

This is a hands-on lesson. By the end you will have opened a real Parquet file, printed its
schema, read rows with typed accessors, narrowed the read with a projection and a filter, and
summed a column the fast columnar way. Follow the steps in order and run each snippet — every
one works as written.

It walks one path that works, start to finish — so just follow along rather than reaching for
options. When you're done, the [how-to guides](../how-to/index.md) cover the full range of
choices, and the [background pages](../concepts/parquet-layout.md) explain how Parquet files
and the reader APIs work underneath.

## Before you start

- Java 21 or newer (`java -version` to check).
- Hardwood on the classpath. If you haven't set up a project yet, follow
  [Getting Started](../getting-started.md) first — you need `hardwood-core`, and because the
  sample file is ZSTD-compressed, also add the `zstd-jni` dependency.

## Step 1 — Get a sample file

Download a month of the public NYC Taxi & Limousine Commission yellow-cab trip data — a real
Parquet file, about 60 MB:

```bash
curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet
```

You now have `yellow_tripdata_2026-01.parquet` in your working directory. Every snippet below
opens this file via `Path.of("yellow_tripdata_2026-01.parquet")`.

## Step 2 — See what's inside

Before reading data, print the schema. This tells you the column names and types you'll use in
the next steps — and introduces metadata access, which never touches row data:

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import java.nio.file.Path;

try (ParquetFileReader reader =
        ParquetFileReader.open(InputFile.of(Path.of("yellow_tripdata_2026-01.parquet")))) {

    System.out.println("Total rows: " + reader.getFileMetaData().numRows());

    FileSchema schema = reader.getFileSchema();
    for (int i = 0; i < schema.getColumnCount(); i++) {
        ColumnSchema column = schema.getColumn(i);
        System.out.println(column.name() + " : " + column.type());
    }
}
```

Run it. You'll see a few million rows and a column list that includes `VendorID` (an `int`),
`passenger_count` (a `long`), and `trip_distance` and `fare_amount` (each a `double`). Those are
the columns this lesson uses.

## Step 3 — Read rows

Open a `RowReader` and walk the rows. The typed accessors return the column's value at the
current row; `hasNext()` / `next()` advance the cursor. Print the first five trips:

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import java.nio.file.Path;

try (ParquetFileReader reader =
            ParquetFileReader.open(InputFile.of(Path.of("yellow_tripdata_2026-01.parquet")));
        RowReader rows = reader.rowReader()) {

    int printed = 0;
    while (rows.hasNext() && printed < 5) {
        rows.next();

        int vendor = rows.getInt("VendorID");
        double distance = rows.getDouble("trip_distance");
        double fare = rows.getDouble("fare_amount");

        System.out.printf("vendor=%d  distance=%.2f mi  fare=$%.2f%n", vendor, distance, fare);
        printed++;
    }
}
```

You just read typed values straight out of Parquet — no decoding ceremony, one row at a time.

## Step 4 — Read less

A whole file is rarely what you want. Three builder options narrow the read, and they combine:

- **Projection** — read only the columns you name; the rest are never fetched or decoded.
- **Filter** — a predicate pushed down to skip data that can't match.
- **`head(n)`** — stop after `n` rows.

Read only the two columns you need, keep just the high-value trips, and cap at ten:

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import java.nio.file.Path;

try (ParquetFileReader reader =
            ParquetFileReader.open(InputFile.of(Path.of("yellow_tripdata_2026-01.parquet")));
        RowReader rows = reader.buildRowReader()
                .projection(ColumnProjection.columns("trip_distance", "fare_amount"))
                .filter(FilterPredicate.gt("fare_amount", 100.0))
                .head(10)
                .build()) {

    while (rows.hasNext()) {
        rows.next();
        System.out.printf("distance=%.2f mi  fare=$%.2f%n",
                rows.getDouble("trip_distance"), rows.getDouble("fare_amount"));
    }
}
```

You get at most ten trips whose fare is over $100, reading just two columns. Because the filter
is pushed down, row groups and pages that can't contain a match are skipped before they're
decoded.

## Step 5 — Sum a column the fast way

When you want to aggregate a single column over the whole file, the `ColumnReader` hands you
typed primitive arrays a batch at a time — no per-row calls. Total the fares:

```java
import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.Validity;

import java.nio.file.Path;

try (ParquetFileReader reader =
            ParquetFileReader.open(InputFile.of(Path.of("yellow_tripdata_2026-01.parquet")));
        ColumnReader fare = reader.columnReader("fare_amount")) {

    double total = 0;
    while (fare.nextBatch()) {
        int count = fare.getValueCount();
        double[] values = fare.getDoubles();
        Validity validity = fare.getLeafValidity();
        boolean hasNulls = validity.hasNulls();

        for (int i = 0; i < count; i++) {
            if (!hasNulls || validity.isNotNull(i)) {
                total += values[i];
            }
        }
    }
    System.out.printf("Total fares: $%.2f%n", total);
}
```

That's the columnar path: iterate batches, read the primitive array, and check the
[`Validity`](/api/latest/dev/hardwood/reader/Validity.html) bitmap for nulls. On analytical work
like this it's markedly faster than reading row by row.

## What you've learned

You opened a file, inspected its schema, read rows with typed accessors, pushed down a
projection and a filter, and aggregated a column columnar-style. That's the core of reading
Parquet with Hardwood.

## Where to go next

- [Read Row by Row](../how-to/row-reader.md) — every typed accessor, plus nested structs, lists,
  and maps.
- [Read Column by Column](../how-to/column-reader.md) — the columnar API in full, including
  nested data and hot-loop patterns.
- [Filter, Project, Limit, and Split](../how-to/query-controls.md) — all the read-narrowing
  options and how they compose.
- [RowReader vs. ColumnReader](../concepts/reader-models.md) — *why* there are two reader
  APIs and how to choose.
- [How a Parquet File Is Laid Out](../concepts/parquet-layout.md) — the structure that makes
  projection and pushdown possible.
