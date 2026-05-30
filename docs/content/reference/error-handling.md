<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Error Handling

Hardwood throws specific exceptions for common error conditions:

| Exception | When |
|-----------|------|
| `IOException` | Any I/O error: invalid Parquet file (bad magic number, corrupt footer), local-disk read errors, S3 transport failures (after retry exhaustion — see [Read from S3](../how-to/s3.md)) |
| `UnsupportedOperationException` | Compression codec library not on classpath — the message names the required dependency |
| `IllegalArgumentException` | Accessing a column not in the projection, or invalid column name |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | Calling `ColumnReader` accessors before `nextBatch()`, or calling nested-column methods on a flat column |
