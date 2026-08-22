<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Error Handling

Hardwood throws specific exceptions for common error conditions.

## Reading

| Exception | When |
|-----------|------|
| `IOException` | Any I/O error: invalid Parquet file (bad magic number, corrupt footer, malformed page index), reading a column chunk whose `file_path` points at another file (the split-file layout is not supported; such a file's metadata still reads), encrypted files (Parquet Modular Encryption is not supported), local-disk read errors, S3 transport failures (after retry exhaustion — see [Read from S3](../how-to/s3.md)) |
| `UnsupportedOperationException` | Compression codec library not on classpath — the message names the required dependency |
| `IllegalArgumentException` | Accessing a column not in the projection, or invalid column name |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | Calling `ColumnReader` accessors before `nextBatch()`, or calling nested-column methods on a flat column |

## Writing

| Exception | When |
|-----------|------|
| `IOException` | The destination cannot be created, written, or finalized. The writer discards its output rather than leaving a truncated file at the target path |
| `UnsupportedOperationException` | A schema column of an unsupported physical type (`INT96`); a refused compression codec (`LZ4`, `LZO`) or one whose library is not on the classpath; an `OPTIONAL` struct group directly enclosing a repeated field |
| `IllegalArgumentException` | An unknown column name or path, a setter that does not fit the column's type, a column set twice, a batch that leaves a column unset or whose arrays disagree in length, a null mask on a `REQUIRED` column, list offsets that do not describe the elements given, a value outside the range its annotation declares, or a `REQUIRED` field a record leaves unset |
| `IndexOutOfBoundsException` | A field index outside `[0, getFieldCount())` |
| `IllegalStateException` | Writing after `close()`, using both write APIs on one file, or using a `ColumnBatch` or nested builder after its scope has ended |

For what each of these means in context, see the [Writer Reference](writer.md#what-the-writer-rejects).
