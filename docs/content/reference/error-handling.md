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

A read that fails leaves you one decision: try again, or stop. The exception
type answers it.

> **Trying again may help:** `IOException`
> **Trying again will not:** `ParquetReadException`, `UnsupportedOperationException`

`IOException` means the bytes did not arrive. Once they have, nothing about them
changes on a second attempt, so parsing and decoding raise `ParquetReadException`
instead — including for a page that will not decompress and a dictionary that will
not decode.

| Exception | When |
|-----------|------|
| `IOException` | Reading the file failed: a local-disk read error, an S3 transport failure (after retry exhaustion — see [Read from S3](../how-to/s3.md)), a file that cannot be opened. Checked, and declared by every method that reaches the file: `ParquetFileReader.open`/`openAll`; the reader factories and their builders' `build()` — `rowReader`, `columnReader`, `columnReaders`; `RowReader.hasNext`/`next`/`close`; `ColumnReader.nextBatch`/`close`; `ColumnReaders.nextBatch`/`close` |
| `ParquetReadException` | The file was read and is not valid Parquet: a bad magic number, a corrupt footer, a malformed page index, a dictionary page the metadata places outside its column chunk, a page whose checksum fails, values that do not decode. Unchecked |
| `SchemaIncompatibleException` | A `ParquetReadException`. In a multi-file read, a file whose schema cannot be reconciled with the first file's; or one file's footer disagreeing with itself about which leaf a column chunk holds |
| `UnsupportedOperationException` | The file is correct and Hardwood cannot read it: Parquet Modular Encryption, an encoding not implemented, a compression codec whose library is absent — the message names the dependency to add — a column chunk stored in a separate file (the legacy split-file layout), a row group whose page-index region exceeds 2 GB, or a file over 2 GB opened with the mmap-backed range cache |
| `IllegalArgumentException` | Accessing a column not in the projection, or an invalid column name |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | Calling `ColumnReader` accessors before `nextBatch()`, or calling nested-column methods on a flat column |

The last four are mistakes in the calling code rather than read failures, and no
file is involved.

Reading a `RowReader` inside a `Stream` or an `Iterator` means adapting a checked
exception to an interface that cannot declare one. Wrap it in
`UncheckedIOException` at that boundary and unwrap it where the stream is
consumed; Hardwood itself raises `UncheckedIOException` from no public method.

## Writing

| Exception | When |
|-----------|------|
| `IOException` | The destination cannot be created, written, or finalized. The writer discards its output rather than leaving a truncated file at the target path |
| `ParquetWriteException` | The file could not be produced, for a reason that is neither your call nor the destination: a compression codec that rejects a page body. Unchecked |
| `UnsupportedOperationException` | A schema column of an unsupported physical type (`INT96`); a refused compression codec (`LZ4`, `LZO`), one whose library is not on the classpath, or one whose native library will not load; a schema shape the writer cannot produce — repetition no `LIST` or `MAP` annotation accounts for |
| `IllegalArgumentException` | An unknown column name or path, a setter that does not fit the column's type, a column set twice, a batch that leaves a column unset or whose arrays disagree in length, a null mask on a `REQUIRED` column, list offsets that do not describe the elements given, a value outside the range its annotation declares, or a `REQUIRED` field a record leaves unset |
| `IndexOutOfBoundsException` | A leaf-column index outside `[0, leaf column count)` on a `ColumnBatch` setter, or a field index outside `[0, getFieldCount())` on a `StructBuilder` setter |
| `IllegalStateException` | Writing after `close()`, using both write APIs on one file, using a `ColumnBatch` or nested builder after its scope has ended, or taking `BufferOutputFile.buffer()` before the writer has closed |

For what each of these means in context, see the [Writer Reference](writer.md#what-the-writer-rejects).
