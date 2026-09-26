<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Error Handling

A failed read leaves you one decision, to try again or to stop, and the exception type answers
it. **Trying again may help for `IOException`, and will not for anything else.**

## The read failed

| Exception | When |
|-----------|------|
| `IOException` | The bytes did not arrive: a local-disk read error, an S3 transport failure (after retry exhaustion; see [Read from S3](../how-to/s3.md)), a file that cannot be opened. Checked, and declared by every method that reaches the file: `ParquetFileReader.open`/`openAll`; the reader factories `rowReader`, `columnReader` and `columnReaders` and their builders' `build()`; `RowReader.hasNext`/`next`/`close`; `ColumnReader.nextBatch`/`close`; `ColumnReaders.nextBatch`/`close` |
| `ParquetReadException` | They arrived and are not valid Parquet: a bad magic number, a corrupt footer, a malformed page index, a dictionary page the metadata places outside its column chunk, a page whose checksum fails, a page that will not decompress, values that do not decode. Unchecked |
| `SchemaIncompatibleException` | A `ParquetReadException`. In a multi-file read, a file whose schema cannot be reconciled with the first file's; or one file's footer disagreeing with itself about which leaf a column chunk holds |
| `UnsupportedOperationException` | The file is correct and Hardwood cannot read it: Parquet Modular Encryption, an encoding not implemented, a compression codec whose library is absent (the message names the dependency to add), a column chunk stored in a separate file (the legacy split-file layout), a row group whose page-index region exceeds 2 GB, or a file over 2 GB opened with the mmap-backed range cache |

## The call was wrong

| Exception | When |
|-----------|------|
| `IllegalArgumentException` | Accessing a column not in the projection, an invalid column name, or asking a column for a type it does not hold, such as `getFloat` on a column that is neither `FLOAT` nor `FLOAT16` |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `IndexOutOfBoundsException` | A field index outside `[0, getFieldCount())` on `getFieldName(int)`, on a `RowReader` or on a `PqStruct` |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | Calling `ColumnReader` accessors before `nextBatch()`, calling `nextBatch()` on a reader obtained from `ColumnReaders`, or calling nested-column methods on a flat column |

Requesting the wrong type for a column, such as `getLong` on an `INT32` column, is a programming
error whose exception type is deliberately unspecified, and is covered in
[Type mismatches](accessors.md#type-mismatches).

Hardwood raises `UncheckedIOException` from no public method. Reading a `RowReader` inside a
`Stream` or an `Iterator` means wrapping the checked exception at that boundary yourself and
unwrapping it where the stream is consumed.

## What a message says

```
[data.parquet: row group 0, column 'category', page 123] CRC mismatch: expected 609e7e3 but computed 2b0b086e
[data.parquet: row group 0, column 'category', dictionary page] CRC mismatch: expected 609e7e3 but computed 2b0b086e
[data.parquet] Not a Parquet file (invalid magic number at end)
```

Every message the reader composes names the file. A message for an invalid file also names
the row group, column and page, leaving out any part the reader could not determine: a
failure before a column chunk's pages are walked names no page, and one while the footer is
parsed names only the file. A message for a wrong call names the file alone. An `IOException`
an `InputFile` raises for itself is unprefixed.

## Writing

The exceptions a write can throw, the condition behind each, and the schema shapes the writer refuses are tabulated in the [Writer Reference](writer.md#what-the-writer-rejects).
