<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Error Handling

Retrying a failed read may succeed after an `IOException`, and will not after any other exception.

## The read failed

| Exception | When |
|-----------|------|
| `IOException` | The bytes did not arrive: a local-disk read error, an S3 transport failure (after retry exhaustion; see [`maxRetries`](s3.md#s3source-builder-options)), a file that cannot be opened. Checked, and declared by every method that reaches the file |
| `StaleMetadataException` | An `IOException`: the footer the context's `MetadataSource` supplied does not describe the file being read. Raised before any data of that file is read, by `open`/`openAll` for the first file and by the call that reaches a later file. The file is intact; evicting the footer the source served for it and opening a new reader reads it afresh. `fileName()` names the file, `check()` the check that failed (`IDENTITY` or `TRAILER`), and `sourceIdentity()` and `fileIdentity()` the identity of the file the footer was read from and of the file being read, each empty where that file has none (see [Reuse a parsed footer across readers](../how-to/metadata.md#reuse-a-parsed-footer-across-readers)) |
| `ParquetReadException` | They arrived and are not valid Parquet: a bad magic number, a corrupt footer, a malformed page index, a dictionary page the metadata places outside its column chunk, a page whose checksum fails, a page that will not decompress, values that do not decode, a shredded Variant `typed_value` column of a type the Variant shredding specification does not list (raised when the reader is built; see [Variant](../how-to/variant.md)). When the context's `MetadataSource` supplied the file's footer, the message ends with `(footer supplied by the MetadataSource; the file may have changed since the footer was read)`: the file may have been rewritten in a way the checks behind `StaleMetadataException` do not detect, and evicting that footer and opening a new reader reads the file afresh. Unchecked |
| `SchemaIncompatibleException` | A `ParquetReadException`. In a multi-file read, a file whose schema cannot be reconciled with the first file's; or one file's footer disagreeing with itself about which leaf a column chunk holds; or a `FIXED_LEN_BYTE_ARRAY` column the read touches that declares no positive width; or, building an `AvroRowReader`, a projected `LIST` group with no element or `MAP` group with no key |
| `UnsupportedOperationException` | The file is correct and Hardwood cannot read it: Parquet Modular Encryption, an encoding not implemented, a compression codec whose library is absent (the message names the dependency to add), a column chunk stored in a separate file (the legacy split-file layout), a column chunk over 2 GB, a file over 2 GB opened with the mmap-backed range cache, or a read of a repeated group annotated `VARIANT` outside a `LIST` or `MAP` group (a list of variants in that form); or, building an `AvroRowReader`, a schema Avro cannot represent (see [Avro Support](../how-to/avro.md)) |

## The call was wrong

| Exception | When |
|-----------|------|
| `IllegalArgumentException` | Accessing a column not in the projection, an invalid column name, or asking a column for a type it does not hold, such as `getFloat` on a column that is neither `FLOAT` nor `FLOAT16` |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `IndexOutOfBoundsException` | A field index outside `[0, getFieldCount())` on `getFieldName(int)`, on a `RowReader` or on a `PqStruct` |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | Calling `ColumnReader` accessors before `nextBatch()`, calling `nextBatch()` on a reader obtained from `ColumnReaders`, or calling nested-column methods on a flat column; a `MetadataSource` that returns `null`; building a row or column reader from a closed `ParquetFileReader`, or while another thread closes it |
| `VariantTypeException` | Calling a `PqVariant` `as*()` method whose type does not match the variant's type tag, such as `asInt()` on a string. Unchecked |

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

Every message about a file names it. A message for an invalid file also names
the row group, column and page, leaving out any part the reader could not determine: a
failure before a column chunk's pages are walked names no page, and one while the footer is
parsed names only the file. A message for a wrong call names at most the file. An `IOException`
an `InputFile` raises while the first file is opened or its footer read carries only the
`InputFile`'s own message. An unchecked exception a `MetadataSource` raises keeps its type and
gains the file name; a checked one reaches the caller as the source raised it when the first
file is opened, and as the cause of an `IOException` naming the file for a later file of a
multi-file read. A schema rejection raised while an `AvroRowReader` is built names
the schema path of the offending group or column, not the file.

## Writing

The exceptions a write can throw, the condition behind each, and the schema shapes the writer refuses are tabulated in the [Writer Reference](writer.md#what-the-writer-rejects).

For S3 output, an `IOException` from close can mean publication could not be
confirmed even though the completed object exists. Suppressed diagnostics describe
verification and cleanup failures. See [S3 publication and cleanup](s3.md#publication-and-cleanup).
