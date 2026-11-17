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
| `ParquetReadException` | They arrived and are not valid Parquet: part of the file is corrupt or breaks the format, such as a footer that does not parse, a page that fails its checksum, or a value outside what its type allows. Raised when the read reaches the invalid part (on open, when building a reader, or when reading the value), so what the read returned before that point was read normally. An S3 object replaced since the file was opened fails the same way (see [Footer pre-fetch](s3.md#footer-pre-fetch); opening the file again reads the new version). When the context's `MetadataSource` supplied the file's footer, the file may have been rewritten since that footer was read, in a way the checks behind `StaleMetadataException` do not detect; the message then says so, and evicting that footer and opening a new reader reads the file afresh. Unchecked |
| `SchemaIncompatibleException` | A `ParquetReadException` for a schema that does not fit the read: in a multi-file read, a file whose schema differs from the first file's in a column the read uses (see [Read multiple files](../how-to/multi-file.md)); in any read, a footer whose schema is inconsistent, or lacks information needed to decode a column the read uses |
| `UnsupportedOperationException` | The file is correct and Hardwood cannot read it, such as Parquet Modular Encryption, an encoding not implemented or a compression codec whose library is absent. The message names what is not supported, and for an absent codec library the dependency to add. When only one column uses the unsupported feature, a read that leaves that column out of its projection reads the other columns |

## The call was wrong

| Exception | When |
|-----------|------|
| `IllegalArgumentException` | Accessing a column not in the projection, an invalid column name, or asking a column for a type it does not hold, such as `getFloat` on a column that is neither `FLOAT` nor `FLOAT16`, or `getFile` on a group not annotated `FILE` |
| `NullPointerException` | Calling a primitive accessor (`getInt`, `getLong`, etc.) on a null field without checking `isNull()` first |
| `IndexOutOfBoundsException` | A field index outside `[0, getFieldCount())` on `getFieldName(int)`, on a `RowReader` or on a `PqStruct` |
| `NoSuchElementException` | Calling `next()` on a `RowReader` when `hasNext()` returns `false` |
| `IllegalStateException` | The call came at a point the object's state does not allow, such as a `ColumnReader` accessor before `nextBatch()`, building a reader from a closed `ParquetFileReader`, or building a reader on a thread of the `HardwoodContext` it reads with; or a `MetadataSource` broke its contract by returning `null` |
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
the row group, column and page, leaving out any part the reader could not determine. A message for a wrong call names at most the file. An `IOException`
an `InputFile` raises while the first file is opened or its footer read carries only the
`InputFile`'s own message. An unchecked exception a `MetadataSource` raises
reaches the caller with the file name in its message: the exception the source raised, or
one with that exception in its cause chain (for an `UncheckedIOException`, with its
`IOException`). A checked one reaches the caller as the source raised it when the first
file is opened, and as the cause of an `IOException` naming the file for a later file of a
multi-file read. A schema rejection raised while an `AvroRowReader` is built names
the schema path of the offending group or column, not the file.

## Writing

The exceptions a write can throw, with what each means and examples of what raises it, are in the [Writer Reference](writer.md#rejected-input).
