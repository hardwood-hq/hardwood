<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Thrift metadata parser: malformed-input policy

**Status:** Completed

`dev.hardwood.internal.thrift` turns the bytes of a Parquet footer, page header and page
index into the metadata records the rest of the reader plans against. It is the first code
to touch a file and the only code that touches it before any bound has been established, so
every value it produces is attacker-controlled until it has been checked.

This document defines what the package does with input that does not match the format, and
the shape the readers share so that policy holds at every field.

## Policy

### What happens to a field the reader cannot use turns on whether it can continue without it

Thrift's forward-compatibility rule is that a reader consumes a field it does not recognise
by that field's own declared type and moves on: the declared type is enough to skip the field
whatever it holds, so the cost is bounded to it and the struct keeps parsing. Hardwood applies
that rule to every field it can do without, and departs from it for the fields it cannot.

| | wrong wire type, or wrong element type for a collection |
|---|---|
| optional field | skipped, reported absent, logged at `WARNING` |
| required field | `ParquetReadException` naming the field and both types |

An optional field already has a representation for *not there* that its consumers handle, so
one that cannot be decoded can use it: the file loses a field and stays readable, which is what
lets a newer writer's output pass through an older reader. A required field has no such
representation — reporting `RowGroup.columns` as an empty list would answer a query with zero
rows instead of failing it.

Thrift's own answer for a required field is to skip it too and fail at the end of the struct,
as a field that never arrived. Hardwood rejects it where it is: the read fails either way, and
"missing" said of a field that did arrive carrying the wrong type points at the wrong question.
Genuine absence is what `ThriftCompactReader.missingFields` reports at the STOP that ends the
struct — the one complaint that is about a struct rather than about a field of it.

Four methods implement the gate, and the typed list reads (`readStructList`, `readStringList`,
`readBinaryList`, `readBoolArray`, `readOptionalI64Array`) are built on the list pair:

- `acceptField(header, expectedType)` / `acceptListHeader(elementType)` — skip and log
- `requireField(header, expectedType)` / `requireListHeader(elementType)` — throw

Neither takes the field's name: the reader knows which field it is standing on, and `ThriftStruct`
holds the names, checked against parquet-format's own metadata. The expected type is always named
through `Codes`, never a raw hex literal — `0x08` and `0x09` differ by one bit and pick a different
branch in silence. Log line and thrown message are built from the same words, so the two read
alike: `wrong Thrift wire type 0x1 (expected 0x5)`.

### A collection is never decoded as some other element type

A list header declares one element type for the whole collection, so decoding its elements as
another consumes the wrong number of bytes each and desynchronises the stream: value bytes are
then read as field headers, and the rest of the enclosing struct — the whole footer, for
`FileMetaData.schema` — is misread rather than merely lost.

A union is the same argument at one element. Its variant carries the meaning in its id and the
empty struct the format gives it as the value, so `readUnionVariant` requires that value to
declare `struct` rather than skipping it by whatever type it claims.

### Sizes, counts and offsets are validated where they are read

A length or offset from the file reaches an allocation, an array index or a
`ByteBuffer.slice` downstream. Checked at the point of use it produces an
`IndexOutOfBoundsException` or a `NegativeArraySizeException` — unchecked, unattributable,
and outside the `IOException` contract the metadata path advertises. Checked at the point
of read it produces a controlled error naming the field, which
`ParquetMetadataReader` then attributes to the file.

Readers use `readNonNegativeI32()` / `readNonNegativeI64()` for every field the format defines
as a size, count, length or file offset — in the footer readers as well as the page-header
readers.

Collection sizes are bounded by the bytes remaining in the buffer, since every Thrift
element occupies at least one byte on the wire. This applies to the long-form list count
and to the map size in `skipField`; a count that overflows `int` is rejected rather than
truncated.

### The page index is internally consistent before it is indexed

`ColumnIndex` carries seven per-page members. `null_pages` is required and defines the page
count; `min_values`, `max_values`, `null_counts` and `nan_counts` must have exactly that
many entries, and the two level histograms a whole multiple of it. `ColumnIndexReader`
rejects a chunk where they disagree, so page filtering never indexes one array with another
array's length.

The page count also has to agree across the two structs: `ColumnIndex` describes the pages
that `OffsetIndex` locates. The two are parsed together in
`PageFilterEvaluator.readIndexPair`, which cross-checks them there.

### An unsupported feature fails, it does not read the wrong bytes

`ColumnChunk.file_path` places a column's data in a different file. Hardwood does not
support the split-file layout; reading its own file at `data_page_offset` regardless would
return whatever happens to sit at that offset. The reader therefore carries `file_path` on
`ColumnChunk` and refuses at the point the chunk's bytes would be read: `requireSameFile()` on
the record, called before the scan takes its first look at a row group, before the dictionary
and bloom-filter sources that prune it, and in the CLI's page and dictionary readers.

The scan checks every chunk of a row group at once, not the projected ones one at a time,
because the page-index region is fetched as a single span across all of them: one chunk
pointing elsewhere misplaces the region for the rest.

The refusal sits there rather than in the footer parse so that the metadata of such a file
stays readable. Its schema, row groups and statistics are all decodable and describe the file
correctly; only the data lives elsewhere. Failing the parse would take a file that `hardwood
inspect` can explain and make it unopenable, which is the opposite of what a diagnostic tool
is for.

### Failures name the file, the row group and the column

The footer is read once per file and `ParquetMetadataReader` attributes its failures. The
page index is read once per column chunk, from `PageFilterEvaluator` and from
`RowGroupIterator`, and those failures carry the file, row group and column at the call
site.

## Record shapes

Per-page metadata is held in primitive arrays: `ColumnIndex.nullPages` is a `boolean[]`,
alongside the `long[]` count arrays. Page filtering reads each of them once per page, and a
primitive array makes that a load rather than a pointer chase and an unbox. It also removes
the question of what a `null` element would mean, which the boxed shape left to an
invariant nothing stated.

An absent optional array stays `null`, distinct from a zero-length one the writer recorded
as empty.
