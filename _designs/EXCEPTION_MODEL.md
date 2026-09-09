# Design: the exception model

Status: implemented

Exception handling is organized along two separate axes: retriability, and whether the
problem is with a file or with the API invocation.

Only IO issues are considered retriable — a network glitch, a disk error — where issues with
a file itself, such as an invalid bloom filter, are not. The second axis tells a caller
whether to fix their code or stop trusting the file.

| Exception | Means | Retriable |
|---|---|---|
| `IOException` | the bytes did not arrive | yes |
| `ParquetReadException` | they arrived and are not valid Parquet | no |
| `SchemaIncompatibleException` | a `ParquetReadException`: schemas that cannot be reconciled across a multi-file read, or a footer disagreeing with itself | no |
| `ParquetWriteException` | the writer could not produce the file, and neither the caller nor the destination is at fault | no |
| `UnsupportedOperationException` | the file is correct and Hardwood cannot read it: encryption, an absent codec library, an unimplemented encoding, a chunk in another file, limits on index size and mmap-backed files | no |
| `IllegalArgumentException`, `NullPointerException`, `NoSuchElementException`, `IllegalStateException` | the reader was asked for something it never held | no |

The caller's side is stated except for one case: asking an accessor for a type the column
does not hold, where validating cost 4% per accessor and 7–8% end-to-end, above the 3% bar,
so the call surfaces as whatever cast the decode makes raises. #971 covers putting a better
error back if it can be made free, which the exception path is: it runs only once the call
has already failed.

`getDate`, `getUuid` and `getInterval` are outside that, because no cast on their way to the
value can fail. A `DATE`, a bare `INT32` and a `TIME(MILLIS)` are one `int[]`, and every
`FIXED_LEN_BYTE_ARRAY` of the right width is one `BinaryBatchValues` — an `INT96` included,
which is 12 bytes and so reads as an `INTERVAL`. Each of the three checks the annotation
first (`LogicalAccessorKind`), since what it buys is not a better exception but the only one
there is.

## Propagating IO issues

**A method declares `IOException` only if it can reach a file.** Parsing a buffer and decoding
a page cannot, so no `catch (IOException)` around them can turn a corrupt file into a failed
read. `PageIterator` is a two-method interface rather than a `java.util.Iterator` precisely
so advancing a column's pages can say it reads.

## Error messages

General structure: `[context] <problem>`.

```
[data.parquet: row group 0, column 'category', page 123] CRC mismatch: expected … but computed …
[data.parquet: row group 0, column 'category', dictionary page] CRC mismatch: …
[data.parquet] Not a Parquet file (invalid magic number at start)
```

The context is the file, the row group, the column and the page, each left out where the
reader could not determine it. A file's fault carries all of it; a caller's error carries the
file name alone, since the fix is the same wherever the reader had got to. Because the
context names the column, the problem does not.

The row group and page are the file's own indexes, not the read's: pruning drops row groups
and a page filter drops pages before either is read. A byte offset is not carried: it is
known only deep inside a parse, with no boundary there to hand it to. The context lives in
the message only; putting it on the exception as fields, so that callers route on it without
parsing, is #1093's remaining half and is not built.

## Crossing a thread boundary

A checked exception cannot travel through a `Runnable`, a `Supplier`, or a mapping function
passed to `computeIfAbsent`. Those are the only places an I/O failure is wrapped in an
`UncheckedIOException`, and each wrap is undone by the method enclosing the lambda, so no
caller sees one. **A boundary counts when the language forbids the declaration, not when an
interface we chose happens to.**

The read pipeline's three threads — page retriever, page decoder, batch assembler — meet at
boundaries that catch `Exception`, record where they were, and hand it to the consumer.
Whatever the decoders raise there becomes a `ParquetReadException` keeping the original as
its cause: a judgement about which mistake to make, since otherwise every corrupt file raises
an `ArrayIndexOutOfBoundsException` out of a reader. An `Error` is neither retyped nor placed
and reaches the consumer as raised, but is recorded on the way past, or a consumer waiting on
work that thread will never finish waits for ever.

A prefetch is the exception: nothing waits on it, so a failure has nobody to report to and is
logged at DEBUG and dropped. That is safe because the buffer field is assigned only on
success, so nothing is cached and the demand path redoes the work with a caller waiting.

## An annotation the reader cannot use is dropped, not raised

A logical annotation that is unknown or that its physical type cannot carry is dropped, and
the column is read as its physical type — parquet-format PR 606 has readers "ignore both the
logical type annotation and column order for that column". No check is needed downstream: the
physical accessors work, a logical accessor fails as on any unannotated column, and
statistics compare under the physical type's ordering. The two are dropped in different
places, `LogicalTypeReader` and `FileSchema`, and warn differently: an unrecognized annotation
means a newer format version, an unusable one means the writer produced something no version
defines. Only the second can be shown wrong, so only it could ever be a candidate for
refusing the read.

A missing width is not in that class. A `FIXED_LEN_BYTE_ARRAY` without one cannot be decoded
at all — the width sizes the buffer and spaces the offsets — so `FixedWidthValidator` refuses
it when the reader is built, over the columns the read touches.

`docs/content/reference/error-handling.md` carries the user-facing table.
