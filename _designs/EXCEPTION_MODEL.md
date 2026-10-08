# Exception model

Describes which exceptions Hardwood raises and what each tells a caller: the exception types and the two axes that sort them, the rule that no internal type reaches a caller, where an `IOException` may be declared or wrapped, the context an error message carries, how failures cross the read pipeline's threads, and why an annotation the reader cannot use is dropped rather than raised.

Related documents:

- [READ_PIPELINE.md](READ_PIPELINE.md): how the pipeline's threads hand a failure to the consumer
- [FILE_METADATA.md](FILE_METADATA.md): the malformed-input policy of the footer parser
- [LOGICAL_TYPES.md](LOGICAL_TYPES.md): which annotations `FileSchema` drops
- [STATISTICS_PRUNING.md](STATISTICS_PRUNING.md): why a column with a dropped annotation has no readable bounds
- [WRITER.md](WRITER.md): which exceptions out of a write call fail the writer
- [WRITER_INPUT.md](WRITER_INPUT.md): which row-writer rejections are returned rather than thrown
- [docs/content/reference/error-handling.md](../docs/content/reference/error-handling.md): the user-facing table

## Exception types

Exception handling is organized along two separate axes: retriability, and whether the
problem is with a file or with the API invocation.

Only IO issues are considered retriable — a network glitch, a disk error — where issues with
a file itself, such as an invalid bloom filter, are not. The second axis tells a caller
whether to fix their code or stop trusting the file.

| Exception | Means | Retriable |
|---|---|---|
| `IOException` | the bytes did not arrive | yes |
| `StaleMetadataException` | an `IOException`: the footer a `MetadataSource` supplied does not describe the file; the file is intact | yes, once the source reads the footer afresh |
| `ParquetReadException` | they arrived and are not valid Parquet | no |
| `SchemaIncompatibleException` | a `ParquetReadException`: schemas that cannot be reconciled across a multi-file read, or a footer disagreeing with itself | no |
| `ParquetWriteException` | the writer could not produce the file, and neither the caller nor the destination is at fault | no |
| `UnsupportedOperationException` | the file is correct and Hardwood cannot read it: encryption, an absent codec library, an unimplemented encoding, a chunk in another file, a column chunk read without an OffsetIndex or a range-backed file over 2 GB | no |
| `IllegalArgumentException`, `NullPointerException`, `IndexOutOfBoundsException`, `NoSuchElementException`, `IllegalStateException` | the reader was asked for something it never held, or the writer was handed input it does not accept | no |
| `VariantTypeException` | unchecked: a Variant `as*` accessor called on a value of another type tag | no |

The caller's side is stated except for one case: asking an accessor for a type the column
does not hold, where validating ahead of the decode would cost every accessor call, so the call surfaces as whatever cast the decode makes raises. #971 covers putting a better
error back if it can be made free, which the exception path is: it runs only once the call
has already failed.

`getDate`, `getUuid`, `getInterval`, `getString` and the `FLOAT16` branch of `getFloat` are
outside that, because no cast on their way to the value can fail. A `DATE`, a bare `INT32` and a `TIME(MILLIS)` are one `int[]`, and every
`FIXED_LEN_BYTE_ARRAY` of the right width is one `BinaryBatchValues` — an `INT96` included,
which is 12 bytes and so reads as an `INTERVAL`. Every byte column decodes to some text, and `getFloat` reads any byte column that is not a
`FLOAT` two bytes at a time. Each of the five checks the annotation
first (`LogicalAccessorKind`), since what it buys is not a better exception but the only one
there is.

## The table above is the whole list

An exception that reaches a caller is one of the types named there, or a JDK type. It is
never one declared under `dev.hardwood.internal`, which a caller could only catch by
importing from an internal package.

An internal type may still carry a condition between two frames, as long as a boundary
catches it and reissues one of the above. `EncryptedFileException` carries "this footer is
encrypted" up to `ParquetMetadataReader`, which raises the `UnsupportedOperationException`
the caller sees. The single catch site is what makes that safe: a type thrown where no frame
is guaranteed to catch it will reach a caller eventually. `ThriftTruncatedException`, an
internal `ParquetReadException` subclass the page-header readers catch to grow a short read, is
restated as a plain `ParquetReadException` at every boundary that classifies a failure, so
no caller holds it.

`RejectedRecordException` carries a rejection of a record's own data (a value its column
cannot hold, a `REQUIRED` field left null) from the row layer's setters and
`PhysicalValueConverter` up to `RowWriter`, the one class that calls `RowPlan.writeRecord`.
`writeRow` restates it as a plain `IllegalArgumentException`. `tryWriteRow` raises nothing
for it and returns `RowWriteResult.Rejected` with its field path and message, the one failure
Hardwood reports as a value rather than an exception. The type is thrown from many sites and
caught in one class only because every path to those sites runs through
`RowPlan.writeRecord`. A caller of `PhysicalValueConverter` outside the row layer, such as
`ColumnBatch`, would have no such boundary and would let the internal type reach a caller
(untested).

A new public type earns its place only where a caller would act on it. Where the response is
the same — retry, give up, report — the distinction belongs in the message every one of these
already carries. An unrecognized bloom filter variant raises `UnsupportedOperationException`
naming the variant, rather than a type of its own.

## Remote output failures

An `IOException` from a remote output can report a failed operation or an outcome
that the client could not confirm. It does not imply that replaying the operation
is safe or that the destination is unchanged. S3 multipart initiation, whole-object
PUT, and multipart completion are not automatically replayed.

After an uncertain publication response, the S3 output checks the object's write
UUID and expected length. A match confirms success; an inconclusive check preserves
the original exception and adds a suppressed publication diagnostic. Verification
and cleanup failures are also suppressed on the original failure. A lost
initialization response can leave an upload whose ID is unknown; discard only
aborts known uploads and never deletes a completed object.

Interruption stops verification and remains set after the cleanup attempt.
[Sequential S3 output](S3_STORAGE.md#sequential-output) defines the operation-specific
retry budgets and publication/cleanup states.

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
[data.parquet] Not a Parquet file (invalid magic number at end)
```

The context is the file, the row group, the column and the page, each left out where the
reader could not determine it. A file's fault carries all of it; a caller's error carries the
file name alone, since the fix is the same wherever the reader had got to. Because the
context names the column, the problem does not (#1158 covers the messages that still do).

The Avro binding's schema conversion is the exception. It runs on the `FileSchema` alone,
before the underlying reader is built, and holds no file name, so the file faults it rejects
carry no context and name the offending node by its schema path in the problem.

The row group and page are the file's own indexes, not the read's: pruning drops row groups
and a page filter drops pages before either is read. A byte offset is not carried: it is
known only deep inside a parse, with no boundary there to hand it to. The context lives in
the message only; `ParquetReadException` carries no position fields.

## Crossing a thread boundary

A checked exception cannot travel through a `Runnable`, a `Supplier`, or a mapping function
passed to `computeIfAbsent`. Those are the only places an I/O failure is wrapped in an
`UncheckedIOException`, and each wrap is undone by the method enclosing the lambda, so no
caller sees one. **A boundary counts when the language forbids the declaration, not when an
interface we chose happens to.**

The read pipeline's threads (retriever, decode tasks, drain) meet at boundaries that catch
`Exception`, record where they were, and hand it to the consumer
([READ_PIPELINE.md](READ_PIPELINE.md#error-propagation)). Whatever the decoders raise there,
other than an `UnsupportedOperationException`, becomes a `ParquetReadException` keeping the
original as its cause: a judgement about which mistake to make, since otherwise every corrupt file raises
an `ArrayIndexOutOfBoundsException` out of a reader. `ExceptionContext.asReadFailure` does
this, also where a footer is loaded and in the byte-level reads outside the pipeline (dive's
`ParquetModel`, `inspect pages`, `inspect dictionary`, `inspect columns`). An `Error` is neither retyped nor placed
and reaches the consumer as raised, but is recorded on the way past, or a consumer waiting on
work that thread will never finish waits for ever.

A prefetch is the exception: nothing waits on it, so a failure has nobody to report to and is
logged at DEBUG and dropped. That is safe because the buffer field is assigned only on
success, so nothing is cached and the demand path redoes the work with a caller waiting. The
iterator's close waits for running prefetches (`PrefetchTasks`) before the file closes. A
footer prefetch is different: `FileMetadataCache` keeps its failure and rethrows it on demand.

## An annotation the reader cannot use is dropped, not raised

A logical annotation that is unknown or that its physical type cannot carry is dropped, and
the column is read as its physical type — parquet-format PR 606 has readers "ignore both the
logical type annotation and column order for that column". The accessors need no check: the
physical accessors work and a logical accessor fails as on any unannotated column. Statistics
need one, because the column's writer ordered its bounds by the dropped annotation:
`BoundsReadability` reads no bounds for the column
([STATISTICS_PRUNING.md](STATISTICS_PRUNING.md#bounds-readability)). The two are dropped in different
places, `LogicalTypeReader` and `FileSchema`, and warn differently: an unrecognized annotation
means a newer format version, an unusable one means the writer produced something no version
defines. Only the second can be shown wrong, so only it could ever be a candidate for
refusing the read.

The same holds for a group: an annotation on a repeated group outside a `LIST` or `MAP` group
has no reading there, so `BareRepeatedGroups` drops it, with one warning per file, and the
group reads as the unannotated list it is in structure. A `VARIANT` annotation there is not
dropped: it makes the field a list of variants, which the reader does not serve. Like the
missing width below, a read that projects the group's columns is refused when the reader is built,
here with `UnsupportedOperationException`, rather than answered as something else, and the
file's other columns stay readable.

A missing width is not in that class. A `FIXED_LEN_BYTE_ARRAY` without one cannot be decoded
at all — the width sizes the buffer and spaces the offsets — so `FixedWidthValidator` refuses
it when the reader is built, over the columns the read touches.
