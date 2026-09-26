# Input files

How the reader gets at a file's bytes: the `InputFile` contract, who opens and closes an `InputFile`, the memory-mapped local backend, the in-memory backend, the backend-agnostic range cache in core, and the size limits each imposes. Which byte ranges a read requests and how they are coalesced is in [FETCH_PLANNING.md](FETCH_PLANNING.md); the S3 backend, its client and its choice of range backing are in [S3_STORAGE.md](S3_STORAGE.md); reading and caching the footer is in [FILE_METADATA.md](FILE_METADATA.md); how transport failures are typed is in [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md). The threads that call into an `InputFile` are described in [READ_PIPELINE.md](READ_PIPELINE.md).

## Contract

`dev.hardwood.InputFile` is the only way a reader reaches storage. It is public so that users can implement it for their own backends, and it is `Closeable`. The read path holds no `Path`, channel or mapping of its own; every byte it decodes comes from `readRange`.

| Method | Contract |
|---|---|
| `open()` | Acquires the backend's resources (mapping, channel, connection, temp file). Must complete before `readRange` or `length`. The built-in backends make it idempotent. |
| `readRange(long offset, int length)` | Returns the bytes `[offset, offset + length)` of the file. `offset` is absolute within the file. The returned buffer has position `0` at `offset` and exactly `length` bytes remaining. It may be a zero-copy view (a slice of a mapping or of a caller's buffer) or a freshly allocated buffer. |
| `length()` | The file's size in bytes. |
| `name()` | A human-readable identifier. It prefixes exception messages (`ExceptionContext.filePrefix`, which renders `[name] …`) and fills the `file` field of JFR events. |
| `close()` | Releases what `open()` acquired. |

The mapped and range-backed backends throw `IllegalStateException` for a `readRange` or `length` call before `open()`. Every built-in backend throws `IndexOutOfBoundsException` for a range outside `[0, length())`, through the shared check `ReadRanges.checkBounds`, with the message `[name] readRange(offset, length) out of bounds (n bytes)`. An I/O failure is an `IOException`; an implementation that reports a failed read as `UncheckedIOException` is still treated as a transport failure, not as a corrupt file (see [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md)).

A returned buffer belongs to the caller that asked for it, and the read pipeline treats it as immutable. The contract does not require it to be read-only: a mapped slice is, an in-memory slice inherits the mutability of the user's buffer, and a range-cache slice is a writable view of the cache.

`InputFile` carries no identity beyond `name()`. Nothing in core keys state on an `InputFile` across readers; the parsed-footer cache lives inside one reader (see [FILE_METADATA.md](FILE_METADATA.md)).

**Thread safety.** Once opened, an `InputFile` must accept concurrent `readRange` and `length` calls from any thread. A read calls it from the thread that opens the reader (the first footer) and the thread that builds a fast-`tail` reader (its planning reads), from every column's retriever thread, and from common-pool tasks (every later file's footer load, and the plan and chunk prefetches) at the same time ([READ_PIPELINE.md](READ_PIPELINE.md#thread-inventory)). `open()` and `close()` need not be thread-safe: the reader calls `open()` once per file on one thread, and the thread start or future hand-off that carries the opened file to the reading threads publishes the fields `open()` wrote. `MappedInputFile` relies on this; its fields are neither `volatile` nor guarded. `RangeBackedInputFile` synchronises `open`, `readRange` and `close`.

**Interruption.** A backend may use an interruptible channel. `FileChannel` is one: interrupting a thread inside a channel operation, or entering one with the interrupt flag set, closes the channel for every user of that `InputFile`. The reader therefore never interrupts a thread that can be inside `readRange`: `ColumnWorker.close()` interrupts only the drain, which does no I/O ([READ_PIPELINE.md](READ_PIPELINE.md#thread-inventory)), and the decode tasks a context's `shutdownNow()` interrupts read no file either. The retrievers and speculative tasks that do read are stopped and awaited, never interrupted ([Closing](#ownership-and-lifecycle)). A caller that cancels its own reads by interrupting the reading thread (`Future.cancel(true)`) closes the channel of a local file larger than 2 GB.

Tests: `ByteBufferInputFileTest`, `MappedInputFileLargeFileTest`, `FileNameInExceptionTest`, `ColumnReadersTransportFailureTest`. The interruption rule is untested.

## Ownership and lifecycle

Passing an `InputFile` to a reader transfers it: the reader opens it and the reader closes it. The caller creates it and does nothing else with it until the reader is closed. The `HardwoodContext` is owned separately; see [concurrency-model.md](../docs/content/concepts/concurrency-model.md).

| Entry point | Opens | Closes the files | Closes the context |
|---|---|---|---|
| `ParquetFileReader.open(InputFile)`, `openAll(List)` | reader | reader | reader (it created the context) |
| `ParquetFileReader.open(InputFile, HardwoodContext[, ReaderConfig])`, `openAll(List, HardwoodContext[, ReaderConfig])` | reader | reader | caller |
| `Hardwood.open(InputFile)`, `Hardwood.openAll(List)` | reader | reader | the `Hardwood` instance, if it created the context |

All of them end in `ParquetFileReader.openInternal`, and every reader it builds owns its files. Child readers (`RowReader`, `ColumnReader`, `ColumnReaders`) never close an `InputFile`; closing one leaves the files open for the next child reader of the same `ParquetFileReader`.

**Opening.** `openInternal` opens the first file on the calling thread and reads its footer, so an unreadable first file fails the `open` call. Later files are opened by the reader's `FileMetadataCache`, at most once per reader ([FILE_METADATA.md](FILE_METADATA.md#per-file-metadata)); when their failures surface is in [READ_PIPELINE.md](READ_PIPELINE.md#incremental-planning).

**Closing.** A failed open and `ParquetFileReader.close()` both end by closing every `InputFile` in the list, opened or not (`InputFileCloser`): a failure of one, checked or unchecked, does not stop the others, and the first is thrown with later ones suppressed. `close()` may therefore be called on a file that was never opened. Before `close()` closes the files, it closes the child readers still open and waits for the speculative tasks their reads started, so no read is in flight when a file closes and none starts afterwards; that order, and the lifecycle lock that fails a child build during or after close, are in [READ_PIPELINE.md](READ_PIPELINE.md#components-and-threading).

**Reuse.** An `InputFile` belongs to one open reader at a time; the reader that closes it releases resources another reader would still be reading from. After that reader is closed, the built-in backends accept `open()` again, and a later reader may be given the same instance.

Tests: `MultiFileRowReaderTest`, `FileMetadataCacheTest`, `ParentCloseTest`, `ParquetFileReaderCloseTest`, `ParquetFileReaderOpenFailureTest`.

## Local files: memory mapping

`InputFile.of(Path)` returns a `MappedInputFile` (`dev.hardwood.internal.reader`). The reader targets Java 21, so the backend maps through `FileChannel.map` into `MappedByteBuffer`s and does not use the FFM API. `open()` reads the file's size and picks one of two modes. A `MappedByteBuffer` is addressed with `int` indices and `FileChannel.map` takes an `int`-bounded size but a `long` position, and the two modes follow from that.

| | File ≤ `Integer.MAX_VALUE` bytes | File > `Integer.MAX_VALUE` bytes |
|---|---|---|
| `open()` | Maps the whole file read-only once, then closes the channel | Keeps the `FileChannel` open; maps nothing |
| `readRange` | A zero-copy slice of the whole-file mapping | Maps exactly `[offset, offset + length)` read-only and returns that mapping |
| `close()` | Nothing to release; the mapping is released by the GC | Closes the channel; mappings already returned stay valid until the GC releases them |
| Interruptible | No: reads touch mapped memory only | Yes: every read is a `FileChannel.map` call |

In per-region mode there are no fixed regions. Each read is its own mapping at a `long` position, so a range that crosses the 2 GB mark, or starts beyond it, is one mapping and is never assembled from pieces. `FileChannel.map` accepts a position that is not page-aligned. Many reads of one file each create a mapping; nothing caches them.

`readRange` checks the range against the file size before mapping, in a form that does not overflow when `offset + length` exceeds `Long.MAX_VALUE`. The check is load-bearing in per-region mode: `FileChannel.map` does not reject a region past end of file, and touching such a mapping raises `SIGBUS`. The whole-file slice narrows `offset` with `Math.toIntExact`, which the size check makes safe.

`name()` is the path's file name, not the full path. Each mapping emits a `FileMappingEvent` with the file, offset and size: once per file in whole-file mode, once per read in per-region mode.

Nothing unmaps explicitly. Mapped address space and, on Windows, the lock on the file persist after `close()` until the GC collects the buffers.

Tests: `MappedInputFileLargeFileTest`, `JfrEventTest`, `LargeFileReadTest` (parquet-testing-runner; opt-in via `hardwood.largeFileTests`).

## In-memory input

`InputFile.of(ByteBuffer)` returns a `ByteBufferInputFile`. The file is the buffer's remaining content, from its position to its limit, captured once as a zero-copy slice when the input file is created; the caller's buffer keeps its position and limit. `open()` and `close()` do nothing, and `readRange` works without `open()`. `length()` is that slice's size, and `readRange` returns a slice of it, writable if the user's buffer is. A range outside the file fails the shared bounds check ([Contract](#contract)). `name()` is `<memory>` for every instance, so exception messages cannot tell two in-memory files apart.

The file is limited to 2 GB because a `ByteBuffer`'s capacity is an `int`.

Tests: `ByteBufferInputFileTest`.

## Range-backed input

`RangeBackedInputFile` (`dev.hardwood.internal.reader`) is a decorator that caches the ranges it reads from a delegate `InputFile` in a sparse, memory-mapped temp file and serves reads as slices of that mapping. It knows nothing about the delegate's transport; `S3InputFile` is its only user, and its design is in [S3_STORAGE.md](S3_STORAGE.md#rangebackedinputfile).

## Size and offset limits

Offsets and file lengths are `long` throughout the contract; a single read's length is an `int`. A backend's file-size limit is therefore its own, and a single read is at most `Integer.MAX_VALUE` bytes on every backend.

| Backend | Largest file | Failure beyond it |
|---|---|---|
| `MappedInputFile` | Unbounded | none |
| `ByteBufferInputFile` | 2 GB, intrinsic to `ByteBuffer` | cannot be constructed |
| `RangeBackedInputFile` | 2 GB | `UnsupportedOperationException` from `open()` |

The single-read bound turns into limits on regions the read path addresses as one range:

| Region addressed with `int` offsets | Limit | Failure beyond it |
|---|---|---|
| A column chunk in the sequential fetch plan, read in pieces but addressed as one range | 2 GB compressed | `UnsupportedOperationException` from `SequentialFetchPlan.build` |
| A page | 2 GB (`compressed_page_size` is `i32` in the format) | cannot occur |
| A row group's coalesced page-index region | 2 GB | `UnsupportedOperationException` from `RowGroupIndexBuffers.fetch` |

For local files the effective limit is therefore per column chunk read without an OffsetIndex, not per file; a chunk with an OffsetIndex is read in page groups that each stay within the gap policy's span limit ([parquet-layout.md](../docs/content/concepts/parquet-layout.md#column-chunk)). Every site that narrows a `long` region size to the `int` `readRange` takes uses `Math.toIntExact`, so an oversized region fails before any read rather than wrapping. How the fetch plans split a column chunk into reads is in [FETCH_PLANNING.md](FETCH_PLANNING.md).

Tests: `MappedInputFileLargeFileTest`, `SequentialFetchPlanChunkSizeTest`. The page-index and range-cache limits are untested.

## Boundaries

- Mappings are released only by the GC, never at `close()` (#199).
- In-memory and range-backed files are limited to 2 GB per file (#501; the range-backed case is in [S3_STORAGE.md](S3_STORAGE.md#rangebackedinputfile)).
- `readRange` does not guarantee a read-only buffer; the in-memory backend can hand out writable views of the user's buffer (#503).
- A row group whose page-index region spans more than 2 GB cannot be read, although each index structure is within the format's `i32` bound (#1113).
