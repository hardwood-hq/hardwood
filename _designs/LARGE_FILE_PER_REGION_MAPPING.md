# Per-region mapping for large local files

**Status: Implemented**

## Context

`MappedInputFile` memory-maps the entire file into a single `MappedByteBuffer` in `open()`. Because `MappedByteBuffer` addresses bytes with `int` offsets, this caps a local file at `Integer.MAX_VALUE` (2 GB). `MappedInputFile.open()` enforces the cap with an explicit `IOException` ("File too large … Maximum supported file size is 2 GB."), so oversized files fail early rather than overflowing.

The cap is whole-file, but the data structure that genuinely needs to fit in 2 GB is far smaller: an individual fetched region (a column chunk, a coalesced page group, or a row-group index region). A file with many normal-sized row groups should be readable even when its total size exceeds 2 GB.

The goal is to support local files larger than 2 GB by mapping **per region** instead of mapping the whole file at once, while:

1. Keeping the Java 21 baseline — no Foreign Memory API, no multi-release-JAR gating.
2. Preserving today's performance for the common (≤ 2 GB) case — a single whole-file mapping with zero-copy slices.
3. Preserving the fail-early guarantee: any single region that cannot be addressed with an `int` length throws, never overflows silently.

## Approach

`FileChannel.map(MapMode, long position, long size)` already accepts a `long` position on Java 21; only `size` is `int`-bounded. A region anywhere in a file of any size can therefore be mapped directly, as long as that region is itself ≤ 2 GB. `readRange(long offset, int length)` already passes the region's `length` as an `int`, and every caller derives that `int` via `Math.toIntExact` — so the per-region size bound is already established and already fails loud.

`MappedInputFile` becomes a hybrid keyed on file size:

- **File ≤ 2 GB** — map the whole file once in `open()` (today's behavior, unchanged). `readRange` returns zero-copy slices of that mapping.
- **File > 2 GB** — keep the `FileChannel` open and map each `readRange` on demand via `channel.map(READ_ONLY, offset, length)`. Each mapping covers exactly the requested region; nothing straddles a boundary, so no stitching or copying is needed. The returned `MappedByteBuffer` is itself the region (its position 0 corresponds to `offset`); the JDK handles page-alignment of the underlying mapping internally.

A non-page-aligned `position` is supported by `FileChannel.map`; the returned buffer is still a zero-copy view of the requested range.

## MappedInputFile implementation

```java
public class MappedInputFile implements InputFile {

    private final Path path;
    private final String name;

    // ≤ 2 GB: whole-file mapping, channel closed eagerly after open().
    private MappedByteBuffer wholeFile;

    // > 2 GB: channel kept open for lazy per-region mapping.
    private FileChannel channel;
    private long size;

    @Override
    public void open() throws IOException {
        if (wholeFile != null || channel != null) {
            return; // idempotent
        }
        FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
        boolean keepOpen = false;
        try {
            long fileSize = ch.size();
            if (fileSize > Integer.MAX_VALUE) {
                channel = ch;        // stays open for per-region mapping
                size = fileSize;
                keepOpen = true;
            } else {
                wholeFile = mapRegion(ch, 0, fileSize); // emits FileMappingEvent
            }
        } finally {
            if (!keepOpen) {
                ch.close();          // whole-file mode (or failure): channel not needed
            }
        }
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        if (wholeFile != null) {
            return wholeFile.slice(Math.toIntExact(offset), length);
        }
        if (channel == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        return mapRegion(channel, offset, length); // length is already int-bounded
    }

    @Override
    public long length() {
        if (wholeFile != null) {
            return wholeFile.capacity();
        }
        if (channel == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        return size;
    }

    @Override
    public void close() throws IOException {
        // Whole-file mode: channel already closed in open(); mapping released by GC.
        // Per-region mode: close the channel held open for lazy mapping.
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }
}
```

`mapRegion` centralizes `channel.map(READ_ONLY, offset, length)` plus the `FileMappingEvent` JFR event (`file`, `offset`, `size`). In whole-file mode it fires once for the whole file; in per-region mode it fires once per region, which gives finer-grained mapping telemetry for large files.

### Lifecycle

In per-region mode the `FileChannel` is held open for the lifetime of the `InputFile` and closed in `close()`. The per-region `MappedByteBuffer`s remain valid after the channel is closed and are released by GC, matching the whole-file mode's reliance on GC for unmapping. `InputFile.close()` is already called by the framework (reader for single-file, `FileManager` for multi-file), so no caller changes are required.

### Concurrency

`InputFile` must be safe for concurrent use once opened. In whole-file mode, concurrent `slice` calls on a shared mapping are already safe. In per-region mode, each `readRange` calls `channel.map` independently; `FileChannel` is thread-safe for `map`, and no mutable state is shared across calls. After `open()` completes, the `wholeFile`/`channel`/`size` fields are only read, never written.

## Limits and fail-early guarantees

The 2 GB whole-file guard in `open()` is removed. The binding limit moves from the file as a whole to the individual fetched region, and is already enforced fail-loud:

- `readRange` takes an `int length`. Every caller computes it via `Math.toIntExact` — `RowGroupIndexBuffers` (index region), `SharedRegion` / `ChunkHandle` (coalesced column data), and the chunk-size logic in `SequentialFetchPlan`. A region whose length would exceed `Integer.MAX_VALUE` throws `ArithmeticException` at that computation, before any mapping is attempted.

The binding limit is **per column chunk**: a local file may be arbitrarily large, and a row group as a whole may exceed 2 GB, but each individual column chunk must be ≤ 2 GB (compressed). This is exactly the granularity the read path enforces:

- `SequentialFetchPlan.build` narrows the chunk's `totalCompressedSize` with `Math.toIntExact(columnChunk.metaData().totalCompressedSize())`, then splits it into bounded sub-reads. A column chunk up to `Integer.MAX_VALUE` bytes is read normally; one beyond that throws `ArithmeticException`.
- `IndexedFetchPlan` reads page group by page group, and a page's `compressedPageSize` is an `int32` in the Parquet format — so a 2 GB column chunk is read as many sub-2 GB ranges regardless of the fetch plan.

Row-group index regions are also read as single ranges and so must be ≤ 2 GB, but that is metadata and effectively never a concern. The user-facing limit should be stated as the real one: **column chunks up to 2 GB for local files**, not a per-file limit.

The two narrowing casts that previously relied on the whole-file guard for safety are hardened to `Math.toIntExact` (`MappedInputFile.readRange` whole-file slice, `ByteBufferInputFile.readRange`). They cannot overflow given their invariants, but `Math.toIntExact` enforces the invariant at the cast site rather than depending on a guard elsewhere, per the project's downcast rule.

## Scope boundaries

These backends keep a 2 GB-per-file limit and are unchanged:

- **`ByteBufferInputFile`** — a single `ByteBuffer`'s capacity is inherently `int`-bounded, so in-memory data cannot exceed 2 GB. The limit is intrinsic to the in-memory representation.
- **`RangeBackedInputFile` / `S3InputFile`** — the range-backing temp file is mapped whole (`READ_WRITE`) and the gap-fill logic is `int`-positioned throughout. Object-store reads keep the 2 GB-per-file limit, with the existing early `IOException` from `RangeBackedInputFile`.

Lifting the object-store path to per-region backing is out of scope and tracked separately.

## Public API and documentation impact

No public API signatures change — `InputFile.readRange`/`length` are already `long`-typed, and the `InputFile` contract already permits returned buffers to be independent mappings rather than slices of one mapping.

Documentation that states the limit must change to describe the new end state, and should spell out the real (per-column-chunk) limit rather than understate it:

- `core/src/main/java/dev/hardwood/reader/ParquetFileReader.java` — the "**Limitation:**" javadoc: local (memory-mapped) files may be arbitrarily large; each column chunk must be at most 2 GB (compressed). The per-file 2 GB limit remains only for in-memory `ByteBuffer` and object-store sources.
- `docs/content/index.md` — replace the "2 GB single-file limit" warning: for local files, individual column chunks (not files) are limited to 2 GB; in-memory `ByteBuffer` and object-store sources keep the 2 GB-per-file limit.

## File change summary

### Modified

| File | Change |
|------|--------|
| `core/src/main/java/dev/hardwood/internal/reader/MappedInputFile.java` | Hybrid whole-file / per-region mapping in a single class; remove 2 GB guard; keep channel open in per-region mode; close it in `close()` |
| `core/src/main/java/dev/hardwood/reader/ParquetFileReader.java` | Update the limitation javadoc |
| `docs/content/index.md` | Update the 2 GB limit warning |

### New tests

| File | Change |
|------|--------|
| `core/src/test/java/dev/hardwood/MappedInputFileLargeFileTest.java` | Sparse-file mapping primitive test (see Verification) |
| `parquet-testing-runner/src/test/java/dev/hardwood/testing/LargeFileReadTest.java` | Opt-in end-to-end >2 GB read |

### Hardened (already applied)

| File | Change |
|------|--------|
| `core/src/main/java/dev/hardwood/internal/reader/MappedInputFile.java` | Whole-file slice cast `(int) offset` → `Math.toIntExact(offset)` |
| `core/src/main/java/dev/hardwood/internal/reader/ByteBufferInputFile.java` | `(int) offset` → `Math.toIntExact(offset)` |

## Verification

Two tiers of tests, matching the two distinct risks.

### Mapping primitive (core, runs in CI)

`MappedInputFileLargeFileTest` exercises the long-offset per-region branch directly, without Parquet:

- Build a temp file just over 2 GB by seeking past the 2 GB mark and writing known sentinel bytes (a sparse hole precedes the sentinel, so the file costs kilobytes and milliseconds on a sparse-capable filesystem — Linux ext4/xfs/tmpfs, the CI environment). `@TempDir` deletes it afterwards.
- Write sentinels at offset 0, straddling the 2 GB boundary, and well beyond it.
- Assert `open()` no longer rejects the file, `length()` returns the full size, and `readRange` returns the correct bytes at each offset — including offsets and ranges that cross or exceed `Integer.MAX_VALUE`.

This is the high-value test: long-offset slicing is the only genuinely new behavior, and this proves it deterministically and cheaply.

### End-to-end (parquet-testing-runner, opt-in)

`LargeFileReadTest` proves a real >2 GB Parquet file reads correctly through the full stack. It is gated with `@Tag("large")` and `@EnabledIfSystemProperty(named = "hardwood.largeFileTests", matches = "true")`, so it is skipped by default and runs only when explicitly enabled (multi-GB generation is slow). It lives in `parquet-testing-runner`, the only module with both parquet-java's writer (`ExampleParquetWriter`) and `hardwood-core` on the test classpath.

- Write a file just over 2 GB with `ExampleParquetWriter` into `@TempDir` (single `int64` column, sequential ids, loop until `writer.getDataSize()` exceeds the threshold).
- Open it with Hardwood — this alone reads the footer at an offset beyond 2 GB.
- Read the last rows via `buildRowReader().tail(n)`, which fetches the final row group's column chunk (located past the 2 GB mark) and validates the ids — targeted end-to-end coverage of the per-region path without iterating the whole file.

### Build

`./mvnw verify -pl core` — existing mmap behavior for ≤ 2 GB files is unchanged; the new primitive test passes.
