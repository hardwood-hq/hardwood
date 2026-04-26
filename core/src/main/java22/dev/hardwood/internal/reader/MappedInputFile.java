/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import dev.hardwood.InputFile;
import dev.hardwood.jfr.FileMappingEvent;

/// [InputFile] backed by a memory-mapped file — Java 22+ implementation.
///
/// Uses [MemorySegment] (Foreign Memory API) instead of [java.nio.MappedByteBuffer],
/// which supports `long` offsets throughout and removes the 2 GB per-segment limit.
/// The entire file is mapped as a single `MemorySegment`; [#readRange] returns
/// zero-copy `ByteBuffer` slices with no segment-boundary logic.
///
/// This class is the `META-INF/versions/22/` override in the multi-release JAR.
/// On Java 21 the base implementation in `dev.hardwood.internal.reader` is used
/// instead, which falls back to multiple `MappedByteBuffer` segments.
///
/// **Arena choice.** [Arena#ofAuto()] is used because:
/// - The `MemorySegment` must be accessible from multiple virtual threads
///   (retriever VThread, drain VThread, and decode tasks) without [WrongThreadException].
///   `ofAuto()` never restricts thread access — `isAccessibleBy(thread)` always
///   returns `true`.
/// - `ofConfined()` restricts access to the creating thread → `WrongThreadException`.
/// - `ofShared()` enforces scope-checking on every `ByteBuffer` access on JDK 22+,
///   which also produces `WrongThreadException` when the session is closed while
///   a slice is in flight.
/// - `ofAuto()` releases the mapping via GC — the same behaviour as the legacy
///   `MappedByteBuffer` returned by the no-Arena `FileChannel.map` overload.
///
/// **Lifecycle.** The `FileChannel` is opened and closed eagerly inside [#open()];
/// the `MemorySegment` mapping survives channel close because `Arena.ofAuto()` does
/// not require an open channel (identical contract to `MappedByteBuffer`). As a
/// result:
/// - [#close()] is a **no-op** — there are no resources to release explicitly.
/// - The only mutable fields after construction are `path`, `name`, and `mapping`.
///
/// **Thread safety.** [#open()] is `synchronized` to prevent concurrent double-open.
/// [#readRange], [#readRangeAsSegment], [#length], and [#close()] operate on
/// immutable state after `open()` completes and are safe for concurrent use.
public class MappedInputFile implements InputFile {

    private final Path path;
    private final String name;

    /// Null until [#open()] completes. Written once under `synchronized`; subsequent
    /// reads from any thread are safe because the field is published via the
    /// `synchronized open()` happens-before edge.
    private MemorySegment mapping;

    public MappedInputFile(Path path) {
        this.path = path;
        this.name = path.getFileName().toString();
    }

    @Override
    public synchronized void open() throws IOException {
        if (mapping != null) {
            return;
        }

        // E6: Use try-with-resources — the channel is closed immediately after
        // mapping is established. Arena.ofAuto() does not keep a reference to the
        // channel; the mapping survives channel close (same contract as MappedByteBuffer).
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();

            // E1: Reject empty files with a clear error rather than an opaque
            // IllegalArgumentException from FileChannel.map(READ_ONLY, 0, 0, arena).
            if (fileSize == 0) {
                throw new IOException("File is empty: " + name);
            }

            FileMappingEvent event = new FileMappingEvent();
            event.begin();

            // Arena.ofAuto(): the segment's scope is always accessible from any thread
            // (isAccessibleBy returns true unconditionally). This is essential because
            // the ColumnWorker's retriever VThread, drain VThread, and decode tasks all
            // call readRange() concurrently after open() completes.
            mapping = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.ofAuto());

            event.file = name;
            event.offset = 0;
            event.size = fileSize;
            event.commit();
        }
        // channel is closed here by try-with-resources; mapping remains valid.
    }

    /// Returns a zero-copy, read-only [ByteBuffer] slice of the mapped region.
    ///
    /// `MemorySegment.asSlice` uses `long` offsets — no `int` overflow regardless
    /// of file size. The returned buffer is **read-only** and is safe to use from
    /// any thread (the [Arena#ofAuto()] session is always accessible).
    @Override
    public ByteBuffer readRange(long offset, int length) {
        MemorySegment m = mapping;
        if (m == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        // E4: Descriptive bounds check — MemorySegment.asSlice() would throw an
        // opaque IOOBE without file name or size context, which makes Parquet footer
        // corruption bugs very difficult to diagnose.
        if (offset < 0 || length < 0 || offset + length > m.byteSize()) {
            throw new IndexOutOfBoundsException(
                    "readRange(" + offset + ", " + length + ") out of bounds for file "
                    + name + " (" + m.byteSize() + " bytes)");
        }
        // E2: asReadOnlyBuffer() enforces the read-only contract explicitly.
        // asByteBuffer() on a READ_ONLY-mapped segment is read-only on current JDKs,
        // but this is an implementation detail — the wrapper makes the contract
        // explicit and guards against future JDK changes.
        return m.asSlice(offset, length).asByteBuffer().asReadOnlyBuffer();
    }

    /// Returns a zero-copy, read-only [MemorySegment] slice of the mapped region,
    /// bypassing the [ByteBuffer] wrapper entirely.
    ///
    /// This method is **not** part of the [dev.hardwood.InputFile] interface (which
    /// is compiled with `--release 21`, where `java.lang.foreign.MemorySegment` is
    /// not yet a standard API). Callers that know they are running on Java 22+ can
    /// cast to `MappedInputFile` to access this method for FFM-native decompression
    /// pipelines (e.g. passing directly to a `LibdeflateDecompressor` that already
    /// uses FFM).
    ///
    /// @param offset absolute byte offset within the file
    /// @param length number of bytes to include in the slice
    /// @return a **read-only** `MemorySegment` slice — pure pointer arithmetic, zero allocation
    public MemorySegment readRangeAsSegment(long offset, long length) {
        MemorySegment m = mapping;
        if (m == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        // E3: asReadOnly() makes the read-only guarantee explicit and prevents
        // accidental writes if the map mode ever changes in a future refactor.
        return m.asSlice(offset, length).asReadOnly();
    }

    /// Returns the total size of the file in bytes.
    ///
    /// Derived from [MemorySegment#byteSize()] — no separate `fileSize` field is
    /// needed (E8), which also eliminates the subtle race window where `fileSize`
    /// is written before `mapping` is published.
    @Override
    public long length() {
        MemorySegment m = mapping;
        if (m == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        return m.byteSize();
    }

    @Override
    public String name() {
        return name;
    }

    /// No-op: the [FileChannel] was closed eagerly inside [#open()] via
    /// try-with-resources. The [MemorySegment] is backed by [Arena#ofAuto()] and
    /// will be released by the GC — this is the same contract as [java.nio.MappedByteBuffer].
    ///
    /// `readRange` and `readRangeAsSegment` remain valid after `close()` is called,
    /// because the mapping is not backed by the channel after `open()` returns.
    @Override
    public void close() {
    }
}
