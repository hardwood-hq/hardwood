/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import dev.hardwood.InputFile;
import dev.hardwood.jfr.FileMappingEvent;

/// [InputFile] backed by a memory-mapped file — Java 21 fallback implementation.
///
/// Maps the entire file using one or more [MappedByteBuffer] segments of at most
/// `Integer.MAX_VALUE` bytes each, removing the previous 2 GB file-size limit.
/// [#readRange] returns zero-copy, **read-only** slices; a multi-segment boundary
/// case assembles a heap buffer (see Javadoc on [#readRange]).
///
/// On Java 22+ the JVM selects the `META-INF/versions/22/` override from the
/// multi-release JAR, which uses `MemorySegment` for long-offset mapping with no
/// segment-boundary logic and deterministic unmapping via `Arena.ofConfined()`.
///
/// **Thread safety.** [#open()] is `synchronized` to prevent a concurrent
/// double-open from mapping the file twice. [#readRange] is safe for concurrent
/// use because it only reads from immutable state (the segment array and each
/// segment's backing native memory) after `open()` completes.
public class MappedInputFile implements InputFile {

    /// Maximum size of a single `MappedByteBuffer` segment.
    ///
    /// Set to `Integer.MAX_VALUE` (2 GB − 1). Files larger than this value are
    /// split across multiple segments. On Java 22+ the MemorySegment override
    /// handles the file as a single mapping, so this constant is only relevant
    /// for this Java 21 fallback.
    private static final long SEGMENT_SIZE = Integer.MAX_VALUE;

    private final Path path;
    private final String name;

    /// Immutable after `open()` completes. Each element is a read-only view of the
    /// corresponding file segment. Declared volatile so the array reference is
    /// visible to threads that call `readRange` after `open()` on another thread.
    private volatile ByteBuffer[] segments;
    private long fileSize;

    public MappedInputFile(Path path) {
        this.path = path;
        this.name = path.getFileName().toString();
    }

    @Override
    public synchronized void open() throws IOException {
        if (segments != null) {
            return;
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            fileSize = channel.size();

            // Reject empty files with a clear error rather than a confusing
            // IllegalArgumentException from FileChannel.map(READ_ONLY, 0, 0).
            if (fileSize == 0) {
                throw new IOException("File is empty: " + name);
            }

            int numSegments = (int) Math.ceilDiv(fileSize, SEGMENT_SIZE);
            ByteBuffer[] mapped = new ByteBuffer[numSegments];

            for (int i = 0; i < numSegments; i++) {
                long segStart = (long) i * SEGMENT_SIZE;
                long segLen = Math.min(SEGMENT_SIZE, fileSize - segStart);

                // Emit one JFR event per segment so profiling tools can observe
                // per-segment mmap timing for multi-GB files.
                FileMappingEvent event = new FileMappingEvent();
                event.begin();

                // asReadOnlyBuffer() aligns the isReadOnly() contract with the Java 22
                // MemorySegment path (which always returns read-only ByteBuffers).
                // It also provides a defensive guard against accidental writes to a
                // shared backing buffer.
                MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, segStart, segLen);
                mapped[i] = mbb.asReadOnlyBuffer();

                event.file = name;
                event.offset = segStart;
                event.size = segLen;
                event.commit();
            }

            // Publish the array atomically. After this assignment, readRange()
            // on any thread sees a fully initialised array.
            this.segments = mapped;
        }
    }

    /// Returns a read-only [ByteBuffer] slice of the mapped region.
    ///
    /// The returned buffer is always **read-only** (consistent with the Java 22+
    /// `MemorySegment`-based implementation). Callers must not write into it.
    ///
    /// Fast path: when the entire range falls within one segment, a zero-copy slice
    /// is returned directly. Slow path: when the range spans a segment boundary
    /// (only possible when `offset` lies within `length` bytes of a 2 GB multiple),
    /// the bytes are assembled into a heap buffer. In practice, individual reads are
    /// at most 128 MB, so the slow path is never triggered for well-formed files.
    @Override
    public ByteBuffer readRange(long offset, int length) {
        ByteBuffer[] segs = segments;
        if (segs == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        int segIndex = (int) (offset / SEGMENT_SIZE);
        int relOffset = Math.toIntExact(offset - (long) segIndex * SEGMENT_SIZE);

        // Fast path: the entire range fits within one segment
        if ((long) relOffset + length <= segs[segIndex].capacity()) {
            return segs[segIndex].slice(relOffset, length);
        }

        // Slow path: the range spans a segment boundary — assemble into a heap buffer.
        ByteBuffer result = ByteBuffer.allocate(length);
        int remaining = length;
        long absPos = offset;
        while (remaining > 0) {
            int seg = (int) (absPos / SEGMENT_SIZE);
            int rel = Math.toIntExact(absPos - (long) seg * SEGMENT_SIZE);
            int available = segs[seg].capacity() - rel;
            int toRead = Math.min(available, remaining);
            result.put(segs[seg].slice(rel, toRead));
            absPos += toRead;
            remaining -= toRead;
        }
        result.flip();
        return result.asReadOnlyBuffer();
    }

    @Override
    public long length() {
        if (segments == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        return fileSize;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void close() {
        // The file channel is closed eagerly after mapping in open().
        // MappedByteBuffer regions are backed by OS page tables and are released
        // by GC (Java 21 provides no API for deterministic unmap). The Java 22+
        // override uses Arena.ofAuto(), which is also GC-released.
    }
}
