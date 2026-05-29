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
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.jfr.FileMappingEvent;

/// [InputFile] backed by a memory-mapped file.
///
/// Starts in an unopened state. [#open()] inspects the file size and selects
/// one of two backing strategies:
///
/// - **Files up to 2 GB** are mapped in their entirety; [#readRange] returns
///   zero-copy slices of the single mapping.
/// - **Files larger than 2 GB** cannot be addressed by a single
///   [MappedByteBuffer] (its offsets are `int`). The [FileChannel] is kept open
///   and each [#readRange] maps just the requested region on demand. A single
///   requested region must still fit in an `int` length — in practice a column
///   chunk, page group, or row-group index region, each at most 2 GB — but the
///   file as a whole may be arbitrarily large.
///
/// **Interruption (larger-than-2 GB path only).** That path keeps the
/// [FileChannel] open across reads, and `FileChannel` is an `InterruptibleChannel`:
/// interrupting a thread while it is inside [#readRange] — or entering one with its
/// interrupt flag already set — closes the channel for every reader, after which
/// reads fail with `ClosedChannelException`. Callers that cancel reads by
/// interrupting the reading thread (for example `Future.cancel(true)`) must not do
/// so against a larger-than-2 GB file they intend to keep reading. The up-to-2 GB
/// path is unaffected: its channel is closed before any read, so reads are pure
/// mapped-memory slices with no channel operation to interrupt.
public class MappedInputFile implements InputFile {

    private final Path path;
    private final String name;

    /// Whole-file mode (file ≤ 2 GB): single mapping; the channel is closed eagerly in [#open()].
    private MappedByteBuffer wholeFile;

    /// Per-region mode (file > 2 GB): channel kept open for lazy per-region mapping.
    private FileChannel channel;
    private long size;

    public MappedInputFile(Path path) {
        this.path = path;
        this.name = path.getFileName().toString();
    }

    @Override
    public void open() throws IOException {
        if (wholeFile != null || channel != null) {
            return;
        }
        FileChannel ch = FileChannel.open(path, StandardOpenOption.READ);
        boolean keepOpen = false;
        try {
            long fileSize = ch.size();
            if (fileSize > Integer.MAX_VALUE) {
                channel = ch;
                size = fileSize;
                keepOpen = true;
            }
            else {
                wholeFile = map(ch, 0L, fileSize);
            }
        }
        finally {
            if (!keepOpen) {
                // Whole-file mode: the mapping survives channel close, released by GC.
                // Failure: release the channel we just opened.
                ch.close();
            }
        }
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        long fileBytes;
        if (wholeFile != null) {
            fileBytes = wholeFile.capacity();
        }
        else if (channel != null) {
            fileBytes = size;
        }
        else {
            throw new IllegalStateException("File not opened: " + name);
        }

        // Validate explicitly: FileChannel.map does not reject a region past EOF
        // (it would map beyond the file and SIGBUS on access), unlike ByteBuffer.slice.
        // The comparison is written to avoid overflow when offset + length wraps.
        if (offset < 0 || length < 0 || offset > fileBytes - length) {
            throw new IndexOutOfBoundsException(ExceptionContext.filePrefix(name)
                    + "readRange(" + offset + ", " + length
                    + ") out of bounds (" + fileBytes + " bytes)");
        }

        if (wholeFile != null) {
            return wholeFile.slice(Math.toIntExact(offset), length);
        }
        return map(channel, offset, length);
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
    public String name() {
        return name;
    }

    @Override
    public void close() throws IOException {
        // Whole-file mode: channel was closed eagerly in open(); the mapping is released by GC.
        // Per-region mode: close the channel held open for lazy mapping.
        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    /// Maps a single region of the file and emits a [FileMappingEvent]. The region
    /// length is bounded by the `int` argument from [#readRange]; the offset is a
    /// `long`, so regions beyond the 2 GB mark in a large file map correctly.
    private MappedByteBuffer map(FileChannel ch, long offset, long length) throws IOException {
        FileMappingEvent event = new FileMappingEvent();
        event.begin();

        MappedByteBuffer region = ch.map(FileChannel.MapMode.READ_ONLY, offset, length);

        event.file = name;
        event.offset = offset;
        event.size = length;
        event.commit();
        return region;
    }
}
