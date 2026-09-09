/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/// An [OutputFile] that keeps the file in memory and hands it back.
///
/// Use it to produce Parquet bytes without a filesystem: a file to upload to an object store,
/// a payload to send over a network, or one written and read back in the same process. It is
/// created with [OutputFile#inMemory()], and the buffer [#buffer()] returns can be passed
/// straight to [InputFile#of(ByteBuffer)]:
///
/// ```java
/// InMemoryOutputFile out = OutputFile.inMemory();
/// try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
///     // write the file
/// }
/// try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
///     // read it back
/// }
/// ```
///
/// The whole file is held on the heap, which costs the size of the finished file in addition
/// to the memory the writer uses for the row group it has open. A file can be at most
/// `Integer.MAX_VALUE - 8` bytes; a write past that fails with an `IOException`.
///
/// **This API is [Experimental]:** the shape may change in future releases.
@Experimental
public final class InMemoryOutputFile implements OutputFile {

    /// The largest array `ByteArrayOutputStream` can grow to, which bounds the file.
    static final int MAX_SIZE = Integer.MAX_VALUE - 8;

    /// The accumulated bytes. `ByteArrayOutputStream` only hands them out as a copy, which
    /// would copy the whole file for every caller of [#buffer()]; the subclass exposes a
    /// buffer over the array instead.
    private static final class Sink extends ByteArrayOutputStream {

        ByteBuffer view() {
            return ByteBuffer.wrap(buf, 0, count).slice();
        }
    }

    private final Sink sink = new Sink();
    private boolean created;
    private boolean closed;
    private boolean discarded;

    InMemoryOutputFile() {
    }

    @Override
    public void create() {
        if (created) {
            throw new IllegalStateException("OutputFile already created");
        }
        created = true;
    }

    @Override
    public void write(ByteBuffer data) throws IOException {
        requireWritable();
        int length = data.remaining();
        requireCapacity(sink.size(), length);
        if (data.hasArray()) {
            // Append the backing array directly: staging the payload in a fresh byte[] first
            // would double the copy on the way into the sink, which shows up as allocation in
            // every benchmark and test that writes through this file.
            sink.write(data.array(), data.arrayOffset() + data.position(), length);
            data.position(data.position() + length);
        }
        else {
            byte[] chunk = new byte[length];
            data.get(chunk);
            sink.writeBytes(chunk);
        }
    }

    @Override
    public long position() {
        requireCreated();
        return sink.size();
    }

    @Override
    public void close() {
        if (discarded) {
            return;
        }
        closed = true;
    }

    @Override
    public void discard() {
        discarded = true;
        sink.reset();
    }

    /// The file that was written.
    ///
    /// The returned buffer contains the whole file: its position is `0` and its limit is the
    /// number of bytes written, which is the range [InputFile#of(ByteBuffer)] reads, so it can
    /// be passed there unchanged. Each call returns a buffer with its own position and limit,
    /// so reading one buffer does not affect another. Do not modify the buffer's contents:
    /// whether a change shows in the buffers other calls return is unspecified.
    ///
    /// @return the bytes of the finished file
    /// @throws IllegalStateException if the file has not been closed, or was discarded
    public ByteBuffer buffer() {
        if (discarded) {
            throw new IllegalStateException("OutputFile was discarded");
        }
        if (!closed) {
            throw new IllegalStateException("OutputFile not closed");
        }
        return sink.view();
    }

    /// Throws when appending `length` bytes to a file of `size` bytes would pass [#MAX_SIZE].
    static void requireCapacity(int size, int length) throws IOException {
        if (length > MAX_SIZE - size) {
            throw new IOException("In-memory OutputFile cannot hold more than " + MAX_SIZE + " bytes; it holds "
                    + size + " and was given " + length + " more");
        }
    }

    private void requireWritable() {
        requireCreated();
        if (discarded) {
            throw new IllegalStateException("OutputFile was discarded");
        }
        if (closed) {
            throw new IllegalStateException("OutputFile already closed");
        }
    }

    private void requireCreated() {
        if (!created) {
            throw new IllegalStateException("OutputFile not created");
        }
    }
}
