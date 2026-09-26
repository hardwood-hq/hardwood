/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import dev.hardwood.OutputFile;

/// [OutputFile] backed by a local file channel.
///
/// Bytes are streamed to a temporary sibling file and atomically renamed onto the
/// target path on [#close()], so a failed or abandoned write never leaves a
/// truncated file presented as a valid Parquet file at the target path. A `close()`
/// that fails before the rename completes deletes the temporary file.
///
/// Writes are coalesced through an in-memory buffer and flushed to the channel in
/// bulk, so the many small `write` calls a Parquet footer produces do not each incur a
/// channel write. A write at least as large as the buffer bypasses it and goes straight
/// to the channel, so a whole column chunk is not chopped into buffer-sized pieces.
public final class ChannelOutputFile implements OutputFile {

    /// Size of the coalescing buffer. Small enough to stay off the heap's radar, large
    /// enough that a footer's handful of small writes flush as one channel write.
    private static final int BUFFER_SIZE = 64 * 1024;

    private final Path target;
    private final Path tempPath;
    private FileChannel channel;
    private ByteBuffer buffer;
    private long position;

    public ChannelOutputFile(Path target) {
        this.target = target;
        this.tempPath = target.resolveSibling(target.getFileName() + ".hardwood-tmp");
    }

    @Override
    public void create() throws IOException {
        if (channel != null) {
            throw new IllegalStateException("OutputFile already created: " + target);
        }
        channel = FileChannel.open(tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        buffer = ByteBuffer.allocate(BUFFER_SIZE);
        position = 0;
    }

    @Override
    public void write(ByteBuffer data) throws IOException {
        requireCreated();
        int accepted = data.remaining();
        if (data.remaining() >= buffer.capacity()) {
            // Too big to be worth buffering: flush what's pending to keep byte order,
            // then hand the data straight to the channel.
            flushBuffer();
            writeFully(data);
        }
        else {
            if (data.remaining() > buffer.remaining()) {
                flushBuffer();
            }
            buffer.put(data);
        }
        position += accepted;
    }

    @Override
    public long position() {
        requireCreated();
        return position;
    }

    /// Flushes the buffer and atomically renames the temporary file onto the target.
    /// When the flush or the rename fails, the channel is released and the temporary
    /// file deleted before the failure is rethrown, so nothing is left behind at either
    /// path; a failure to delete is attached to it as suppressed.
    @Override
    public void close() throws IOException {
        if (channel == null) {
            return;
        }
        try {
            flushBuffer();
            releaseChannel();
            Files.move(tempPath, target,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        }
        catch (IOException | RuntimeException e) {
            deleteAfterFailure(e);
            throw e;
        }
    }

    @Override
    public void discard() throws IOException {
        if (channel == null) {
            return;
        }
        // Drop the buffered bytes unwritten; the temp file is thrown away anyway.
        releaseChannel();
        Files.deleteIfExists(tempPath);
    }

    private void releaseChannel() throws IOException {
        FileChannel open = channel;
        channel = null;
        buffer = null;
        open.close();
    }

    private void deleteAfterFailure(Exception failure) {
        if (channel != null) {
            try {
                releaseChannel();
            }
            catch (IOException e) {
                failure.addSuppressed(e);
            }
        }
        try {
            Files.deleteIfExists(tempPath);
        }
        catch (IOException e) {
            failure.addSuppressed(e);
        }
    }

    private void flushBuffer() throws IOException {
        if (buffer.position() == 0) {
            return;
        }
        buffer.flip();
        writeFully(buffer);
        buffer.clear();
    }

    private void writeFully(ByteBuffer data) throws IOException {
        while (data.hasRemaining()) {
            channel.write(data);
        }
    }

    private void requireCreated() {
        if (channel == null) {
            throw new IllegalStateException("OutputFile not created: " + target);
        }
    }
}
