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

import dev.hardwood.AbstractOutputFile;
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
public final class ChannelOutputFile extends AbstractOutputFile {

    /// Size of the coalescing buffer. Small enough to stay off the heap's radar, large
    /// enough that a footer's handful of small writes flush as one channel write.
    private static final int BUFFER_SIZE = 64 * 1024;

    private final Path target;
    private final Path tempPath;
    private FileChannel channel;
    private ByteBuffer buffer;

    public ChannelOutputFile(Path target) {
        super(target.toString());
        this.target = target;
        this.tempPath = target.resolveSibling(target.getFileName() + ".hardwood-tmp");
    }

    @Override
    protected void doCreate() throws IOException {
        // Allocated first, so that a failure leaves no channel open.
        buffer = ByteBuffer.allocate(BUFFER_SIZE);
        channel = FileChannel.open(tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    @Override
    protected void doWrite(ByteBuffer data) throws IOException {
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
    }

    /// Flushes the buffer and atomically renames the temporary file onto the target. When the
    /// flush or the rename fails, [#doDiscard()] releases the channel and deletes the
    /// temporary file, so nothing is left behind at either path.
    @Override
    protected void doClose() throws IOException {
        flushBuffer();
        releaseChannel();
        Files.move(tempPath, target,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }

    /// Drops the buffered bytes unwritten, releases the channel if [#doClose()] has not, and
    /// deletes the temporary file, also when the channel fails to close: the output is already
    /// discarded, so nothing retries the deletion.
    @Override
    protected void doDiscard() throws IOException {
        try {
            releaseChannel();
        }
        catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(tempPath);
            }
            catch (IOException deleteFailure) {
                e.addSuppressed(deleteFailure);
            }
            throw e;
        }
        Files.deleteIfExists(tempPath);
    }

    private void releaseChannel() throws IOException {
        FileChannel open = channel;
        channel = null;
        buffer = null;
        if (open != null) {
            open.close();
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
}
