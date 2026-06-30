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
/// truncated file presented as a valid Parquet file at the target path.
public final class ChannelOutputFile implements OutputFile {

    private final Path target;
    private final Path tempPath;
    private FileChannel channel;
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
        position = 0;
    }

    @Override
    public void write(ByteBuffer data) throws IOException {
        requireCreated();
        while (data.hasRemaining()) {
            position += channel.write(data);
        }
    }

    @Override
    public long position() {
        requireCreated();
        return position;
    }

    @Override
    public void close() throws IOException {
        if (channel == null) {
            return;
        }
        channel.close();
        channel = null;
        Files.move(tempPath, target,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void discard() throws IOException {
        if (channel == null) {
            return;
        }
        channel.close();
        channel = null;
        // Drop the partially written temp file; nothing is published at the target.
        Files.deleteIfExists(tempPath);
    }

    private void requireCreated() {
        if (channel == null) {
            throw new IllegalStateException("OutputFile not created: " + target);
        }
    }
}
