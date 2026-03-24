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

/// [InputFile] backed by a memory-mapped file.
///
/// Starts in an unopened state. [#open()] memory-maps the entire file;
/// [#readRange] then returns zero-copy slices of the mapping.
public class MappedInputFile implements InputFile {

    private final Path path;
    private final String name;
    private MappedByteBuffer mapping;

    public MappedInputFile(Path path) {
        this.path = path;
        this.name = path.getFileName().toString();
    }

    @Override
    public void open() throws IOException {
        if (mapping != null) {
            return;
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize > Integer.MAX_VALUE) {
                throw new IOException("File too large: " + path + " (" + (fileSize / (1024 * 1024)) +
                        " MB). Maximum supported file size is 2 GB.");
            }

            FileMappingEvent event = new FileMappingEvent();
            event.begin();

            mapping = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize);

            event.file = name;
            event.offset = 0;
            event.size = fileSize;
            event.commit();
        }
    }

    @Override
    public ByteBuffer readRange(long offset, int length) {
        if (mapping == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        return mapping.slice((int) offset, length);
    }

    @Override
    public long length() {
        if (mapping == null) {
            throw new IllegalStateException("File not opened: " + name);
        }
        return mapping.capacity();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void close() {
        // The file channel is closed eagerly in open() after mapping.
        // The MappedByteBuffer remains valid and is released by GC.
    }
}
