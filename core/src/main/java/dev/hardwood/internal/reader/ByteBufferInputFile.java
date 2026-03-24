/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;

import dev.hardwood.InputFile;

/// [InputFile] backed by an in-memory [ByteBuffer].
///
/// Since the data is already in memory, [#open()] is a no-op.
/// [#readRange] returns slices of the backing buffer (zero-copy).
public class ByteBufferInputFile implements InputFile {

    private final ByteBuffer buffer;

    public ByteBufferInputFile(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public void open() {
        // Nothing to open for an in-memory buffer
    }

    @Override
    public ByteBuffer readRange(long offset, int length) {
        return buffer.slice((int) offset, length);
    }

    @Override
    public long length() {
        return buffer.capacity();
    }

    @Override
    public String name() {
        return "<memory>";
    }

    @Override
    public void close() {
        // Nothing to close for an in-memory buffer
    }
}
