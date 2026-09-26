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
/// The file is the buffer's remaining content, from its position to its limit,
/// captured as a slice at construction; the caller's buffer is not modified.
/// Since the data is already in memory, [#open()] is a no-op.
/// [#readRange] returns slices of that content (zero-copy).
public class ByteBufferInputFile implements InputFile {

    private final ByteBuffer buffer;

    public ByteBufferInputFile(ByteBuffer buffer) {
        this.buffer = buffer.slice();
    }

    @Override
    public void open() {
        // Nothing to open for an in-memory buffer
    }

    @Override
    public ByteBuffer readRange(long offset, int length) {
        return buffer.slice(Math.toIntExact(offset), length);
    }

    @Override
    public long length() {
        return buffer.limit();
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
