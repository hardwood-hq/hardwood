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
import java.util.concurrent.atomic.AtomicInteger;

import dev.hardwood.InputFile;

/// An [InputFile] wrapper that delegates to another `InputFile` and
/// counts the number of [#readRange] calls. Useful in tests that need to
/// assert on I/O patterns (e.g. verifying coalesced reads).
class CountingInputFile implements InputFile {

    private final InputFile delegate;
    private final AtomicInteger readRangeCount = new AtomicInteger();

    CountingInputFile(InputFile delegate) {
        this.delegate = delegate;
    }

    /// Convenience constructor that wraps a [ByteBuffer] as the delegate.
    CountingInputFile(ByteBuffer buffer) {
        this(InputFile.of(buffer));
    }

    int readCount() {
        return readRangeCount.get();
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        readRangeCount.incrementAndGet();
        return delegate.readRange(offset, length);
    }

    @Override
    public long length() throws IOException {
        return delegate.length();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
