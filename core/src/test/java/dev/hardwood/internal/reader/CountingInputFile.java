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
import java.util.concurrent.atomic.AtomicLong;

import dev.hardwood.InputFile;

/// An [InputFile] wrapper that delegates to another `InputFile` and counts both the number of
/// [#readRange] calls and the total bytes read. Useful in tests that need to assert on I/O patterns
/// (e.g. verifying coalesced reads, or that a single read served a request).
public class CountingInputFile implements InputFile {

    private final InputFile delegate;
    private final AtomicInteger readRangeCount = new AtomicInteger();
    private final AtomicLong bytesRead = new AtomicLong();

    public CountingInputFile(InputFile delegate) {
        this.delegate = delegate;
    }

    /// Convenience constructor that wraps a [ByteBuffer] as the delegate.
    public CountingInputFile(ByteBuffer buffer) {
        this(InputFile.of(buffer));
    }

    public int readCount() {
        return readRangeCount.get();
    }

    public long bytesRead() {
        return bytesRead.get();
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
        readRangeCount.incrementAndGet();
        bytesRead.addAndGet(length);
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
