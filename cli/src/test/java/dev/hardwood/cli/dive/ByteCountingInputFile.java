/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.dive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import dev.hardwood.InputFile;

/// Tracks total bytes returned by [#readRange]. The Parquet footer
/// fetch and the data-page fetches both flow through it.
final class ByteCountingInputFile implements InputFile {

    private final InputFile delegate;
    private final AtomicLong bytesRead = new AtomicLong();

    ByteCountingInputFile(InputFile delegate) {
        this.delegate = delegate;
    }

    long bytesRead() {
        return bytesRead.get();
    }

    @Override
    public void open() throws IOException {
        delegate.open();
    }

    @Override
    public ByteBuffer readRange(long offset, int length) throws IOException {
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
