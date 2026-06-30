/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import dev.hardwood.OutputFile;

/// [OutputFile] backed by a growable in-memory buffer.
///
/// The write-side counterpart to `ByteBufferInputFile`, used for round-trip tests
/// and for producing Parquet bytes without touching the filesystem. The accumulated
/// bytes are retrieved with [#toByteArray()] after [#close()].
public final class ByteBufferOutputFile implements OutputFile {

    private final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    private boolean created;
    private boolean closed;
    private boolean discarded;

    @Override
    public void create() {
        if (created) {
            throw new IllegalStateException("OutputFile already created");
        }
        created = true;
    }

    @Override
    public void write(ByteBuffer data) {
        requireCreated();
        int length = data.remaining();
        byte[] chunk = new byte[length];
        data.get(chunk);
        sink.writeBytes(chunk);
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

    /// Returns a copy of the bytes written so far.
    public byte[] toByteArray() {
        if (discarded) {
            throw new IllegalStateException("OutputFile was discarded");
        }
        if (!closed) {
            throw new IllegalStateException("OutputFile not closed");
        }
        return sink.toByteArray();
    }

    private void requireCreated() {
        if (!created) {
            throw new IllegalStateException("OutputFile not created");
        }
    }
}
