/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

import java.io.ByteArrayOutputStream;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/// parquet-java [OutputFile] backed by a growable in-memory buffer, the counterpart of
/// Hardwood's `InMemoryOutputFile`.
///
/// Write benchmarks measure encode throughput, so both sides write to memory: filesystem
/// noise would otherwise swamp the differences being measured. parquet-java ships no
/// in-memory sink of its own, so [FlatWriteBenchmark] brings this one.
final class MemoryOutputFile implements OutputFile {

    private final ByteArrayOutputStream sink = new ByteArrayOutputStream();

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return createOrOverwrite(blockSizeHint);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        sink.reset();
        return new PositionOutputStream() {

            @Override
            public long getPos() {
                return sink.size();
            }

            @Override
            public void write(int b) {
                sink.write(b);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) {
                sink.write(bytes, offset, length);
            }
        };
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    /// The number of bytes written so far.
    long size() {
        return sink.size();
    }

    /// A copy of the bytes written so far.
    byte[] toByteArray() {
        return sink.toByteArray();
    }
}
