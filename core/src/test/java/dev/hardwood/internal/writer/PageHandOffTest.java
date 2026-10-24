/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.writer.ParquetWriteException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A [DataPageWriter] hands a page body off while a worker is idle in [PageCompression#help], and
/// raises a failure compressing it unchanged, whichever thread took the body.
class PageHandOffTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void reportsAHandedOffPageFailureUnchanged() throws Exception {
        ParquetWriteException failure = new ParquetWriteException("handed-off page failed");
        Compressor failing = new Compressor() {
            @Override
            public byte[] compress(byte[] data, int offset, int length) {
                throw failure;
            }

            @Override
            public String getName() {
                return "TEST";
            }
        };
        PageCompression handoff = new PageCompression();
        AtomicBoolean done = new AtomicBoolean();
        Thread worker = Thread.ofPlatform().start(() -> handoff.help(done::get));
        try {
            while (LockSupport.getBlocker(worker) != handoff) {
                Thread.sleep(1);
            }
            DataPageWriter pages = new DataPageWriter(new ChunkSink.Buffered(), CompressionCodec.GZIP,
                    failing, handoff, 0);
            ByteArrayBuilder body = new ByteArrayBuilder();
            body.writeBytes(new byte[]{ 1, 2, 3, 4 });

            // The worker may take the body and fail before the write returns, which then raises it
            assertThatThrownBy(() -> {
                pages.write(body, 4, Encoding.PLAIN, 0);
                pages.finish();
            }).isSameAs(failure);
        }
        finally {
            done.set(true);
            handoff.wakeAll();
            worker.join();
        }
    }
}
