/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// A read gives back the buffers of a row group's data requests ([InputFile#release]) once nothing
/// reads them, and reads none of them after. Each buffer here is a copy that its release overwrites,
/// so a read of a buffer given back reads other bytes than the file's.
class BufferReleaseTest {

    private static final Path RESOURCES = Path.of("src/test/resources");
    /// Four row groups of 50 rows.
    private static final Path MULTI_ROW_GROUP = RESOURCES.resolve("differential/diff_order_multi.parquet");
    private static final int THREADS = 4;

    private String tailBytes;

    /// A tail too short to hold a file's data, so that the data are requests of their own.
    @BeforeEach
    void shortTail() {
        tailBytes = System.getProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, "64");
    }

    @AfterEach
    void restoreTail() {
        if (tailBytes == null) {
            System.clearProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        }
        else {
            System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, tailBytes);
        }
    }

    static Stream<String> fixtures() throws IOException {
        try (Stream<Path> files = Files.list(RESOURCES)) {
            return files.map(f -> f.getFileName().toString())
                    .filter(name -> name.endsWith(".parquet"))
                    .sorted()
                    .toList()
                    .stream();
        }
    }

    /// Every fixture reads the same rows through a file that frees each buffer it gives back, and is
    /// given back only buffers it returned, each once, before it closes.
    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtures")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadReadsNoBufferItGaveBack(String fixture) throws Exception {
        Path path = RESOURCES.resolve(fixture);
        List<String> expected;
        try {
            expected = rows(InputFile.of(path), 0);
        }
        catch (Exception e) {
            assumeTrue(false, "The fixture does not read: " + e);
            return;
        }
        for (long head : new long[]{ 0, 7 }) {
            ReleasingInputFile file = new ReleasingInputFile(InputFile.of(path));
            assertThat(rows(file, head)).as("head %d", head)
                    .isEqualTo(head == 0 ? expected : expected.subList(0, (int) Math.min(head, expected.size())));
            assertThat(file.misuse()).as("head %d", head).isEmpty();
        }
    }

    /// The buffers of the row groups the read has passed are given back while it goes on.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void buffersOfPassedRowGroupsAreGivenBackBeforeTheReadEnds() throws Exception {
        ReleasingInputFile file = new ReleasingInputFile(InputFile.of(MULTI_ROW_GROUP));
        int releasedInRead;
        try (HardwoodContext context = HardwoodContext.create(THREADS);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             RowReader rows = reader.rowReader()) {
            // into the fourth row group, past the first
            for (int i = 0; i < 160; i++) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
            }
            releasedInRead = file.released();
        }
        assertThat(releasedInRead).isPositive();
        assertThat(file.misuse()).isEmpty();
    }

    /// The rows of `file`, each field rendered as text, at most `head` unless it is `0`.
    private static List<String> rows(InputFile file, long head) throws Exception {
        List<String> rows = new ArrayList<>();
        try (HardwoodContext context = HardwoodContext.create(THREADS);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             RowReader rowReader = head > 0 ? reader.buildRowReader().head(head).build() : reader.rowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                StringBuilder row = new StringBuilder();
                for (int f = 0; f < rowReader.getFieldCount(); f++) {
                    Object value = rowReader.getValue(f);
                    row.append(value instanceof byte[] bytes ? Arrays.toString(bytes) : String.valueOf(value))
                            .append('|');
                }
                rows.add(row.toString());
            }
        }
        return rows;
    }

    /// A file whose every read is a direct copy of its own, overwritten as the read gives it back,
    /// which records what is given back amiss.
    private static final class ReleasingInputFile implements InputFile {

        /// What a buffer given back holds: no valid page header or value run starts with it.
        private static final byte POISON = (byte) 0xA5;

        private final InputFile delegate;
        private final Map<ByteBuffer, Boolean> live = new IdentityHashMap<>();
        private final List<String> misuse = new CopyOnWriteArrayList<>();
        private final AtomicInteger released = new AtomicInteger();
        private boolean closed;

        ReleasingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        List<String> misuse() {
            return misuse;
        }

        int released() {
            return released.get();
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            ByteBuffer source = delegate.readRange(offset, length);
            ByteBuffer copy = ByteBuffer.allocateDirect(length);
            copy.put(source.duplicate()).clear();
            synchronized (live) {
                live.put(copy, Boolean.TRUE);
            }
            return copy;
        }

        @Override
        public void release(ByteBuffer buffer) {
            synchronized (live) {
                if (closed) {
                    misuse.add("released after close");
                }
                if (live.remove(buffer) == null) {
                    misuse.add("released a buffer not live");
                    return;
                }
            }
            for (int i = 0; i < buffer.capacity(); i++) {
                buffer.put(i, POISON);
            }
            released.incrementAndGet();
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
        public Optional<String> identity() throws IOException {
            return delegate.identity();
        }

        @Override
        public void close() throws IOException {
            synchronized (live) {
                closed = true;
                live.clear();
            }
            delegate.close();
        }
    }
}
