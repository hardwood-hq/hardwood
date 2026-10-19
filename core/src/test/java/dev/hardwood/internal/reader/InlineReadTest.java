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
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A read on a context that runs reads inline ([HardwoodContextImpl#inlineReads()]) does every
/// file access on the thread consuming it, starts no speculative work, and returns what the
/// threaded read returns. The values themselves are compared by the result-comparing suites,
/// which run with inline reads forced on in the `small-batches` profile.
@Timeout(30)
class InlineReadTest {

    private static final Path RESOURCES = Path.of("src/test/resources");

    @ParameterizedTest
    @ValueSource(strings = { "misaligned_pages.parquet", "misaligned_pages_dict_no_index.parquet",
            "nested_v1_no_index.parquet", "nested_v1_long_record_pages.parquet", "address_book_test.parquet" })
    void aRowReadTouchesTheFileOnTheConsumerThreadAlone(String fixture) throws Exception {
        long expected;
        try (HardwoodContextImpl threaded = HardwoodContextImpl.create(1, false)) {
            expected = countRows(List.of(new ThreadRecordingInputFile(RESOURCES.resolve(fixture))), threaded);
        }
        ThreadRecordingInputFile file = new ThreadRecordingInputFile(RESOURCES.resolve(fixture));
        try (HardwoodContextImpl inline = HardwoodContextImpl.create(1, true)) {
            assertThat(countRows(List.of(file), inline)).isEqualTo(expected);
        }
        assertThat(file.threads).containsExactly(Thread.currentThread());
    }

    @Test
    void aMultiFileReadPlansEveryFileOnTheConsumerThread() throws Exception {
        List<ThreadRecordingInputFile> files = List.of(
                new ThreadRecordingInputFile(RESOURCES.resolve("misaligned_pages.parquet")),
                new ThreadRecordingInputFile(RESOURCES.resolve("misaligned_pages.parquet")),
                new ThreadRecordingInputFile(RESOURCES.resolve("misaligned_pages.parquet")));
        long single;
        try (HardwoodContextImpl threaded = HardwoodContextImpl.create(1, false)) {
            single = countRows(List.of(new ThreadRecordingInputFile(RESOURCES.resolve("misaligned_pages.parquet"))),
                    threaded);
        }
        try (HardwoodContextImpl inline = HardwoodContextImpl.create(1, true)) {
            assertThat(countRows(files, inline)).isEqualTo(3 * single);
        }
        for (ThreadRecordingInputFile file : files) {
            assertThat(file.threads).containsExactly(Thread.currentThread());
        }
    }

    @Test
    void aColumnReadTouchesTheFileOnTheConsumerThreadAlone() throws Exception {
        ThreadRecordingInputFile file = new ThreadRecordingInputFile(RESOURCES.resolve("misaligned_pages.parquet"));
        long records = 0;
        try (HardwoodContextImpl inline = HardwoodContextImpl.create(1, true);
             ParquetFileReader reader = ParquetFileReader.open(file, inline);
             ColumnReaders columns = reader.columnReaders(ColumnProjection.all())) {
            while (columns.nextBatch()) {
                records += columns.getRecordCount();
            }
            assertThat(records).isEqualTo(reader.getFileMetaData().numRows());
        }
        assertThat(file.threads).containsExactly(Thread.currentThread());
    }

    @Test
    void aReadClosedPartWayReleasesEverything() throws Exception {
        ThreadRecordingInputFile file = new ThreadRecordingInputFile(RESOURCES.resolve("misaligned_pages.parquet"));
        try (HardwoodContextImpl inline = HardwoodContextImpl.create(1, true);
             ParquetFileReader reader = ParquetFileReader.open(file, inline)) {
            try (RowReader rows = reader.rowReader()) {
                assertThat(rows.hasNext()).isTrue();
                rows.next();
            }
        }
        assertThat(file.closed).isTrue();
    }

    @Test
    void aTransportFailureReachesTheConsumer() throws Exception {
        ThreadRecordingInputFile file = new ThreadRecordingInputFile(RESOURCES.resolve("plain_uncompressed.parquet"));
        try (HardwoodContextImpl inline = HardwoodContextImpl.create(1, true);
             ParquetFileReader reader = ParquetFileReader.open(file, inline)) {
            file.failing = true;
            assertThatThrownBy(() -> {
                try (RowReader rows = reader.rowReader()) {
                    while (rows.hasNext()) {
                        rows.next();
                    }
                }
            })
                    .isInstanceOf(IOException.class)
                    .hasMessage("[plain_uncompressed.parquet] Failed to fetch region at offset 4 (length 174)");
        }
    }

    private static long countRows(List<? extends InputFile> files, HardwoodContextImpl context) throws IOException {
        long rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.openAll(files, context);
             RowReader rowReader = reader.rowReader()) {
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        return rows;
    }

    /// Records every thread that reads it, and fails its reads once [#failing] is set.
    private static final class ThreadRecordingInputFile implements InputFile {

        private final InputFile delegate;
        final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        volatile boolean failing;
        volatile boolean closed;

        ThreadRecordingInputFile(Path path) {
            this.delegate = InputFile.of(path);
        }

        @Override
        public void open() throws IOException {
            threads.add(Thread.currentThread());
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            threads.add(Thread.currentThread());
            if (failing) {
                throw new IOException("simulated transport failure");
            }
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
            closed = true;
            delegate.close();
        }
    }
}
