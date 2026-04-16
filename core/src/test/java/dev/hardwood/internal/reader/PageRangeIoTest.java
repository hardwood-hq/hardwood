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
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import dev.hardwood.Hardwood;
import dev.hardwood.InputFile;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.MultiFileColumnReaders;
import dev.hardwood.reader.MultiFileParquetReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;

import static org.assertj.core.api.Assertions.assertThat;

/// Integration tests verifying that page-range I/O reduces bytes read
/// when page-level Column Index filtering is active.
///
/// Uses column_index_pushdown.parquet: 10000 rows, sorted id [0,9999],
/// Parquet v2 with Column Index, ~10 pages of 1024 values each.
class PageRangeIoTest {

    private static final Path TEST_FILE = Path.of("src/test/resources/column_index_pushdown.parquet");

    // Filter matches only the first page (~1024 rows out of 10000)
    private static final FilterPredicate SELECTIVE_FILTER = FilterPredicate.lt("id", 1000L);

    // ColumnReader path (single-file, single-column)

    @Test
    void testColumnReaderPageRangeIoReducesBytes() throws Exception {
        long unfilteredBytes = readColumnReaderBytes(null);
        long filteredBytes = readColumnReaderBytes(SELECTIVE_FILTER);

        assertThat(filteredBytes)
                .as("Filtered ColumnReader should read fewer bytes than unfiltered")
                .isLessThan(unfilteredBytes);
    }

    private long readColumnReaderBytes(FilterPredicate filter) throws Exception {
        ByteCountingInputFile inputFile = new ByteCountingInputFile(InputFile.of(TEST_FILE));
        inputFile.open();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            ColumnReader col = (filter != null)
                    ? reader.createColumnReader("id", filter)
                    : reader.createColumnReader("id");
            try (col) {
                while (col.nextBatch()) {
                    col.getRecordCount(); // consume the batch
                }
            }
        }
        return inputFile.bytesRead();
    }

    // Single-file, multi-column path

    @Test
    void testRowReaderPageRangeIoReducesBytes() throws Exception {
        long unfilteredBytes = readRowReaderBytes(null);
        long filteredBytes = readRowReaderBytes(SELECTIVE_FILTER);

        assertThat(filteredBytes)
                .as("Filtered RowReader should read fewer bytes than unfiltered")
                .isLessThan(unfilteredBytes);
    }

    private long readRowReaderBytes(FilterPredicate filter) throws Exception {
        ByteCountingInputFile inputFile = new ByteCountingInputFile(InputFile.of(TEST_FILE));
        inputFile.open();
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            ColumnProjection projection = ColumnProjection.columns("id", "value");
            RowReader rows = (filter != null)
                    ? reader.createRowReader(projection, filter)
                    : reader.createRowReader(projection);
            try (rows) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        }
        return inputFile.bytesRead();
    }

    // FileManager path (multi-file, via MultiFileParquetReader)

    @Test
    void testMultiFileReaderPageRangeIoReducesBytes() throws Exception {
        long unfilteredBytes = readMultiFileBytes(null);
        long filteredBytes = readMultiFileBytes(SELECTIVE_FILTER);

        assertThat(filteredBytes)
                .as("Filtered MultiFileReader should read fewer bytes than unfiltered")
                .isLessThan(unfilteredBytes);
    }

    private long readMultiFileBytes(FilterPredicate filter) throws Exception {
        ByteCountingInputFile inputFile = new ByteCountingInputFile(InputFile.of(TEST_FILE));
        inputFile.open();
        try (Hardwood hardwood = Hardwood.create();
             MultiFileParquetReader parquet = hardwood.openAll(List.of(inputFile))) {
            ColumnProjection projection = ColumnProjection.columns("id", "value");
            MultiFileColumnReaders columns = (filter != null)
                    ? parquet.createColumnReaders(projection, filter)
                    : parquet.createColumnReaders(projection);
            try (columns) {
                ColumnReader col0 = columns.getColumnReader("id");
                ColumnReader col1 = columns.getColumnReader("value");
                while (col0.nextBatch() & col1.nextBatch()) {
                    col0.getRecordCount();
                }
            }
        }
        return inputFile.bytesRead();
    }

    // Dictionary-encoded column path

    private static final Path DICT_TEST_FILE = Path.of("src/test/resources/column_index_pushdown_dict.parquet");

    @Test
    void testColumnReaderPageRangeIoWithDictionary() throws Exception {
        // Read a dictionary-encoded column with filtering to exercise
        // the parseDictionaryFromSlice path in PageScanner
        ByteCountingInputFile unfilteredFile = new ByteCountingInputFile(InputFile.of(DICT_TEST_FILE));
        unfilteredFile.open();
        long unfilteredRows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(unfilteredFile);
             ColumnReader col = reader.createColumnReader("category")) {
            while (col.nextBatch()) {
                unfilteredRows += col.getRecordCount();
            }
        }

        ByteCountingInputFile filteredFile = new ByteCountingInputFile(InputFile.of(DICT_TEST_FILE));
        filteredFile.open();
        long filteredRows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(filteredFile);
             ColumnReader col = reader.createColumnReader("category", SELECTIVE_FILTER)) {
            while (col.nextBatch()) {
                filteredRows += col.getRecordCount();
                // Verify we can actually read the string values (requires dictionary)
                String[] values = col.getStrings();
                assertThat(values).isNotEmpty();
            }
        }

        assertThat(unfilteredRows).isEqualTo(10000);
        assertThat(filteredRows).isLessThan(unfilteredRows);
        assertThat(filteredFile.bytesRead())
                .as("Dictionary-encoded filtered read should fetch fewer bytes")
                .isLessThan(unfilteredFile.bytesRead());
    }

    /// InputFile wrapper that tracks total bytes fetched via readRange.
    private static class ByteCountingInputFile implements InputFile {

        private final InputFile delegate;
        private final AtomicLong totalBytesRead = new AtomicLong();

        ByteCountingInputFile(InputFile delegate) {
            this.delegate = delegate;
        }

        long bytesRead() {
            return totalBytesRead.get();
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            totalBytesRead.addAndGet(length);
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
}
