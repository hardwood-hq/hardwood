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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.writer.ByteBufferOutputFile;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.RowWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// The next row group's prefetch fetches the first chunk of a [SequentialFetchPlan] (#1324), and
/// that fetch is the one the read of the row group then uses.
class SequentialNextRowGroupPrefetchTest {

    private static final int ROWS_PER_ROW_GROUP = 1_000;

    @Test
    void theNextRowGroupsFirstStandaloneChunkIsPrefetchedAndReadOnce() throws Exception {
        // One column: its first chunk is a standalone handle.
        CountingInputFile file = read(write("a"));

        assertThat(file.reads())
                .filteredOn(read -> read.reason().contains("rg=1 col='a' seqChunk@0"))
                .extracting(CountingInputFile.Read::reason)
                .containsExactly("prefetch rg=1 | rg=1 col='a' seqChunk@0");
    }

    @Test
    void theNextRowGroupsFirstSharedRegionIsPrefetchedAndReadOnce() throws Exception {
        // Two back-to-back columns: their first chunks are coalesced into one shared region.
        CountingInputFile file = read(write("a", "b"));

        assertThat(file.reads())
                .filteredOn(read -> read.reason().contains("rg=1 "))
                .extracting(CountingInputFile.Read::reason)
                .containsExactly("prefetch rg=1 | rg=1 region=0..1");
    }

    /// Reads every row of `data`, holding back the first row group's data until the second row
    /// group's first data read has been issued, so the prefetch cannot lose the race to the read.
    private static CountingInputFile read(byte[] data) throws Exception {
        CountingInputFile file = new CountingInputFile(new HoldBackFirstRowGroup(InputFile.of(ByteBuffer.wrap(data))));
        file.open();
        int rows = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(file);
                RowReader rowReader = reader.rowReader()) {
            assertThat(reader.getFileMetaData().rowGroups()).hasSize(3);
            while (rowReader.hasNext()) {
                rowReader.next();
                rows++;
            }
        }
        assertThat(rows).isEqualTo(3 * ROWS_PER_ROW_GROUP);
        return file;
    }

    /// Three row groups of required `INT64` columns with the given names, without a page index.
    private static byte[] write(String... columns) throws Exception {
        FileSchema.Builder schemaBuilder = FileSchema.builder("prefetch");
        for (String column : columns) {
            schemaBuilder.addColumn(column, PhysicalType.INT64, RepetitionType.REQUIRED);
        }
        WriterConfig config = WriterConfig.builder()
                .codec(CompressionCodec.UNCOMPRESSED)
                .rowGroupTargetRows(ROWS_PER_ROW_GROUP)
                .build();
        ByteBufferOutputFile out = new ByteBufferOutputFile();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schemaBuilder.build(), config)) {
            RowWriter rowWriter = writer.rowWriter();
            for (int i = 0; i < 3 * ROWS_PER_ROW_GROUP; i++) {
                long value = i;
                rowWriter.writeRow(row -> {
                    for (String column : columns) {
                        row.setLong(column, value);
                    }
                });
            }
        }
        return out.toByteArray();
    }

    /// Holds back the data reads of row group 0 until row group 1's prefetch has issued a data
    /// read, or for at most five seconds.
    private static final class HoldBackFirstRowGroup implements InputFile {

        private final InputFile delegate;
        private final CountDownLatch nextRowGroupPrefetched = new CountDownLatch(1);

        HoldBackFirstRowGroup(InputFile delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws IOException {
            delegate.open();
        }

        @Override
        public ByteBuffer readRange(long offset, int length) throws IOException {
            String reason = FetchReason.current();
            if (reason.startsWith("prefetch rg=1 |")) {
                nextRowGroupPrefetched.countDown();
            }
            else if (reason.startsWith("rg=0 ")) {
                try {
                    nextRowGroupPrefetched.await(5, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
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
            delegate.close();
        }
    }
}
