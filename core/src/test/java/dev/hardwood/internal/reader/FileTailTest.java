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
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

/// A file's tail, which its footer entry reads in one request: the reads that fall inside it are
/// sliced from it, so a file up to the tail's size costs one request (READ_PATH_CONSOLIDATION.md,
/// stage 2, item 2, and rule 5).
class FileTailTest {

    private static final int ROWS = 20_000;

    private String tailBytes;

    @BeforeEach
    void saveTail() {
        tailBytes = System.getProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
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

    /// At the default size a small file arrives whole with its footer, and its data, of every
    /// column, is sliced from it.
    @Test
    void aFileWithinTheTailCostsOneRequest() throws Exception {
        System.clearProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        byte[] bytes = write();
        assertThat(bytes.length).isLessThan(TailedInputFile.DEFAULT_TAIL_BYTES);
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));

        assertThat(readAll(file)).isEqualTo(ROWS);

        assertThat(file.reads()).extracting(CountingInputFile.Read::offset, CountingInputFile.Read::length,
                CountingInputFile.Read::reason)
                .containsExactly(tuple(0L, bytes.length, "footer-info"));
    }

    /// Rule 5: a range of a column chunk that straddles the tail's start is requested only up to
    /// it, and the rest is sliced from the tail, so no byte is requested twice.
    @Test
    void aRangeStraddlingTheTailsStartIsRequestedOnlyUpToIt() throws Exception {
        byte[] bytes = write();
        int tail = bytes.length / 2;
        System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, Integer.toString(tail));
        long tailStart = bytes.length - tail;
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(bytes));

        assertThat(readAll(file)).isEqualTo(ROWS);

        List<CountingInputFile.Read> reads = file.reads();
        assertThat(reads.getFirst()).isEqualTo(new CountingInputFile.Read(tailStart, tail, "footer-info"));
        assertThat(reads.subList(1, reads.size()))
                .isNotEmpty()
                .allSatisfy(read -> assertThat(read.end()).isLessThanOrEqualTo(tailStart));
        assertThat(file.bytesRead()).as("no byte requested twice").isLessThanOrEqualTo(bytes.length);
    }

    /// A read inside the tail is a slice of it, one outside it or across its start the file's own
    /// read, and every read once the tail is dropped.
    @Test
    void theTailServesTheReadsInsideItUntilItIsDropped() throws Exception {
        System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, "100");
        CountingInputFile file = new CountingInputFile(ByteBuffer.allocate(300));
        InputFile tailed = TailedInputFile.take(file);
        assertThat(TailedInputFile.tailOffset(tailed)).isEqualTo(200);

        tailed.readRange(250, 50);
        tailed.readRange(150, 100);
        TailedInputFile.dropTail(tailed);
        tailed.readRange(250, 50);

        assertThat(file.reads()).extracting(CountingInputFile.Read::offset, CountingInputFile.Read::length)
                .containsExactly(tuple(200L, 100), tuple(150L, 100), tuple(250L, 50));
        assertThat(TailedInputFile.tailOffset(tailed)).isEqualTo(Long.MAX_VALUE);
    }

    /// A file whose bytes are in memory takes no tail: it slices every read in place.
    @Test
    void aFileInMemoryTakesNoTail() throws Exception {
        InputFile inMemory = InputFile.of(ByteBuffer.allocate(300));

        assertThat(TailedInputFile.take(inMemory)).isSameAs(inMemory);
    }

    @Test
    void aTailShorterThanTheTrailerIsRejected() {
        System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, "7");

        assertThatThrownBy(TailedInputFile::tailBytes)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("hardwood.internal.tailBytes must be a byte count of at least 8, but was: 7");
    }

    private static long readAll(InputFile file) throws IOException {
        long rows = 0;
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ParquetFileReader reader = ParquetFileReader.open(file, context);
             ColumnReaders columns = reader.buildColumnReaders(ColumnProjection.all()).build()) {
            while (columns.nextBatch()) {
                rows += columns.getRecordCount();
            }
        }
        return rows;
    }

    /// One row group of [#ROWS] rows of two required columns.
    private static byte[] write() throws IOException {
        long[] ids = new long[ROWS];
        long[] values = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            ids[i] = i;
            values[i] = (long) i * 31;
        }
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema, WriterConfig.builder().build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("id", ids).longs("v", values));
        }
        return InMemoryFiles.toByteArray(out);
    }
}
