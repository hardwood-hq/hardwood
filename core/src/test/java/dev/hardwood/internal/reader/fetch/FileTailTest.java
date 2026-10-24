/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.reader.plan.FileMetadataCache;
import dev.hardwood.internal.reader.plan.RowGroupIterator;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.schema.ReadProjection;
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
/// sliced from it, so a file up to the tail's size costs one request (FETCH_PLANNING.md, The
/// file's tail).
class FileTailTest {

    private static final int ROWS = 20_000;
    /// Twelve row groups of twenty columns with a page index, which lies inside the default tail.
    private static final Path PAGE_INDEX_WINDOWS = Path.of("src/test/resources/page_index_windows.parquet");

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

    /// A range of a column chunk that straddles the tail's start is requested only up to
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
        assertThat(HeldBytes.heldFrom(tailed)).isEqualTo(200);

        tailed.readRange(250, 50);
        tailed.readRange(150, 100);
        HeldBytes.drop(tailed);
        tailed.readRange(250, 50);

        assertThat(file.reads()).extracting(CountingInputFile.Read::offset, CountingInputFile.Read::length)
                .containsExactly(tuple(200L, 100), tuple(150L, 100), tuple(250L, 50));
        assertThat(HeldBytes.heldFrom(tailed)).isEqualTo(Long.MAX_VALUE);
    }

    /// The tail is the reader's, shared by its reads: a read that has listed the file keeps the
    /// page index it plans to slice from the tail when another read of the reader drops the tail
    /// by closing. The two reads are iterators over one metadata cache, as a reader's child reads
    /// are.
    @Test
    void aReadKeepsWhatItSlicesFromTheTailWhenAnotherReadDropsIt() throws Exception {
        System.clearProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        CountingInputFile file = new CountingInputFile(InputFile.of(PAGE_INDEX_WINDOWS));
        file.open();
        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            FileMetadataCache cache = new FileMetadataCache(List.of(file), context);
            RowGroupIterator planning = iterator(cache, context);
            RowGroupIterator closing = iterator(cache, context);
            try {
                // Lists the file, whose page index lies inside the tail, and plans nothing yet
                RowGroupIterator.WorkItem workItem = planning.workItemAt(0);
                closing.workItemAt(0);
                closing.close();

                assertThat(planning.getColumnPlan(workItem, 0).isEmpty()).isFalse();
            }
            finally {
                planning.close();
                cache.close();
                file.close();
            }
        }
    }

    private static RowGroupIterator iterator(FileMetadataCache cache, HardwoodContextImpl context)
            throws IOException {
        RowGroupIterator iterator = new RowGroupIterator(cache, ignored -> {}, context, 0, 0, 0);
        iterator.openFirst();
        iterator.initialize(ReadProjection.of(
                ProjectedSchema.create(iterator.referenceSchema(), ColumnProjection.columns("c05"))), null, true);
        return iterator;
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
