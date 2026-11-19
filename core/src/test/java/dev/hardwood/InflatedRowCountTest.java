/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.management.ThreadMXBean;

import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.RowReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A file whose footer declares a row group of [#DECLARED_ROWS] rows, and the file as many, while
/// its column chunks hold [#ROWS], and a second row group of [#ROWS] rows follows it. Without a page index, the pages refute the count once a column
/// has placed them, and what the read holds before then grows with the rows the pages deliver,
/// not with the count: a few kilobytes of footer buy no allocation in proportion to the rows they
/// declare. With one, the OffsetIndex's last page spans the declared rows, more than a page holds.
class InflatedRowCountTest {

    private static final int ROWS = 100;
    private static final long DECLARED_ROWS = 1_000_000_000_000L;
    /// Far below what one object per batch of the declared rows takes, far above what a read of
    /// the file's real rows does
    private static final long ALLOCATION_BUDGET = 64L * 1024 * 1024;

    private static final String SHORT_OF_THE_ROW_GROUP = "[<memory>: row group 0, column 'id', page 0] Invalid column "
            + "chunk: the column holds 100 of the 1000000000000 rows the read takes from its row group";

    private static final String PAGE_SPANS_THE_COUNT = "[<memory>: row group 0, column 'id'] Failed to compute the fetch plan: Malformed Parquet "
            + "metadata: OffsetIndex page 0 spans rows 0 to 1000000000000, more than 2147483638 (the longest array"
            + " a page's values decode into, less one), and every row holds a value in each column";

    /// the file as written, its footer inflated
    private static byte[] indexed;
    /// the same without its page index
    private static byte[] file;

    @BeforeAll
    static void writeFile() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED)
                .primitive("v", PhysicalType.INT64, RepetitionType.REQUIRED)
                .build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            for (int i = 0; i < ROWS; i++) {
                long value = i;
                writer.rowWriter().writeRow(row -> row.setLong("id", value).setLong("v", value));
            }
            writer.endRowGroup();
            // v from -50 to 49: a filter v >= 0 decides this row group only by its values
            for (int i = 0; i < ROWS; i++) {
                long value = ROWS + i;
                long v = i - ROWS / 2;
                writer.rowWriter().writeRow(row -> row.setLong("id", value).setLong("v", v));
            }
        }
        indexed = FooterRewriter.rewrite(InMemoryFiles.toByteArray(out), InflatedRowCountTest::inflated);
        file = FooterRewriter.withoutPageIndex(indexed);
    }

    @ParameterizedTest(name = "threads: {0}, read by ranges: {1}")
    @CsvSource({ "0, false", "0, true", "4, false", "4, true" })
    void columnReadersFailWithinBudget(int threads, boolean byRanges) {
        assertFailsWithinBudget(() -> {
            try (HardwoodContext context = HardwoodContext.create(threads);
                 ParquetFileReader reader = open(context, byRanges);
                 ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("id"))) {
                while (columns.nextBatch()) {
                    // read to the end
                }
            }
        });
    }

    @ParameterizedTest(name = "threads: {0}, read by ranges: {1}")
    @CsvSource({ "0, false", "0, true", "4, false", "4, true" })
    void rowReaderFailsWithinBudget(int threads, boolean byRanges) {
        assertFailsWithinBudget(() -> {
            try (HardwoodContext context = HardwoodContext.create(threads);
                 ParquetFileReader reader = open(context, byRanges);
                 RowReader rows = reader.buildRowReader().projection(ColumnProjection.columns("id")).build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        });
    }

    /// Statistics prove every row of the first row group matches `v >= 0`, so the filter-only
    /// column `v` is not read there and passes the batches the payload column fills without
    /// opening a cell in any; it reads the second row group, whose values decide the filter.
    @ParameterizedTest(name = "threads: {0}, read by ranges: {1}")
    @CsvSource({ "0, false", "0, true", "4, false", "4, true" })
    void filteredRowReaderSkippingAFilterOnlyColumnFailsWithinBudget(int threads, boolean byRanges) {
        assertFailsWithinBudget(() -> {
            try (HardwoodContext context = HardwoodContext.create(threads);
                 ParquetFileReader reader = open(context, byRanges);
                 RowReader rows = reader.buildRowReader()
                         .projection(ColumnProjection.columns("id"))
                         .filter(FilterPredicate.gtEq("v", 0L))
                         .build()) {
                while (rows.hasNext()) {
                    rows.next();
                }
            }
        });
    }

    /// A row group declaring more batches than a read counts fails the read with the file's
    /// fault, on the thread that plans it as on any other.
    @ParameterizedTest(name = "threads: {0}")
    @ValueSource(ints = { 0, 4 })
    void aRowGroupOfMoreBatchesThanAReadCountsFails(int threads) throws IOException {
        byte[] overlong = FooterRewriter.rewrite(file, metaData -> withFirstRowGroupDeclaring(metaData, 100_000_000_000_000_000L));
        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(threads);
                 ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(overlong)), context);
                 ColumnReader column = reader.buildColumnReader("id").batchSize(64).build()) {
                while (column.nextBatch()) {
                    // read to the end
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0] Malformed Parquet metadata: the read takes 100000000000000000 rows from the row group, more than"
                        + " 2147483647 batches of 64 rows");
    }

    @Test
    void anOffsetIndexPageSpanningTheCountFails() {
        assertThatThrownBy(() -> {
            try (HardwoodContext context = HardwoodContext.create(0);
                 ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(indexed)), context);
                 ColumnReaders columns = reader.columnReaders(ColumnProjection.columns("id"))) {
                while (columns.nextBatch()) {
                    // read to the end
                }
            }
        })
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(PAGE_SPANS_THE_COUNT);
    }

    /// Runs `read` and asserts that it fails as the pages refute the row group's count, the
    /// JVM's threads, the context's among them, having allocated less than the budget meanwhile.
    private static void assertFailsWithinBudget(Executable read) {
        ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        long before = threads.getTotalThreadAllocatedBytes();
        assertThatThrownBy(read::execute)
                .isInstanceOf(ParquetReadException.class)
                .hasMessage(SHORT_OF_THE_ROW_GROUP);
        assertThat(threads.getTotalThreadAllocatedBytes() - before).isLessThan(ALLOCATION_BUDGET);
    }

    /// Opens the file held in memory, which a read plans within its look-ahead, or read by
    /// ranges, which a read plans ahead of it, as it would a remote file.
    private static ParquetFileReader open(HardwoodContext context, boolean byRanges) throws IOException {
        InputFile input = byRanges ? new CountingInputFile(ByteBuffer.wrap(file)) : InputFile.of(ByteBuffer.wrap(file));
        return ParquetFileReader.open(input, context);
    }

    /// `metaData` with its first row group declaring [#DECLARED_ROWS] rows.
    private static FileMetaData inflated(FileMetaData metaData) {
        return withFirstRowGroupDeclaring(metaData, DECLARED_ROWS);
    }

    /// `metaData` with its first row group declaring `rows` rows, and the file as many more as
    /// that row group holds beyond its own.
    private static FileMetaData withFirstRowGroupDeclaring(FileMetaData metaData, long rows) {
        RowGroup first = metaData.rowGroups().get(0);
        RowGroup second = metaData.rowGroups().get(1);
        return new FileMetaData(metaData.version(), metaData.schema(), rows + second.numRows(),
                List.of(new RowGroup(first.columns(), first.totalByteSize(), rows), second),
                metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
    }
}
