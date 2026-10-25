/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.fetch.TailedInputFile;
import dev.hardwood.internal.reader.plan.RowGroupIterators;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// A read holds the requests of the work items its columns have not passed, and no others: those
/// of a work item every column has passed are forgotten, and a column whose placement ends at the
/// row cap passes every section it holds.
class RequestRetentionTest {

    private static final String MAX_FILES_AHEAD_PROPERTY = "hardwood.internal.maxFilesAhead";
    /// One row group of two columns.
    private static final Path SMALL_FILE = Path.of("src/test/resources/compat_order_ab.parquet");
    private static final int FILES = 40;
    private static final int FILES_AHEAD = 4;
    /// More rows than a file of [#SMALL_FILE] holds, so that each file is one batch.
    private static final int SMALL_BATCH_SIZE = 4;
    private static final int BATCH_SIZE = 1024;

    private String tailBytes;

    /// A tail too short to hold a file's data, so that the data are requests of their own.
    @BeforeEach
    void shortTail() {
        tailBytes = System.getProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, "64");
    }

    @AfterEach
    void restoreProperties() {
        System.clearProperty(MAX_FILES_AHEAD_PROPERTY);
        if (tailBytes == null) {
            System.clearProperty(TailedInputFile.TAIL_BYTES_PROPERTY);
        }
        else {
            System.setProperty(TailedInputFile.TAIL_BYTES_PROPERTY, tailBytes);
        }
    }

    /// The requests held stay within those of the files ahead and the file read, however many files
    /// the read has passed, and none is held once every file is read.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aLongMultiFileReadHoldsTheRequestsOfTheFilesAheadOnly() throws Exception {
        System.setProperty(MAX_FILES_AHEAD_PROPERTY, String.valueOf(FILES_AHEAD));
        List<InputFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            files.add(new CountingInputFile(InputFile.of(SMALL_FILE)));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             RowGroupIterator iterator = RowGroupIterators.initialized(files, context, ColumnProjection.all(), null,
                     0)) {
            // a read-ahead of a few rows keeps the columns a few files past the consumer
            List<BatchExchange<?>> exchanges = start(iterator, SMALL_FILE, SMALL_BATCH_SIZE, 1, 0);
            int read = 0;
            int mostHeld = 0;
            while (takeBatch(exchanges) > 0) {
                read++;
                iterator.awaitQuiescent();
                mostHeld = Math.max(mostHeld, iterator.requestsHeld());
                // the file read, the next and the files ahead, two columns each at most
                assertThat(iterator.requestsHeld()).as("after file %d", read)
                        .isLessThanOrEqualTo(2 * (FILES_AHEAD + 2));
            }
            assertThat(read).isEqualTo(FILES);
            assertThat(mostHeld).isPositive();
            iterator.awaitQuiescent();
            assertThat(iterator.requestsHeld()).isZero();
        }
    }

    /// Once `head(n)` has ended every column's placement, the read holds no data bytes and no
    /// charge to the read-ahead buffer: the sections a walk stopped in holds are passed.
    @ParameterizedTest(name = "{0}, head {1}")
    @CsvSource({
            // 2,000 rows in pages of about 512 bytes, without a page index
            "misaligned_pages_no_index.parquet, 100",
            // four row groups of 50 rows, with a page index
            "differential/diff_order_multi.parquet, 30"
    })
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadStoppedAtItsRowCapHoldsNoDataBytes(String fixture, long head) throws Exception {
        List<InputFile> files = List.of(
                new CountingInputFile(InputFile.of(Path.of("src/test/resources", fixture))),
                new CountingInputFile(InputFile.of(Path.of("src/test/resources", fixture))));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             RowGroupIterator iterator = RowGroupIterators.initialized(files, context, ColumnProjection.all(), null,
                     head)) {
            List<BatchExchange<?>> exchanges = start(iterator, Path.of("src/test/resources", fixture), BATCH_SIZE,
                    BATCH_SIZE, head);
            long rows = 0;
            int batch;
            while ((batch = takeBatch(exchanges)) > 0) {
                rows += batch;
            }
            assertThat(rows).isEqualTo(head);
            iterator.awaitQuiescent();
            assertThat(iterator.dataBytesHeld()).isZero();
            assertThat(iterator.requestsHeld()).isZero();
            assertThat(context.readAheadBuffer().held()).isZero();
        }
    }

    /// Starts every column of `file`'s schema into detaching exchanges of `batchSize` rows, with a
    /// batch budget of `budgetRows`, capped at `maxRows` unless it is `0`.
    private static List<BatchExchange<?>> start(RowGroupIterator iterator, Path file, int batchSize, long budgetRows,
                                                long maxRows) throws Exception {
        FileSchema schema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            schema = reader.getFileSchema();
        }
        ProjectedSchema decoded = ProjectedSchema.create(schema, ColumnProjection.all());
        List<BatchExchange<?>> exchanges = new ArrayList<>();
        List<NestedLevelComputer.Layers> flat = new ArrayList<>();
        for (int column = 0; column < decoded.getProjectedColumnCount(); column++) {
            exchanges.add(BatchExchange.detachingFlat(decoded.getProjectedColumns().get(column).name(), batchSize));
            flat.add(null);
        }
        iterator.start(decoded, flat, NestedIndexMode.ALL_ITEMS, false, batchSize, budgetRows, maxRows, exchanges,
                null);
        return exchanges;
    }

    /// Takes the next batch of every column, returning its rows; `0` once the read has ended.
    private static int takeBatch(List<BatchExchange<?>> exchanges) throws Exception {
        int rows = 0;
        for (BatchExchange<?> exchange : exchanges) {
            BatchExchange.Batch batch = (BatchExchange.Batch) exchange.poll();
            if (batch == null || batch.recordCount == 0) {
                exchange.checkError();
            }
            else {
                rows = batch.recordCount;
            }
        }
        return rows;
    }
}
