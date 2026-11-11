/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.plan.ReadEngines;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A failure of one column's page, met by its walk, its decode or its placement, ends the read at
/// its row: a read that takes its columns in lockstep, as every column and row read does, takes
/// every batch before the failure and then meets it, placed at its file, row group, column and
/// page (READ_PIPELINE.md, Error propagation).
class ColumnFailureTest {

    private static final int ROW_GROUPS = 6;
    private static final int ROWS_PER_ROW_GROUP = 100;
    private static final int BATCH_SIZE = 32;
    /// The rows a lockstep read takes before each failure: none before column `a`'s first page, and
    /// the seven whole batches before the last row group's partial one.
    private static final long ROWS_CORRUPT = 0;
    private static final long ROWS_OVERLONG = 224;
    private static final long ROWS_SHORT = 224;
    private static final FileSchema SCHEMA = FileSchema.builder("longs")
            .addColumn("a", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED)
            .build();

    enum Corruption {
        /// The page header of column `a`'s first page, which its walk parses.
        HEADER("[<memory>: row group 0, column 'a', page 0] PageHeader has unknown page type: -1408") {
            @Override
            long offset(ColumnMetaData chunk) {
                return chunk.dataPageOffset() + 1;
            }
        },
        /// The last byte of column `a`'s first chunk, which fails its page's CRC as it decodes.
        DATA("[<memory>: row group 0, column 'a', page 0] CRC mismatch: expected d36a2984 but computed fe68c609") {
            @Override
            long offset(ColumnMetaData chunk) {
                return chunk.dataPageOffset() + chunk.totalCompressedSize() - 1;
            }
        };

        final String message;

        Corruption(String message) {
            this.message = message;
        }

        abstract long offset(ColumnMetaData chunk);
    }

    @ParameterizedTest(name = "{0}, threads: {1}")
    @MethodSource("cases")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aCorruptPageEndsTheReadAtItsRow(Corruption corruption, int threads) throws Exception {
        assertReadEndsAt(corrupted(corruption), threads, false, ROWS_CORRUPT, corruption.message);
    }

    static Stream<Arguments> cases() {
        return Stream.of(Corruption.values())
                .flatMap(corruption -> Stream.of(Arguments.of(corruption, 0), Arguments.of(corruption, 4)));
    }

    /// Row group 2 declares 50 rows, and column `a`'s chunk there is row group 1's, of 100: the
    /// placement of its page fails, for a flat column as its walk's result is applied, for one
    /// decoded through levels as its decode is.
    @ParameterizedTest(name = "decoded through levels: {0}, threads: {1}")
    @CsvSource({ "false, 0", "false, 4", "true, 0", "true, 4" })
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aPlacementFailureEndsTheReadAtItsRow(boolean throughLevels, int threads) throws Exception {
        assertReadEndsAt(withOverlongChunk(), threads, throughLevels, ROWS_OVERLONG,
                "[<memory>: row group 2, column 'a', page 0] Batch plan mismatch: the column holds more than the "
                        + "50 rows the plan takes from the row group");
    }

    /// The last row group declares 60 rows and its chunks hold 50: the columns are short of the
    /// plan at the read's end, and the read ends with the shortfall placed at the last page of the
    /// first column.
    @ParameterizedTest(name = "decoded through levels: {0}, threads: {1}")
    @CsvSource({ "false, 0", "false, 4", "true, 0", "true, 4" })
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aShortfallAtTheReadsEndEndsTheReadAtItsLastPage(boolean throughLevels, int threads) throws Exception {
        assertReadEndsAt(withShortLastRowGroup(), threads, throughLevels, ROWS_SHORT,
                "[<memory>: row group 2, column 'a', page 0] Batch plan mismatch: the column holds 50 of the 60 "
                        + "rows the plan takes from the row group");
    }

    /// Reads `file` in lockstep: `rows` rows, then the failure, `message`.
    private static void assertReadEndsAt(ByteBuffer file, int threads, boolean throughLevels, long rows,
                                         String message) throws Exception {
        try (Read read = Read.start(file, threads, throughLevels)) {
            AtomicLong taken = new AtomicLong();
            assertThatThrownBy(() -> drainInLockstep(read.exchanges(), taken)).hasMessage(message);
            assertThat(taken).hasValue(rows);
        }
    }

    /// A read of both columns of a file, through the context it creates and closes.
    private record Read(HardwoodContextImpl context, ReadEngine engine, List<BatchExchange<?>> exchanges)
            implements AutoCloseable {

        /// Starts a read of `file`, its columns flat or decoded through levels.
        static Read start(ByteBuffer file, int threads, boolean throughLevels) throws Exception {
            HardwoodContextImpl context = HardwoodContextImpl.create(threads);
            ReadEngine engine = ReadEngines.initialized(List.of(InputFile.of(file)), context,
                    ColumnProjection.all(), null, 0);
            ProjectedSchema decoded = ProjectedSchema.create(SCHEMA, ColumnProjection.all());
            NestedLevelComputer.Layers[] allLayers = NestedLevelComputer.computeLayers(SCHEMA);
            List<BatchExchange<?>> exchanges = new ArrayList<>();
            List<NestedLevelComputer.Layers> layers = new ArrayList<>();
            for (int column = 0; column < SCHEMA.getColumnCount(); column++) {
                String name = SCHEMA.getColumn(column).name();
                exchanges.add(throughLevels
                        ? BatchExchange.detachingNested(name, BATCH_SIZE)
                        : BatchExchange.detachingFlat(name, BATCH_SIZE));
                layers.add(throughLevels ? allLayers[column] : null);
            }
            engine.start(decoded, layers, NestedIndexMode.ALL_ITEMS, false, BATCH_SIZE, BATCH_SIZE,
                    exchanges, null);
            return new Read(context, engine, exchanges);
        }

        @Override
        public void close() throws Exception {
            try (HardwoodContextImpl closing = context) {
                engine.close();
            }
        }
    }

    /// Takes the batches of `exchanges` in lockstep, a batch of every column in column order at a
    /// time, as a column read does; adds the rows of each step to `rows`, and raises the first
    /// failure it meets.
    private static void drainInLockstep(List<BatchExchange<?>> exchanges, AtomicLong rows) throws Exception {
        while (true) {
            int records = 0;
            for (BatchExchange<?> exchange : exchanges) {
                Object batch = exchange.poll();
                records = batch instanceof NestedBatch nested ? nested.recordCount
                        : batch == null ? 0 : ((BatchExchange.Batch) batch).recordCount;
                if (records == 0) {
                    exchange.checkError();
                    return;
                }
            }
            rows.addAndGet(records);
        }
    }

    private static ByteBuffer corrupted(Corruption corruption) throws Exception {
        byte[] bytes = file(ROW_GROUPS * ROWS_PER_ROW_GROUP);
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(bytes)))) {
            ColumnMetaData chunk = reader.getFileMetaData().rowGroups().get(0).columns().get(0).metaData();
            bytes[Math.toIntExact(corruption.offset(chunk))] ^= (byte) 0xFF;
        }
        return ByteBuffer.wrap(bytes);
    }

    /// Row groups of 100, 100 and 50 rows without a page index, the last one's chunk of column `a`
    /// replaced by row group 1's.
    private static ByteBuffer withOverlongChunk() throws Exception {
        return ByteBuffer.wrap(FooterRewriter.rewrite(file(250), metaData -> {
            assertThat(metaData.rowGroups()).hasSize(3);
            List<RowGroup> rowGroups = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                RowGroup rowGroup = metaData.rowGroups().get(i);
                List<ColumnChunk> chunks = new ArrayList<>();
                chunks.add(withoutIndex(metaData.rowGroups().get(i == 2 ? 1 : i).columns().get(0)));
                chunks.add(withoutIndex(rowGroup.columns().get(1)));
                rowGroups.add(new RowGroup(chunks, rowGroup.totalByteSize(), rowGroup.numRows()));
            }
            return new FileMetaData(metaData.version(), metaData.schema(), metaData.numRows(), rowGroups,
                    metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
        }));
    }

    /// Row groups of 100, 100 and 50 rows without a page index, the last one declaring 60.
    private static ByteBuffer withShortLastRowGroup() throws Exception {
        return ByteBuffer.wrap(FooterRewriter.rewrite(file(250), metaData -> {
            assertThat(metaData.rowGroups()).hasSize(3);
            List<RowGroup> rowGroups = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                RowGroup rowGroup = metaData.rowGroups().get(i);
                rowGroups.add(new RowGroup(rowGroup.columns().stream().map(ColumnFailureTest::withoutIndex).toList(),
                        rowGroup.totalByteSize(), i == 2 ? 60 : rowGroup.numRows()));
            }
            return new FileMetaData(metaData.version(), metaData.schema(), 260, rowGroups,
                    metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
        }));
    }

    private static ColumnChunk withoutIndex(ColumnChunk chunk) {
        return new ColumnChunk(chunk.metaData(), null, null, null, null, chunk.filePath());
    }

    /// `rows` rows of two required `INT64` columns, in row groups of [#ROWS_PER_ROW_GROUP].
    private static byte[] file(int rows) throws Exception {
        return write(rows, WriterConfig.builder().rowGroupTargetRows(ROWS_PER_ROW_GROUP));
    }

    private static byte[] write(int rows, WriterConfig.Builder config) throws Exception {
        long[] values = new long[rows];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA, config.build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("a", values).longs("b", values));
        }
        return InMemoryFiles.toByteArray(out);
    }
}
