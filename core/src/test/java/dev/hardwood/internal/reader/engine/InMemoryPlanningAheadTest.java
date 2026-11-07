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

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.plan.RowGroupIterators;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A read of files whose bytes are in memory, which no request bounds, plans one work item past
/// its slowest column on a pool, and on a context of zero threads none past the furthest work item
/// a column has entered.
class InMemoryPlanningAheadTest {

    private static final int FILES = 2;
    private static final int ROW_GROUPS = 20;
    private static final int ROWS_PER_ROW_GROUP = 100;
    private static final int COLUMNS = 2;
    private static final FileSchema SCHEMA = FileSchema.builder("longs")
            .addColumn("a", PhysicalType.INT64, RepetitionType.REQUIRED)
            .addColumn("b", PhysicalType.INT64, RepetitionType.REQUIRED)
            .build();

    @ParameterizedTest(name = "threads: {0}")
    @ValueSource(ints = {0, 1, 4})
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadOfFilesInMemoryPlansOneWorkItemAhead(int threads) throws Exception {
        byte[] file = file();
        List<InputFile> files = new ArrayList<>();
        for (int i = 0; i < FILES; i++) {
            files.add(InputFile.of(ByteBuffer.wrap(file)));
        }
        try (HardwoodContextImpl context = HardwoodContextImpl.create(threads);
             RowGroupIterator iterator = RowGroupIterators.initialized(files, context, ColumnProjection.all(), null,
                     0)) {
            ProjectedSchema decoded = ProjectedSchema.create(SCHEMA, ColumnProjection.all());
            List<BatchExchange<?>> exchanges = new ArrayList<>();
            List<NestedLevelComputer.Layers> flat = new ArrayList<>();
            for (int column = 0; column < COLUMNS; column++) {
                exchanges.add(BatchExchange.detachingFlat(SCHEMA.getColumn(column).name(), ROWS_PER_ROW_GROUP));
                flat.add(null);
            }
            iterator.start(decoded, flat, NestedIndexMode.ALL_ITEMS, false, ROWS_PER_ROW_GROUP, ROWS_PER_ROW_GROUP,
                    0, exchanges, null);
            long rows = 0;
            int batch;
            while ((batch = takeBatch(exchanges)) > 0) {
                rows += batch;
                // the read's jobs run as far as they go while the consumer holds back
                iterator.awaitQuiescent();
                RowGroupIterator.PlanningPosition position = iterator.planningPosition();
                if (threads == 0) {
                    assertThat(position.plannedThrough()).as("%s", position)
                            .isLessThanOrEqualTo(position.furthestUnit());
                }
                else {
                    assertThat(position.plannedThrough()).as("%s", position)
                            .isLessThanOrEqualTo(position.slowestUnit() + 1);
                }
            }
            assertThat(rows).isEqualTo((long) FILES * ROW_GROUPS * ROWS_PER_ROW_GROUP);
        }
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

    /// Twenty row groups of 100 rows of two required `INT64` columns.
    private static byte[] file() throws Exception {
        long[] values = new long[ROW_GROUPS * ROWS_PER_ROW_GROUP];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA,
                WriterConfig.builder().rowGroupTargetRows(ROWS_PER_ROW_GROUP).build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("a", values).longs("b", values));
        }
        return InMemoryFiles.toByteArray(out);
    }
}
