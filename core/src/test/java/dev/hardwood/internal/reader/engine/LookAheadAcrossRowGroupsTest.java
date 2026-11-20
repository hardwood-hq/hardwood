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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.plan.ReadEngines;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A read that keeps a few rows of each row group of a file held in memory looks ahead by the rows
/// it keeps, not by the rows its row groups hold: before its consumer takes a batch, it plans and
/// batches as many row groups as their kept rows fill the look-ahead.
class LookAheadAcrossRowGroupsTest {

    private static final FileSchema SCHEMA = FileSchema.builder("ranked")
            .primitive("t", PhysicalType.INT64, RepetitionType.REQUIRED)
            .build();
    private static final int ROW_GROUPS = 20;
    private static final int ROW_GROUP_ROWS = 50_000;
    private static final int PAGE_ROWS = 1024;
    private static final int BATCH_SIZE = 1024;

    /// Each row group keeps one page, 1,024 rows, by its page index, and the look-ahead is eight
    /// budgets of 1,024 rows: it reaches about eight row groups, though one row group's rows alone
    /// exceed it.
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aSelectiveReadBatchesTheRowGroupsItsKeptRowsFitInTheLookAheadBeforeItsConsumerTakesABatch()
            throws Exception {
        InputFile file = InputFile.of(ByteBuffer.wrap(file()));
        BatchExchange<?> exchange = BatchExchange.detachingFlat("t", BATCH_SIZE);
        List<NestedLevelComputer.Layers> layers = new ArrayList<>();
        layers.add(null);
        FilterPredicate filter = FilterPredicate.and(FilterPredicate.gtEq("t", 25_000L),
                FilterPredicate.lt("t", 25_010L));
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ReadEngine engine = ReadEngines.prepared(List.of(file), context, ColumnProjection.all(), filter, 0)
                     .start(ProjectedSchema.create(SCHEMA, ColumnProjection.all()), layers, NestedIndexMode.ALL_ITEMS,
                             false, BATCH_SIZE, BATCH_SIZE, List.of(exchange), null)) {
            engine.awaitQuiescent();

            assertThat(engine.batchedKey()).isGreaterThanOrEqualTo(6L * ROW_GROUP_ROWS);
        }
    }

    private static byte[] file() throws Exception {
        WriterConfig config = WriterConfig.builder().pageTargetRows(PAGE_ROWS).rowGroupTargetRows(ROW_GROUP_ROWS).build();
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA, config)) {
            long[] values = new long[ROW_GROUP_ROWS];
            for (int i = 0; i < ROW_GROUP_ROWS; i++) {
                values[i] = i;
            }
            for (int g = 0; g < ROW_GROUPS; g++) {
                writer.columnWriter().writeBatch(b -> b.longs("t", values.clone()));
            }
        }
        return InMemoryFiles.toByteArray(out);
    }
}
