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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.io.CountingInputFile;
import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.NestedIndexMode;
import dev.hardwood.internal.reader.batch.NestedLevelComputer;
import dev.hardwood.internal.reader.fetch.CoalescingPolicy;
import dev.hardwood.internal.reader.plan.ReadEngines;
import dev.hardwood.internal.schema.ProjectedSchema;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.FilterPredicate;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;

import static org.assertj.core.api.Assertions.assertThat;

/// A read whose row cap ends every column's placement holds nothing in R once it is quiescent,
/// before it closes: the pieces of a chunk past the cap, whose last reader is done before their
/// fetch starts, are not fetched. A filter its statistics prove keeps the unit whole, so that its
/// chunk is pieces up to its end.
class RowCapRetentionTest {

    private static final FileSchema SCHEMA = FileSchema.builder("capped")
            .addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED)
            .build();
    private static final int ROWS = 200_000;
    private static final int BATCH_SIZE = 1024;

    @AfterEach
    void clearSpan() {
        System.clearProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aReadStoppedAtItsRowCapHoldsNothingInR() throws Exception {
        // pieces of 16 KiB, so that the one chunk is many requests
        System.setProperty(CoalescingPolicy.MAX_SPAN_BYTES_PROPERTY, "16384");
        CountingInputFile file = new CountingInputFile(ByteBuffer.wrap(file()));
        BatchExchange<?> exchange = BatchExchange.detachingFlat("v", BATCH_SIZE);
        List<NestedLevelComputer.Layers> layers = new ArrayList<>();
        layers.add(null);
        try (HardwoodContextImpl context = HardwoodContextImpl.create(2);
             ReadEngine engine = ReadEngines.prepared(List.of(file), context, ColumnProjection.all(),
                     FilterPredicate.gtEq("v", 0L), 10).start(ProjectedSchema.create(SCHEMA, ColumnProjection.all()),
                     layers, NestedIndexMode.ALL_ITEMS, false, BATCH_SIZE, BATCH_SIZE, List.of(exchange), null)) {
            long rows = 0;
            Object batch;
            while ((batch = exchange.poll()) != null && ((BatchExchange.Batch) batch).recordCount > 0) {
                rows += ((BatchExchange.Batch) batch).recordCount;
            }
            exchange.checkError();

            assertThat(rows).isEqualTo(10);
            engine.awaitQuiescent();
            assertThat(context.readAheadBuffer().held()).isZero();
        }
    }

    private static byte[] file() throws Exception {
        long[] values = new long[ROWS];
        for (int i = 0; i < ROWS; i++) {
            values[i] = i * 7919L;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, SCHEMA)) {
            writer.columnWriter().writeBatch(b -> b.longs("v", values));
        }
        return InMemoryFiles.toByteArray(out);
    }
}
