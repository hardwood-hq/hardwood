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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.InMemoryFiles;
import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.thrift.FooterRewriter;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A failure between two work items, here the planning of the second row group, is met by every
/// column at the same row: a column that holds every row of its row group the plan takes publishes
/// its partial batch ahead of the failure, and a column short of the plan abandons it, with the
/// shortfall attached to the failure as a suppressed "Batch plan mismatch" (READ_PIPELINE.md,
/// Error propagation).
class BetweenWorkItemsFailureTest {

    private static final String PLANNING_FAILURE = "[<memory>] Cannot read column 0 in row group 1: Column chunk "
            + "stores its data in a separate file ('elsewhere.parquet'); the split-file layout is not supported";

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aColumnHoldingItsPlannedRowsPublishesItsPartialBatch() throws Exception {
        List<Integer> published = new ArrayList<>();

        assertThatThrownBy(() -> read(file(100), published))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(PLANNING_FAILURE)
                .satisfies(e -> assertThat(e.getSuppressed()).isEmpty());
        assertThat(published).containsExactly(64, 36);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aColumnShortOfItsPlanAbandonsItsPartialBatch() throws Exception {
        List<Integer> published = new ArrayList<>();

        // The footer gives the first row group 120 rows, its chunk holds 100 values
        assertThatThrownBy(() -> read(file(120), published))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(PLANNING_FAILURE)
                .satisfies(e -> assertThat(e.getSuppressed()).singleElement(InstanceOfAssertFactories.THROWABLE)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessage("[<memory>: row group 0, column 'v'] Batch plan mismatch: the column holds 100 "
                                + "of the 120 rows the plan takes from the row group"));
        assertThat(published).containsExactly(64);
    }

    private static void read(byte[] file, List<Integer> published) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)));
             ColumnReader column = reader.buildColumnReader("v").batchSize(64).build()) {
            while (column.nextBatch()) {
                published.add(column.getRecordCount());
            }
        }
    }

    /// Two row groups of 100 required `INT64` values without a page index, the first declaring
    /// `firstRowGroupRows` rows, the second's chunk stored in another file, which fails its
    /// planning.
    private static byte[] file(long firstRowGroupRows) throws Exception {
        long[] values = new long[200];
        for (int i = 0; i < values.length; i++) {
            values[i] = i;
        }
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out,
                FileSchema.builder("longs").addColumn("v", PhysicalType.INT64, RepetitionType.REQUIRED).build(),
                WriterConfig.builder().rowGroupTargetRows(100).build())) {
            writer.columnWriter().writeBatch(batch -> batch.longs("v", values));
        }
        return FooterRewriter.rewrite(InMemoryFiles.toByteArray(out), metaData -> {
            assertThat(metaData.rowGroups()).hasSize(2);
            RowGroup first = metaData.rowGroups().get(0);
            RowGroup second = metaData.rowGroups().get(1);
            return new FileMetaData(metaData.version(), metaData.schema(),
                    firstRowGroupRows + second.numRows(),
                    List.of(new RowGroup(first.columns().stream().map(BetweenWorkItemsFailureTest::withoutIndex)
                                    .toList(), first.totalByteSize(), firstRowGroupRows),
                            new RowGroup(second.columns().stream()
                                    .map(chunk -> new ColumnChunk(chunk.metaData(), null, null, null, null,
                                            "elsewhere.parquet"))
                                    .toList(), second.totalByteSize(), second.numRows())),
                    metaData.keyValueMetadata(), metaData.createdBy(), metaData.columnOrders());
        });
    }

    private static ColumnChunk withoutIndex(ColumnChunk chunk) {
        return new ColumnChunk(chunk.metaData(), null, null, null, null, chunk.filePath());
    }
}
