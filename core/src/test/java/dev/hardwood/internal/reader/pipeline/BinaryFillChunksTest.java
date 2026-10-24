/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.pipeline;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.batch.BatchExchange;
import dev.hardwood.internal.reader.batch.BinaryBatchValues;
import dev.hardwood.internal.reader.decode.BinaryPageValues;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.plan.BatchPlan;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// The plain bytes of a binary segment go into chunks of their own ([BinaryFill]): no chunk is larger
/// than [BinaryBatchValues#CHUNK_BYTES], a value never spans two, and a value longer than a chunk
/// gets a buffer of its own length.
class BinaryFillChunksTest {

    private static final ColumnSchema COLUMN = FileSchema.builder("binary")
            .addColumn("v", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL)
            .build()
            .getColumn(0);

    /// A segment's plain bytes go into chunks of at most [BinaryBatchValues#CHUNK_BYTES], a value
    /// never spanning two; a value longer than a chunk gets a buffer of its own length.
    @Test
    void plainBytesFillChunksNoLargerThanAChunk() {
        int chunk = BinaryBatchValues.CHUNK_BYTES;
        List<String> page = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            page.add(String.valueOf((char) ('a' + i)).repeat(100_000));
        }
        page.add(null);
        page.add("z".repeat(chunk + 1));
        page.add("tail");
        Cell cell = new Cell("f.parquet", false, page.size(), page.size(), 0);
        BinaryBatchValues out = (BinaryBatchValues) cell.allocate(BatchExchange.detachingFlat("v", page.size()), COLUMN)
                .values;
        BatchPlan.RowGroupBatches rowGroup = new BatchPlan(page.size(), false, false, 0)
                .plan(0, 0, "f.parquet", false, page.size());
        Segment segment = place(cell, rowGroup, 0, page.size());
        BinaryFill.fill(segment, plain(page), out, 1);
        FlatFill.backfillValidity(cell);
        BinaryFill.finish(cell, out, new BinaryBatchValues.ViewDemand());

        byte[][] buffers = out.buffers();
        int[] bufferIds = out.bufferIds();
        int[] starts = out.starts();
        int[] ends = out.ends();
        for (int i = 0; i < page.size(); i++) {
            byte[] buffer = buffers[bufferIds[i]];
            assertThat(ends[i]).as("value %d within its buffer", i).isBetween(starts[i], buffer.length);
            String expected = page.get(i);
            assertThat(new String(buffer, starts[i], ends[i] - starts[i], StandardCharsets.UTF_8))
                    .as("value %d", i).isEqualTo(expected == null ? "" : expected);
        }
        // Two 100,000-byte values per chunk: four chunks for seven, then the long value's own
        assertThat(out.bufferCount()).isEqualTo(1 + 4 + 1 + 1);
        for (int b = 1; b < out.bufferCount(); b++) {
            byte[] buffer = buffers[b];
            assertThat(buffer.length).as("buffer %d", b).isLessThanOrEqualTo(buffer == buffers[bufferIds[8]] ? chunk + 1 : chunk);
        }
        assertThat(buffers[bufferIds[8]]).hasSize(chunk + 1);
    }

    /// Places one page of `length` values, page ordinal `index`, into `cell`.
    private static Segment place(Cell cell, BatchPlan.RowGroupBatches rowGroup, int index, int length) {
        PageTask task = new PageTask(new PageInfo(null, null, null, null, PageRowMask.ALL, length),
                "f.parquet", 0, index, rowGroup, null, cell.records());
        Segment segment = cell.add(task, 0, length);
        task.segments.add(segment);
        return segment;
    }

    private static int[] definitionLevels(List<String> page) {
        int[] levels = new int[page.size()];
        for (int i = 0; i < levels.length; i++) {
            levels[i] = page.get(i) == null ? 0 : 1;
        }
        return levels;
    }

    /// The page as an in-place `PLAIN` page: length-prefixed values of its non-null rows.
    private static BinaryPageValues plain(List<String> page) {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        for (String value : page) {
            if (value != null) {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                data.write(bytes.length);
                data.write(bytes.length >>> 8);
                data.write(bytes.length >>> 16);
                data.write(bytes.length >>> 24);
                data.writeBytes(bytes);
            }
        }
        byte[] bytes = data.toByteArray();
        return BinaryPageValues.plain(PhysicalType.BYTE_ARRAY, 0, definitionLevels(page), 1, page.size(), bytes, 0,
                bytes.length, new PageDecoder.LevelScratch());
    }
}
