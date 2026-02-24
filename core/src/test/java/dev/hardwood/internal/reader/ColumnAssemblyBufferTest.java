/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ColumnAssemblyBufferTest {

    private static final int BATCH_SIZE = 4;

    /**
     * Tests that exceptions in the producer thread are propagated to the consumer.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testProducerExceptionPropagatedToConsumer() {
        ColumnSchema column = new ColumnSchema(
                "test_col", PhysicalType.INT32, RepetitionType.REQUIRED,
                null, 0, 0, 0, null);

        ColumnAssemblyBuffer buffer = new ColumnAssemblyBuffer(column, BATCH_SIZE);

        Page page1 = new Page.IntPage(new int[]{1, 2, 3, 4}, null, null, 0, 4);
        Page page2 = new Page.IntPage(new int[]{5, 6, 7}, null, null, 0, 3);
        Page page3 = new Page.IntPage(new int[]{8, 9, 10}, null, null, 0, 3);

        // Produce a series of pages (triggering an explicit failure after page 2)
        producePagesWithError(buffer, List.of(page1, page2, page3), 2);

        // First batch should be available (page 1 filled it)
        TypedColumnData batch1 = buffer.awaitNextBatch();
        assertThat(batch1).as("First batch should be available").isNotNull();
        assertThat(batch1.recordCount()).isEqualTo(4);

        // Second call throws error: page 2's partial batch wasn't published,
        // queue is empty, so checkError() throws the producer's error
        assertThatThrownBy(buffer::awaitNextBatch)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated error on page 3");
    }

    /**
     * Tests normal operation: all pages processed successfully.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testNormalOperationAllPagesProcessed() {
        ColumnSchema column = new ColumnSchema(
                "test_col", PhysicalType.INT32, RepetitionType.REQUIRED,
                null, 0, 0, 0, null);

        ColumnAssemblyBuffer buffer = new ColumnAssemblyBuffer(column, BATCH_SIZE);

        Page page1 = new Page.IntPage(new int[]{1, 2, 3, 4}, null, null, 0, 4);
        Page page2 = new Page.IntPage(new int[]{5, 6, 7}, null, null, 0, 3);
        Page page3 = new Page.IntPage(new int[]{8, 9, 10}, null, null, 0, 3);

        producePages(buffer, List.of(page1, page2, page3));

        // Consume all batches
        int totalRows = 0;
        while (true) {
            TypedColumnData batch = buffer.awaitNextBatch();
            if (batch == null) {
                break;
            }
            totalRows += batch.recordCount();
        }

        // All 10 rows should be received
        assertThat(totalRows).isEqualTo(10);
    }

    private static void producePages(ColumnAssemblyBuffer buffer, List<Page> pages) {
        Thread.startVirtualThread(() -> {
            for (Page page : pages) {
                buffer.appendPage(page);
            }
            buffer.finish();
        });
    }

    private static void producePagesWithError(ColumnAssemblyBuffer buffer, List<Page> pages, int errorAfterPage) {
        Thread.startVirtualThread(() -> {
            for (int i = 0; i < pages.size(); i++) {
                if (i >= errorAfterPage) {
                    buffer.signalError(new RuntimeException("Simulated error on page " + (i + 1)));
                    return;
                }
                buffer.appendPage(pages.get(i));
            }
            buffer.finish();
        });
    }
}
