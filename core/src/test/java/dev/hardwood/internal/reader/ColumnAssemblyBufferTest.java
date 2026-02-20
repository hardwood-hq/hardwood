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

import dev.hardwood.HardwoodContext;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ColumnAssemblyBufferTest {

    private static final int BATCH_SIZE = 4;

    /**
     * Tests that exceptions in the assembly thread are propagated to the consumer.
     * Uses a PageCursor subclass that throws an error after returning two pages.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testAssemblyThreadExceptionPropagatedToConsumer() throws Exception {
        ColumnSchema column = new ColumnSchema(
                "test_col", PhysicalType.INT32, RepetitionType.REQUIRED,
                null, 0, 0, 0, null);

        ColumnAssemblyBuffer buffer = new ColumnAssemblyBuffer(column, BATCH_SIZE);

        // 3 pages: the cursor will return 2 successfully, then throw on the 3rd
        Page page1 = new Page.IntPage(new int[]{1, 2, 3, 4}, null, null, 0, 4);
        Page page2 = new Page.IntPage(new int[]{5, 6, 7}, null, null, 0, 3);
        Page page3 = new Page.IntPage(new int[]{8, 9, 10}, null, null, 0, 3);

        try (HardwoodContext context = HardwoodContext.create()) {
            ErrorInducingPageCursor cursor = new ErrorInducingPageCursor(
                    List.of(page1, page2, page3),
                    context,
                    buffer,
                    2  // throw after returning 2 pages
            );

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
    }

    /**
     * Tests normal operation: all pages processed successfully.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testNormalOperationAllPagesProcessed() throws Exception {
        ColumnSchema column = new ColumnSchema(
                "test_col", PhysicalType.INT32, RepetitionType.REQUIRED,
                null, 0, 0, 0, null);

        ColumnAssemblyBuffer buffer = new ColumnAssemblyBuffer(column, BATCH_SIZE);

        Page page1 = new Page.IntPage(new int[]{1, 2, 3, 4}, null, null, 0, 4);
        Page page2 = new Page.IntPage(new int[]{5, 6, 7}, null, null, 0, 3);
        Page page3 = new Page.IntPage(new int[]{8, 9, 10}, null, null, 0, 3);

        try (HardwoodContext context = HardwoodContext.create()) {
            // Create a PageCursor that processes all pages successfully
            ErrorInducingPageCursor cursor = new ErrorInducingPageCursor(
                    List.of(page1, page2, page3),
                    context,
                    buffer,
                    -1  // no error
            );

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
    }

    /**
     * A PageCursor subclass that returns pre-defined pages and optionally
     * throws an error after a specified number of pages.
     */
    private static class ErrorInducingPageCursor extends PageCursor {

        private final List<Page> pages;
        private final int errorAfterPage;  // -1 means no error
        private int currentPage = 0;

        ErrorInducingPageCursor(List<Page> pages, HardwoodContext context,
                                ColumnAssemblyBuffer assemblyBuffer, int errorAfterPage) {
            // Pass empty pageInfos - we override nextPage() and hasNext()
            super(List.of(), context, assemblyBuffer);
            this.pages = pages;
            this.errorAfterPage = errorAfterPage;
        }

        @Override
        public boolean hasNext() {
            return currentPage < pages.size();
        }

        @Override
        public Page nextPage() {
            if (currentPage >= pages.size()) {
                return null;
            }

            // Check if we should throw an error
            if (errorAfterPage >= 0 && currentPage >= errorAfterPage) {
                throw new RuntimeException("Simulated error on page " + (currentPage + 1));
            }

            return pages.get(currentPage++);
        }
    }
}
