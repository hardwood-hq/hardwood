/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.row.PqList;

import static dev.hardwood.internal.reader.memory.WorkingSet.MAX_WORKING_SET_BYTES_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// A record whose values span many pages counts every page its batch holds against the read's
/// working set, not its largest page alone.
class RecordSpanningPagesTest {

    private static final int PAGE_VALUES = 64 * 1024;
    private static final int PAGES = 32;

    @AfterEach
    void clearLimit() {
        System.clearProperty(MAX_WORKING_SET_BYTES_PROPERTY);
    }

    /// One record of 2 Mi values over 32 pages, each page well within the limit and the pages
    /// together not: the read fails as its batch takes the page that crosses the limit, on a context
    /// of zero threads, whose read places the pages in the same order each run.
    @Test
    void recordWhosePagesTogetherExceedTheLimitFails() {
        System.setProperty(MAX_WORKING_SET_BYTES_PROPERTY, Long.toString(16L * 1024 * 1024));
        byte[] file = RleExpansionFile.buildOneRecord(PAGE_VALUES, PAGES);

        assertThatThrownBy(() -> readOneRecord(file))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("[<memory>: row group 0, column 'b', page 7] Batch of values from 8 pages needs 16777386 "
                        + "bytes to decode, which takes the read's working set to 16777402 bytes, more than its limit "
                        + "of 16777216 (hardwood.internal.maxWorkingSetBytes)");
    }

    /// The same record within the limit reads every value.
    @Test
    void recordSpanningPagesWithinTheLimitReads() throws IOException {
        assertThat(readOneRecord(RleExpansionFile.buildOneRecord(PAGE_VALUES, PAGES)))
                .isEqualTo((long) PAGE_VALUES * PAGES);
    }

    /// Reads the file's one record and returns how many values its list holds, each checked.
    private static long readOneRecord(byte[] file) throws IOException {
        try (HardwoodContext context = HardwoodContext.create(0);
                ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                RowReader rows = reader.buildRowReader().build()) {
            assertThat(rows.hasNext()).isTrue();
            rows.next();
            PqList values = rows.getList("b");
            long sum = 0;
            for (int i = 0; i < values.size(); i++) {
                sum += values.longs().get(i);
            }
            assertThat(sum).isZero();
            assertThat(rows.hasNext()).isFalse();
            return values.size();
        }
    }
}
