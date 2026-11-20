/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.memory;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkingSetOutcomeTablesTest {

    private static final int ENTRIES = 1000;
    private static final int PAGE_BYTES = 10_000;
    /// The page's bytes, an offset per entry and one more, and the page again while it is parsed.
    private static final long WITHOUT_TABLES = PAGE_BYTES + Integer.BYTES * (ENTRIES + 1L) + PAGE_BYTES;

    @Test
    void binaryDictionaryCountsAByteForEachEntryOfEachOutcomeTable() {
        long withTwoTables = WITHOUT_TABLES + 2L * ENTRIES;

        assertThatCode(() -> column(withTwoTables, 2, PhysicalType.BYTE_ARRAY).dictionary(ENTRIES, PAGE_BYTES))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> column(withTwoTables - 1, 2, PhysicalType.BYTE_ARRAY)
                .dictionary(ENTRIES, PAGE_BYTES))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Dictionary page needs 26004 bytes to decode, which takes the read's working set to "
                        + "26004 bytes, more than its limit of 26003 (hardwood.internal.maxWorkingSetBytes)");
    }

    @Test
    void columnWithoutOutcomeTablesCountsTheEntriesAlone() {
        assertThatCode(() -> column(WITHOUT_TABLES, 0, PhysicalType.BYTE_ARRAY).dictionary(ENTRIES, PAGE_BYTES))
                .doesNotThrowAnyException();
    }

    private static WorkingSet.Column column(long limit, int outcomeTables, PhysicalType type) {
        ColumnSchema schema = new ColumnSchema(FieldPath.of("c"), type, RepetitionType.REQUIRED, null, 0, 0, 0,
                null, null);
        return new WorkingSet(1, limit, new int[]{ outcomeTables }).column(0, schema);
    }
}
