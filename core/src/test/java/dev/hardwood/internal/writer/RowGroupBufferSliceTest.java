/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RowGroupBufferSliceTest {

    /// A narrow schema takes more records per slice than a wide one, in powers of two, within
    /// the bounds: 2^18 values over the columns, rounded down.
    @ParameterizedTest
    @CsvSource({
            "1, 32768",
            "4, 32768",
            "5, 32768",
            "6, 32768",
            "13, 16384",
            "16, 16384",
            "17, 8192",
            "64, 4096",
            "100, 4096",
            "10000, 4096"
    })
    void sizesSlicesByValuesPerColumn(int columns, int expectedRecords) {
        assertThat(RowGroupBuffer.recordsPerSlice(columns)).isEqualTo(expectedRecords);
    }

    @Test
    void rejectsASchemaWithoutColumns() {
        assertThatThrownBy(() -> RowGroupBuffer.recordsPerSlice(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A schema has at least one column: 0");
    }
}
