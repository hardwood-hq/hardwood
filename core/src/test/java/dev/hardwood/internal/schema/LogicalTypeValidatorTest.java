/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The digits a `DECIMAL` carrier holds, as [LogicalTypeValidator#maxDecimalPrecision] counts
/// them for both the writer and the reader.
class LogicalTypeValidatorTest {

    @Test
    void theIntegerCarriersHoldNineAndEighteenDigits() {
        assertThat(LogicalTypeValidator.maxDecimalPrecision(PhysicalType.INT32, null)).isEqualTo(9);
        assertThat(LogicalTypeValidator.maxDecimalPrecision(PhysicalType.INT64, null)).isEqualTo(18);
        assertThat(LogicalTypeValidator.maxDecimalPrecision(PhysicalType.BYTE_ARRAY, null))
                .isEqualTo(Long.MAX_VALUE);
    }

    /// Counted against the digits of the largest value each width holds, `2^(8n - 1) - 1`.
    @Test
    void aFixedWidthHoldsTheDigitsOfItsLargestValue() {
        for (int width = 1; width <= 1024; width++) {
            int exact = BigInteger.ONE.shiftLeft(8 * width - 1).subtract(BigInteger.ONE)
                    .toString().length() - 1;
            assertThat(LogicalTypeValidator.maxDecimalPrecision(PhysicalType.FIXED_LEN_BYTE_ARRAY, width))
                    .as("width %d", width)
                    .isEqualTo(exact);
        }
    }

    /// The widest width an `i32` can declare holds more digits than an `int` counts.
    @Test
    void theWidestWidthIsCountedWithoutOverflow() {
        assertThat(LogicalTypeValidator.maxDecimalPrecision(PhysicalType.FIXED_LEN_BYTE_ARRAY,
                Integer.MAX_VALUE)).isEqualTo(5_171_655_943L);
    }

    @Test
    void aTypeThatStoresNoDecimalIsRefused() {
        assertThatThrownBy(() -> LogicalTypeValidator.maxDecimalPrecision(PhysicalType.BOOLEAN, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("DECIMAL is not stored in BOOLEAN");
    }

    /// A width that is absent or not positive has no digits to count.
    @Test
    void aFixedWidthThatIsNotPositiveIsRefused() {
        assertThatThrownBy(() -> LogicalTypeValidator.maxDecimalPrecision(PhysicalType.FIXED_LEN_BYTE_ARRAY, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A FIXED_LEN_BYTE_ARRAY DECIMAL needs a positive width, not 0");
        assertThatThrownBy(() -> LogicalTypeValidator.maxDecimalPrecision(PhysicalType.FIXED_LEN_BYTE_ARRAY, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("A FIXED_LEN_BYTE_ARRAY DECIMAL needs a positive width, not null");
    }
}
