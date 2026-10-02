/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.conversion;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FixedWidthsTest {

    @Test
    void aFixedWidthTypeHasThePlainEncodingsWidth() {
        assertThat(FixedWidths.of(PhysicalType.INT32)).isEqualTo(4);
        assertThat(FixedWidths.of(PhysicalType.FLOAT)).isEqualTo(4);
        assertThat(FixedWidths.of(PhysicalType.INT64)).isEqualTo(8);
        assertThat(FixedWidths.of(PhysicalType.DOUBLE)).isEqualTo(8);
        assertThat(FixedWidths.of(PhysicalType.INT96)).isEqualTo(12);
    }

    /// A `BOOLEAN` is one bit, a `BYTE_ARRAY` varies per value and a `FIXED_LEN_BYTE_ARRAY` is as
    /// wide as its column declares, so none has a width the type alone states.
    @Test
    void aTypeWithoutAWidthOfItsOwnIsRefused() {
        assertThatThrownBy(() -> FixedWidths.of(PhysicalType.BOOLEAN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("BOOLEAN has no fixed byte width");
        assertThatThrownBy(() -> FixedWidths.of(PhysicalType.BYTE_ARRAY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("BYTE_ARRAY has no fixed byte width");
        assertThatThrownBy(() -> FixedWidths.of(PhysicalType.FIXED_LEN_BYTE_ARRAY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("FIXED_LEN_BYTE_ARRAY has no fixed byte width");
    }
}
