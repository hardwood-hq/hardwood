/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.ColumnOrder;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [ColumnOrderReader], decoding the `ColumnOrder` Thrift union.
///
/// Each union entry is a struct holding one member field (an empty marker struct) followed by a
/// STOP byte. The field header byte packs the field-id delta in the high nibble and the Thrift
/// type (0x0C = STRUCT) in the low nibble.
class ColumnOrderReaderTest {

    private static ColumnOrder decode(byte... bytes) throws Exception {
        return ColumnOrderReader.read(new ThriftCompactReader(ByteBuffer.wrap(bytes)));
    }

    @Test
    void decodesTypeDefinedOrder() throws Exception {
        // field id 1 (TYPE_ORDER), STRUCT type → header 0x1C; empty marker struct STOP; union STOP
        assertThat(decode((byte) 0x1C, (byte) 0x00, (byte) 0x00))
                .isEqualTo(ColumnOrder.TYPE_DEFINED_ORDER);
    }

    @Test
    void decodesIeee754TotalOrder() throws Exception {
        // field id 2 (IEEE_754_TOTAL_ORDER) → header 0x2C
        assertThat(decode((byte) 0x2C, (byte) 0x00, (byte) 0x00))
                .isEqualTo(ColumnOrder.IEEE754_TOTAL_ORDER);
    }

    @Test
    void decodesUnknownUnionMemberAsUnknown() throws Exception {
        // field id 3 — a future union member Hardwood does not recognize → header 0x3C
        assertThat(decode((byte) 0x3C, (byte) 0x00, (byte) 0x00))
                .isEqualTo(ColumnOrder.UNKNOWN);
    }

    @Test
    void decodesEmptyUnionAsUnknown() throws Exception {
        // An empty union (immediate STOP) carries no member; treat leniently as UNKNOWN.
        assertThat(decode((byte) 0x00)).isEqualTo(ColumnOrder.UNKNOWN);
    }
}
