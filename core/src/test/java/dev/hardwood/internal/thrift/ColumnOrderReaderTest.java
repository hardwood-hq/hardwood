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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;
import dev.hardwood.metadata.ColumnOrder;

import static dev.hardwood.internal.thrift.ThriftCompactConstants.STOP;
import static dev.hardwood.internal.thrift.ThriftStructBuilder.fieldHeader;
import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [ColumnOrderReader], decoding the `ColumnOrder` Thrift union.
///
/// Each union entry is a struct holding one member field (an empty marker struct) followed by a
/// STOP byte.
class ColumnOrderReaderTest {

    /// The header byte of a union member. Every member is an empty marker struct, so the wire
    /// type is always STRUCT and only the field id tells the members apart.
    private static byte member(int fieldId) {
        return fieldHeader(fieldId, FieldType.STRUCT);
    }

    private static ColumnOrder decode(byte... bytes) throws Exception {
        return ColumnOrderReader.read(new ThriftCompactReader(ByteBuffer.wrap(bytes)));
    }

    @Test
    void decodesTypeDefinedOrder() throws Exception {
        // field id 1 (TYPE_ORDER); empty marker struct STOP; union STOP
        assertThat(decode(member(1), STOP, STOP))
                .isEqualTo(ColumnOrder.TYPE_DEFINED_ORDER);
    }

    @Test
    void decodesIeee754TotalOrder() throws Exception {
        // field id 2 (IEEE_754_TOTAL_ORDER)
        assertThat(decode(member(2), STOP, STOP))
                .isEqualTo(ColumnOrder.IEEE754_TOTAL_ORDER);
    }

    @Test
    void decodesInt96TimestampOrder() throws Exception {
        // field id 3 (INT96_TIMESTAMP_ORDER)
        assertThat(decode(member(3), STOP, STOP))
                .isEqualTo(ColumnOrder.INT96_TIMESTAMP_ORDER);
    }

    @Test
    void decodesUnknownUnionMemberAsUnknown() throws Exception {
        // field id 4 — a future union member Hardwood does not recognize
        assertThat(decode(member(4), STOP, STOP))
                .isEqualTo(ColumnOrder.UNKNOWN);
    }

    /// [ColumnOrderWriter] is the inverse: every order it can write reads back equal.
    @ParameterizedTest
    @EnumSource(value = ColumnOrder.class, names = "UNKNOWN", mode = EnumSource.Mode.EXCLUDE)
    void readsBackWhatTheWriterWrote(ColumnOrder order) throws Exception {
        ThriftCompactWriter writer = new ThriftCompactWriter();
        ColumnOrderWriter.write(writer, order);

        assertThat(ColumnOrderReader.read(new ThriftCompactReader(ByteBuffer.wrap(writer.toByteArray()))))
                .isEqualTo(order);
    }

    @Test
    void decodesEmptyUnionAsUnknown() throws Exception {
        // An empty union (immediate STOP) carries no member; treat leniently as UNKNOWN.
        assertThat(decode(STOP)).isEqualTo(ColumnOrder.UNKNOWN);
    }
}
