/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

class NestedLeafDecoderTest {

    @Test
    void decodesStringWhenStringLogicalTypeIsSet() {
        SchemaNode.PrimitiveNode schema = primitive(PhysicalType.BYTE_ARRAY, LogicalType.string());
        byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);

        Object result = NestedLeafDecoder.decode(bytes, schema);

        assertThat(result).isInstanceOf(String.class).isEqualTo("hello");
    }

    /// A bare BYTE_ARRAY with no logical type may carry arbitrary binary payloads
    /// (Protobuf, WKB, custom encodings). UTF-8 decoding would silently corrupt
    /// non-UTF-8 bytes with U+FFFD replacement characters, so the raw bytes must
    /// pass through unchanged.
    @Test
    void returnsRawBytesForBareByteArray() {
        SchemaNode.PrimitiveNode schema = primitive(PhysicalType.BYTE_ARRAY, null);
        byte[] bytes = new byte[] {(byte) 0xC3, (byte) 0x28, (byte) 0xA0, (byte) 0xA1};

        Object result = NestedLeafDecoder.decode(bytes, schema);

        assertThat(result).isInstanceOf(byte[].class).isEqualTo(bytes);
    }

    @Test
    void returnsRawBytesForBsonLogicalType() {
        SchemaNode.PrimitiveNode schema = primitive(PhysicalType.BYTE_ARRAY, LogicalType.bson());
        byte[] bytes = new byte[] {0x05, 0x00, 0x00, 0x00, 0x00};

        Object result = NestedLeafDecoder.decode(bytes, schema);

        assertThat(result).isInstanceOf(byte[].class).isEqualTo(bytes);
    }

    @Test
    void passesThroughNull() {
        SchemaNode.PrimitiveNode schema = primitive(PhysicalType.BYTE_ARRAY, null);

        assertThat(NestedLeafDecoder.decode(null, schema)).isNull();
    }

    private static SchemaNode.PrimitiveNode primitive(PhysicalType type, LogicalType logicalType) {
        return new SchemaNode.PrimitiveNode(
                "field", type, RepetitionType.REQUIRED, logicalType, 0, 0, 0);
    }
}
