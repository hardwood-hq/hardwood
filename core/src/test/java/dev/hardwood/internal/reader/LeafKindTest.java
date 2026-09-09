/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

/// The classification both read paths share. `FlatRowReader` asks it once per column
/// and the nested flyweights per leaf, so a rule that moves here has to move for both.
class LeafKindTest {

    /// `ENUM` and `JSON` decode to `String` exactly as `UTF8` does, so all three sit
    /// on the interned-`String` path rather than decoding per value.
    @Test
    void utf8EnumAndJsonOverByteArrayAreStrings() {
        assertThat(LeafKind.of(PhysicalType.BYTE_ARRAY, new LogicalType.StringType()))
                .isEqualTo(LeafKind.STRING);
        assertThat(LeafKind.of(PhysicalType.BYTE_ARRAY, new LogicalType.EnumType()))
                .isEqualTo(LeafKind.STRING);
        assertThat(LeafKind.of(PhysicalType.BYTE_ARRAY, new LogicalType.JsonType()))
                .isEqualTo(LeafKind.STRING);
    }

    /// The annotation alone does not make a string leaf: the interning path reads a
    /// `BinaryBatchValues`, which only a `BYTE_ARRAY` column is stored as.
    @Test
    void aStringAnnotationOverAnotherPhysicalTypeIsNotAStringLeaf() {
        assertThat(LeafKind.of(PhysicalType.FIXED_LEN_BYTE_ARRAY, new LogicalType.StringType()))
                .isEqualTo(LeafKind.CONVERT);
    }

    @Test
    void unannotatedInt96IsTheConventionalTimestamp() {
        assertThat(LeafKind.of(PhysicalType.INT96, null)).isEqualTo(LeafKind.INT96_TIMESTAMP);
    }

    /// An annotation that survives into the schema is one no physical type is imposed
    /// on, `FileSchema` having dropped any that `INT96` cannot carry, so it decides the
    /// decode as it would on any other column.
    @Test
    void anAnnotatedInt96DecodesThroughItsAnnotation() {
        assertThat(LeafKind.of(PhysicalType.INT96, new LogicalType.StringType()))
                .isEqualTo(LeafKind.CONVERT);
    }

    @Test
    void anUnannotatedLeafOfAnyOtherTypeIsRaw() {
        for (PhysicalType type : List.of(PhysicalType.INT32, PhysicalType.INT64,
                PhysicalType.FLOAT, PhysicalType.DOUBLE, PhysicalType.BOOLEAN,
                PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY)) {
            assertThat(LeafKind.of(type, null)).as("%s", type).isEqualTo(LeafKind.RAW);
        }
    }

    @Test
    void anAnnotatedLeafConverts() {
        assertThat(LeafKind.of(PhysicalType.INT32, new LogicalType.DateType()))
                .isEqualTo(LeafKind.CONVERT);
    }

    @Test
    void aGroupCarriesNoLeafDecode() {
        SchemaNode group = new SchemaNode.GroupNode(
                "s", RepetitionType.REQUIRED, null, null, List.of(), 0, 0);

        assertThat(LeafKind.of(group)).isNull();
    }

    @Test
    void aLeafNodeClassifiesAsItsTypeAndAnnotationDo() {
        SchemaNode leaf = new SchemaNode.PrimitiveNode(
                "d", PhysicalType.INT32, RepetitionType.REQUIRED, new LogicalType.DateType(), 0, 0, 0);

        assertThat(LeafKind.of(leaf)).isEqualTo(LeafKind.CONVERT);
    }
}
