/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal.table;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;

class RowTableTest {

    @Test
    void rendersUint32MaxAsUnsigned() {
        SchemaNode schema = uint32Node("col");
        // 0xFFFF_FFFF = 4294967295 unsigned, but -1 as signed int
        assertThat(RowTable.renderValue(-1, schema)).isEqualTo("4294967295");
    }

    @Test
    void rendersUint32LargeValueAsUnsigned() {
        SchemaNode schema = uint32Node("col");
        // 3000000000 unsigned = 0xB2D05E00, which is -1294967296 as signed int
        int signed = (int) 3_000_000_000L;
        assertThat(RowTable.renderValue(signed, schema)).isEqualTo("3000000000");
    }

    @Test
    void rendersUint32PositiveValueUnchanged() {
        SchemaNode schema = uint32Node("col");
        assertThat(RowTable.renderValue(42, schema)).isEqualTo("42");
    }

    @Test
    void rendersUint64MaxAsUnsigned() {
        SchemaNode schema = uint64Node("col");
        // 0xFFFF_FFFF_FFFF_FFFF = 18446744073709551615 unsigned, but -1 as signed long
        assertThat(RowTable.renderValue(-1L, schema)).isEqualTo("18446744073709551615");
    }

    @Test
    void rendersUint64LargeValueAsUnsigned() {
        SchemaNode schema = uint64Node("col");
        // 10000000000000000000 unsigned = 0x8AC7230489E80000, negative as signed long
        long signed = Long.parseUnsignedLong("10000000000000000000");
        assertThat(RowTable.renderValue(signed, schema)).isEqualTo("10000000000000000000");
    }

    @Test
    void rendersUint64PositiveValueUnchanged() {
        SchemaNode schema = uint64Node("col");
        assertThat(RowTable.renderValue(42L, schema)).isEqualTo("42");
    }

    @Test
    void rendersSignedInt32AsIs() {
        SchemaNode schema = int32Node("col");
        assertThat(RowTable.renderValue(-1, schema)).isEqualTo("-1");
    }

    @Test
    void rendersSignedInt64AsIs() {
        SchemaNode schema = int64Node("col");
        assertThat(RowTable.renderValue(-1L, schema)).isEqualTo("-1");
    }

    @Test
    void rendersNullValue() {
        SchemaNode schema = uint32Node("col");
        assertThat(RowTable.renderValue(null, schema)).isEqualTo("null");
    }

    private static SchemaNode uint32Node(String name) {
        return new SchemaNode.PrimitiveNode(name, PhysicalType.INT32, RepetitionType.OPTIONAL,
                new LogicalType.IntType(32, false), 0, 1, 0);
    }

    private static SchemaNode uint64Node(String name) {
        return new SchemaNode.PrimitiveNode(name, PhysicalType.INT64, RepetitionType.OPTIONAL,
                new LogicalType.IntType(64, false), 0, 1, 0);
    }

    private static SchemaNode int32Node(String name) {
        return new SchemaNode.PrimitiveNode(name, PhysicalType.INT32, RepetitionType.OPTIONAL,
                new LogicalType.IntType(32, true), 0, 1, 0);
    }

    private static SchemaNode int64Node(String name) {
        return new SchemaNode.PrimitiveNode(name, PhysicalType.INT64, RepetitionType.OPTIONAL,
                new LogicalType.IntType(64, true), 0, 1, 0);
    }
}
