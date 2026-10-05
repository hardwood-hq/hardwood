/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.row;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.reader.batch.NestedBatch;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.SchemaNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The generic decode of a leaf inside a struct, list or map, which every nested flyweight's
/// `getValue` reaches through [NestedBatchIndex#decodeLeaf].
class NestedBatchIndexDecodeTest {

    private static final LogicalType INT_8 = LogicalType.intType(8, true);

    @Test
    void leafInsideItsAnnotationIsNarrowed() {
        NestedBatchIndex batch = int8Batch(-128, 127);

        assertThat(batch.decodeLeaf(0, 0, leaf())).isEqualTo((byte) -128);
        assertThat(batch.decodeLeaf(0, 1, leaf())).isEqualTo((byte) 127);
    }

    /// A value past its annotation fails naming the file and the column it was read from.
    @Test
    void leafPastItsAnnotationIsRefusedWithItsColumn() {
        NestedBatchIndex batch = int8Batch(300);

        assertThatThrownBy(() -> batch.decodeLeaf(0, 0, leaf()))
                .isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[nested.parquet: column 's.i8'] The INT_8 value 300 lies outside its range, "
                        + "which holds -128 to 127");
    }

    private static SchemaNode.PrimitiveNode leaf() {
        return new SchemaNode.PrimitiveNode("i8", PhysicalType.INT32, RepetitionType.REQUIRED, INT_8, 0, 0, 0, null);
    }

    private static NestedBatchIndex int8Batch(int... values) {
        NestedBatch col = new NestedBatch();
        col.values = values;
        col.valueCount = values.length;
        col.recordCount = values.length;
        col.definitionLevels = new int[values.length];
        col.fileName = "nested.parquet";
        ColumnSchema schema = new ColumnSchema(FieldPath.of("s", "i8"), PhysicalType.INT32,
                RepetitionType.REQUIRED, null, 0, 0, 0, INT_8, null);
        return NestedBatchIndex.buildFromBatches(new NestedBatch[] { col }, new ColumnSchema[] { schema },
                null, null, null);
    }
}
