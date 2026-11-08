/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Levels above their column's maximum. A level is stored at the bit width of its maximum, so it
/// can exceed the maximum wherever that is not one less than a power of two; such a level
/// describes no position in the schema, and a page holding one is refused.
class LevelBoundsTest {

    /// An optional leaf in an optional group: definition levels up to 2, stored at two bits.
    private static final ColumnSchema FLAT = new ColumnSchema(FieldPath.of("g", "c"), PhysicalType.INT32,
            RepetitionType.OPTIONAL, null, 0, 2, 0, null, null);

    /// A list of lists of required elements: repetition levels up to 2, definition levels up to 3,
    /// both stored at two bits.
    private static final ColumnSchema NESTED = new ColumnSchema(FieldPath.of("c", "list", "element", "list",
            "element"), PhysicalType.INT32, RepetitionType.REQUIRED, null, 0, 3, 2, null, null);

    @Test
    void readsLevelsAtTheirMaximum() {
        byte[] body = DataPages.concat(DataPages.levelSection(2, 2, 0, 2, 1), plainInts(10, 20));

        assertThat((int[]) read(FLAT, DataPages.page(Encoding.PLAIN, 4, body))).containsExactly(10, 0, 20, 0);
    }

    @Test
    void rejectsADefinitionLevelAboveItsMaximumInABitPackedRun() {
        byte[] body = DataPages.concat(DataPages.levelSection(3, 2, 3, 2, 1), plainInts(10, 20));

        assertThatThrownBy(() -> read(FLAT, DataPages.page(Encoding.PLAIN, 4, body)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: definition level 3 exceeds the column's maximum of 2");
    }

    /// No level reaches the maximum, so the page holds no values, and without the check every
    /// entry would read as null.
    @Test
    void rejectsADefinitionLevelAboveItsMaximumInAnRleRun() {
        byte[] body = DataPages.levelSection(3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3);

        assertThatThrownBy(() -> read(FLAT, DataPages.page(Encoding.PLAIN, 10, body)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: definition level 3 exceeds the column's maximum of 2");
    }

    @Test
    void rejectsADefinitionLevelAboveItsMaximumInBitPackedLevels() {
        byte[] body = DataPages.concat(DataPages.bitPackedLevels(3, 2, 3, 2, 1), plainInts(10, 20));

        assertThatThrownBy(() -> read(FLAT,
                DataPages.page(Encoding.PLAIN, Encoding.BIT_PACKED, Encoding.BIT_PACKED, 4, body)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: definition level 3 exceeds the column's maximum of 2");
    }

    @Test
    void rejectsARepetitionLevelAboveItsMaximum() {
        byte[] body = DataPages.concat(DataPages.levelSection(3, 0, 3, 2), DataPages.levelSection(3, 3, 3, 3),
                plainInts(1, 2, 3));

        assertThatThrownBy(() -> read(NESTED, DataPages.page(Encoding.PLAIN, 3, body)))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: repetition level 3 exceeds the column's maximum of 2");
    }

    @Test
    void rejectsARepetitionLevelAboveItsMaximumInAV2Page() {
        ByteBuffer page = DataPages.pageV2(Encoding.PLAIN, 3, 1, DataPages.levels(3, 0, 3, 2),
                DataPages.levels(3, 3, 3, 3), plainInts(1, 2, 3), CompressionCodec.UNCOMPRESSED, false);

        assertThatThrownBy(() -> read(NESTED, page))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Invalid column chunk: repetition level 3 exceeds the column's maximum of 2");
    }

    private static Object read(ColumnSchema column, ByteBuffer page) {
        return DataPages.read(DataPages.decoder(column, CompressionCodec.UNCOMPRESSED), column, page, null);
    }

    private static byte[] plainInts(int... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int value : values) {
            buffer.putInt(value);
        }
        return buffer.array();
    }
}
