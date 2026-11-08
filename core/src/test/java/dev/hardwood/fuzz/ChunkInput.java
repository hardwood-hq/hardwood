/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.fuzz;

import java.nio.ByteBuffer;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.ColumnSchema;

/// The input of [PageFuzzTest]: the column a chunk's pages belong to and the chunk's codec in its
/// first [#SHAPE_BYTES] bytes, the chunk's pages, header first, after them. Every byte string of
/// at least [#SHAPE_BYTES] bytes is an input; [#encode] writes the input of a column and pages a
/// file holds, as the seeds are.
///
/// @param pages the pages, from the chunk's first
record ChunkInput(ColumnSchema column, CompressionCodec codec, ByteBuffer pages) {

    static final int SHAPE_BYTES = 5;
    static final int MAX_DEFINITION_LEVEL = 7;
    static final int MAX_REPETITION_LEVEL = 3;
    static final int MAX_TYPE_LENGTH = 64;

    private static final PhysicalType[] TYPES = PhysicalType.values();
    private static final CompressionCodec[] CODECS = CompressionCodec.values();
    /// In the last shape byte, beside the type length less one: whether a leaf with as many
    /// definition as repetition levels is itself `REPEATED`, rather than a `REQUIRED` element
    private static final int REPEATED_BIT = 0x80;

    /// The input `bytes` read as, `null` where they are shorter than its shape.
    static ChunkInput of(byte[] bytes) {
        if (bytes.length < SHAPE_BYTES) {
            return null;
        }
        PhysicalType type = TYPES[unsigned(bytes, 0) % TYPES.length];
        int maxRepetition = unsigned(bytes, 2) % (MAX_REPETITION_LEVEL + 1);
        int maxDefinition = Math.max(maxRepetition, unsigned(bytes, 1) % (MAX_DEFINITION_LEVEL + 1));
        CompressionCodec codec = CODECS[unsigned(bytes, 3) % CODECS.length];
        int last = unsigned(bytes, 4);
        Integer typeLength = type == PhysicalType.FIXED_LEN_BYTE_ARRAY ? 1 + (last & 0x7F) % MAX_TYPE_LENGTH : null;
        RepetitionType repetition = maxDefinition > maxRepetition ? RepetitionType.OPTIONAL
                : maxRepetition > 0 && (last & REPEATED_BIT) != 0 ? RepetitionType.REPEATED
                : RepetitionType.REQUIRED;
        FieldPath path = maxRepetition > 0 ? FieldPath.of("c", "list", "element") : FieldPath.of("c");
        ColumnSchema column = new ColumnSchema(path, type, repetition, typeLength, 0, maxDefinition, maxRepetition,
                null, null);
        return new ChunkInput(column, codec, ByteBuffer.wrap(bytes, SHAPE_BYTES, bytes.length - SHAPE_BYTES).slice());
    }

    /// The input of `pages` of `column`'s chunk compressed with `codec`, `null` where the column
    /// has a shape [#of] does not express.
    static byte[] encode(ColumnSchema column, CompressionCodec codec, byte[] pages) {
        Integer typeLength = column.typeLength();
        if (column.maxDefinitionLevel() > MAX_DEFINITION_LEVEL || column.maxRepetitionLevel() > MAX_REPETITION_LEVEL
                || column.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY
                        && (typeLength == null || typeLength < 1 || typeLength > MAX_TYPE_LENGTH)) {
            return null;
        }
        byte[] input = new byte[SHAPE_BYTES + pages.length];
        input[0] = (byte) column.type().ordinal();
        input[1] = (byte) column.maxDefinitionLevel();
        input[2] = (byte) column.maxRepetitionLevel();
        input[3] = (byte) codec.ordinal();
        int last = column.type() == PhysicalType.FIXED_LEN_BYTE_ARRAY ? typeLength - 1 : 0;
        if (column.repetitionType() == RepetitionType.REPEATED) {
            last |= REPEATED_BIT;
        }
        input[4] = (byte) last;
        System.arraycopy(pages, 0, input, SHAPE_BYTES, pages.length);
        return input;
    }

    private static int unsigned(byte[] bytes, int at) {
        return bytes[at] & 0xFF;
    }
}
