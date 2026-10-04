/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType;

/// Writer for `BloomFilterHeader` to Thrift Compact Protocol, the inverse of
/// [BloomFilterHeaderReader]. The writer produces one kind of filter, so only its size varies:
/// a split-block filter (`BLOCK`), hashed with `XXHASH`, stored `UNCOMPRESSED`.
public class BloomFilterHeaderWriter {

    public static void write(ThriftCompactWriter writer, int numBytes) {
        short saved = writer.pushFieldIdContext();
        try {
            // 1: numBytes
            writer.writeFieldBegin(1, FieldType.I32);
            writer.writeI32(numBytes);
            // 2: algorithm, 3: hash, 4: compression — each a union whose first variant is an
            // empty struct
            writeFirstVariant(writer, 2);
            writeFirstVariant(writer, 3);
            writeFirstVariant(writer, 4);
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }

    /// Writes a union under `fieldId` holding variant 1 with an empty struct as its value.
    private static void writeFirstVariant(ThriftCompactWriter writer, int fieldId) {
        writer.writeFieldBegin(fieldId, FieldType.STRUCT);
        short union = writer.pushFieldIdContext();
        writer.writeFieldBegin(1, FieldType.STRUCT);
        short variant = writer.pushFieldIdContext();
        writer.writeFieldStop();
        writer.popFieldIdContext(variant);
        writer.writeFieldStop();
        writer.popFieldIdContext(union);
    }
}
