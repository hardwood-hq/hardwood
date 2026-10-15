/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.ColumnOrder;

/// Writer for the `ColumnOrder` union to Thrift Compact Protocol, the inverse of
/// [ColumnOrderReader]. Each member is an empty marker struct whose field id names the ordering.
public class ColumnOrderWriter {

    /// Thrift union field id of the `TYPE_ORDER` (`TypeDefinedOrder`) member.
    private static final int TYPE_ORDER = 1;
    /// Thrift union field id of the `IEEE_754_TOTAL_ORDER` (`IEEE754TotalOrder`) member.
    private static final int IEEE_754_TOTAL_ORDER = 2;
    /// Thrift union field id of the `INT96_TIMESTAMP_ORDER` (`Int96TimestampOrder`) member.
    private static final int INT96_TIMESTAMP_ORDER = 3;

    /// @throws IllegalArgumentException if the order is [ColumnOrder#UNKNOWN], which names no
    ///         union member and so cannot be written
    public static void write(ThriftCompactWriter writer, ColumnOrder order) {
        short saved = writer.pushFieldIdContext();
        try {
            writer.writeFieldBegin(memberFieldId(order), ThriftCompactConstants.FieldType.STRUCT);
            short member = writer.pushFieldIdContext();
            writer.writeFieldStop(); // empty marker struct
            writer.popFieldIdContext(member);
            writer.writeFieldStop();
        }
        finally {
            writer.popFieldIdContext(saved);
        }
    }

    private static int memberFieldId(ColumnOrder order) {
        return switch (order) {
            case TYPE_DEFINED_ORDER -> TYPE_ORDER;
            case IEEE754_TOTAL_ORDER -> IEEE_754_TOTAL_ORDER;
            case INT96_TIMESTAMP_ORDER -> INT96_TIMESTAMP_ORDER;
            case UNKNOWN -> throw new IllegalArgumentException(
                    "An unrecognized column order names no union member and cannot be written");
        };
    }
}
