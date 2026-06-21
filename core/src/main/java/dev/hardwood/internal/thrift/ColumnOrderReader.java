/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.ColumnOrder;

/// Reader for the `ColumnOrder` union from Thrift Compact Protocol.
///
/// `ColumnOrder` is a union, so exactly one member field is set per entry. The set field id
/// identifies the ordering; its value is an empty marker struct that carries no further data.
public class ColumnOrderReader {

    /// Thrift union field id of the `TYPE_ORDER` (`TypeDefinedOrder`) member.
    private static final short TYPE_ORDER = 1;
    /// Thrift union field id of the `IEEE_754_TOTAL_ORDER` (`IEEE754TotalOrder`) member.
    private static final short IEEE_754_TOTAL_ORDER = 2;

    public static ColumnOrder read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            ColumnOrder order = ColumnOrder.UNKNOWN;
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            while (header != null) {
                order = switch (header.fieldId()) {
                    case TYPE_ORDER -> ColumnOrder.TYPE_DEFINED_ORDER;
                    case IEEE_754_TOTAL_ORDER -> ColumnOrder.IEEE754_TOTAL_ORDER;
                    default -> ColumnOrder.UNKNOWN;
                };
                reader.skipField(header.type());
                header = reader.readFieldHeader();
            }
            return order;
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }
}
