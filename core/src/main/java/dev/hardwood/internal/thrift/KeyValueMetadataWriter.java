/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Map;

final class KeyValueMetadataWriter {
    private KeyValueMetadataWriter() {}

    static void write(ThriftCompactWriter writer, Map<String, String> metadata) {
        writer.writeListBegin(metadata.size(), ThriftCompactConstants.ElementType.STRUCT);
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            short saved = writer.pushFieldIdContext();
            writer.writeFieldBegin(1, ThriftCompactConstants.FieldType.BINARY);
            writer.writeString(entry.getKey());
            if (entry.getValue() != null) {
                writer.writeFieldBegin(2, ThriftCompactConstants.FieldType.BINARY);
                writer.writeString(entry.getValue());
            }
            writer.writeFieldStop();
            writer.popFieldIdContext(saved);
        }
    }
}
