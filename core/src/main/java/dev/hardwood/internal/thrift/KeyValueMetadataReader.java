/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/// Reads a Thrift-encoded `list<KeyValue>` into an unmodifiable `Map<String, String>`.
class KeyValueMetadataReader {

    /// Reads a key-value metadata list from the given reader, which must be positioned
    /// right after the list field header has been consumed (i.e. ready to read the list header).
    static Map<String, String> read(ThriftCompactReader reader) throws IOException {
        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
        Map<String, String> result = new LinkedHashMap<>(listHeader.size());
        for (int i = 0; i < listHeader.size(); i++) {
            readKeyValue(reader, result);
        }
        return Collections.unmodifiableMap(result);
    }

    /// Reads a single KeyValue Thrift struct (field 1: key, field 2: value) and puts it into the map.
    private static void readKeyValue(ThriftCompactReader reader, Map<String, String> target) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            String key = null;
            String value = null;

            while (true) {
                ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
                if (header == null) {
                    break;
                }

                switch (header.fieldId()) {
                    case 1: // key (required string)
                        if (header.type() == 0x08) {
                            key = reader.readString();
                        }
                        else {
                            reader.skipField(header.type());
                        }
                        break;
                    case 2: // value (optional string)
                        if (header.type() == 0x08) {
                            value = reader.readString();
                        }
                        else {
                            reader.skipField(header.type());
                        }
                        break;
                    default:
                        reader.skipField(header.type());
                        break;
                }
            }

            if (key != null) {
                target.put(key, value);
            }
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }
}
