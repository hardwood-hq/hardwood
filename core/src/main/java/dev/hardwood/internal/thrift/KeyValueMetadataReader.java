/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;

/// Reads a Thrift-encoded `list<KeyValue>` into an unmodifiable `Map<String, String>`.
///
/// The field is optional wherever it appears, so a list whose entries are not all key-value
/// pairs is reported as absent (an empty map) and logged at WARNING. Bytes that are not valid
/// Thrift, such as an invalid wire type or a list running past the buffer, still fail the read.
class KeyValueMetadataReader {

    private static final System.Logger LOG = System.getLogger(KeyValueMetadataReader.class.getName());

    /// Reads a key-value metadata list from the given reader, which must be positioned
    /// right after the list field header has been consumed (i.e. ready to read the list header).
    ///
    /// A list declaring anything but struct elements, or holding an entry without a `key` of
    /// wire type `binary`, is reported as an empty map. The reader is left positioned on the
    /// byte after the list either way.
    static Map<String, String> read(ThriftCompactReader reader) {
        long listHeader = reader.acceptListHeader(Codes.STRUCT);
        if (listHeader == ThriftCompactReader.ABSENT_LIST) {
            return Map.of();
        }
        int size = ThriftCompactReader.listSize(listHeader);
        Map<String, String> result = new LinkedHashMap<>(size);
        int malformed = 0;
        for (int i = 0; i < size; i++) {
            if (!readKeyValue(reader, result)) {
                malformed++;
            }
        }
        if (malformed > 0) {
            int dropped = malformed;
            LOG.log(System.Logger.Level.WARNING, () -> "Ignoring " + reader.fieldHere() + ": "
                    + dropped + " of " + size + " entries are missing a required field");
            return Map.of();
        }
        return Collections.unmodifiableMap(result);
    }

    /// Reads a single KeyValue Thrift struct (field 1: key, field 2: value) and puts it into the
    /// map.
    ///
    /// Returns `false` for a struct that does not carry its required `key` as `binary`, which is
    /// then not put. The struct is consumed either way, leaving the reader on the next element.
    private static boolean readKeyValue(ThriftCompactReader reader, Map<String, String> target) {
        int saved = reader.pushFieldIdContext(ThriftStruct.KEY_VALUE);
        try {
            String key = null;
            String value = null;

            while (true) {
                int header = reader.readFieldHeader();
                if (header == ThriftCompactReader.STOP_FIELD) {
                    break;
                }

                switch (ThriftCompactReader.fieldId(header)) {
                    case 1: // key (required string)
                        if (reader.acceptField(header, Codes.BINARY)) {
                            key = reader.readString();
                        }
                        break;
                    case 2: // value (optional string)
                        if (reader.acceptField(header, Codes.BINARY)) {
                            value = reader.readString();
                        }
                        break;
                    default:
                        reader.skipField(ThriftCompactReader.fieldType(header));
                        break;
                }
            }

            if (key == null) {
                return false;
            }
            target.put(key, value);
            return true;
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }
}
