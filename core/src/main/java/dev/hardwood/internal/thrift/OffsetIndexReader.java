/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.metadata.PageLocation;

/// Reader for OffsetIndex from Thrift Compact Protocol.
public class OffsetIndexReader {

    public static OffsetIndex read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static OffsetIndex readInternal(ThriftCompactReader reader) throws IOException {
        List<PageLocation> pageLocations = new ArrayList<>();

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1: // page_locations (list<PageLocation>)
                    if (header.type() == 0x09) { // LIST
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            pageLocations.add(PageLocationReader.read(reader));
                        }
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

        return new OffsetIndex(pageLocations);
    }
}
