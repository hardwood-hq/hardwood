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

import dev.hardwood.metadata.BoundingBox;
import dev.hardwood.metadata.GeospatialStatistics;

/// Reader for the Thrift GeospatialStatistics struct from Parquet metadata.
public class GeospatialStatisticsReader {

    public static GeospatialStatistics read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static GeospatialStatistics readInternal(ThriftCompactReader reader) throws IOException {
        BoundingBox bbox = null;
        List<Integer> geospatialTypes = new ArrayList<>();

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1:
                    if (header.type() == 0x0C) {
                        bbox = BoundingBoxReader.read(reader);
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2:
                    if (header.type() == 0x09) {
                        ThriftCompactReader.CollectionHeader listHeader = reader.readListHeader();
                        for (int i = 0; i < listHeader.size(); i++) {
                            geospatialTypes.add(reader.readI32());
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

        return new GeospatialStatistics(bbox, List.copyOf(geospatialTypes));
    }
}
