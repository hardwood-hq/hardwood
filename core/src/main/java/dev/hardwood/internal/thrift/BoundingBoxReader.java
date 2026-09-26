/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.BoundingBox;

/// Reader for the Thrift BoundingBox struct from Parquet metadata.
public class BoundingBoxReader {

    /// Ids of the fields the format requires of a `BoundingBox`.
    private static final int[] REQUIRED_FIELDS = { 1, 2, 3, 4 };

    public static BoundingBox read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.BOUNDING_BOX);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static BoundingBox readInternal(ThriftCompactReader reader) {
        Double xmin = null;
        Double xmax = null;
        Double ymin = null;
        Double ymax = null;
        Double zmin = null;
        Double zmax = null;
        Double mmin = null;
        Double mmax = null;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                case 1 -> xmin = requiredDouble(reader, header);
                case 2 -> xmax = requiredDouble(reader, header);
                case 3 -> ymin = requiredDouble(reader, header);
                case 4 -> ymax = requiredDouble(reader, header);
                case 5 -> zmin = optionalDouble(reader, header);
                case 6 -> zmax = optionalDouble(reader, header);
                case 7 -> mmin = optionalDouble(reader, header);
                case 8 -> mmax = optionalDouble(reader, header);
                default -> reader.skipField(ThriftCompactReader.fieldType(header));
            }
        }

        long seen = (xmin != null ? 1L << 1 : 0) | (xmax != null ? 1L << 2 : 0)
                | (ymin != null ? 1L << 3 : 0) | (ymax != null ? 1L << 4 : 0);
        ThriftCompactReader.requireFields(ThriftStruct.BOUNDING_BOX, seen, REQUIRED_FIELDS);

        return new BoundingBox(xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax);
    }

    /// One of the four coordinates a bounding box cannot do without, so a wrong
    /// wire type fails here rather than being reported as a coordinate that never
    /// arrived.
    private static double requiredDouble(ThriftCompactReader reader, int header) {
        reader.requireField(header, Codes.DOUBLE);
        return reader.readDouble();
    }

    /// One of the four a bounding box can, so a wrong wire type is skipped and
    /// logged and the box keeps the coordinates it does have.
    private static Double optionalDouble(ThriftCompactReader reader, int header) {
        return reader.acceptField(header, Codes.DOUBLE) ? reader.readDouble() : null;
    }
}
