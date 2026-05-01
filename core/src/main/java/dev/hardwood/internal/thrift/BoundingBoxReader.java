/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.metadata.BoundingBox;

/// Reader for the Thrift BoundingBox struct from Parquet metadata.
public class BoundingBoxReader {

    private static final int TYPE_DOUBLE = 0x07;

    public static BoundingBox read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static BoundingBox readInternal(ThriftCompactReader reader) throws IOException {
        Double xmin = null;
        Double xmax = null;
        Double ymin = null;
        Double ymax = null;
        Double zmin = null;
        Double zmax = null;
        Double mmin = null;
        Double mmax = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1 -> xmin = readRequiredDouble(reader, header.type(), "xmin");
                case 2 -> xmax = readRequiredDouble(reader, header.type(), "xmax");
                case 3 -> ymin = readRequiredDouble(reader, header.type(), "ymin");
                case 4 -> ymax = readRequiredDouble(reader, header.type(), "ymax");
                case 5 -> zmin = readOptionalDouble(reader, header.type());
                case 6 -> zmax = readOptionalDouble(reader, header.type());
                case 7 -> mmin = readOptionalDouble(reader, header.type());
                case 8 -> mmax = readOptionalDouble(reader, header.type());
                default -> reader.skipField(header.type());
            }
        }

        if (xmin == null || xmax == null || ymin == null || ymax == null) {
            throw new IllegalStateException(
                    "Invalid BoundingBox: missing required field(s) "
                            + (xmin == null ? "xmin " : "")
                            + (xmax == null ? "xmax " : "")
                            + (ymin == null ? "ymin " : "")
                            + (ymax == null ? "ymax " : ""));
        }

        return new BoundingBox(xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax);
    }

    private static double readRequiredDouble(ThriftCompactReader reader, byte type, String name)
            throws IOException {
        if (type != TYPE_DOUBLE) {
            throw new IllegalStateException(
                    "Invalid BoundingBox: required field '" + name
                            + "' has wrong wire type 0x" + Integer.toHexString(type & 0xFF));
        }
        return reader.readDouble();
    }

    private static Double readOptionalDouble(ThriftCompactReader reader, byte type) throws IOException {
        if (type == TYPE_DOUBLE) {
            return reader.readDouble();
        }
        reader.skipField(type);
        return null;
    }
}
