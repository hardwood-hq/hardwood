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
        double xmin = 0;
        double xmax = 0;
        double ymin = 0;
        double ymax = 0;
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
                case 1:
                    if (header.type() == 0x07) {
                        xmin = reader.readDouble();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 2:
                    if (header.type() == 0x07) {
                        xmax = reader.readDouble();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 3:
                    if (header.type() == 0x07) {
                        ymin = reader.readDouble();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 4:
                    if (header.type() == 0x07) {
                        ymax = reader.readDouble();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 5:
                    if (header.type() == 0x07) {
                        zmin = reader.readDouble();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 6:
                    if (header.type() == 0x07) {
                        zmax = reader.readDouble();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 7:
                    if (header.type() == 0x07) {
                        mmin = reader.readDouble();
                    }
                    else {
                        reader.skipField(header.type());
                    }
                    break;
                case 8:
                    if (header.type() == 0x07) {
                        mmax = reader.readDouble();
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

        return new BoundingBox(xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax);
    }
}
