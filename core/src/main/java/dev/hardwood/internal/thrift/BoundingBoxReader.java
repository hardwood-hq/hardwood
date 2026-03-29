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
        double xmin = Double.NaN;
        double xmax = Double.NaN;
        double ymin = Double.NaN;
        double ymax = Double.NaN;
        Double zmin = null;
        Double zmax = null;
        Double mmin = null;
        Double mmax = null;
        while(true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1:
                    xmin = readRequiredField(header, reader);
                    break;
                case 2:
                    xmax = readRequiredField(header, reader);
                    break;
                case 3:
                    ymin = readRequiredField(header, reader);
                    break;
                case 4:
                    ymax = readRequiredField(header, reader);
                    break;
                case 5:
                    zmin = readOptionalField(header, reader);
                    break;
                case 6:
                    zmax = readOptionalField(header, reader);
                    break;
                case 7:
                    mmin = readOptionalField(header, reader);
                    break;
                case 8:
                    mmax = readOptionalField(header, reader);
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }
        if(Double.isNaN(xmin) || Double.isNaN(xmax) || Double.isNaN(ymin) || Double.isNaN((ymax)))
            return null;
        else
            return new BoundingBox(xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax);
    }

    private static double readRequiredField(ThriftCompactReader.FieldHeader header, ThriftCompactReader reader) throws IOException {
        if(header.type() == 0x07) {
            return reader.readDouble();
        }
        else {
            reader.skipField(header.type());
            return Double.NaN;
        }
    }

    private static Double readOptionalField(ThriftCompactReader.FieldHeader header, ThriftCompactReader reader) throws IOException {
        if(header.type() == 0x07) {
            return reader.readDouble();
        }
        else {
            reader.skipField(header.type());
            return null;
        }
    }
}
