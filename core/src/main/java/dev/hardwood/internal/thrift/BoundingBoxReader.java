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
        Double xmin = null;
        Double xmax = null;
        Double ymin = null;
        Double ymax = null;
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
                    xmin = readField(header, reader);
                    break;
                case 2:
                    xmax = readField(header, reader);
                    break;
                case 3:
                    ymin = readField(header, reader);
                    break;
                case 4:
                    ymax = readField(header, reader);
                    break;
                case 5:
                    zmin = readField(header, reader);
                    break;
                case 6:
                    zmax = readField(header, reader);
                    break;
                case 7:
                    mmin = readField(header, reader);
                    break;
                case 8:
                    mmax = readField(header, reader);
                    break;
                default:
                    reader.skipField(header.type());
                    break;
            }
        }
        if(xmin == null || xmax == null || ymin == null || ymax == null)
            return null;
        else
            return new BoundingBox(xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax);
    }

    private static Double readField(ThriftCompactReader.FieldHeader header, ThriftCompactReader reader) throws IOException {
        Double value = null;
        if(header.type() == 0x07) {
            value = reader.readDouble();
        }
        else {
            reader.skipField(header.type());
        }

        return value;
    }
}
