/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.io.IOException;

import dev.hardwood.internal.bloomfilter.BloomFilterHeader;

public class BloomFilterHeaderReader {

    private static final int TYPE_INT_32 = 0x05;
    private static final int TYPE_STRUCT = 0x0C;

    public static BloomFilterHeader read(ThriftCompactReader reader) throws IOException {
        short saved = reader.pushFieldIdContext();
        try {
            return readInternal(reader);
        } finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static BloomFilterHeader readInternal(ThriftCompactReader reader) throws IOException {
        int numBytes = -1;
        BloomFilterHeader.Algorithm algorithm = null;
        BloomFilterHeader.Hash hash = null;
        BloomFilterHeader.Compression compression = null;

        while (true) {
            ThriftCompactReader.FieldHeader header = reader.readFieldHeader();
            if (header == null) {
                break;
            }

            switch (header.fieldId()) {
                case 1 -> numBytes = readRequiredBitsetSize(reader, header.type());
                case 2 -> {
                    short variant = readUnionVariant(reader, header.type(), "algorithm");
                    algorithm = BloomFilterHeader.Algorithm.fromVariant(variant);
                }
                case 3 -> {
                    short variant = readUnionVariant(reader, header.type(), "hash");
                    hash = BloomFilterHeader.Hash.fromVariant(variant);
                }
                case 4 -> {
                    short variant = readUnionVariant(reader, header.type(), "compression");
                    compression = BloomFilterHeader.Compression.fromVariant(variant);
                }
                default -> reader.skipField(header.type());
            }
        }

        if (numBytes < 0) {
            throw invalidHeader("missing required field 'numBytes'");
        }
        if (algorithm == null || hash == null || compression == null) {
            throw invalidHeader("missing required field(s) "
                    + (algorithm == null ? "algorithm " : "")
                    + (hash == null ? "hash " : "")
                    + (compression == null ? "compression " : ""));
        }

        return new BloomFilterHeader(numBytes, algorithm, hash, compression);
    }

    private static int readRequiredBitsetSize(ThriftCompactReader reader, byte type) throws IOException {
        if (type != TYPE_INT_32) {
            throw wrongWireType("required field 'numBytes'", type);
        }
        return reader.readNonNegativeI32("BloomFilterHeader.numBytes");
    }

    private static short readUnionVariant(ThriftCompactReader reader, byte type, String name) throws IOException {
        if (type != TYPE_STRUCT) {
            throw wrongWireType("union field '" + name + "'", type);
        }
        short saved = reader.pushFieldIdContext();
        try {
            ThriftCompactReader.FieldHeader variant = reader.readFieldHeader();
            if (variant == null) {
                throw invalidHeader("union field '" + name + "' has no variant set");
            }
            // The variant's value is an empty struct; a different wire type would make skipField
            // consume the wrong number of bytes and desync the rest of the header.
            if (variant.type() != TYPE_STRUCT) {
                throw wrongWireType("union field '" + name + "' variant " + variant.fieldId(), variant.type());
            }
            reader.skipField(variant.type()); // consume the variant's value (the empty inner struct)
            if (reader.readFieldHeader() != null) {
                throw invalidHeader("union field '" + name + "' has more than one variant set");
            }
            return variant.fieldId();
        } finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static IllegalStateException wrongWireType(String fieldDescription, byte type) {
        return invalidHeader(fieldDescription + " has wrong wire type 0x" + Integer.toHexString(type & 0xFF));
    }

    private static IllegalStateException invalidHeader(String message) {
        return new IllegalStateException("Invalid BloomFilterHeader: " + message);
    }
}
