/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.internal.bloomfilter.BloomFilterHeader;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;

public class BloomFilterHeaderReader {

    /// Ids of the fields the format requires of a `BloomFilterHeader`.
    private static final int[] REQUIRED_FIELDS = { 1, 2, 3, 4 };

    public static BloomFilterHeader read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.BLOOM_FILTER_HEADER);
        try {
            return readInternal(reader);
        } finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static BloomFilterHeader readInternal(ThriftCompactReader reader) {
        int numBytes = -1;
        BloomFilterHeader.Algorithm algorithm = null;
        BloomFilterHeader.Hash hash = null;
        BloomFilterHeader.Compression compression = null;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            switch (ThriftCompactReader.fieldId(header)) {
                // Every field of a bloom filter header is required, so a wrong wire
                // type fails here rather than being reported as a field that never
                // arrived.
                case 1 -> {
                    reader.requireField(header, Codes.I32);
                    numBytes = reader.readNonNegativeI32();
                }
                case 2 -> {
                    reader.requireField(header, Codes.STRUCT);
                    algorithm = readAlgorithm(reader);
                }
                case 3 -> {
                    reader.requireField(header, Codes.STRUCT);
                    hash = readHash(reader);
                }
                case 4 -> {
                    reader.requireField(header, Codes.STRUCT);
                    compression = readCompression(reader);
                }
                default -> reader.skipField(ThriftCompactReader.fieldType(header));
            }
        }

        long seen = (numBytes >= 0 ? 1L << 1 : 0) | (algorithm != null ? 1L << 2 : 0)
                | (hash != null ? 1L << 3 : 0) | (compression != null ? 1L << 4 : 0);
        ThriftCompactReader.requireFields(ThriftStruct.BLOOM_FILTER_HEADER, seen, REQUIRED_FIELDS);

        return new BloomFilterHeader(numBytes, algorithm, hash, compression);
    }

    private static BloomFilterHeader.Algorithm readAlgorithm(ThriftCompactReader reader) {
        short variant = reader.readUnionVariant(ThriftStruct.BLOOM_FILTER_ALGORITHM);
        return switch (variant) {
            case 1 -> BloomFilterHeader.Algorithm.BLOCK;
            default -> throw unsupportedVariantOf(ThriftStruct.BLOOM_FILTER_ALGORITHM, variant,
                    "bloom filter algorithm");
        };
    }

    private static BloomFilterHeader.Hash readHash(ThriftCompactReader reader) {
        short variant = reader.readUnionVariant(ThriftStruct.BLOOM_FILTER_HASH);
        return switch (variant) {
            case 1 -> BloomFilterHeader.Hash.XXHASH;
            default -> throw unsupportedVariantOf(ThriftStruct.BLOOM_FILTER_HASH, variant,
                    "bloom filter hash");
        };
    }

    private static BloomFilterHeader.Compression readCompression(ThriftCompactReader reader) {
        short variant = reader.readUnionVariant(ThriftStruct.BLOOM_FILTER_COMPRESSION);
        return switch (variant) {
            case 1 -> BloomFilterHeader.Compression.UNCOMPRESSED;
            default -> throw unsupportedVariantOf(ThriftStruct.BLOOM_FILTER_COMPRESSION, variant,
                    "bloom filter compression");
        };
    }

    private static UnsupportedOperationException unsupportedVariantOf(ThriftStruct union,
            short variant, String what) {
        return new UnsupportedOperationException(
                union.describe(variant) + " is not a " + what + " this version implements");
    }

}
