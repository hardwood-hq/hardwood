/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Arrays;

import dev.hardwood.internal.bloomfilter.BloomFilterHeader;
import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;

public class BloomFilterHeaderReader {

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

        int[] missing = new int[4];
        int absent = 0;
        if (numBytes < 0) {
            missing[absent++] = 1;
        }
        if (algorithm == null) {
            missing[absent++] = 2;
        }
        if (hash == null) {
            missing[absent++] = 3;
        }
        if (compression == null) {
            missing[absent++] = 4;
        }
        if (absent > 0) {
            throw ThriftCompactReader.missingFields(ThriftStruct.BLOOM_FILTER_HEADER,
                    Arrays.copyOf(missing, absent));
        }

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
