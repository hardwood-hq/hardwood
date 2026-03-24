/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.ConvertedType;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;

/// Maps Thrift integer values to public enum constants.
/// Keeps the Thrift-specific mapping out of the public API types.
class ThriftEnumLookup {

    // Indexed by Thrift value (0-7)
    private static final PhysicalType[] PHYSICAL_TYPES = {
            PhysicalType.BOOLEAN,               // 0
            PhysicalType.INT32,                  // 1
            PhysicalType.INT64,                  // 2
            PhysicalType.INT96,                  // 3
            PhysicalType.FLOAT,                  // 4
            PhysicalType.DOUBLE,                 // 5
            PhysicalType.BYTE_ARRAY,             // 6
            PhysicalType.FIXED_LEN_BYTE_ARRAY    // 7
    };

    // Indexed by Thrift value (0-2)
    private static final RepetitionType[] REPETITION_TYPES = {
            RepetitionType.REQUIRED,  // 0
            RepetitionType.OPTIONAL,  // 1
            RepetitionType.REPEATED   // 2
    };

    // Indexed by Thrift value (0-21)
    private static final ConvertedType[] CONVERTED_TYPES = {
            ConvertedType.UTF8,              // 0
            ConvertedType.MAP,               // 1
            ConvertedType.MAP_KEY_VALUE,     // 2
            ConvertedType.LIST,              // 3
            ConvertedType.ENUM,              // 4
            ConvertedType.DECIMAL,           // 5
            ConvertedType.DATE,              // 6
            ConvertedType.TIME_MILLIS,       // 7
            ConvertedType.TIME_MICROS,       // 8
            ConvertedType.TIMESTAMP_MILLIS,  // 9
            ConvertedType.TIMESTAMP_MICROS,  // 10
            ConvertedType.UINT_8,            // 11
            ConvertedType.UINT_16,           // 12
            ConvertedType.UINT_32,           // 13
            ConvertedType.UINT_64,           // 14
            ConvertedType.INT_8,             // 15
            ConvertedType.INT_16,            // 16
            ConvertedType.INT_32,            // 17
            ConvertedType.INT_64,            // 18
            ConvertedType.JSON,              // 19
            ConvertedType.BSON,              // 20
            ConvertedType.INTERVAL           // 21
    };

    // Indexed by Thrift value (0-9), with gap at index 1
    private static final Encoding[] ENCODINGS = new Encoding[10];

    static {
        ENCODINGS[0] = Encoding.PLAIN;
        ENCODINGS[2] = Encoding.PLAIN_DICTIONARY;
        ENCODINGS[3] = Encoding.RLE;
        ENCODINGS[4] = Encoding.BIT_PACKED;
        ENCODINGS[5] = Encoding.DELTA_BINARY_PACKED;
        ENCODINGS[6] = Encoding.DELTA_LENGTH_BYTE_ARRAY;
        ENCODINGS[7] = Encoding.DELTA_BYTE_ARRAY;
        ENCODINGS[8] = Encoding.RLE_DICTIONARY;
        ENCODINGS[9] = Encoding.BYTE_STREAM_SPLIT;
    }

    // Indexed by Thrift value (0-7)
    private static final CompressionCodec[] COMPRESSION_CODECS = {
            CompressionCodec.UNCOMPRESSED,  // 0
            CompressionCodec.SNAPPY,        // 1
            CompressionCodec.GZIP,          // 2
            CompressionCodec.LZO,           // 3
            CompressionCodec.BROTLI,        // 4
            CompressionCodec.LZ4,           // 5
            CompressionCodec.ZSTD,          // 6
            CompressionCodec.LZ4_RAW        // 7
    };

    static PhysicalType physicalType(int value) {
        if (value >= 0 && value < PHYSICAL_TYPES.length) {
            return PHYSICAL_TYPES[value];
        }
        throw new IllegalArgumentException("Invalid or corrupt physical type value: " + value
                + " (expected 0-7). File metadata may be corrupted");
    }

    static RepetitionType repetitionType(int value) {
        if (value >= 0 && value < REPETITION_TYPES.length) {
            return REPETITION_TYPES[value];
        }
        throw new IllegalArgumentException("Unknown repetition type: " + value);
    }

    static ConvertedType convertedType(int value) {
        if (value >= 0 && value < CONVERTED_TYPES.length) {
            return CONVERTED_TYPES[value];
        }
        return null;
    }

    static Encoding encoding(int value) {
        if (value >= 0 && value < ENCODINGS.length) {
            Encoding e = ENCODINGS[value];
            if (e != null) {
                return e;
            }
        }
        return Encoding.UNKNOWN;
    }

    static CompressionCodec compressionCodec(int value) {
        if (value >= 0 && value < COMPRESSION_CODECS.length) {
            return COMPRESSION_CODECS[value];
        }
        throw new IllegalArgumentException("Unknown compression codec: " + value);
    }
}
