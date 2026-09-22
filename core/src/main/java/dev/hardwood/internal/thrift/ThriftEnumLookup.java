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
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;

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

    // Indexed by Thrift value (0-3)
    private static final PageType[] PAGE_TYPES = {
            PageType.DATA_PAGE,        // 0
            PageType.INDEX_PAGE,       // 1
            PageType.DICTIONARY_PAGE,  // 2
            PageType.DATA_PAGE_V2      // 3
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

    // Indexed by Thrift value (0-9); 1 is a hole the format left behind
    private static final Encoding[] ENCODINGS = {
            Encoding.PLAIN,                    // 0
            null,                              // 1 - GROUP_VAR_INT, withdrawn by the format
            Encoding.PLAIN_DICTIONARY,         // 2
            Encoding.RLE,                      // 3
            Encoding.BIT_PACKED,               // 4
            Encoding.DELTA_BINARY_PACKED,      // 5
            Encoding.DELTA_LENGTH_BYTE_ARRAY,  // 6
            Encoding.DELTA_BYTE_ARRAY,         // 7
            Encoding.RLE_DICTIONARY,           // 8
            Encoding.BYTE_STREAM_SPLIT         // 9
    };

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

    // Indexed by Thrift value (0-4)
    private static final LogicalType.EdgeInterpolationAlgorithm[] EDGE_INTERPOLATION_ALGORITHMS = {
            LogicalType.EdgeInterpolationAlgorithm.SPHERICAL,  // 0
            LogicalType.EdgeInterpolationAlgorithm.VINCENTY,   // 1
            LogicalType.EdgeInterpolationAlgorithm.THOMAS,     // 2
            LogicalType.EdgeInterpolationAlgorithm.ANDOYER,    // 3
            LogicalType.EdgeInterpolationAlgorithm.KARNEY      // 4
    };

    /// Maps a Thrift `EdgeInterpolationAlgorithm` value. `GeographyType.algorithm` is a Thrift
    /// enum, so it arrives as an `i32` holding one of these values — not as a union whose set
    /// variant names the algorithm.
    ///
    /// A value this release does not recognize yields
    /// [LogicalType.EdgeInterpolationAlgorithm#UNKNOWN] rather than failing, as
    /// [#encoding] and [#pageType] do: the algorithm annotates how to interpolate between
    /// the column's values and does not bear on decoding them, so a file the format has moved
    /// past stays readable.
    static LogicalType.EdgeInterpolationAlgorithm edgeInterpolationAlgorithm(int value) {
        if (value >= 0 && value < EDGE_INTERPOLATION_ALGORITHMS.length) {
            return EDGE_INTERPOLATION_ALGORITHMS[value];
        }
        return LogicalType.EdgeInterpolationAlgorithm.UNKNOWN;
    }

    static PhysicalType physicalType(int value) {
        if (value >= 0 && value < PHYSICAL_TYPES.length) {
            return PHYSICAL_TYPES[value];
        }
        throw new ParquetReadException("Invalid or corrupt physical type value: " + value
                + " (expected 0-7). File metadata may be corrupted");
    }

    static RepetitionType repetitionType(int value) {
        if (value >= 0 && value < REPETITION_TYPES.length) {
            return REPETITION_TYPES[value];
        }
        throw new ParquetReadException("Unknown repetition type: " + value);
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

    /// Maps a Thrift `PageType` value, yielding [PageType#UNKNOWN] for one this release does not
    /// recognize. Callers that must decode the page reject `UNKNOWN` themselves; `encoding_stats`
    /// carries it through, so an unrecognized page type in that optional field does not make the
    /// file unreadable.
    static PageType pageType(int value) {
        if (value >= 0 && value < PAGE_TYPES.length) {
            return PAGE_TYPES[value];
        }
        return PageType.UNKNOWN;
    }

    static CompressionCodec compressionCodec(int value) {
        if (value >= 0 && value < COMPRESSION_CODECS.length) {
            return COMPRESSION_CODECS[value];
        }
        throw new ParquetReadException("Unknown compression codec: " + value);
    }

    static int thriftValue(PageType type) {
        return indexOf(PAGE_TYPES, type, "page type");
    }

    static int thriftValue(PhysicalType type) {
        return indexOf(PHYSICAL_TYPES, type, "physical type");
    }

    static int thriftValue(RepetitionType type) {
        return indexOf(REPETITION_TYPES, type, "repetition type");
    }

    static int thriftValue(ConvertedType type) {
        return indexOf(CONVERTED_TYPES, type, "converted type");
    }

    /// @throws IllegalArgumentException for
    ///         [LogicalType.EdgeInterpolationAlgorithm#UNKNOWN], which stands for an algorithm
    ///         this release cannot name and so has no Thrift value to write
    static int thriftValue(LogicalType.EdgeInterpolationAlgorithm algorithm) {
        return indexOf(EDGE_INTERPOLATION_ALGORITHMS, algorithm, "edge interpolation algorithm");
    }

    static int thriftValue(Encoding encoding) {
        return indexOf(ENCODINGS, encoding, "encoding");
    }

    static int thriftValue(CompressionCodec codec) {
        return indexOf(COMPRESSION_CODECS, codec, "compression codec");
    }

    private static <T> int indexOf(T[] table, T value, String what) {
        for (int i = 0; i < table.length; i++) {
            if (table[i] == value) {
                return i;
            }
        }
        throw new IllegalArgumentException("No Thrift value for " + what + ": " + value);
    }
}
