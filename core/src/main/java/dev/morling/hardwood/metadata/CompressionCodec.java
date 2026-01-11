/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

/**
 * Compression codecs supported by Parquet.
 */
public enum CompressionCodec {
    UNCOMPRESSED(0),
    SNAPPY(1),
    GZIP(2),
    LZO(3),
    BROTLI(4),
    LZ4(5),
    ZSTD(6),
    LZ4_RAW(7);

    private final int thriftValue;

    CompressionCodec(int thriftValue) {
        this.thriftValue = thriftValue;
    }

    public int getThriftValue() {
        return thriftValue;
    }

    public static CompressionCodec fromThriftValue(int value) {
        for (CompressionCodec codec : values()) {
            if (codec.thriftValue == value) {
                return codec;
            }
        }
        throw new IllegalArgumentException("Unknown compression codec: " + value);
    }
}
