/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Compression codecs supported by Parquet.
///
/// @see <a href="https://parquet.apache.org/docs/file-format/data-pages/compression/">File Format – Compression</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public enum CompressionCodec {
    /// No compression.
    UNCOMPRESSED,
    /// Snappy compression (fast, moderate ratio).
    SNAPPY,
    /// Gzip compression (slower, higher ratio).
    GZIP,
    /// LZO compression.
    LZO,
    /// Brotli compression (high ratio).
    BROTLI,
    /// LZ4 compression (Hadoop framing).
    LZ4,
    /// Zstandard compression.
    ZSTD,
    /// LZ4 raw block compression (no Hadoop framing).
    LZ4_RAW
}
