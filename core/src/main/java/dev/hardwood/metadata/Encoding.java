/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/// Encoding types for Parquet data.
///
/// @see <a href="https://parquet.apache.org/docs/file-format/data-pages/encodings/">File Format – Encodings</a>
/// @see <a href="https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift">parquet.thrift</a>
public enum Encoding {
    /// Plain encoding: values are stored back-to-back.
    PLAIN,
    /// Dictionary encoding using a plain-encoded dictionary page (deprecated in favor of [#RLE_DICTIONARY]).
    PLAIN_DICTIONARY,
    /// Run-length / bit-packed hybrid encoding.
    RLE,
    /// Bit-packed encoding (deprecated in favor of [#RLE]).
    BIT_PACKED,
    /// Delta encoding for integers.
    DELTA_BINARY_PACKED,
    /// Delta encoding for byte array lengths.
    DELTA_LENGTH_BYTE_ARRAY,
    /// Incremental (delta) encoding for byte arrays.
    DELTA_BYTE_ARRAY,
    /// Dictionary encoding with an RLE-encoded index page.
    RLE_DICTIONARY,
    /// Byte-stream split encoding for floating-point data.
    BYTE_STREAM_SPLIT,
    /// Placeholder for unknown/unsupported encodings found in metadata.
    /// This allows reading files that list encodings we don't recognize
    /// in the column metadata, as long as the actual pages use supported encodings.
    UNKNOWN
}
