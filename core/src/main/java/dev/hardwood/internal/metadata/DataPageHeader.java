/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.metadata;

import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.Statistics;

/// Header for DataPage (v1).
///
/// [#statistics] carries the inline per-page statistics from Thrift field 5.
/// It is `null` when the writer omitted them (the common case when a separate
/// `ColumnIndex` is written instead). When both the `ColumnIndex` and inline
/// statistics are present, the `ColumnIndex` is authoritative.
///
/// [#encodingValue] is the raw Thrift value [#encoding] was mapped from. Every
/// value this release does not recognize maps to the single
/// [Encoding#UNKNOWN] constant, so the raw value is the only thing left that
/// identifies which encoding the page actually uses. [#definitionLevelEncodingValue] and
/// [#repetitionLevelEncodingValue] are the same for the level encodings.
public record DataPageHeader(
        int numValues,
        Encoding encoding,
        int encodingValue,
        Encoding definitionLevelEncoding,
        int definitionLevelEncodingValue,
        Encoding repetitionLevelEncoding,
        int repetitionLevelEncodingValue,
        Statistics statistics) {
}
