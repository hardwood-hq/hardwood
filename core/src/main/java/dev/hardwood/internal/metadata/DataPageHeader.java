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
public record DataPageHeader(
        int numValues,
        Encoding encoding,
        Encoding definitionLevelEncoding,
        Encoding repetitionLevelEncoding,
        Statistics statistics) {
}
