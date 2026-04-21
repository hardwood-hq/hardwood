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

/// Header for DataPage v2.
///
/// [#statistics] carries the inline per-page statistics from Thrift field 8.
/// It is `null` when the writer omitted them. When both the `ColumnIndex` and
/// inline statistics are present, the `ColumnIndex` is authoritative.
public record DataPageHeaderV2(
        int numValues,
        int numNulls,
        int numRows,
        Encoding encoding,
        int definitionLevelsByteLength,
        int repetitionLevelsByteLength,
        boolean isCompressed,
        Statistics statistics) {
}
