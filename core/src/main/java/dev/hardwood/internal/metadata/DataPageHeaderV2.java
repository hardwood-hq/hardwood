/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.metadata;

import dev.hardwood.metadata.Encoding;

/**
 * Header for DataPage v2.
 */
public record DataPageHeaderV2(
        int numValues,
        int numNulls,
        int numRows,
        Encoding encoding,
        int definitionLevelsByteLength,
        int repetitionLevelsByteLength,
        boolean isCompressed) {
}
