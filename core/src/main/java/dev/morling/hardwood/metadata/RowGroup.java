/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

import java.util.List;

/**
 * Row group metadata.
 */
public record RowGroup(
        List<ColumnChunk> columns,
        long totalByteSize,
        long numRows) {
}
