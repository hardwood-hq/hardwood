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
 * Top-level file metadata for a Parquet file.
 */
public record FileMetaData(
        int version,
        List<SchemaElement> schema,
        long numRows,
        List<RowGroup> rowGroups,
        String createdBy) {
}
