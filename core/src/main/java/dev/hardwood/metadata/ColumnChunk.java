/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Column chunk metadata.
 *
 * @param metaData column metadata
 * @param offsetIndexOffset file offset of the offset index for this column chunk, or null if absent
 * @param offsetIndexLength length of the offset index in bytes, or null if absent
 */
public record ColumnChunk(ColumnMetaData metaData, Long offsetIndexOffset, Integer offsetIndexLength) {
}
