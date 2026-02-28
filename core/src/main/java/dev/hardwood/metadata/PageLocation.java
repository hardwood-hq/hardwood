/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Location of a data page within a column chunk.
 *
 * @param offset absolute file offset of the page
 * @param compressedPageSize total page size in file including header
 * @param firstRowIndex index of the first row in this page within the row group
 */
public record PageLocation(long offset, int compressedPageSize, long firstRowIndex) {
}
