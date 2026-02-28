/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

import java.util.List;

/**
 * Offset index for a column chunk, providing page locations for direct lookup.
 *
 * @param pageLocations locations of each data page in the column chunk
 */
public record OffsetIndex(List<PageLocation> pageLocations) {
}
